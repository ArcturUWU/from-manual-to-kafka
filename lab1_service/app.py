from flask import Flask, request, jsonify
from Lab1 import LectureMaterialSearcher, AttendanceFinder 
import redis
import os

app = Flask(__name__)


NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "strongpassword")
ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_PORT = int(os.getenv("ES_PORT", 9200))
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS", "secret")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
PG_CONFIG = {
    'dbname': os.getenv("POSTGRES_DB", "postgres_db"),
    'user': os.getenv("POSTGRES_USER", "postgres_user"),
    'password': os.getenv("POSTGRES_PASSWORD", "postgres_password"),
    'host': os.getenv("POSTGRES_HOST", "postgres"),
    'port': os.getenv("POSTGRES_PORT", 5432),
}


@app.route('/api/lab1/report', methods=['POST'])
def generate_attendance_report():

    if not request.is_json:
        return jsonify({'error': 'Request must be JSON'}), 400

    data = request.get_json()
    required_fields = ['term', 'start_date', 'end_date']
    if not all(field in data for field in required_fields):
        return jsonify({
            'error': f"Missing required fields: {required_fields}",
            'received': list(data.keys())
        }), 400

    es_searcher = LectureMaterialSearcher(
        es_host=ES_HOST,
        es_port=ES_PORT,
        es_user=ES_USER,
        es_password=ES_PASS
    )
    lecture_ids = es_searcher.search(data['term'])
    if not lecture_ids:
        return jsonify({'error': 'No lectures found for the term'}), 404

    finder = AttendanceFinder()
    redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    try:
        worst = finder.find_worst_attendees(
            lecture_ids,
            top_n=10,
            start_date=data['start_date'],
            end_date=data['end_date']
        )

        def format_student(record):
            redis_info = redis_conn.hgetall(f"student:{record['studentId']}")
            return {
                **record,
                'redis_info': {
                    'name': redis_info.get('name'),
                    'age': redis_info.get('age'),
                    'mail': redis_info.get('mail'),
                    'group': redis_info.get('group')
                }
            }

        report = {
            'search_term': data['term'],
            'period': f"{data['start_date']} - {data['end_date']}",
            'found_lectures': len(lecture_ids),
            'worst_attendees': [format_student(r) for r in worst]
        }
        return jsonify(report=report, meta={'status': 'success', 'results': len(worst)}), 200

    except Exception as e:
        app.logger.error(f"Error: {e}")
        return jsonify({'error': 'Data processing failed'}), 500

    finally:
        finder.close()
        redis_conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
