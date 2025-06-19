from flask import Flask, request, jsonify
import redis
import os
import neo4j_sync

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

@app.route('/api/lab2/audience_report', methods=['POST'])
def get_audience_report():
    data = request.get_json(force=True)
    year = data.get('year')
    semester = data.get('semester')
    if year is None or semester is None:
        return jsonify({'error': 'Required fields: year, semester'}), 400
    try:
        service = neo4j_sync.SyncService()
        report = service.generate_audience_report(year=year, semester=semester)
        return jsonify(report=report, meta={'status': 'success', 'count': len(report)}), 200
    except Exception as e:
        app.logger.error(f"Audience report error: {e}")
        return jsonify({'error': 'Failed to generate audience report'}), 500
    finally:
        try: service.close()
        except: pass


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)
