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

@app.route('/api/lab3/group_report', methods=['POST'])
def get_group_report():
    data = request.get_json(force=True)
    group_id = data.get('group_id')
    if group_id is None:
        return jsonify({'error': 'Required field: group_id'}), 400
    try:
        service = neo4j_sync.SyncService(PG_CONFIG, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        report = service.generate_group_report(group_id=group_id)
        return jsonify(report=report, meta={'status': 'success', 'group_id': group_id, 'count': len(report)}), 200
    except Exception as e:
        app.logger.error(f"Group report error: {e}")
        return jsonify({'error': 'Failed to generate group report'}), 500
    finally:
        try: service.close()
        except: pass


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)
