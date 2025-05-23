from neo4j import GraphDatabase
from datetime import datetime
import neo4j_sync

PG_CONFIG = {
    'dbname': "postgres_db",
    'user': "postgres_user",
    'password': "postgres_password",
    'host': 'postgres',
    'port': 5432,
}

NEO4J_URI = 'bolt://neo4j:7687'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'strongpassword'

if __name__ == '__main__':
    sync_service = neo4j_sync.SyncService(PG_CONFIG, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    report = sync_service.generate_group_report(group_id=1)

    for entry in report:
        print(f"Группа: {entry['group_info']['name']}")
        print(f"Студент: {entry['student_info']['name']}")
        print(f"Курс: {entry['course_info']['name']}")
        print(f"Запланировано часов: {entry['planned_hours']}")
        print(f"Прослушано часов: {entry['attended_hours']}\n")

    sync_service.close()
