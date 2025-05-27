import random
import redis
import psycopg2
from elasticsearch import Elasticsearch
from neo4j import GraphDatabase
from typing import List, Dict, Optional

class LectureMaterialSearcher:
    def __init__(self, es_host: str = "elasticsearch", es_port: int = 9200,
                 es_user: str = "elastic", es_password: str = "secret"):
        self.es = Elasticsearch(
            hosts=[f"http://{es_host}:{es_port}"],
            basic_auth=(es_user, es_password),
            verify_certs=False
        )

    def search(self, query: str) -> List[int]:
        resp = self.es.search(
            index="lecture_materials",
            query={
                "multi_match": {
                    "query": query,
                    "fields": ["lecture_name^3", "course_name^2", "content", "keywords"],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            }
        )
        return [hit["_source"]["lecture_id"] for hit in resp["hits"]["hits"]]

class AttendanceFinder:
    def __init__(
        self,
        neo4j_uri: str = 'bolt://neo4j:7687',
        neo4j_user: str = 'neo4j',
        neo4j_password: str = 'strongpassword',
        pg_dsn: str = "dbname=postgres_db user=postgres_user password=postgres_password host=postgres port=5432"
    ):
        # Neo4j driver
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        # Postgres connection
        self.pg_conn = psycopg2.connect(pg_dsn)
        self.pg_conn.autocommit = True

    def close(self):
        self.driver.close()
        self.pg_conn.close()

    def find_worst_attendees(
        self,
        lecture_ids: List[int],
        top_n: int = 10,
        start_date: Optional[str] = None,
        end_date:   Optional[str] = None
    ) -> List[Dict]:
        return self._compute_attendance(lecture_ids, worst=True, limit=top_n,
                                        start_date=start_date, end_date=end_date)

    def get_attendance_summary(
        self,
        lecture_ids: List[int],
        start_date: Optional[str] = None,
        end_date:   Optional[str] = None
    ) -> List[Dict]:
        return self._compute_attendance(lecture_ids, worst=False, limit=None,
                                        start_date=start_date, end_date=end_date)

    def _compute_attendance(
        self,
        lecture_ids: List[int],
        worst: bool,
        limit: Optional[int],
        start_date: Optional[str],
        end_date: Optional[str]
    ) -> List[Dict]:
        if not lecture_ids:
            return []

        # --- 1) Находим всех студентов, которые должны были присутствовать ---
        cypher = """
        UNWIND $lecture_ids AS lid
        MATCH (l:Lecture {id: lid})-[:SCHEDULED_AT]->(e:Schedule)
        MATCH (g:Group)<-[:FOR_GROUP]-(e)
        MATCH (st:Student)<-[:HAS_STUDENT]-(g)
        RETURN DISTINCT st.id AS student_id, st.name AS student_name
        """
        with self.driver.session() as session:
            neo4j_results = session.run(cypher, lecture_ids=lecture_ids)
            students = [rec.data() for rec in neo4j_results]

        if not students:
            return []

        student_ids = [s["student_id"] for s in students]

        # --- 2) Находим все schedule_id и semester из Postgres для заданных лекций и дат ---
        sql = """
            SELECT s.id AS schedule_id,
                   s.semester
              FROM Schedule s
             WHERE s.lecture_id = ANY(%s)
               AND (%s::date IS NULL OR s.date >= %s::date)
               AND (%s::date IS NULL OR s.date <= %s::date)
        """
        with self.pg_conn.cursor() as cur:
            cur.execute(sql, (
                lecture_ids,
                start_date, start_date,
                end_date,   end_date
            ))
            rows = cur.fetchall()
        if not rows:
            return []

        schedule_ids = [r[0] for r in rows]
        sid2sem = {r[0]: r[1] for r in rows}

        semesters = list({sem for sem in sid2sem.values()})

        # используем partition key semester, но Postgres сам разложит по нужным PARTITION
        stats_sql = """
            SELECT student_id,
                SUM((attended)::int) AS attended_count,
                COUNT(*)             AS total_count
            FROM Attendance
            WHERE student_id = ANY(%s)
            AND schedule_id = ANY(%s)
            AND semester    = ANY(%s)
            GROUP BY student_id
        """
        with self.pg_conn.cursor() as cur:
            cur.execute(stats_sql, (student_ids, schedule_ids, semesters))
            stats = cur.fetchall()

        stat_map = {sid: (att, tot) for sid, att, tot in
                    ((row[0], row[1], row[2]) for row in stats)}

        results = []
        for s in students:
            sid = s["student_id"]
            name = s["student_name"]
            attended_count, total_count = stat_map.get(sid, (0, 0))
            if total_count == 0:
                continue

            pct = round(attended_count / total_count * 100, 2)
            results.append({
                "studentId": sid,
                "studentName": name,
                "attendedCount": attended_count,
                "totalCount": total_count,
                "attendancePercent": pct
            })

        if worst:
            results.sort(key=lambda x: x["attendancePercent"])
        else:
            results.sort(key=lambda x: x["studentName"])

        if limit:
            results = results[:limit]

        return results

if __name__ == '__main__':
    term = "физика"
    searcher = LectureMaterialSearcher(es_password="secret")
    lecture_ids = searcher.search(term)
    print(f"Найдены лекции: {lecture_ids}")

    finder = AttendanceFinder()
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    start = "2023-09-01"
    end   = "2023-12-31"

    try:
        worst = finder.find_worst_attendees(
            lecture_ids,
            top_n=10,
            start_date=start,
            end_date=end
        )
        print("\n10 студентов с худшей посещаемостью:")
        for idx, rec in enumerate(worst, 1):
            info = r.hgetall(f"student:{rec['studentId']}")
            info_str = f"[Redis] Name: {info.get('name')}, Age: {info.get('age')}, Mail: {info.get('mail')}, Group: {info.get('group')}"
            print(f"{idx}. {rec['studentName']} — {rec['attendancePercent']}% ({rec['attendedCount']}/{rec['totalCount']}) {info_str}")

        summary = finder.get_attendance_summary(
            lecture_ids,
            start_date=start,
            end_date=end
        )
        print("\nСводка посещаемости всех студентов:")
        for rec in summary:
            info = r.hgetall(f"student:{rec['studentId']}")
            info_str = f"[Redis] Name: {info.get('name')}, Age: {info.get('age')}, Mail: {info.get('mail')}, Group: {info.get('group')}"
            print(f"{rec['studentName']}: {rec['attendancePercent']}% ({rec['attendedCount']}/{rec['totalCount']}) {info_str}")

    finally:
        finder.close()
        r.close()
