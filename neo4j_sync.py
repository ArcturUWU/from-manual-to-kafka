#curl -X POST http://localhost:1337/api/lab1/report -H "Content-Type: application/json" -H "Authorization: Bearer <JWT>" -d @query1.json --compressed | python -c "import sys,json; print(json.dumps(json.load(sys.stdin), indent=2, ensure_ascii=False))"
#curl -X POST "http://localhost:1337/api/lab2/audience_report" -H "Content-Type: application/json" -H "Authorization: Bearer <JWT>" --data @query2.json --compressed | python -c "import sys,json; print(json.dumps(json.load(sys.stdin), indent=2, ensure_ascii=False))"
#curl -X POST "http://localhost:1337/api/lab3/group_report" -H "Content-Type: application/json" -H "Authorization: Bearer <JWT>" --data @query3.json --compressed | python -c "import sys,json; print(json.dumps(json.load(sys.stdin), indent=2, ensure_ascii=False))"
from neo4j import GraphDatabase
from datetime import date, timedelta
import psycopg2

# PostgreSQL connection parameters
PG_CONFIG = {
    'dbname': "postgres_db",
    'user': "postgres_user",
    'password': "postgres_password",
    'host': "localhost",
    'port': "5430"
}

# Neo4j connection parameters
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "strongpassword"

class SyncService:
    def __init__(self):
        # Initialize Postgres connection
        self.pg_conn = psycopg2.connect(**PG_CONFIG)
        self.pg_cur = self.pg_conn.cursor()
        # Initialize Neo4j driver
        self.neo4j_driver = GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
        )

    def close(self):
        self.pg_cur.close()
        self.pg_conn.close()
        self.neo4j_driver.close()

    def sync_universities(self):
        self.pg_cur.execute("SELECT id, name, location FROM University")
        for id, name, location in self.pg_cur.fetchall():
            with self.neo4j_driver.session() as session:
                session.run(
                    "MERGE (u:University {id: $id}) "
                    "SET u.name = $name, u.location = $location",
                    id=id, name=name, location=location
                )

    def sync_institutes(self):
        self.pg_cur.execute("SELECT id, name, university_id FROM Institute")
        for id, name, university_id in self.pg_cur.fetchall():
            with self.neo4j_driver.session() as session:
                session.run(
                    "MATCH (u:University {id: $uid})"
                    " MERGE (i:Institute {id: $id}) "
                    "SET i.name = $name "
                    "MERGE (u)-[:HAS_INSTITUTE]->(i)",
                    uid=university_id, id=id, name=name
                )

    def sync_departments(self):
        self.pg_cur.execute("SELECT id, name, institute_id FROM Department")
        for id, name, institute_id in self.pg_cur.fetchall():
            with self.neo4j_driver.session() as session:
                session.run(
                    "MATCH (i:Institute {id: $iid})"
                    " MERGE (d:Department {id: $id}) "
                    "SET d.name = $name "
                    "MERGE (i)-[:HAS_DEPARTMENT]->(d)",
                    iid=institute_id, id=id, name=name
                )

    def sync_specialties(self):
        self.pg_cur.execute("SELECT id, name, department_id FROM Specialty")
        for id, name, department_id in self.pg_cur.fetchall():
            with self.neo4j_driver.session() as session:
                session.run(
                    "MATCH (d:Department {id: $did})"
                    " MERGE (s:Specialty {id: $id}) "
                    "SET s.name = $name "
                    "MERGE (d)-[:HAS_SPECIALTY]->(s)",
                    did=department_id, id=id, name=name
                )

    def sync_groups(self):
        self.pg_cur.execute("SELECT id, name, speciality_id FROM St_group")
        for id, name, speciality_id in self.pg_cur.fetchall():
            with self.neo4j_driver.session() as session:
                session.run(
                    "MATCH (s:Specialty {id: $sid})"
                    " MERGE (g:Group {id: $id}) "
                    "SET g.name = $name "
                    "MERGE (s)-[:HAS_GROUP]->(g)",
                    sid=speciality_id, id=id, name=name
                )

    def sync_courses(self):
        self.pg_cur.execute(
            "SELECT id, name, department_id, specialty_id FROM Course_of_lecture"
        )
        for id, name, dept_id, spec_id in self.pg_cur.fetchall():
            with self.neo4j_driver.session() as session:
                session.run(
                    "MATCH (d:Department {id: $did}), (s:Specialty {id: $sid})"
                    " MERGE (c:Course {id: $id}) "
                    "SET c.name = $name "
                    "MERGE (d)-[:OFFERS]->(c) "
                    "MERGE (s)-[:INCLUDES_COURSE]->(c)",
                    did=dept_id, sid=spec_id, id=id, name=name
                )

    def sync_lectures(self):
        self.pg_cur.execute("SELECT id, name, course_of_lecture_id FROM Lecture")
        for id, name, course_id in self.pg_cur.fetchall():
            with self.neo4j_driver.session() as session:
                session.run(
                    "MATCH (c:Course {id: $cid})"
                    " MERGE (l:Lecture {id: $id}) "
                    "SET l.name = $name "
                    "MERGE (c)-[:HAS_LECTURE]->(l)",
                    cid=course_id, id=id, name=name
                )

    def sync_materials(self):
        self.pg_cur.execute(
            "SELECT id, name, course_of_lecture_id FROM Material_of_lecture"
        )
        for id, name, lecture_id in self.pg_cur.fetchall():
            with self.neo4j_driver.session() as session:
                session.run(
                    "MATCH (l:Lecture {id: $lid})"
                    " MERGE (m:Material {id: $id}) "
                    "SET m.name = $name "
                    "MERGE (l)-[:HAS_MATERIAL]->(m)",
                    lid=lecture_id, id=id, name=name
                )

    def sync_schedules(self):
        self.pg_cur.execute(
            "SELECT id, date, lecture_id, group_id FROM Schedule"
        )
        for id, date, lecture_id, group_id in self.pg_cur.fetchall():
            with self.neo4j_driver.session() as session:
                session.run(
                    "MATCH (l:Lecture {id: $lid}), (g:Group {id: $gid})"
                    " MERGE (sch:Schedule {id: $id}) "
                    "SET sch.date = $date "
                    "MERGE (l)-[:SCHEDULED_AT]->(sch) "
                    "MERGE (sch)-[:FOR_GROUP]->(g)",
                    lid=lecture_id, gid=group_id,
                    id=id, date=date
                )

    def sync_students(self):
        self.pg_cur.execute(
            "SELECT id, name, age, mail, group_id FROM Students"
        )
        for id, name, age, mail, group_id in self.pg_cur.fetchall():
            with self.neo4j_driver.session() as session:
                session.run(
                    "MATCH (g:Group {id: $gid})"
                    " MERGE (s:Student {id: $id}) "
                    "SET s.name = $name, s.age = $age, s.mail = $mail "
                    "MERGE (g)-[:HAS_STUDENT]->(s)",
                    gid=group_id, id=id, name=name, age=age, mail=mail
                )

    def sync_all(self):
        # Выполняем все шаги синхронизации
        self.sync_universities()
        self.sync_institutes()
        self.sync_departments()
        self.sync_specialties()
        self.sync_groups()
        self.sync_courses()
        self.sync_lectures()
        self.sync_materials()
        self.sync_schedules()
        self.sync_students()
        # Учёт посещаемости теперь проверяется напрямую в PostgreSQL

    # --------------------- Report Functions ---------------------
    def _calculate_semester_dates(self, year: int, semester: int):
        """
        Возвращает кортеж (start_date, end_date) для заданного семестра:
        Семестр 1: с 1 февраля по 30 июня; Семестр 2: с 1 сентября по 31 января следующего года.
        """
        if semester == 1:
            start = date(year, 2, 1)
            end = date(year, 6, 30)
        else:
            start = date(year, 9, 1)
            end = date(year + 1, 1, 31)
        return start, end
    def get_scheduled_students(self, schedule_id):
        """
        Извлечь список студентов, которым назначена лекция по расписанию.
        Данные берутся из Neo4j.
        Возвращает список словарей: [{'id': ..., 'name': ...}, ...]
        """
        query = (
            "MATCH (sch:Schedule {id: $sid})-[:FOR_GROUP]->(g:Group)"
            "-[:HAS_STUDENT]->(s:Student)"
            " RETURN s.id AS id, s.name AS name"
        )
        with self.neo4j_driver.session() as session:
            result = session.run(query, sid=schedule_id)
            return [record.data() for record in result]

    def check_attendance(self, student_id, schedule_id):
        """
        Проверить факт посещения конкретного студента на конкретной лекции.
        Делается через PostgreSQL.
        Возвращает Boolean.
        """
        self.pg_cur.execute(
            "SELECT attended FROM Attendance WHERE student_id = %s AND schedule_id = %s",
            (student_id, schedule_id)
        )
        row = self.pg_cur.fetchone()
        return bool(row[0]) if row else False

    def generate_audience_report(self, year: int, semester: int):
        """
        Генерирует отчёт по аудитории: для всех лекций в семестре возвращает курс, лекцию,
        технические требования и общее число студентов.
        Использует только Neo4j.
        """
        start_date, end_date = self._calculate_semester_dates(year, semester)
        cypher = (
            "MATCH (sch:Schedule) "
            "WHERE sch.date >= date($start) AND sch.date <= date($end) "
            "// подсчитать студентов по каждому расписанию "
            "MATCH (sch)-[:FOR_GROUP]->(g:Group)-[:HAS_STUDENT]->(s:Student) "
            "WITH sch, COUNT(DISTINCT s) AS total_students "
            "// связать лекцию, курс, материалы "
            "MATCH (l:Lecture)-[:SCHEDULED_AT]->(sch) "
            "MATCH (c:Course)-[:HAS_LECTURE]->(l) "
            "OPTIONAL MATCH (l)-[:HAS_MATERIAL]->(m:Material) "
            "RETURN c.name AS course_name, l.name AS lecture_name, "
            "COLLECT(DISTINCT m.name) AS tech_requirements, total_students "
            "ORDER BY course_name, lecture_name"
        )
        with self.neo4j_driver.session() as session:
            results = session.run(
                cypher,
                start=str(start_date), end=str(end_date)
            )
            return [record.data() for record in results]

    def generate_group_report(self, group_id: int):
        """
        Генерирует отчёт по заданной группе студентов:
        для каждого студента и каждого курса возвращает:
          - group_info: {{id, name}}
          - student_info: {{id, name}}
          - course_info: {{id, name}}
          - planned_hours: число запланированных часов (= lectures_count * 2)
          - attended_hours: число посещённых часов (= attended_count * 2)
        Комбинирует данные из Neo4j (список студентов, курсов, расписаний) и PostgreSQL (Attendance).
        """
        report = []
        # Получить студентов и информацию о группе из Neo4j
        with self.neo4j_driver.session() as session:
            group_rec = session.run(
                "MATCH (g:Group {id:$gid}) RETURN g.id AS id, g.name AS name",
                gid=group_id
            ).single()
            if not group_rec:
                return []
            group_info = dict(group_rec)
            # Получить всех студентов группы
            students = session.run(
                "MATCH (g:Group {id:$gid})-[:HAS_STUDENT]->(s:Student) "
                "RETURN s.id AS id, s.name AS name",
                gid=group_id
            )
            student_list = [dict(r) for r in students]
            # Получить курсы и связанные расписания
            course_sched = session.run(
                "MATCH (g:Group {id:$gid})<-[:FOR_GROUP]-(sch:Schedule)<-[:SCHEDULED_AT]-(l:Lecture)"
                "<-[:HAS_LECTURE]-(c:Course) "
                "RETURN c.id AS course_id, c.name AS course_name, COLLECT(DISTINCT sch.id) AS sched_ids",
                gid=group_id
            )
        # Обработать каждый студент и курс
        for stud in student_list:
            sid = stud['id']
            for rec in course_sched:
                cid = rec['course_id']
                cname = rec['course_name']
                sched_ids = rec['sched_ids']
                total_lects = len(sched_ids)
                # Проверить посещения через PostgreSQL
                if sched_ids:
                    self.pg_cur.execute(
                        "SELECT COUNT(*) FROM Attendance "
                        "WHERE student_id = %s "
                        "AND schedule_id = ANY(%s) "
                        "AND attended = TRUE",
                        (sid, sched_ids)
                    )
                    attended_count = self.pg_cur.fetchone()[0]
                else:
                    attended_count = 0
                report.append({
                    'group_info': group_info,
                    'student_info': stud,
                    'course_info': {'id': cid, 'name': cname},
                    'planned_hours': total_lects * 2,
                    'attended_hours': attended_count * 2
                })
        return report

# Пример использования
if __name__ == '__main__':
    service = SyncService()
    try:
        service.sync_all()
        # Пример генерации отчёта по расписанию
        schedule_id = 1
        report = service.generate_attendance_report(schedule_id)
        print(f"Attendance report for schedule {schedule_id}:", report)
        # Пример сводки по студенту
        student_id = 1
        summary = service.generate_student_attendance_summary(student_id)
        print(f"Attendance summary for student {student_id}:", summary)
    finally:
        service.close()
