import psycopg2
from neo4j import GraphDatabase
import datetime
from redis_module import StudentSearch

# Конфигурация подключения
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

class SyncService:
    def __init__(self, pg_conf, neo4j_uri, neo4j_user, neo4j_password):
        self.pg_conn = psycopg2.connect(**pg_conf)
        self.neo_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    def close(self):
        self.pg_conn.close()
        self.neo_driver.close()

    def fetch_all(self, query):
        with self.pg_conn.cursor() as cur:
            cur.execute(query)
            cols = [desc[0] for desc in cur.description]
            for row in cur.fetchall():
                yield dict(zip(cols, row))

    def sync_universities(self):
        cypher = '''
        UNWIND $rows AS row
        MERGE (u:University {postgres_id: row.id})
        SET u.name = row.name, u.location = row.location
        '''
        rows = list(self.fetch_all("SELECT id, name, location FROM University"))
        with self.neo_driver.session() as session:
            session.run(cypher, rows=rows)

    def sync_institutes(self):
        cypher = '''
        UNWIND $rows AS row
        MATCH (u:University {postgres_id: row.university_id})
        MERGE (i:Institute {postgres_id: row.id})
        SET i.name = row.name
        MERGE (u)-[:HAS_INSTITUTE]->(i)
        '''
        rows = list(self.fetch_all("SELECT id, name, university_id FROM Institute"))
        with self.neo_driver.session() as session:
            session.run(cypher, rows=rows)

    def sync_departments(self):
        cypher = '''
        UNWIND $rows AS row
        MATCH (i:Institute {postgres_id: row.institute_id})
        MERGE (d:Department {postgres_id: row.id})
        SET d.name = row.name
        MERGE (i)-[:HAS_DEPARTMENT]->(d)
        '''
        rows = list(self.fetch_all("SELECT id, name, institute_id FROM Department"))
        with self.neo_driver.session() as session:
            session.run(cypher, rows=rows)

    def sync_specialties(self):
        cypher = '''
        UNWIND $rows AS row
        MATCH (d:Department {postgres_id: row.department_id})
        MERGE (s:Specialty {postgres_id: row.id})
        SET s.name = row.name
        MERGE (d)-[:HAS_SPECIALTY]->(s)
        '''
        rows = list(self.fetch_all("SELECT id, name, department_id FROM Specialty"))
        with self.neo_driver.session() as session:
            session.run(cypher, rows=rows)

    def sync_groups(self):
        cypher = '''
        UNWIND $rows AS row
        MATCH (s:Specialty {postgres_id: row.speciality_id})
        MERGE (g:Group {postgres_id: row.id})
        SET g.name = row.name
        MERGE (s)-[:HAS_GROUP]->(g)
        '''
        rows = list(self.fetch_all("SELECT id, name, speciality_id FROM St_group"))
        with self.neo_driver.session() as session:
            session.run(cypher, rows=rows)

    def sync_courses_and_lectures(self):
        # Courses
        cypher_course = '''
        UNWIND $rows AS row
        MATCH (d:Department {postgres_id: row.department_id}),
              (s:Specialty {postgres_id: row.specialty_id})
        MERGE (c:Course {postgres_id: row.id})
        SET c.name = row.name
        MERGE (d)-[:OFFERS_COURSE]->(c)
        '''
        rows = list(self.fetch_all("SELECT id, name, department_id, specialty_id FROM Course_of_lecture"))
        with self.neo_driver.session() as session:
            session.run(cypher_course, rows=rows)

        # Lectures
        cypher_lec = '''
        UNWIND $rows AS row
        MATCH (c:Course {postgres_id: row.course_of_lecture_id})
        MERGE (l:Lecture {postgres_id: row.id})
        SET l.name = row.name
        MERGE (c)-[:INCLUDES_LECTURE]->(l)
        '''
        rows = list(self.fetch_all("SELECT id, name, course_of_lecture_id FROM Lecture"))
        with self.neo_driver.session() as session:
            session.run(cypher_lec, rows=rows)

    def sync_students(self):
        cypher = '''
        UNWIND $rows AS row
        MATCH (g:Group {postgres_id: row.group_id})
        MERGE (st:Student {postgres_id: row.id})
        SET st.name = row.name, st.age = row.age, st.mail = row.mail
        MERGE (st)-[:MEMBER_OF]->(g)
        '''
        rows = list(self.fetch_all("SELECT id, name, age, mail, group_id FROM Students"))
        with self.neo_driver.session() as session:
            session.run(cypher, rows=rows)

    def sync_schedule(self):
        cypher = '''
        UNWIND $rows AS row
        MATCH (l:Lecture {postgres_id: row.lecture_id}),
              (g:Group {postgres_id: row.group_id})
        MERGE (e:ScheduleEvent {postgres_id: row.id})
        SET e.date = date(row.date)
        MERGE (g)-[:SCHEDULED_FOR]->(e)
        MERGE (e)-[:OF_LECTURE]->(l)
        '''
        rows = list(self.fetch_all("SELECT id, date, lecture_id, group_id FROM Schedule"))
        with self.neo_driver.session() as session:
            session.run(cypher, rows=rows)

    def sync_attendance(self):
        cypher = '''
        UNWIND $rows AS row
        MATCH (st:Student {postgres_id: row.student_id}),
              (e:ScheduleEvent {postgres_id: row.schedule_id})
        MERGE (st)-[a:ATTENDED]->(e)
        SET a.attended = row.attended, a.updated = row.id
        '''
        rows = list(self.fetch_all("SELECT id, student_id, schedule_id, attended FROM Attendance"))
        with self.neo_driver.session() as session:
            session.run(cypher, rows=rows)
    
    
    def sync_materials(self):
        cypher = '''
        UNWIND $rows AS row
        MATCH (lec:Lecture {postgres_id: row.lecture_id})
        MERGE (mat:Material {postgres_id: row.id})
        SET mat.name = row.name
        MERGE (lec)-[:USES_MATERIAL]->(mat)
        '''
        # Добавьте JOIN с таблицей Lecture, чтобы исключить недействительные ID
        sql = """
        SELECT m.id, m.name, m.course_of_lecture_id AS lecture_id
        FROM Material_of_lecture m
        INNER JOIN Lecture l ON m.course_of_lecture_id = l.id
        """
        rows = list(self.fetch_all(sql))
        
        if rows:
            with self.neo_driver.session() as session:
                session.run(cypher, rows=rows)


    def generate_audience_report(self, year: int, semester: int):
        start_date, end_date = self._calculate_semester_dates(year, semester)
        print(start_date)
        print(end_date)
        
        cypher_query = """
MATCH (e:ScheduleEvent)
WHERE e.date >= date($start_date) AND e.date <= date($end_date)

// Сначала считаем общее число студентов на каждую лекцию
MATCH (g:Group)-[:SCHEDULED_FOR]->(e)
MATCH (s:Student)-[:MEMBER_OF]->(g)
WITH e, COUNT(s) AS students_in_group
WITH e, SUM(students_in_group) AS total_students

// Теперь привязываем лекцию к курсу и материалам
MATCH (e)-[:OF_LECTURE]->(l:Lecture)
MATCH (c:Course)-[:INCLUDES_LECTURE]->(l)
OPTIONAL MATCH (l)-[:USES_MATERIAL]->(m:Material)

RETURN
  c.name            AS course_name,
  l.name            AS lecture_name,
  COLLECT(DISTINCT m.name) AS tech_requirements,
  total_students
ORDER BY course_name, lecture_name;
        """

        with self.neo_driver.session() as session:
            result = session.run(cypher_query, start_date=str(start_date), end_date=str(end_date))
            return [dict(record) for record in result]
        
    def generate_audience_report(self, year: int, semester: int):
        start_date, end_date = self._calculate_semester_dates(year, semester)
        print(start_date)
        print(end_date)
        
        cypher_query = """
MATCH (e:ScheduleEvent)
WHERE e.date >= date($start_date) AND e.date <= date($end_date)

// Сначала считаем общее число студентов на каждую лекцию
MATCH (g:Group)-[:SCHEDULED_FOR]->(e)
MATCH (s:Student)-[:MEMBER_OF]->(g)
WITH e, COUNT(s) AS students_in_group
WITH e, SUM(students_in_group) AS total_students

// Теперь привязываем лекцию к курсу и материалам
MATCH (e)-[:OF_LECTURE]->(l:Lecture)
MATCH (c:Course)-[:INCLUDES_LECTURE]->(l)
OPTIONAL MATCH (l)-[:USES_MATERIAL]->(m:Material)

RETURN
  c.name            AS course_name,
  l.name            AS lecture_name,
  COLLECT(DISTINCT m.name) AS tech_requirements,
  total_students
ORDER BY course_name, lecture_name;
        """

        with self.neo_driver.session() as session:
            result = session.run(cypher_query, start_date=str(start_date), end_date=str(end_date))
            return [dict(record) for record in result]
        
    def generate_group_report(self, group_id: int):
        """
        Генерирует отчёт по заданной группе студентов:
        подсчитывает общее (planned_hours) и фактическое (attended_hours) время.
        Информация о студенте теперь берётся из Redis, а не из Neo4j.
        """
        cypher_query = """
        MATCH (g:Group {postgres_id: $group_id})
        MATCH (g)<-[:HAS_GROUP]-(s:Specialty)<-[:HAS_SPECIALTY]-(d:Department)
        MATCH (d)-[:OFFERS_COURSE]->(c:Course)
        MATCH (c)-[:INCLUDES_LECTURE]->(l:Lecture)
        MATCH (e:ScheduleEvent)-[:OF_LECTURE]->(l),
            (g)-[:SCHEDULED_FOR]->(e)
        MATCH (st:Student)-[:MEMBER_OF]->(g)
        OPTIONAL MATCH (st)-[a:ATTENDED]->(e) WHERE a.attended = true
        WITH g, c, st,
            COUNT(DISTINCT e) AS total_lectures,
            COUNT(DISTINCT CASE WHEN a IS NOT NULL THEN e END) AS attended_lectures
        RETURN
        g { .postgres_id, .name }       AS group_info,
        st.postgres_id                  AS student_id,
        c { .postgres_id, .name }       AS course_info,
        total_lectures * 2              AS planned_hours,
        attended_lectures * 2           AS attended_hours
        ORDER BY g.name, st.name, c.name
        """

        # 1) Querry for Neo4j
        with self.neo_driver.session() as session:
            result = session.run(cypher_query, group_id=group_id)
            raw_records = [dict(rec) for rec in result]

        # 2) Init Redis
        student_search = StudentSearch(redis_host='redis', redis_port=6379)

        # 3) For every index(id) result - applying Redis search by id
        report = []
        for rec in raw_records:
            sid = rec.pop("student_id")
            try:
                full_student = student_search.get_student_full(int(sid))
            except ValueError:
                full_student = {"id": sid, "name": None, "age": None, "mail": None, "group": None}
            rec["student_info"] = full_student
            report.append(rec)

        return report

    @staticmethod
    def _calculate_semester_dates(year: int, semester: int):
        """Вычисляет даты начала и конца семестра"""
        if semester == 1:
            return datetime.datetime(year, 9, 1).date(), datetime.datetime(year, 12, 31).date()
        else:
            return datetime.datetime(year, 2, 1).date(), datetime.datetime(year, 6, 30).date()



    def run_all(self):
        self.sync_universities()
        self.sync_institutes()
        self.sync_departments()
        self.sync_specialties()
        self.sync_groups()
        self.sync_courses_and_lectures()
        self.sync_students()
        self.sync_schedule()
        self.sync_attendance()
        self.sync_materials()
        print("Синхронизация завершена.")


if __name__ == '__main__':
    service = SyncService(PG_CONFIG, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        service.run_all()
    finally:
        service.close()
