import psycopg2
from psycopg2 import sql

# Параметры подключения
DB_NAME     = "postgres_db"
DB_USER     = "postgres_user"
DB_PASSWORD = "postgres_password"
DB_HOST     = "localhost"
DB_PORT     = "5430"

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.autocommit = False
    cur = conn.cursor()
    
    try:
        # 1. Справочные таблицы
        cur.execute("""
        CREATE TABLE IF NOT EXISTS University (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            location VARCHAR(100))
    """)

        # Table Institute
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Institute (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                university_id INTEGER REFERENCES University(id))
        """)

        # Table Department
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Department (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                institute_id INTEGER REFERENCES Institute(id))
        """)

        # Table Specialty
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Specialty (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                department_id INTEGER REFERENCES Department(id))
        """)

        # Table St_group
        cur.execute("""
            CREATE TABLE IF NOT EXISTS St_group (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                speciality_id INTEGER REFERENCES Specialty(id))
        """)

        # Таблица Course_of_lecture
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Course_of_lecture (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                department_id INTEGER REFERENCES Department(id),
                specialty_id INTEGER REFERENCES Specialty(id))
        """)

        # Таблица Lecture
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Lecture (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                course_of_lecture_id INTEGER REFERENCES Course_of_lecture(id))
        """)

        # Таблица Material_of_lecture
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Material_of_lecture (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                course_of_lecture_id INTEGER REFERENCES Lecture(id))
        """)

        
        # 2. Schedule + вычисляемый столбец semester
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Schedule (
                id         SERIAL PRIMARY KEY,
                date       TIMESTAMP NOT NULL,
                lecture_id INTEGER REFERENCES Lecture(id),
                group_id   INTEGER REFERENCES St_group(id)
            );
            ALTER TABLE Schedule
                ADD COLUMN IF NOT EXISTS semester TEXT;
                    
            CREATE OR REPLACE FUNCTION trg_compute_schedule_semester() RETURNS TRIGGER
                LANGUAGE plpgsql AS $$
                BEGIN
                NEW.semester :=
                    CASE
                    WHEN EXTRACT(MONTH FROM NEW.date) BETWEEN 1 AND 6
                        THEN (EXTRACT(YEAR FROM NEW.date)::INT || '_spring')
                    ELSE (EXTRACT(YEAR FROM NEW.date)::INT || '_fall')
                    END;
                RETURN NEW;
                END;
                $$;
            DROP TRIGGER IF EXISTS schedule_set_semester ON Schedule;
            CREATE TRIGGER schedule_set_semester
            BEFORE INSERT OR UPDATE OF date
            ON Schedule
            FOR EACH ROW
            EXECUTE FUNCTION trg_compute_schedule_semester();
        """)
        
        # 3. Студенты
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Students (
                id       SERIAL PRIMARY KEY,
                name     VARCHAR(100) NOT NULL,
                age      INTEGER,
                mail     VARCHAR(100),
                group_id INTEGER REFERENCES St_group(id)
            );
        """)
        
        # 4. Attendance — родительская партиционированная таблица
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Attendance (
                student_id  INTEGER NOT NULL REFERENCES Students(id),
                schedule_id INTEGER NOT NULL REFERENCES Schedule(id),
                attended    BOOLEAN NOT NULL,
                semester    TEXT NOT NULL
            )
            PARTITION BY LIST (semester);
        """)

        cur.execute("""
            CREATE OR REPLACE FUNCTION ensure_attendance_partition(sem TEXT) RETURNS VOID AS $$
            BEGIN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF Attendance FOR VALUES IN (%L)',
                'Attendance_' || sem,
                sem
            );
            END;
            $$ LANGUAGE plpgsql;
        """)
        
        # 5. Функция и триггер для автоматического заполнения semester
        cur.execute("""
            CREATE OR REPLACE FUNCTION trg_set_attendance_semester() RETURNS TRIGGER AS $$
            DECLARE
                rec_date TIMESTAMP;
                sem_val  TEXT;
            BEGIN
                -- Берём дату занятия из Schedule
                SELECT date INTO rec_date
                FROM Schedule
                WHERE id = NEW.schedule_id;

                IF rec_date IS NULL THEN
                    RAISE EXCEPTION 'Schedule % not found', NEW.schedule_id;
                END IF;

                -- Вычисляем семестр прямо по дате
                IF EXTRACT(MONTH FROM rec_date) BETWEEN 1 AND 6 THEN
                    sem_val := EXTRACT(YEAR FROM rec_date)::INT || '_spring';
                ELSE
                    sem_val := EXTRACT(YEAR FROM rec_date)::INT || '_fall';
                END IF;

                NEW.semester := sem_val;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            -- Пересоздаём триггер
            DROP TRIGGER IF EXISTS attendance_set_semester ON Attendance;
            CREATE TRIGGER attendance_set_semester
            BEFORE INSERT OR UPDATE OF schedule_id
            ON Attendance
            FOR EACH ROW
            EXECUTE FUNCTION trg_set_attendance_semester();
        """)
        
        cur.execute("""
        CREATE OR REPLACE FUNCTION ensure_attendance_partition(sem TEXT) RETURNS VOID AS $$
        BEGIN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF Attendance FOR VALUES IN (%L)',
            -- Формируем полное имя таблицы-части:
            'Attendance_' || sem,
            -- А это значение списка:
            sem
        );
        END;
        $$ LANGUAGE plpgsql;
    """)

        conn.commit()
        print("Схема успешно создана и настроена на партиционирование!")

    except Exception as e:
        conn.rollback()
        print(f"Ошибка при создании схемы: {e}")
    
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
