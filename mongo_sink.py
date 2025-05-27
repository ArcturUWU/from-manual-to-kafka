import psycopg2
from pymongo import MongoClient
from collections import defaultdict

def sync_postgres_to_mongo(mongo_uri='mongodb://localhost:27017/', db_name='university_db'):
    """
    Synchronize data from PostgreSQL to MongoDB with the specified schema
    
    Args:
        pg_conn_params (dict): PostgreSQL connection parameters
        mongo_uri (str): MongoDB connection URI
        db_name (str): Name of the MongoDB database
    """
    DB_NAME = "postgres_db"
    DB_USER = "postgres_user"
    DB_PASSWORD = "postgres_password"
    DB_HOST = "localhost"
    DB_PORT = "5430"

    pg_conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )


    pg_cur = pg_conn.cursor()
    
    mongo_client = MongoClient(mongo_uri,  username='admin', password='secret')
    mongo_db = mongo_client[db_name]
    
    mongo_db.drop_collection('universities')
    mongo_db.create_collection('universities', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['name', 'location', 'institutes'],
            'properties': {
                'name': {'bsonType': 'string'},
                'location': {'bsonType': 'string'},
                'institutes': {
                    'bsonType': 'array',
                    'items': {
                        'bsonType': 'object',
                        'required': ['name', 'departments'],
                        'properties': {
                            'name': {'bsonType': 'string'},
                            'departments': {
                                'bsonType': 'array',
                                'items': {
                                    'bsonType': 'object',
                                    'required': ['name'],
                                    'properties': {
                                        'name': {'bsonType': 'string'},
                                        'specializations': {
                                            'bsonType': 'array',
                                            'items': {'bsonType': 'string'}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })
    
    universities_col = mongo_db['universities']
    
    try:
        pg_cur.execute("SELECT id, name, location FROM University")
        universities = pg_cur.fetchall()
        
        for uni_id, uni_name, uni_location in universities:
            pg_cur.execute("""
                SELECT id, name FROM Institute 
                WHERE university_id = %s
            """, (uni_id,))
            institutes = pg_cur.fetchall()
            
            uni_institutes = []
            
            for inst_id, inst_name in institutes:
                pg_cur.execute("""
                    SELECT id, name FROM Department 
                    WHERE institute_id = %s
                """, (inst_id,))
                departments = pg_cur.fetchall()
                
                inst_departments = []
                
                for dept_id, dept_name in departments:
                    pg_cur.execute("""
                        SELECT name FROM Specialty 
                        WHERE department_id = %s
                    """, (dept_id,))
                    specializations = [row[0] for row in pg_cur.fetchall()]
                    
                    inst_departments.append({
                        'name': dept_name,
                        'specializations': specializations
                    })
                
                uni_institutes.append({
                    'name': inst_name,
                    'departments': inst_departments
                })
            
            university_doc = {
                'name': uni_name,
                'location': uni_location,
                'institutes': uni_institutes
            }
            
            universities_col.insert_one(university_doc)
        
        print(f"Successfully synchronized {len(universities)} universities to MongoDB")
        
    except Exception as e:
        print(f"Error during synchronization: {e}")
    finally:
        pg_cur.close()
        pg_conn.close()
        mongo_client.close()

if __name__ == "__main__":
    sync_postgres_to_mongo()
