import psycopg2
import redis
from typing import Dict, List

class StudentSearch:
    def get_student_full(self, student_id: int) -> Dict:
        key = f"student:{student_id}"
        student_data = self.r.hgetall(key)
        
        if not student_data:
            raise ValueError(f"Student with id {student_id} not found in Redis.")
        
        return {
            "id": int(student_data["id"]),
            "name": student_data["name"],
            "age": int(student_data["age"]),
            "mail": student_data["mail"],
            "group": student_data["group"]
        }
    def __init__(self, redis_host='redis', redis_port=6379):
        self.r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    
    def get_by_id(self, student_id: int) -> Dict:
        """Get student by ID"""
        return self.r.hgetall(f"student:{student_id}")
    
    def search_by_name(self, name: str) -> List[Dict]:
        """Search students by name (case-insensitive partial match)"""
        keys = self.r.keys(f"index:student:name:*{name.lower()}*")
        student_ids = set()
        for key in keys:
            student_ids.update(self.r.smembers(key))
        return [self.r.hgetall(f"student:{id}") for id in student_ids]
    
    def search_by_email(self, email: str) -> List[Dict]:
        """Search students by email (case-insensitive partial match)"""
        keys = self.r.keys(f"index:student:email:*{email.lower()}*")
        student_ids = set()
        for key in keys:
            student_ids.update(self.r.smembers(key))
        return [self.r.hgetall(f"student:{id}") for id in student_ids]
    
    def search_by_group(self, group_name: str) -> List[Dict]:
        """Search students by group name (case-insensitive partial match)"""
        keys = self.r.keys(f"index:student:group:*{group_name.lower()}*")
        student_ids = set()
        for key in keys:
            student_ids.update(self.r.smembers(key))
        return [self.r.hgetall(f"student:{id}") for id in student_ids]
    
    def full_text_search(self, query: str) -> List[Dict]:
        """Full-text search across all student fields"""
        terms = query.lower().split()
        if not terms:
            return []
        first_term = terms[0]
        keys = self.r.keys(f"index:student:search:*{first_term}*")
        student_ids = set()
        for key in keys:
            student_ids.update(self.r.smembers(key))

        for term in terms[1:]:
            term_keys = self.r.keys(f"index:student:search:*{term}*")
            term_ids = set()
            for key in term_keys:
                term_ids.update(self.r.smembers(key))
            student_ids.intersection_update(term_ids)
        
        return [self.r.hgetall(f"student:{id}") for id in student_ids]