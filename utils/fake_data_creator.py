from faker import Faker
import random
import psycopg2
from utils.constants import CONNECTION_STRING
import datetime

class FakeDataSeeder:
    IT_POSITIONS = [
         "DevOps Engineer", "Data Scientist","Frontend Developer",
        "Backend Developer", "QA Engineer"
    ]

    DEPARTMENT_NAMES = [
        "Frontend", "Backend", "Testing", "Automation", "CICD", "DevOps", "Cloud", "Security", "Support", "QA"
    ]

    def __init__(self, conn):
        self.conn = conn
        self.fake = Faker()

    def seed_departments(self):
        cur = self.conn.cursor()
        # Remove all departments to avoid duplicates (optional: use with caution)
        cur.execute("DELETE FROM departments")
        departments = []
        for name in self.DEPARTMENT_NAMES:
            location = self.fake.city()
            budget = random.randint(100000, 1000000)
            cur.execute(
                "INSERT INTO departments (department_name, location, budget) VALUES (%s, %s, %s) RETURNING department_id, department_name",
                (name, location, budget)
            )
            departments.append(cur.fetchone())
        self.conn.commit()
        cur.close()
        return departments

    def seed(self, emp_count=100):
        # Seed departments first
        departments = self.seed_departments()
        cur = self.conn.cursor()
        start = datetime.date(2015, 1, 1)
        end = datetime.date(2024, 12, 31)
        for _ in range(emp_count):
            name = self.fake.name()
            position = random.choice(self.IT_POSITIONS)
            start_date = self.fake.date_between(start_date=start, end_date=end)
            salary = random.randint(60000, 200000)
            dept_id, _ = random.choice(departments)
            cur.execute(
                "INSERT INTO employees (name, position, start_date, salary, department_id) VALUES (%s, %s, %s, %s, %s)",
                (name, position, start_date, salary, dept_id)
            )
        self.conn.commit()
        cur.close()

    @staticmethod
    def generate_and_insert():
        conn = psycopg2.connect(CONNECTION_STRING)
        seeder = FakeDataSeeder(conn)
        seeder.seed()
        conn.close()