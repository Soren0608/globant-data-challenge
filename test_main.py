import pytest
from fastapi.testclient import TestClient
from main import app  
import sqlite3

client = TestClient(app)

TEST_DB = "test_database.db"

def setup_test_db():
    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    
    cursor.execute("DROP TABLE IF EXISTS departments")
    cursor.execute("DROP TABLE IF EXISTS jobs")
    cursor.execute("DROP TABLE IF EXISTS employees")
    
    cursor.execute('''CREATE TABLE departments (
                        id INTEGER PRIMARY KEY,
                        department TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE jobs (
                        id INTEGER PRIMARY KEY,
                        job TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE employees (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        datetime TEXT NOT NULL,
                        department_id INTEGER,
                        job_id INTEGER,
                        FOREIGN KEY(department_id) REFERENCES departments(id),
                        FOREIGN KEY(job_id) REFERENCES jobs(id))''')
    
    conn.commit()
    conn.close()

@pytest.fixture(scope="module", autouse=True)
def setup():
    setup_test_db()


def test_upload_departments():
    data = "id,department\n1,Sales\n2,Engineering\n"
    response = client.post(
        "/upload/departments",
        files={"file": ("departments.csv", data, "text/csv")}
    )
    assert response.status_code == 200
    assert "Datos insertados en la tabla departments" in response.json()["message"]

def test_upload_jobs():
    data = "id,job\n1,Manager\n2,Engineer\n"
    response = client.post(
        "/upload/jobs",
        files={"file": ("jobs.csv", data, "text/csv")}
    )
    assert response.status_code == 200
    assert "Datos insertados en la tabla jobs" in response.json()["message"]

def test_upload_employees():
    data = "id,name,datetime,department_id,job_id\n1,Alice,2021-06-15T12:00:00Z,1,1\n2,Bob,2021-09-20T14:00:00Z,2,2\n"
    response = client.post(
        "/upload/employees",
        files={"file": ("employees.csv", data, "text/csv")}
    )
    assert response.status_code == 200
    assert "Datos insertados en la tabla employees" in response.json()["message"]

def test_get_hired_per_quarter():
    response = client.get("/employees/hired-per-quarter")
    assert response.status_code == 200
    assert isinstance(response.json(), list)  
    assert len(response.json()) > 0  

def test_get_departments_above_average():
    response = client.get("/departments/above-average")
    assert response.status_code == 200
    assert isinstance(response.json(), list)  
