from fastapi import FastAPI, UploadFile, File, HTTPException
import sqlite3
import pandas as pd
import io
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield
app = FastAPI()

def get_db_connection():
    try:
        conn = sqlite3.connect("database.db")
        return conn
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {str(e)}")

def init_db():
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS departments (
                        id INTEGER PRIMARY KEY,
                        department TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobs (
                        id INTEGER PRIMARY KEY,
                        job TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS employees (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        datetime TEXT NOT NULL,
                        department_id INTEGER,
                        job_id INTEGER,
                        FOREIGN KEY(department_id) REFERENCES departments(id),
                        FOREIGN KEY(job_id) REFERENCES jobs(id))''')
    conn.commit()
    conn.close()



@app.post("/upload/{table}")
async def upload_csv(table: str, file: UploadFile = File(...)):
    if table not in ["departments", "jobs", "employees"]:
        raise HTTPException(status_code=400, detail="Tabla no válida")
    try:
        content = await file.read()
        df = pd.read_csv(io.StringIO(content.decode("utf-8")))
        if df.empty:
            raise HTTPException(status_code=400, detail="El archivo CSV está vacío")
        expected_columns = {
            "departments": ["id", "department"],
            "jobs": ["id", "job"],
            "employees": ["id", "name", "datetime", "department_id", "job_id"]
        }

        if list(df.columns) != expected_columns[table]:
            raise HTTPException(status_code=400, detail=f"Las columnas del archivo no coinciden con la tabla {table}")
        conn = get_db_connection()
        df.to_sql(table, conn, if_exists="replace", index=False)
        conn.close()
        return {"message": f"Datos insertados en la tabla {table}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar el archivo: {str(e)}")



@app.get("/employees/hired-per-quarter")
def get_hired_employees():
    try:
        conn = get_db_connection()
        query = '''SELECT d.department, j.job,
                        SUM(CASE WHEN strftime('%m', e.datetime) IN ('01', '02', '03') THEN 1 ELSE 0 END) AS Q1,
                        SUM(CASE WHEN strftime('%m', e.datetime) IN ('04', '05', '06') THEN 1 ELSE 0 END) AS Q2,
                        SUM(CASE WHEN strftime('%m', e.datetime) IN ('07', '08', '09') THEN 1 ELSE 0 END) AS Q3,
                        SUM(CASE WHEN strftime('%m', e.datetime) IN ('10', '11', '12') THEN 1 ELSE 0 END) AS Q4
                 FROM employees e
                 JOIN departments d ON e.department_id = d.id
                 JOIN jobs j ON e.job_id = j.id
                 WHERE strftime('%Y', e.datetime) = '2021'
                 GROUP BY d.department, j.job
                 ORDER BY d.department, j.job'''
        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            return {"message": "No hay datos para mostrar"}

        return df.to_dict(orient="records")

    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {str(e)}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error desconocido: {str(e)}")


@app.get("/departments/above-average")
def get_departments_above_average():
    try:
        conn = sqlite3.connect("database.db")
        query = '''WITH hired_counts AS (
                        SELECT department_id, COUNT(*) AS hired
                        FROM employees
                        WHERE strftime('%Y', datetime) = '2021'
                        GROUP BY department_id),
                avg_hired AS (
                        SELECT AVG(hired) AS avg_hiring FROM hired_counts)
                SELECT d.id, d.department, hc.hired
                FROM hired_counts hc
                JOIN departments d ON hc.department_id = d.id
                WHERE hc.hired > (SELECT avg_hiring FROM avg_hired)
                ORDER BY hc.hired DESC'''
        df = pd.read_sql_query(query, conn)
        conn.close()
        if df.empty:
            return {"message": "No hay datos para mostrar"}

        return df.to_dict(orient="records")

    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {str(e)}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error desconocido: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
