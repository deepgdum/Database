from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2
import pandas as pd
from fastapi.responses import JSONResponse
from psycopg2 import sql as psycopg2_sql

app = FastAPI()

# PostgreSQL connection settings
DB_CONFIG = {
    "host": "dpg-d0necthr0fns738ula70-a",
    "port": 5432,
    "database": "nocodeql_demo",
    "user": "nocodeql_demo_user",
    "password": "EkHW4T1ToLLTDGXPhAvZhbzEE2zAtEae"
}

# Request body format
class QueryRequest(BaseModel):
    sql: str

@app.post("/run-sql")
def run_sql(request: QueryRequest):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql_query(request.sql, conn)
        conn.close()
        return {"result": df.to_dict(orient="records")}
    except Exception as e:
        return {"error": str(e)}

@app.get("/init-db")
def init_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Drop if exists
        cur.execute("DROP TABLE IF EXISTS orders;")
        cur.execute("DROP TABLE IF EXISTS users;")

        # Create users table
        cur.execute("""
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            signup_date DATE NOT NULL
        );
        """)

        # Create orders table
        cur.execute("""
        CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            product TEXT NOT NULL,
            amount INTEGER NOT NULL,
            order_date DATE NOT NULL
        );
        """)

        # Insert sample users
        cur.execute("""
        INSERT INTO users (name, email, signup_date) VALUES
        ('Alice', 'alice@example.com', '2024-01-01'),
        ('Bob', 'bob@example.com', '2024-02-15'),
        ('Charlie', 'charlie@example.com', '2024-03-01');
        """)

        # Insert sample orders
        cur.execute("""
        INSERT INTO orders (user_id, product, amount, order_date) VALUES
        (1, 'Laptop', 1200, '2024-03-02'),
        (1, 'Mouse', 50, '2024-03-05'),
        (2, 'Keyboard', 100, '2024-03-06'),
        (3, 'Monitor', 250, '2024-03-08');
        """)

        conn.commit()
        conn.close()
        return JSONResponse({"message": "✅ Tables created and test data inserted successfully."})

    except Exception as e:
        return JSONResponse({"error": str(e)})
