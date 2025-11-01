# app/db.py
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool 

POOL = ConnectionPool(
    conninfo="postgresql://user:pass@localhost:5432/tipitaka",
    min_size=1, max_size=10,
    kwargs={"autocommit": False, "row_factory": dict_row}
)

def fetch_one(sql: str, params: dict | tuple = ()):
    with POOL.connection() as cx, cx.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()

def fetch_all(sql: str, params: dict | tuple = ()):
    with POOL.connection() as cx, cx.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def execute(sql: str, params: dict | tuple = ()):
    with POOL.connection() as cx, cx.cursor() as cur:
        cur.execute(sql, params)
        cx.commit()
        return cur.rowcount

def execute_returning(sql: str, params: dict | tuple = ()):
    with POOL.connection() as cx, cx.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        cx.commit()
        return row