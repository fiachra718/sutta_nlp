# app/db/db.py
from __future__ import annotations

from functools import lru_cache
from importlib import import_module
from typing import Any, Dict, Iterable

import psycopg
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row


@lru_cache(maxsize=1)
def default_dsn() -> str:
    """
    Resolve the database connection string from local settings.
    Supports both app.local_settings and project root local_settings.
    """
    settings: Dict[str, Any] | None = None
    for module_name in ("app.local_settings", "local_settings"):
        try:
            module = import_module(module_name)
        except ImportError:
            continue
        candidate = getattr(module, "settings", None)
        if isinstance(candidate, dict):
            settings = candidate
            break

    if not isinstance(settings, dict):
        settings = {}

    db = settings.get("DB", {})
    if not isinstance(db, dict):
        db = {}

    url = db.get("URL")
    if isinstance(url, str) and url:
        if url.startswith("postgresql+"):
            return "postgresql://" + url.split("://", 1)[1]
        return url

    parts = []
    mapping = {
        "NAME": "dbname",
        "USER": "user",
        "PASSWORD": "password",
        "HOST": "host",
        "PORT": "port",
    }
    for key, conn_key in mapping.items():
        value = db.get(key)
        if value:
            parts.append(f"{conn_key}={value}")

    return " ".join(parts)


def connect(dsn: str | None = None):
    """
    Return a new psycopg connection with dict_row row factory.
    """
    conninfo = dsn or default_dsn()
    return psycopg.connect(conninfo, row_factory=dict_row)


def fetch_all(sql: str, params: Dict[str, Any] | Iterable[Any] | None = None, *, dsn: str | None = None):
    with connect(dsn) as cx, cx.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def fetch_one(sql: str, params: Dict[str, Any] | Iterable[Any] | None = None, *, dsn: str | None = None):
    with connect(dsn) as cx, cx.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchone()


def execute(sql: str, params: Dict[str, Any] | Iterable[Any] | None = None, *, dsn: str | None = None):
    with connect(dsn) as cx, cx.cursor() as cur:
        cur.execute(sql, params or ())
        cx.commit()
        return cur.rowcount


def save_training_record(record: Dict[str, Any], *, dsn: str | None = None):
    """
    Insert a training record into gold_training. Returns a dict describing the result.
    The record must include: id, text, text_hash, spans (Json), spans_hash, source, from_file.
    """
    insert_sql = """
        INSERT INTO gold_training (id, text, text_hash, spans, spans_hash, source, from_file)
        VALUES (%(id)s, %(text)s, %(text_hash)s, %(spans)s, %(spans_hash)s, %(source)s, %(from_file)s)
    """
    conflict_sql = """
        SELECT id FROM gold_training
        WHERE text_hash=%(text_hash)s AND spans_hash=%(spans_hash)s
    """
    id_sql = "SELECT id FROM gold_training WHERE id=%(id)s"

    with connect(dsn) as cx, cx.cursor() as cur:
        try:
            cur.execute(insert_sql, record)
            cx.commit()
            return {"ok": True, "id": record["id"], "created": True}
        except UniqueViolation:
            cx.rollback()
            cur.execute(conflict_sql, {"text_hash": record["text_hash"], "spans_hash": record["spans_hash"]})
            existing = cur.fetchone()
            if existing:
                return {
                    "ok": False,
                    "id": existing["id"],
                    "created": False,
                    "message": f"An identical training doc already exists (id={existing['id']}).",
                }

            cur.execute(id_sql, {"id": record["id"]})
            duplicate_id = cur.fetchone()
            if duplicate_id:
                return {
                    "ok": False,
                    "id": duplicate_id["id"],
                    "created": False,
                    "message": f"Training doc id {duplicate_id['id']} already exists.",
                }

            raise
