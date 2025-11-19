# app/db/db.py
from __future__ import annotations

from importlib import import_module
from typing import Any, Dict, Iterable

import psycopg
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Json

VERSE_LOOKUP_SQL = """
    SELECT s.identifier,
           ordinality - 1 AS verse_num,
           verse_elem->>'text' AS text,
           s.nikaya,
           COALESCE(s.vagga, '') AS vagga,
           s.book_number,
           s.translator,
           s.title,
           s.subtitle
    FROM ati_suttas AS s
    CROSS JOIN LATERAL jsonb_array_elements(s.verses)
        WITH ORDINALITY AS t(verse_elem, ordinality)
    WHERE s.identifier = %(identifier)s
      AND ordinality - 1 = %(verse_num)s
    LIMIT 1;
"""

RANDOM_SUTTA_VERSE_SQL = """
    SELECT
        s.identifier,
        s.nikaya,
        COALESCE(s.vagga, '') AS vagga,
        ordinality - 1 AS verse_num,
        verse_elem->>'text' AS verse_text
    FROM ati_suttas AS s
    CROSS JOIN LATERAL jsonb_array_elements(s.verses) WITH ORDINALITY AS t(verse_elem, ordinality)
    WHERE s.doc_type = 'sutta'
    ORDER BY random()
    LIMIT 1;
"""

VERSE_SEARCH_SQL = """
    SELECT
        s.identifier,
        s.nikaya,
        COALESCE(s.vagga, '') AS vagga,
        s.book_number,
        s.translator,
        s.title,
        s.subtitle,
        ordinality - 1 AS verse_num,
        verse_elem->>'text' AS text,
        md5(verse_elem->>'text') AS text_hash
    FROM ati_suttas AS s
    CROSS JOIN LATERAL jsonb_array_elements(s.verses)
        WITH ORDINALITY AS t(verse_elem, ordinality)
    WHERE s.doc_type = 'sutta'
      AND (CAST(%(nikaya)s AS text) IS NULL OR s.nikaya = %(nikaya)s)
      AND (CAST(%(book_number)s AS text) IS NULL OR s.book_number = %(book_number)s)
      AND (CAST(%(vagga)s AS text) IS NULL OR COALESCE(s.vagga, '') = %(vagga)s)
      AND (CAST(%(verse_num)s AS integer) IS NULL OR ordinality - 1 = %(verse_num)s)
    ORDER BY s.nikaya NULLS LAST, s.identifier, verse_num
    LIMIT %(limit)s
"""


def fetch_sutta_verse(identifier, verse_num, *, dsn=None):
    return fetch_one(VERSE_LOOKUP_SQL, {"identifier": identifier, "verse_num": verse_num}, dsn=dsn)


def search_sutta_verses(*, nikaya=None, book_number=None, vagga=None, verse_num=None, limit=50, dsn=None):
    params = {
        "nikaya": nikaya,
        "book_number": book_number,
        "vagga": vagga if vagga not in (None, "") else None,
        "verse_num": verse_num,
        "limit": max(1, min(int(limit), 500)),
    }
    return fetch_all(VERSE_SEARCH_SQL, params, dsn=dsn)


def list_nikayas(*, dsn=None):
    rows = fetch_all(
        "SELECT DISTINCT nikaya FROM ati_suttas WHERE nikaya IS NOT NULL ORDER BY nikaya",
        dsn=dsn,
    )
    return [row["nikaya"] for row in rows if row.get("nikaya")]


def list_book_numbers(*, dsn=None):
    rows = fetch_all(
        "SELECT DISTINCT book_number FROM ati_suttas WHERE book_number IS NOT NULL ORDER BY book_number",
        dsn=dsn,
    )
    return [row["book_number"] for row in rows if row.get("book_number")]


def list_vaggas(*, dsn=None):
    rows = fetch_all(
        "SELECT DISTINCT COALESCE(vagga, '') AS vagga FROM ati_suttas WHERE vagga IS NOT NULL AND vagga <> '' ORDER BY vagga",
        dsn=dsn,
    )
    return [row["vagga"] for row in rows if row.get("vagga")]


def get_ner_verse_spans(identifier, *, dsn=None):
    row = fetch_one(
        "SELECT ner_verse_spans FROM ati_suttas WHERE identifier = %(identifier)s",
        {"identifier": identifier},
        dsn=dsn,
    )
    if not row:
        return None
    return row.get("ner_verse_spans") or []


def update_ner_verse_spans(identifier, verse_num, entries, *, dsn=None):
    with connect(dsn) as cx, cx.cursor() as cur:
        cur.execute(
            "SELECT ner_verse_spans FROM ati_suttas WHERE identifier = %(identifier)s FOR UPDATE",
            {"identifier": identifier},
        )
        row = cur.fetchone()
        if not row:
            cx.rollback()
            return False
        existing = row.get("ner_verse_spans") or []
        filtered = [item for item in existing if isinstance(item, dict) and item.get("verse_num") != verse_num]
        filtered.extend(entries)
        cur.execute(
            "UPDATE ati_suttas SET ner_verse_spans = %(payload)s WHERE identifier = %(identifier)s",
            {"payload": Json(filtered), "identifier": identifier},
        )
        cx.commit()
        return True

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

    conninfo = " ".join(parts).strip()
    if conninfo:
        return conninfo
    return "dbname=tipitaka user=alee"


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
