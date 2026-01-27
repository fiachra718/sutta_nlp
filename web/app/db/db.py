# app/db/db.py
from __future__ import annotations

from importlib import import_module
from typing import Any, Dict, Iterable, Sequence
import html
import unicodedata
import re

import psycopg
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Json

NUMERIC_NIKAYAS = {"DN", "MN", "SN", "AN"}
CANONICAL_BOOKS = {
    "DN": [str(i) for i in range(1, 33)],
    "MN": [str(i) for i in range(1, 153)],
    "AN": [str(i) for i in range(1, 12)],
    "SN": [str(i) for i in range(1, 57)],
}
CANONICAL_SN_VAGGAS = [str(i) for i in range(1, 57)]


def _first_number(value: str | None):
    if not value:
        return None
    match = re.search(r"\d+", value)
    return int(match.group(0)) if match else None


def _numeric_sort_key(value: str):
    first = _first_number(value)
    if first is None:
        return (float("inf"), value)
    return (first, value.lower())


def _matches_numeric(value: str | None, target: str | None):
    if value is None or target is None:
        return False
    return str(_first_number(value)) == str(target)


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
    clean_book = (book_number or "").strip() or None
    clean_vagga = (vagga or "").strip() or None

    params = {
        "nikaya": nikaya,
        "book_number": clean_book,
        "vagga": clean_vagga,
        "verse_num": verse_num,
        "limit": max(1, min(int(limit), 500)),
    }
    numeric = nikaya and nikaya.upper() in NUMERIC_NIKAYAS
    book_filter = str(clean_book) if (clean_book and numeric) else None
    vagga_filter = str(clean_vagga) if (clean_vagga and numeric and nikaya.upper() in {"AN", "SN"}) else None

    sql = VERSE_SEARCH_SQL
    if numeric and book_filter:
        sql = sql.replace("AND (CAST(%(book_number)s AS text) IS NULL OR s.book_number = %(book_number)s)", "")
        params["book_number"] = None
    if numeric and vagga_filter:
        sql = sql.replace("AND (CAST(%(vagga)s AS text) IS NULL OR COALESCE(s.vagga, '') = %(vagga)s)", "")
        params["vagga"] = None

    rows = fetch_all(sql, params, dsn=dsn)

    if book_filter:
        rows = [row for row in rows if _matches_numeric(row.get("book_number"), book_filter)]
    if vagga_filter:
        rows = [row for row in rows if _matches_numeric(row.get("vagga"), vagga_filter)]

    return rows


def list_nikayas(*, dsn=None):
    rows = fetch_all(
        "SELECT DISTINCT nikaya FROM ati_suttas WHERE nikaya IS NOT NULL ORDER BY nikaya",
        dsn=dsn,
    )
    return [row["nikaya"] for row in rows if row.get("nikaya")]


def list_book_numbers(*, nikaya=None, dsn=None):
    if not nikaya:
        return []
    upper = nikaya.upper()
    if upper in CANONICAL_BOOKS:
        return CANONICAL_BOOKS[upper]

    where = ["book_number IS NOT NULL", "book_number <> ''", "nikaya = %(nikaya)s"]
    params: Dict[str, Any] = {"nikaya": nikaya}
    sql = "SELECT DISTINCT book_number FROM ati_suttas WHERE " + " AND ".join(where)
    rows = fetch_all(sql, params, dsn=dsn)
    values = [row["book_number"] for row in rows if row.get("book_number")]
    return sorted(values)


def list_vaggas(*, nikaya=None, book_number=None, dsn=None):
    if not nikaya:
        return []

    upper = nikaya.upper()
    if upper in {"DN", "MN"}:
        return []
    if upper == "SN":
        return CANONICAL_SN_VAGGAS

    if upper == "AN":
        clauses = ["vagga IS NOT NULL", "vagga <> ''", "nikaya = %(nikaya)s"]
        params: Dict[str, Any] = {"nikaya": nikaya}
        if book_number:
            clauses.append("book_number = %(book_number)s")
            params["book_number"] = book_number
        sql = "SELECT DISTINCT vagga FROM ati_suttas WHERE " + " AND ".join(clauses)
        rows = fetch_all(sql, params, dsn=dsn)
        numbers = sorted(
            { _first_number(row["vagga"]) for row in rows if _first_number(row.get("vagga")) is not None }
        )
        return [str(num) for num in numbers]

    clauses = ["vagga IS NOT NULL", "vagga <> ''", "nikaya = %(nikaya)s"]
    params = {"nikaya": nikaya}
    sql = "SELECT DISTINCT vagga FROM ati_suttas WHERE " + " AND ".join(clauses)
    rows = fetch_all(sql, params, dsn=dsn)
    values = [row["vagga"] for row in rows if row.get("vagga")]
    return sorted(values)


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


FACET_SQL = """
    SELECT
        v.identifier,
        v.nikaya,
        v.book_number,
        v.vagga,
        v.canon_ref,
        v.verse_num,
        v.text
    FROM ati_verses AS v
    WHERE EXISTS (
        SELECT 1
        FROM jsonb_array_elements(v.ner_span) AS ent
        WHERE ent->>'label' = %(label)s
          AND ent->>'text' = ANY(%(terms)s)
    )
    ORDER BY v.identifier, v.verse_num
    LIMIT %(limit)s
"""


def _normalize_entity_term(value: str) -> str:
    decoded = html.unescape(value or "")
    normalized = unicodedata.normalize("NFD", decoded)
    stripped = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return unicodedata.normalize("NFC", stripped).lower().strip()


def fetch_entity_id(entity_type: str, name: str, *, dsn: str | None = None):
    if not entity_type or not name:
        return None
    label = entity_type.strip().upper()
    normalized = _normalize_entity_term(name)
    if not normalized:
        return None
    row = fetch_one(
        """
        WITH matches AS (
            SELECT e.id, 0 AS priority
            FROM ati_entities e
            WHERE e.entity_type = %(label)s
              AND e.normalized = %(term)s
            UNION ALL
            SELECT a.entity_id AS id, 1 AS priority
            FROM ati_entity_aliases a
            JOIN ati_entities e ON e.id = a.entity_id
            WHERE e.entity_type = %(label)s
              AND a.normalized = %(term)s
        )
        SELECT id
        FROM matches
        ORDER BY priority, id
        LIMIT 1
        """,
        {"label": label, "term": normalized},
        dsn=dsn,
    )
    return row["id"] if row else None


def fetch_verse_by_cleaned_text(text: str, *, dsn: str | None = None):
    if not text:
        return None
    row = fetch_one(
        """
        SELECT id, identifier, verse_num
        FROM ati_verses
        WHERE cleaned_text_hash = md5(
            regexp_replace(unaccent(lower(%(text)s)), '[^a-z0-9]+', ' ', 'g')
        )
        LIMIT 1
        """,
        {"text": text},
        dsn=dsn,
    )
    return row


def fetch_verse_by_identifier(identifier: str, verse_num: int, *, dsn: str | None = None):
    if not identifier:
        return None
    row = fetch_one(
        """
        SELECT id, identifier, verse_num
        FROM ati_verses
        WHERE identifier = %(identifier)s
          AND verse_num = %(verse_num)s
        LIMIT 1
        """,
        {"identifier": identifier, "verse_num": verse_num},
        dsn=dsn,
    )
    return row


def update_discourse_spans(verse_id: int, payload: dict, *, dsn: str | None = None):
    if verse_id is None:
        return {"ok": False, "message": "verse_id is required"}
    rowcount = execute(
        """
        UPDATE ati_verses
        SET discourse_spans = %(payload)s
        WHERE id = %(verse_id)s
        """,
        {"payload": Json(payload), "verse_id": verse_id},
        dsn=dsn,
    )
    if rowcount < 1:
        return {"ok": False, "message": "Verse not found."}
    return {"ok": True, "updated": rowcount}


def _list_entities_by_label(label: str, limit: int, *, dsn=None):
    rows = fetch_all(
        """
        SELECT canonical
        FROM ati_entities
        WHERE entity_type = %(label)s
        ORDER BY canonical
        LIMIT %(limit)s
        """,
        {"label": label, "limit": limit},
        dsn=dsn,
    )
    return [row["canonical"] for row in rows if row.get("canonical")]

def list_entities_by_label(label: str, limit: int = 200, *, dsn=None):
    return _list_entities_by_label(label, max(1, min(int(limit), 1000)), dsn=dsn)


def facet_search(*, label: str, terms: Sequence[str], limit: int = 200, dsn=None):
    clean_terms = [term.strip() for term in terms if term and term.strip()]
    if not clean_terms:
        return []
    params = {
        "label": label,
        "terms": clean_terms,
        "limit": max(1, min(int(limit), 1000)),
    }
    return fetch_all(FACET_SQL, params, dsn=dsn)


FACET_CONTEXT_SQL = """
    WITH selected_terms AS (
        SELECT label, term
        FROM unnest(%(labels)s::text[], %(terms)s::text[]) AS t(label, term)
    ),
    entity_names AS (
        SELECT e.id AS entity_id, e.entity_type AS label, e.normalized AS normalized
        FROM ati_entities e
        UNION ALL
        SELECT a.entity_id, e.entity_type AS label, a.normalized AS normalized
        FROM ati_entity_aliases a
        JOIN ati_entities e ON e.id = a.entity_id
    ),
    matched AS (
        SELECT m.verse_id, st.label
        FROM selected_terms st
        JOIN entity_names en
          ON en.label = st.label
         AND en.normalized = st.term
        JOIN ati_entity_mentions m
          ON m.entity_id = en.entity_id
        GROUP BY m.verse_id, st.label
    ),
    qualified AS (
        SELECT verse_id
        FROM matched
        GROUP BY verse_id
        HAVING COUNT(DISTINCT label) = (SELECT COUNT(DISTINCT label) FROM selected_terms)
    ),
    facets AS (
        SELECT e.entity_type AS label, e.canonical AS name
        FROM qualified q
        JOIN ati_entity_mentions m ON m.verse_id = q.verse_id
        JOIN ati_entities e ON e.id = m.entity_id
        WHERE e.entity_type IN ('PERSON', 'GPE', 'LOC')
    ),
    ranked AS (
        SELECT label,
               name,
               ROW_NUMBER() OVER (PARTITION BY label ORDER BY name) AS rn
        FROM (SELECT DISTINCT label, name FROM facets) deduped
    )
    SELECT label, name
    FROM ranked
    WHERE rn <= %(limit)s
    ORDER BY label, name
"""


def facet_context(
    *,
    label_terms: Dict[str, Sequence[str]],
    limit: int = 200,
    dsn=None,
):
    clean_pairs: list[tuple[str, str]] = []
    for label, terms in (label_terms or {}).items():
        for term in terms or []:
            norm = _normalize_entity_term(term)
            if norm:
                clean_pairs.append((label, norm))

    limited = max(1, min(int(limit), 1000))
    if not clean_pairs:
        return {
            "PERSON": _list_entities_by_label("PERSON", limited, dsn=dsn),
            "GPE": _list_entities_by_label("GPE", limited, dsn=dsn),
            "LOC": _list_entities_by_label("LOC", limited, dsn=dsn),
        }

    labels = [label for label, _ in clean_pairs]
    terms = [term for _, term in clean_pairs]
    rows = fetch_all(
        FACET_CONTEXT_SQL,
        {"labels": labels, "terms": terms, "limit": limited},
        dsn=dsn,
    )
    facets = {"PERSON": [], "GPE": [], "LOC": []}
    for row in rows:
        label = row.get("label")
        name = row.get("name")
        if label in facets and name:
            facets[label].append(name)
    return facets


FACET_VERSES_SQL = """
    WITH selected_terms AS (
        SELECT label, term
        FROM unnest(%(labels)s::text[], %(terms)s::text[]) AS t(label, term)
    ),
    entity_names AS (
        SELECT e.id AS entity_id, e.entity_type AS label, e.normalized AS normalized
        FROM ati_entities e
        UNION ALL
        SELECT a.entity_id, e.entity_type AS label, a.normalized AS normalized
        FROM ati_entity_aliases a
        JOIN ati_entities e ON e.id = a.entity_id
    ),
    matched AS (
        SELECT m.verse_id, st.label
        FROM selected_terms st
        JOIN entity_names en
          ON en.label = st.label
         AND en.normalized = st.term
        JOIN ati_entity_mentions m
          ON m.entity_id = en.entity_id
        GROUP BY m.verse_id, st.label
    ),
    qualified AS (
        SELECT verse_id
        FROM matched
        GROUP BY verse_id
        HAVING COUNT(DISTINCT label) = (SELECT COUNT(DISTINCT label) FROM selected_terms)
    )
    SELECT
        v.identifier,
        v.nikaya,
        v.book_number,
        v.vagga,
        v.canon_ref,
        v.verse_num,
        v.text
    FROM qualified q
    JOIN ati_verses v ON v.id = q.verse_id
    ORDER BY v.identifier, v.verse_num
    LIMIT %(limit)s
"""


def facet_verses(
    *,
    label_terms: Dict[str, Sequence[str]],
    limit: int = 50,
    dsn=None,
):
    clean_pairs: list[tuple[str, str]] = []
    for label, terms in (label_terms or {}).items():
        for term in terms or []:
            norm = _normalize_entity_term(term)
            if norm:
                clean_pairs.append((label, norm))

    if not clean_pairs:
        return []

    labels = [label for label, _ in clean_pairs]
    terms = [term for _, term in clean_pairs]
    return fetch_all(
        FACET_VERSES_SQL,
        {"labels": labels, "terms": terms, "limit": max(1, min(int(limit), 500))},
        dsn=dsn,
    )
