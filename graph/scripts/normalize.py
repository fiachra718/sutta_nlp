import re
import unicodedata
from collections import defaultdict
from typing import Dict, Tuple, List, Optional, Any
import psycopg

HONORIFICS = {
    "ven", "ven.", "venerable",
    "bhante", "ayasma", "āyasmā", "thera", "therī",
    "bhikkhu", "bhikkhuni", "bhikkhunī",
}

_whitespace_re = re.compile(r"\s+")
_nonword_re = re.compile(r"[^a-z0-9\s]+")


################
# strip_diacritics
################
def strip_diacritics(s: str) -> str:
    # NFKD splits letters/diacritics; drop combining marks
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

#################
# normalize_mention
#################
def normalize_mention(s: str) -> str:
    if s is None:
        return ""
    s = s.strip().lower()
    s = strip_diacritics(s)

    # normalize apostrophes / punctuation to spaces then drop leftovers
    s = s.replace("’", "'").replace("–", "-").replace("—", "-")
    s = re.sub(r"[._\-/,;:()\[\]{}\"'“”]", " ", s)
    s = _nonword_re.sub(" ", s)
    s = _whitespace_re.sub(" ", s).strip()

    if not s:
        return ""

    toks = [t for t in s.split(" ") if t and t not in HONORIFICS]
    return " ".join(toks)

##########################
# build_alias_index
#########################
def build_alias_index(rows) -> tuple[
    Dict[Tuple[str, str], int],
    Dict[Tuple[str, str], List[int]]
]:
    """
    rows: iterable of dict-like with keys:
      entity_id (int), entity_type (str), alias_raw (str), alias_norm (str)
    """
    index: Dict[Tuple[str, str], int] = {}
    collisions: Dict[Tuple[str, str], List[int]] = defaultdict(list)

    for r in rows:
        etype = r["entity_type"]
        eid = int(r["entity_id"])

        # Normalize whatever we got
        raw_candidates = [r.get("alias_raw"), r.get("alias_norm")]
        for cand in raw_candidates:
            if not cand:
                continue
            key_norm = normalize_mention(str(cand))
            if not key_norm:
                continue

            key = (etype, key_norm)
            if key not in index:
                index[key] = eid
            else:
                if index[key] != eid:
                    # record collision (keep all ids)
                    existing = index[key]
                    if existing not in collisions[key]:
                        collisions[key].append(existing)
                    if eid not in collisions[key]:
                        collisions[key].append(eid)

    return index, collisions



#########################
##  lod_alias_index_pg
##  Big Ugly SQL
#########################
def load_alias_index_pg(dsn: str):
    ALIAS_SQL = """
        WITH base AS (
            SELECT
                e.id AS entity_id,
                e.entity_type,
                e.canonical AS alias_raw,
                e.normalized AS alias_norm
            FROM ati_entities e

            UNION ALL

            SELECT
                e.id AS entity_id,
                e.entity_type,
                e.normalized AS alias_raw,
                e.normalized AS alias_norm
            FROM ati_entities e

            UNION ALL

            SELECT
                ea.entity_id,
                e.entity_type,
                ea.alias AS alias_raw,
                COALESCE(ea.normalized, ea.alias) AS alias_norm
            FROM ati_entity_aliases ea
            JOIN ati_entities e ON e.id = ea.entity_id
        )
        SELECT entity_id, entity_type, alias_raw, alias_norm
        FROM base
        WHERE alias_norm IS NOT NULL
        AND btrim(alias_norm) <> '';
        """
    with psycopg.connect(dsn) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(ALIAS_SQL)
            rows = cur.fetchall()

    alias_index, collisions = build_alias_index(rows)
    return alias_index, collisions


##########################
# resolve_span
##########################
def resolve_span(
    alias_index: Dict[Tuple[str, str], int],
    collisions: Dict[Tuple[str, str], List[int]],
    entity_type: str,
    surface: Optional[str],
) -> Dict[str, Any]:
    """
    Resolve a span surface form to a canonical entity_id.

    Returns a dict:
      {
        "status": "resolved" | "ambiguous" | "unresolved" | "invalid",
        "entity_id": int | None,
        "normalized": str,
        "candidates": list[int],
      }
    """
    normalized = normalize_mention(surface or "")
    if entity_type not in {"PERSON", "GPE", "LOC", "NORP"}:
        return {
            "status": "invalid",
            "entity_id": None,
            "normalized": normalized,
            "candidates": [],
        }
    if not normalized:
        return {
            "status": "unresolved",
            "entity_id": None,
            "normalized": normalized,
            "candidates": [],
        }

    key = (entity_type, normalized)
    if key in collisions:
        return {
            "status": "ambiguous",
            "entity_id": None,
            "normalized": normalized,
            "candidates": collisions[key],
        }

    entity_id = alias_index.get(key)
    if entity_id is None:
        return {
            "status": "unresolved",
            "entity_id": None,
            "normalized": normalized,
            "candidates": [],
        }

    return {
        "status": "resolved",
        "entity_id": entity_id,
        "normalized": normalized,
        "candidates": [entity_id],
    }
