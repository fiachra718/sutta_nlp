#!/usr/bin/env python3
"""Load/cross-link Pali Names crawl output into Postgres.

Input: pages.jsonl from crawl_pali_names.py
Output tables: pali_names_pages, pali_names_entries, pali_names_see_also
"""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import psycopg


DEFAULT_DSN = "postgresql://localhost/tipitaka?user=alee"


ENTRY_HEAD_RE = re.compile(r"^\s*(\d+)\.\s+(.+?)\.\s*-\s*(.*)$", re.DOTALL)
ENTRY_SPLIT_RE = re.compile(r"(?=(?:^|\s)(\d+)\.\s+)", re.DOTALL)
SEE_RE = re.compile(r"\b[Ss]ee(?:\s+also)?\s+([^.;:\n]{1,180})")
PTS_REF_RE = re.compile(
    r"\b(?:D|M|S|A|J|Vin|Dhp|Ud|Thag|Thig)\.[ivxIVX]+\.\d+(?:\s*f{1,2}\.)?(?:\s*,\s*\d+(?:\s*f{1,2}\.)?)*"
)


def norm(s: str) -> str:
    t = unicodedata.normalize("NFD", (s or "").strip())
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    t = unicodedata.normalize("NFC", t).lower().strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\s+[.,;:!?]+$", "", t)
    return t


def clean_text(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip())
    return s


def _is_index_like_page(path: str, title: str, text: str) -> bool:
    fname = Path(path).name.lower()
    if re.fullmatch(r"[a-z]\.html", fname):
        return True
    if fname in {"dic_idx.html", "abreviations.htm", "abbreviations.htm"}:
        return True
    t = (title or "").strip().lower()
    if t in {"a", "b", "c", "d", "e", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "r", "s", "t", "u", "v", "w", "y", "z"}:
        return True
    txt = (text or "").lower()
    if "pāli proper names —" in txt and "dictionary of pāli proper names" in txt:
        return True
    return False


def _single_entry_body(page_title: str, page_text: str) -> str:
    text = clean_text(page_text)
    if not text:
        return ""

    # Remove common local-site chrome.
    text = re.sub(
        r"^\s*[A-Z](?:\s+[A-Z]){6,}\s+\?\s+Dictionary of Pāli Proper Names\s+•\s+G\.P\.\s*Malalasekera\s+Page last updated on [^.]{1,60}?\d{4}\s+",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"^Dictionary of Pāli Proper Names\s+•\s+G\.P\.\s*Malalasekera\s+Page last updated on [^.]{1,60}?\d{4}\s+",
        "",
        text,
        flags=re.IGNORECASE,
    )

    title = clean_text(page_title)
    if title:
        # Many pages repeat title at start before real body.
        text = re.sub(rf"^\s*{re.escape(title)}\s+", "", text, count=1, flags=re.IGNORECASE)
        # Sometimes title appears again immediately before content.
        text = re.sub(rf"^\s*{re.escape(title)}\s+", "", text, count=1, flags=re.IGNORECASE)

    return clean_text(text)


def parse_entries(page_text: str, page_title: str = "", page_path: str = "") -> list[tuple[int, str, str]]:
    """Return list of (ord, entry_name, entry_text) parsed from crawled plain text."""
    text = clean_text(page_text)
    if not text:
        return []

    # split into likely chunks beginning with N.
    starts = [m.start() for m in re.finditer(r"(?:^|\s)(\d+)\.\s+", text)]
    if not starts:
        return []

    chunks: list[str] = []
    for i, st in enumerate(starts):
        en = starts[i + 1] if i + 1 < len(starts) else len(text)
        chunks.append(text[st:en].strip())

    parsed: list[tuple[int, str, str]] = []
    for ch in chunks:
        m = ENTRY_HEAD_RE.match(ch)
        if not m:
            continue
        ord_s, name, body = m.group(1), clean_text(m.group(2)), clean_text(m.group(3))
        if not name or len(name) > 220:
            continue
        if not re.search(r"[A-Za-z]", name):
            continue
        try:
            entry_ord = int(ord_s)
        except ValueError:
            continue
        parsed.append((entry_ord, name, body))

    if parsed:
        return parsed

    # Fallback for local DPPN single-entry pages.
    if _is_index_like_page(page_path, page_title, text):
        return []
    title = clean_text(page_title)
    if not title:
        return []
    body = _single_entry_body(title, text)
    if not body:
        return []
    return [(1, title, body)]


def extract_see_also(entry_text: str) -> list[str]:
    refs: list[str] = []
    for m in SEE_RE.finditer(entry_text or ""):
        raw = clean_text(m.group(1))
        # split small lists: "Foo and Bar", "Foo, Bar"
        parts = re.split(r"\s+and\s+|,\s*", raw)
        for p in parts:
            p = re.sub(r"^[\"'(\[]+|[\"')\].]+$", "", p.strip())
            if p and re.search(r"[A-Za-z]", p):
                refs.append(p)
    # dedupe preserve order
    out: list[str] = []
    seen = set()
    for r in refs:
        k = norm(r)
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def extract_raw_citations(entry_text: str) -> list[str]:
    refs: list[str] = []
    for m in PTS_REF_RE.finditer(entry_text or ""):
        refs.append(clean_text(m.group(0)))
    out: list[str] = []
    seen = set()
    for r in refs:
        k = norm(r)
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


@dataclass
class Page:
    url: str
    path: str
    title: str
    text: str
    text_sha256: str | None
    fetched_at_utc: str | None
    source_run: str


def load_pages(jsonl_path: Path, source_run: str) -> list[Page]:
    pages: list[Page] = []
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            url = rec.get("url")
            if not url:
                continue
            path = urlparse(url).path
            pages.append(
                Page(
                    url=url,
                    path=path,
                    title=(rec.get("title") or "").strip(),
                    text=(rec.get("text") or "").strip(),
                    text_sha256=rec.get("text_sha256"),
                    fetched_at_utc=rec.get("fetched_at_utc"),
                    source_run=source_run,
                )
            )
    return pages


UPSERT_PAGE_SQL = """
INSERT INTO pali_names_pages
(url, path, title, text, text_sha256, fetched_at_utc, visited_at, source_run)
VALUES
(%(url)s, %(path)s, %(title)s, %(text)s, %(text_sha256)s, %(fetched_at_utc)s, now(), %(source_run)s)
ON CONFLICT (url) DO UPDATE SET
  title = EXCLUDED.title,
  text = EXCLUDED.text,
  text_sha256 = EXCLUDED.text_sha256,
  fetched_at_utc = EXCLUDED.fetched_at_utc,
  visited_at = now(),
  source_run = EXCLUDED.source_run,
  updated_at = now()
RETURNING id;
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages-jsonl", required=True)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--source-run", default="")
    ap.add_argument("--rebuild", action="store_true", help="Delete existing entries/see-also before reload.")
    args = ap.parse_args()

    pages_jsonl = Path(args.pages_jsonl).resolve()
    source_run = args.source_run or pages_jsonl.parent.name
    pages = load_pages(pages_jsonl, source_run=source_run)
    if not pages:
        raise SystemExit(f"No pages in {pages_jsonl}")

    with psycopg.connect(args.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            if args.rebuild:
                cur.execute(
                    "TRUNCATE TABLE pali_names_citations, pali_names_see_also, pali_names_entries RESTART IDENTITY"
                )

            page_id_by_url: dict[str, int] = {}
            title_norm_to_pages: dict[str, list[tuple[int, str]]] = {}

            for p in pages:
                cur.execute(
                    UPSERT_PAGE_SQL,
                    {
                        "url": p.url,
                        "path": p.path,
                        "title": p.title or None,
                        "text": p.text,
                        "text_sha256": p.text_sha256,
                        "fetched_at_utc": p.fetched_at_utc,
                        "source_run": p.source_run,
                    },
                )
                page_id = cur.fetchone()[0]
                page_id_by_url[p.url] = page_id
                tn = norm(p.title)
                if tn:
                    title_norm_to_pages.setdefault(tn, []).append((page_id, p.url))

            inserted_entries = 0
            inserted_refs = 0
            inserted_citations = 0

            for p in pages:
                page_id = page_id_by_url[p.url]
                entries = parse_entries(p.text, page_title=p.title, page_path=p.path)
                for entry_ord, entry_name, entry_text in entries:
                    cur.execute(
                        """
                        INSERT INTO pali_names_entries
                        (page_id, entry_ord, entry_name, entry_name_norm, entry_text)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (page_id, entry_ord) DO UPDATE SET
                          entry_name = EXCLUDED.entry_name,
                          entry_name_norm = EXCLUDED.entry_name_norm,
                          entry_text = EXCLUDED.entry_text
                        RETURNING id
                        """,
                        (page_id, entry_ord, entry_name, norm(entry_name), entry_text),
                    )
                    entry_id = cur.fetchone()[0]
                    inserted_entries += 1

                    refs = extract_see_also(entry_text)
                    for raw_ref in refs:
                        nref = norm(raw_ref)
                        resolved_page_id = None
                        resolved_url = None
                        method = None

                        candidates = title_norm_to_pages.get(nref, [])
                        if len(candidates) == 1:
                            resolved_page_id, resolved_url = candidates[0]
                            method = "title_exact"
                        else:
                            # fallback: unique title prefix match
                            prefix = [
                                c for tnorm, pages2 in title_norm_to_pages.items()
                                if tnorm.startswith(nref + " ") for c in pages2
                            ]
                            if len(prefix) == 1:
                                resolved_page_id, resolved_url = prefix[0]
                                method = "title_prefix"

                        cur.execute(
                            """
                            INSERT INTO pali_names_see_also
                            (entry_id, raw_ref, raw_ref_norm, resolved_page_id, resolved_url, resolution_method)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (entry_id, raw_ref_norm) DO UPDATE SET
                              raw_ref = EXCLUDED.raw_ref,
                              resolved_page_id = EXCLUDED.resolved_page_id,
                              resolved_url = EXCLUDED.resolved_url,
                              resolution_method = EXCLUDED.resolution_method
                            """,
                            (entry_id, raw_ref, nref, resolved_page_id, resolved_url, method),
                        )
                        inserted_refs += 1

                    citations = extract_raw_citations(entry_text)
                    for raw_citation in citations:
                        cur.execute(
                            """
                            INSERT INTO pali_names_citations
                            (entry_id, raw_citation, citation_norm)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (entry_id, citation_norm) DO UPDATE SET
                              raw_citation = EXCLUDED.raw_citation
                            """,
                            (entry_id, raw_citation, norm(raw_citation)),
                        )
                        inserted_citations += 1

            cur.execute("SELECT count(*) FROM pali_names_pages")
            pages_total = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM pali_names_entries")
            entries_total = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM pali_names_see_also")
            refs_total = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM pali_names_see_also WHERE resolved_page_id IS NULL")
            unresolved = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM pali_names_citations")
            citations_total = cur.fetchone()[0]

    print(
        f"Loaded pages={len(pages)} | pages_total={pages_total} | "
        f"entries_total={entries_total} | see_also_total={refs_total} | "
        f"unresolved_see_also={unresolved} | citations_total={citations_total}"
    )


if __name__ == "__main__":
    main()
