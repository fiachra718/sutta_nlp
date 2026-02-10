#!/usr/bin/env python3
import argparse
import json
import re
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import psycopg
from bs4 import BeautifulSoup, Tag


DEFAULT_ROOT = Path("/Users/alee/Downloads/ati/tipitaka")
DEFAULT_DSN = "postgresql://localhost/tipitaka?user=alee"
DEFAULT_OUT = Path("graph/data/ati_related_links.jsonl")

SKIP_FILE_PATTERNS = (
    "index.html",
    "translators.html",
    "sutta.html",
    "renumber*.html",
    "mil_utf8.html",
    "mil.html",
    "miln.html",
    "wheel*.html",
)

TARGET_SKIP_NAMES = {
    "index.html",
    "random-sutta.html",
    "random-article.html",
    "abbrev.html",
    "glossary.html",
    "help.html",
    "sutta.html",
    "translators.html",
}

SUTTA_NAME_RE = re.compile(
    r"^(?:an|sn|mn|dn|kn|ud|iti|snp|dhp|khp|thag|thig|vin|ab)[\w.\-]*\.html$",
    re.IGNORECASE,
)

RELATED_HINTS = (
    "see also",
    "also appears",
    "parallel",
    "related",
    "cf.",
    "compare",
)

CANON_REF_RE = re.compile(r"\b(?:AN|SN|MN|DN|Ud|Iti|Sn|Dhp)\s+\d+(?:\.\d+)?\b")


@dataclass(frozen=True)
class RelatedLink:
    from_identifier: str
    from_path: str
    to_identifier: str
    to_href: str
    to_ref_label: str
    source_kind: str
    confidence: float
    context: str

    def as_dict(self) -> dict:
        return {
            "from_identifier": self.from_identifier,
            "from_path": self.from_path,
            "to_identifier": self.to_identifier,
            "to_href": self.to_href,
            "to_ref_label": self.to_ref_label,
            "source_kind": self.source_kind,
            "confidence": self.confidence,
            "context": self.context,
        }


def textify(node) -> str:
    if node is None:
        return ""
    if hasattr(node, "get_text"):
        txt = node.get_text(" ", strip=True)
    else:
        txt = str(node)
    return re.sub(r"\s+", " ", txt).strip()


def is_note_container(tag: Tag) -> bool:
    if tag.name not in {"div", "section"}:
        return False
    classes = tag.get("class") or []
    if any(isinstance(c, str) and c.lower() == "notes" for c in classes):
        return True
    tag_id = tag.get("id")
    return isinstance(tag_id, str) and tag_id.lower() == "notes"


def is_within_notes(anchor: Tag) -> bool:
    cur = anchor
    while isinstance(cur, Tag):
        if is_note_container(cur):
            return True
        parent = cur.parent
        cur = parent if isinstance(parent, Tag) else None
    return False


def href_to_local_target(from_file: Path, href: str, root_dir: Path) -> tuple[str, str] | None:
    parsed = urlparse(href)
    if parsed.scheme or parsed.netloc:
        return None

    raw_path = (parsed.path or "").strip()
    if not raw_path:
        return None

    if not raw_path.lower().endswith(".html"):
        return None

    resolved = (from_file.parent / raw_path).resolve()
    if not resolved.exists():
        return None

    # Keep only ATI-local links under root parent.
    try:
        resolved.relative_to(root_dir.parent)
    except ValueError:
        return None

    if "sltp" in (p.lower() for p in resolved.parts):
        return None

    target_identifier = resolved.name
    if target_identifier.lower() in TARGET_SKIP_NAMES:
        return None
    if not SUTTA_NAME_RE.match(target_identifier):
        return None

    normalized_href = raw_path + (f"#{parsed.fragment}" if parsed.fragment else "")
    return target_identifier, normalized_href


def anchor_source_kind(anchor: Tag) -> tuple[str, float]:
    parent_text = textify(anchor.parent).lower() if isinstance(anchor.parent, Tag) else ""

    if parent_text.startswith("see also"):
        return "see_also_anchor", 1.0
    if any(hint in parent_text for hint in ("also appears", "parallel", "compare", "cf.")):
        return "parallel_anchor", 0.95

    if is_within_notes(anchor):
        note_parent = anchor.find_parent(["p", "li", "dd", "dt"])
        note_text = textify(note_parent).lower() if isinstance(note_parent, Tag) else parent_text
        if any(h in note_text for h in RELATED_HINTS):
            return "note_related_anchor", 0.9
        return "note_anchor", 0.75

    # Heading-based "See also" section fallback.
    heading = anchor.find_previous(re.compile(r"^h[1-6]$"))
    if isinstance(heading, Tag):
        label = textify(heading).lower()
        if label == "see also":
            return "see_also_section_anchor", 0.95

    return "body_anchor", 0.6


def iter_html_files(root_dir: Path) -> Iterable[Path]:
    for path in root_dir.rglob("*.html"):
        name = path.name.lower()
        if any(fnmatch(name, pat) for pat in SKIP_FILE_PATTERNS):
            continue
        parts = [p.lower() for p in path.parts]
        if "sltp" in parts:
            continue
        yield path


def extract_related_from_file(path: Path, root_dir: Path) -> list[RelatedLink]:
    raw = path.read_bytes()
    soup = BeautifulSoup(raw, "html.parser")
    from_identifier = path.name
    from_path = str(path)

    links: list[RelatedLink] = []
    seen: set[tuple] = set()
    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        target = href_to_local_target(path, href, root_dir)
        if not target:
            continue

        to_identifier, normalized_href = target
        if to_identifier == from_identifier:
            continue

        source_kind, confidence = anchor_source_kind(anchor)
        context_tag = anchor.find_parent(["p", "li", "dd", "dt", "div", "section"])
        context = textify(context_tag)[:600] if isinstance(context_tag, Tag) else ""
        context_l = context.lower()
        looks_related = any(h in context_l for h in RELATED_HINTS) or bool(CANON_REF_RE.search(context))
        if source_kind == "body_anchor" and not looks_related:
            continue
        label = textify(anchor)
        key = (from_identifier, to_identifier, normalized_href, label, source_kind)
        if key in seen:
            continue
        seen.add(key)

        links.append(
            RelatedLink(
                from_identifier=from_identifier,
                from_path=from_path,
                to_identifier=to_identifier,
                to_href=normalized_href,
                to_ref_label=label,
                source_kind=source_kind,
                confidence=confidence,
                context=context,
            )
        )
    return links


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ati_related_links (
  id BIGSERIAL PRIMARY KEY,
  from_identifier TEXT NOT NULL,
  from_path TEXT NOT NULL,
  to_identifier TEXT NOT NULL,
  to_href TEXT NOT NULL DEFAULT '',
  to_ref_label TEXT NOT NULL DEFAULT '',
  source_kind TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 0.5,
  context TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ati_related_links_uni
ON ati_related_links (from_identifier, to_identifier, to_href, to_ref_label, source_kind);
"""

INSERT_SQL = """
INSERT INTO ati_related_links
  (from_identifier, from_path, to_identifier, to_href, to_ref_label, source_kind, confidence, context)
VALUES
  (%(from_identifier)s, %(from_path)s, %(to_identifier)s, %(to_href)s, %(to_ref_label)s, %(source_kind)s, %(confidence)s, %(context)s)
ON CONFLICT DO NOTHING;
"""


def write_jsonl(records: list[RelatedLink], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec.as_dict(), ensure_ascii=False) + "\n")


def load_into_pg(dsn: str, records: list[RelatedLink], create_table: bool) -> int:
    inserted = 0
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            if create_table:
                cur.execute(CREATE_TABLE_SQL)
            for rec in records:
                cur.execute(INSERT_SQL, rec.as_dict())
                inserted += cur.rowcount
    return inserted


def main():
    parser = argparse.ArgumentParser(description="Extract ATI related links from HTML files.")
    parser.add_argument("--root", default=str(DEFAULT_ROOT), help="Path to ATI tipitaka HTML root.")
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="Output JSONL file.")
    parser.add_argument("--max-files", type=int, default=0, help="Limit number of files for testing (0 = all).")
    parser.add_argument("--dsn", default="", help="Optional Postgres DSN to load results.")
    parser.add_argument("--create-table", action="store_true", help="Create ati_related_links table/index before insert.")
    args = parser.parse_args()

    root_dir = Path(args.root).expanduser().resolve()
    out_path = Path(args.out)

    records: list[RelatedLink] = []
    scanned = 0
    for html_path in iter_html_files(root_dir):
        scanned += 1
        records.extend(extract_related_from_file(html_path, root_dir))
        if args.max_files and scanned >= args.max_files:
            break

    write_jsonl(records, out_path)
    print(f"Scanned files: {scanned}")
    print(f"Extracted links: {len(records)}")
    print(f"Wrote JSONL: {out_path}")

    if args.dsn:
        inserted = load_into_pg(args.dsn, records, args.create_table)
        print(f"Inserted into Postgres: {inserted}")


if __name__ == "__main__":
    main()
