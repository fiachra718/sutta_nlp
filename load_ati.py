#!/usr/bin/env python3
import html
import json
import os
import re
import unicodedata
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Iterator

import psycopg
from bs4 import BeautifulSoup, Tag

# ======================
# CONFIG
# ======================
ROOT_DIR = Path("/Users/alee/Downloads/ati/tipitaka")
DB_DSN   = "postgresql://localhost/tipitaka?user=alee"

# Walk only these subdirectories under ROOT_DIR
START_SUBDIRS = ("an", "kn", "dn", "mn", "sn")

# Skip these filenames/patterns anywhere
SKIP_FILE_PATTERNS = (
    "index.html",
    "translators.html",
    "sutta.html",
    "renumber*.html",
    "mil_utf8.html",
    "mil.html",
    "miln.html",
)

# ======================
# Helpers
# ======================
def deent(s: str | None) -> str:
    """HTML entity decode + NFC normalize (ā/ī/ñ etc.)."""
    return unicodedata.normalize("NFC", html.unescape((s or "").strip()))

def textify(node):
    """Get readable text, collapsing whitespace."""
    if node is None:
        return ""
    txt = node.get_text(" ", strip=True) if hasattr(node, "get_text") else str(node)
    return re.sub(r"\s+", " ", txt).strip()

META_LINE = re.compile(
    r'^\s*(?:<!--\s*)?\[(?P<key>[A-Z0-9_]+)\]\s*=\s*(?P<blocks>(?:\{.*?\}\s*)*)(?:\s*-->)?\s*$',
    re.MULTILINE | re.DOTALL
)
BRACED_BLOCK = re.compile(r'\{(.*?)\}', re.DOTALL)

NUM_PREFIX = re.compile(r'^\s*\(?(\d+(?:\.\d+)*)\)?[\s:,-]*', re.UNICODE)

NIKAYA_MAP = {
    "dn": "DN", "mn": "MN", "sn": "SN", "an": "AN",
    "kn": "KN", "khp": "KN", "snp": "KN", "dhp": "KN", "ud": "KN", "iti": "KN",
    "vin": "Vinaya", "ab": "Abhidhamma"
}

def parse_metadata_text(full_html_text: str) -> dict:
    """
    Parse the [KEY]={...}{...} style header blocks (top of ATI HTML).
    Returns dict with keys as found (upper in KEY) and values de-entitized.
    """
    meta = {}
    for m in META_LINE.finditer(full_html_text):
        key = m.group("key").strip()
        blocks = m.group("blocks") or ""
        values = BRACED_BLOCK.findall(blocks)
        meta[key] = deent("\n".join(v.strip() for v in values if v.strip()))
    return meta

def infer_identifier(html_path: Path) -> str:
    # Keep basename exactly (e.g., 'mn.020.than.html')
    return html_path.name

def infer_nikaya_and_book_number(html_path: Path, meta: dict):
    nikaya = meta.get("NIKAYA_ABBREV") or meta.get("NIKAYA")
    number = meta.get("NUMBER")
    # Fall back to path inference
    parts = [p.lower() for p in html_path.parts]
    for token in ("dn", "mn", "sn", "an", "kn", "khp", "snp", "dhp", "ud", "iti", "vin", "ab"):
        if token in parts:
            nikaya = nikaya or NIKAYA_MAP.get(token, token.upper())
            break
    stem = html_path.stem  # e.g., 'sn01.010.olen'
    if not number:
        m = re.search(r'(\d+(?:\.\d+){0,2})', stem)
        if m:
            number = m.group(1)
    return nikaya, number

def infer_doc_type(html_path: Path, meta: dict):
    t = (meta.get("TYPE") or "").strip().lower()
    if t in {"sutta","vinaya","abhidhamma","commentary","other"}:
        return t
    parts = [p.lower() for p in html_path.parts]
    if "vin" in parts or "vinaya" in parts:
        return "vinaya"
    if "ab" in parts or "abhidhamma" in parts:
        return "abhidhamma"
    return "sutta"

def extract_alternative_translations(soup: BeautifulSoup):
    alts = []
    headings = soup.find_all(re.compile(r'^h[1-4]$', re.I))
    for h in headings:
        if "alternative" in h.get_text(" ", strip=True).lower():
            nxt = h.find_next_sibling()
            limit = 0
            while nxt and limit < 5:
                if nxt.name in ("ul","ol"):
                    for li in nxt.find_all("li", recursive=False):
                        txt = textify(li)
                        if txt:
                            alts.append(deent(txt))
                    break
                if nxt.name == "p":
                    txt = textify(nxt)
                    if txt:
                        alts.append(deent(txt))
                if nxt.name and nxt.name.startswith("h"):
                    break
                nxt = nxt.find_next_sibling()
                limit += 1
            break
    return alts

def extract_title_subtitle(soup: BeautifulSoup, meta: dict):
    title = meta.get("MY_TITLE") or meta.get("TITLE") or ""
    subtitle = meta.get("SUBTITLE") or meta.get("SUBTITLE2") or ""
    if not title:
        page_title = soup.find("title")
        if page_title:
            t = textify(page_title)
            t = re.sub(r'\s*\|\s*Access to Insight.*$', '', t)
            title = t.strip()
    return deent(title), deent(subtitle)

def extract_translator_and_copyright(soup: BeautifulSoup, meta: dict):
    translator = deent(meta.get("AUTHOR")) or None
    if meta.get("DERIVED_LICENSE_DATA"):
        copyright_text = meta["DERIVED_LICENSE_DATA"]
    else:
        parts = []
        lic = meta.get("LICENSE")
        year = meta.get("SOURCE_COPYRIGHT_YEAR")
        owner = meta.get("SOURCE_COPYRIGHT_OWNER")
        for s in (lic, year, owner):
            if s:
                parts.append(str(s))
        copyright_text = " | ".join(parts)
    if not copyright_text:
        cdiv = soup.find(id="COPYRIGHTED_TEXT_CHUNK")
        if cdiv:
            blob = textify(cdiv)
            m = re.search(r'(Creative Commons.*|©.*?\d{4}.*?$)', blob, re.IGNORECASE | re.MULTILINE)
            if m:
                copyright_text = m.group(1)
    return translator, deent(copyright_text or "")

def extract_vagga(meta: dict, html_path: Path):
    vagga = meta.get("SECTION") or meta.get("VAGGA") or ""
    if vagga:
        return vagga
    parts = [p for p in html_path.parts]
    try:
        idx = [p.lower() for p in parts].index("sn")
        if idx + 1 < len(parts):
            vagga = parts[idx + 1]  # 'sn01', etc.
    except ValueError:
        pass
    return vagga

def extract_verses(soup: BeautifulSoup):
    """
    Return [{"num": "1", "text": "..."}...]. Preserve explicit numbers; if missing, assign
    an auto-incrementing integer per chapter. De-entitize text.
    """
    verses = []
    root = soup.find(id="COPYRIGHTED_TEXT_CHUNK") or soup
    chapters = root.find_all("div", class_="chapter") or [root]

    for ch in chapters:
        auto_n = 0
        for p in ch.find_all("p", recursive=True):
            if isinstance(p, Tag) and _is_within_notes_section(p):
                continue

            txt = deent(textify(p))
            if not txt:
                continue
            if re.match(r"^See also\b", txt, flags=re.I):
                continue

            num = None
            # explicit number in shallow child spans/sup/a
            for span in p.find_all(["span", "sup", "a"], recursive=False):
                s = textify(span)
                if re.fullmatch(r"\d[\d.\-]*", s):
                    num = s
                    break

            # leading numeric prefix
            if not num:
                m = NUM_PREFIX.match(txt)
                if m:
                    num = m.group(1)
                    txt = txt[m.end():].strip()

            # assign auto-number if missing
            if not num:
                auto_n += 1
                num = str(auto_n)
            else:
                m0 = re.match(r"(\d+)", num)
                if m0:
                    try:
                        auto_n = int(m0.group(1))
                    except ValueError:
                        pass

            verses.append({"num": num, "text": txt})
    return verses

# -------- Notes extraction (DL + P + "See also …")
_WS = re.compile(r"\s+")

def _clean_note(t: str) -> str:
    t = re.sub(r"\s*\[?\s*back\s*\]?\s*$", "", t, flags=re.I)  # drop trailing “[back]”
    t = re.sub(r"\s*↑\s*$", "", t)                             # drop backlink arrow
    return deent(_WS.sub(" ", (t or "")).strip())

def extract_notes_from_soup(soup: BeautifulSoup) -> list[str]:
    """
    Collects:
      • Footnotes under <div class="notes"> including <dl><dt><a>n</a>.</dt><dd>text</dd></dl>
      • Paragraph/list notes under a 'Notes' heading
      • 'See also …' as a single synthetic note at the top
    """
    notes: list[str] = []

    for container in _iter_notes_containers(soup):
        notes.extend(_notes_from_container(container))

    if not notes:
        notes.extend(_notes_from_heading_section(soup))

    see_also = _see_also_note_from_document(soup)
    if see_also:
        notes.insert(0, see_also)

    return notes

def _iter_notes_containers(soup: BeautifulSoup) -> Iterator[Tag]:
    return soup.find_all(_is_note_container)

def _is_note_container(tag: Tag) -> bool:
    if tag.name not in {"div", "section"}:
        return False
    classes = tag.get("class") or []
    if any(isinstance(cls, str) and cls.lower() == "notes" for cls in classes):
        return True
    tag_id = tag.get("id")
    return isinstance(tag_id, str) and tag_id.lower() == "notes"

def _is_within_notes_section(tag: Tag) -> bool:
    current = tag
    while current:
        if _is_note_container(current):
            return True
        parent = current.parent
        current = parent if isinstance(parent, Tag) else None
    return False

def _notes_from_container(container: Tag) -> list[str]:
    collected: list[str] = []
    dls = container.find_all("dl", recursive=False)
    for dl in dls:
        dts = dl.find_all("dt", recursive=False)
        dds = dl.find_all("dd", recursive=False)
        for idx, dt in enumerate(dts):
            num = _extract_note_number(dt)
            body = _note_body_from_dt_dd(idx, dt, dds)
            if body:
                prefix = f"{num}. " if num else ""
                collected.append(prefix + body)

    if not collected:
        for li in container.find_all("li"):
            txt = _clean_note(textify(li))
            if txt:
                collected.append(txt)

    if not collected:
        for p in container.find_all("p", recursive=False):
            txt = _clean_note(textify(p))
            if txt:
                collected.append(txt)

    return collected

def _extract_note_number(dt: Tag) -> str | None:
    anchor = dt.find("a")
    if anchor:
        num = (anchor.get_text("", strip=True) or "").strip(".")
        if num:
            return num
    match = re.search(r"\d+", dt.get_text(" ", strip=True) or "")
    return match.group(0) if match else None

def _note_body_from_dt_dd(idx: int, dt: Tag, dds: list[Tag]) -> str | None:
    body_block = dds[idx] if idx < len(dds) else dt.find_next_sibling("dd")
    if not body_block:
        return None
    cleaned = _clean_note(textify(body_block))
    return cleaned or None

def _notes_from_heading_section(soup: BeautifulSoup) -> list[str]:
    for heading in soup.find_all(re.compile(r"^h[1-6]$")):
        label = (heading.get_text(" ", strip=True) or "").strip().lower()
        if label not in {"note", "notes", "end notes", "endnotes"}:
            continue
        notes: list[str] = []
        nxt = heading.find_next_sibling()
        steps = 0
        while nxt and steps < 10:
            if nxt.name in ("ol", "ul"):
                for li in nxt.find_all("li", recursive=False):
                    txt = _clean_note(textify(li))
                    if txt:
                        notes.append(txt)
                break
            if nxt.name == "p":
                txt = _clean_note(textify(nxt))
                if txt:
                    notes.append(txt)
            if nxt.name and nxt.name.startswith("h"):
                break
            nxt = nxt.find_next_sibling()
            steps += 1
        if notes:
            return notes
    return []

def _see_also_note_from_document(soup: BeautifulSoup) -> str | None:
    see_also_texts: list[str] = []
    for paragraph in soup.find_all("p"):
        text = (paragraph.get_text(" ", strip=True) or "").strip()
        if re.match(r"^See also\b", text, flags=re.I):
            see_also_texts.append(_clean_note(text))

    if not see_also_texts:
        for heading in soup.find_all(re.compile(r"^h[1-6]$")):
            if (heading.get_text(" ", strip=True) or "").strip().lower() != "see also":
                continue
            pieces: list[str] = []
            nxt = heading.find_next_sibling()
            steps = 0
            while nxt and steps < 10:
                if nxt.name in ("p", "ul", "ol"):
                    pieces.append(textify(nxt))
                if nxt.name and nxt.name.startswith("h"):
                    break
                nxt = nxt.find_next_sibling()
                steps += 1
            joined = _clean_note("; ".join(pieces))
            if joined:
                see_also_texts.append("See also: " + joined)
            break

    if not see_also_texts:
        return None

    merged = see_also_texts[0] if len(see_also_texts) == 1 else _clean_note("; ".join(see_also_texts))
    if not merged.lower().startswith("see also"):
        merged = "See also: " + merged
    return merged

# ======================
# Record extraction
# ======================
@dataclass(frozen=True)
class LoaderConfig:
    root_dir: Path
    db_dsn: str
    start_subdirs: tuple[str, ...]
    skip_file_patterns: tuple[str, ...]

@dataclass
class LoadStats:
    imported: int = 0
    skipped: int = 0

class AtiHtmlDocument:
    def __init__(self, html_path: Path, config: LoaderConfig):
        self.path = html_path
        self._config = config
        self._raw_bytes = html_path.read_bytes()
        self.raw_text = self._raw_bytes.decode("utf-8", "ignore")
        self.soup = BeautifulSoup(self._raw_bytes, "html.parser")
        self.meta = parse_metadata_text(self.raw_text)

    def build_record(self) -> dict:
        meta = self.meta
        identifier = infer_identifier(self.path)
        nikaya, book_number = infer_nikaya_and_book_number(self.path, meta)
        doc_type = infer_doc_type(self.path, meta)
        vagga = extract_vagga(meta, self.path)
        title, subtitle = extract_title_subtitle(self.soup, meta)
        translator, copyright_text = extract_translator_and_copyright(self.soup, meta)
        alternative_translations = extract_alternative_translations(self.soup)
        verses = extract_verses(self.soup)

        return {
            "identifier": identifier,
            "raw_path": self._raw_path(),
            "nikaya": nikaya,
            "vagga": vagga or None,
            "book_number": book_number or None,
            "doc_type": doc_type,
            "translator": translator or None,
            "copyright": copyright_text or None,
            "title": deent(title) or identifier,
            "subtitle": deent(subtitle) or None,
            "alternative_translations": alternative_translations,
            "verses": verses,
            "notes": meta.get("SUTTA_NOTE") or meta.get("EDITORS_NOTE") or None,
        }

    def collect_notes(self) -> list[str]:
        return extract_notes_from_soup(self.soup)

    def _raw_path(self) -> str:
        root_dir = self._config.root_dir
        if root_dir in self.path.parents:
            return str(self.path.relative_to(root_dir.parent))
        return str(self.path)

# ======================
# DB I/O
# ======================
UPSERT_SQL = """
INSERT INTO ati_suttas
(identifier, raw_path, nikaya, vagga, book_number, doc_type,
 translator, copyright, title, subtitle,
 alternative_translations, verses, notes)
VALUES
(%(identifier)s, %(raw_path)s, %(nikaya)s, %(vagga)s, %(book_number)s, %(doc_type)s,
 %(translator)s, %(copyright)s, %(title)s, %(subtitle)s,
 %(alternative_translations)s, %(verses)s, %(notes)s)
ON CONFLICT (identifier) DO UPDATE SET
  raw_path = EXCLUDED.raw_path,
  nikaya = EXCLUDED.nikaya,
  vagga = EXCLUDED.vagga,
  book_number = EXCLUDED.book_number,
  doc_type = EXCLUDED.doc_type,
  translator = EXCLUDED.translator,
  copyright = EXCLUDED.copyright,
  title = EXCLUDED.title,
  subtitle = EXCLUDED.subtitle,
  alternative_translations = EXCLUDED.alternative_translations,
  verses = EXCLUDED.verses,
  notes = EXCLUDED.notes,
  updated_at = now()
RETURNING id;
"""

def upsert_sutta(conn, rec: dict) -> int:
    with conn.cursor() as cur:
        cur.execute(
            UPSERT_SQL,
            {
                **rec,
                "alternative_translations": json.dumps(rec["alternative_translations"]),
                "verses": json.dumps(rec["verses"]),
            }
        )
        new_id = cur.fetchone()[0]
    return new_id

def upsert_page_notes(conn, sutta_id: int, notes: list[str]) -> int:
    """Insert notes for this sutta; skip duplicates of exact body."""
    if not notes:
        return 0
    inserted = 0
    with conn.cursor() as cur:
        for body in notes:
            cur.execute(
                "SELECT 1 FROM ati_notes WHERE sutta_id = %s AND body = %s LIMIT 1;",
                (sutta_id, body),
            )
            if cur.fetchone():
                continue
            cur.execute(
                "INSERT INTO ati_notes (sutta_id, body) VALUES (%s, %s);",
                (sutta_id, body),
            )
            inserted += 1
    return inserted

# ======================
# Main
# ======================
class AtiLoader:
    def __init__(self, config: LoaderConfig):
        self.config = config

    def run(self) -> LoadStats:
        stats = LoadStats()
        with psycopg.connect(self.config.db_dsn, autocommit=True) as conn:
            for subdir in self._iter_start_dirs():
                for path in self._iter_html_files(subdir, stats):
                    self._process_file(conn, path, stats)
        return stats

    def _iter_start_dirs(self) -> Iterator[Path]:
        for sub in self.config.start_subdirs:
            base = self.config.root_dir / sub
            if base.exists():
                yield base

    def _iter_html_files(self, base: Path, stats: LoadStats) -> Iterator[Path]:
        for root, _dirs, files in os.walk(base):
            if "sltp" in (part.lower() for part in Path(root).parts):
                continue

            for fn in files:
                if not fn.lower().endswith(".html"):
                    continue
                if any(fnmatch(fn.lower(), pat) for pat in self.config.skip_file_patterns):
                    stats.skipped += 1
                    continue
                yield Path(root) / fn

    def _process_file(self, conn, html_path: Path, stats: LoadStats) -> None:
        try:
            document = AtiHtmlDocument(html_path, self.config)
            record = document.build_record()
            sutta_id = upsert_sutta(conn, record)
            notes = document.collect_notes()
            upsert_page_notes(conn, sutta_id, notes)

            stats.imported += 1
            if stats.imported % 50 == 0:
                print(f"...{stats.imported} imported")
        except Exception as exc:
            stats.skipped += 1
            print(f"[SKIP] {html_path}: {exc}")

def main():
    config = LoaderConfig(
        root_dir=ROOT_DIR,
        db_dsn=DB_DSN,
        start_subdirs=START_SUBDIRS,
        skip_file_patterns=SKIP_FILE_PATTERNS,
    )
    loader = AtiLoader(config)
    stats = loader.run()
    print(f"Done. Imported: {stats.imported}, Skipped: {stats.skipped}")

if __name__ == "__main__":
    main()
