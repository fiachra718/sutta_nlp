from pathlib import Path
from typing import Iterator, Union, Dict, List
from bs4 import BeautifulSoup, Comment
import re
from dataclasses import dataclass
import psycopg2
import html

_KEY_RE = re.compile(r'^\s*\[(?P<key>[^\]]+)\]\s*=\s*(?P<rest>.*)$')
_BRACE_RE = re.compile(r'\{(.*?)\}')

MetaVal = Union[str, List[str]]
MetaDict = Dict[str, MetaVal]


@dataclass
class Doc:
    identifier: str | None # identifier to ATI
    nikaya: str | None   # e.g. "mn", missing for type = vinaya or abhidhamma
    doc_type: str | None   # e.g.  sutta, vinaya, abhidhamma
    number: str | None          # e.g. "10"
    title: str | None
    subtitle: str | None
    translator: str | None
    license: str | None
    source: str | None
    copyright_owner: str | None
    copyright_year: str | None
    alternate_translations: str| None
    raw_text: str            # main body text (normalized)
    raw_path: str        # filesystem source (for traceability)


def load_to_postgres(conn_str: str, glob_pattern: str) -> None:
    conn = psycopg2.connect(conn_str)
    try:
        with conn, conn.cursor() as cur:
            for doc in iter_docs(glob_pattern):
                try:
                    cur.execute(INSERT, vars(doc))  # dataclass → dict via __dict__/vars
                except psycopg2.Error as e:
                    print("error {}, for document at {}".format(e.pgerror, str(glob_pattern)))
                    continue
    finally:
        conn.close()


def parse_metadata(soup: BeautifulSoup) -> MetaDict:
    # get the comments
    # find the comments that start with meta_header_str
    # build up metadata dict
    comments = soup.find_all(string=lambda t: isinstance(t, Comment))
    meta_comment = next(
        (c for c in comments if re.search(r'atidoc metadata', c, re.I)),
        None
    )
    if not meta_comment:
        return {}
    
    # go through each line and look for the pattern:
    # [KEY]={v1, v2, v3, ...}
    lines = [ln.strip() for ln in meta_comment.splitlines()]
    data: Dict[str, List[str]] = {}
    current_key: str | None = None

    for raw in lines:
        if not raw:
            continue
        if raw.lower().startswith("end atidoc metadata"):
            break
        if raw.lower().startswith("begin atidoc metadata"):
            continue
    
        # found '=', is this a new key?
        m = _KEY_RE.match(raw)
        if m:
            current_key = m.group("key").strip().upper()
            data.setdefault(current_key, [])
            # inline {...} on same line?
            rest = m.group("rest")
            if rest:
                for val in _BRACE_RE.findall(rest):
                    data[current_key].append(html.unescape(val.strip()))
            continue
        # line continues and we have no vals from above
        # this handles derived license data
        if current_key and raw.startswith("{"):
            for val in _BRACE_RE.findall(raw):
                data[current_key].append(val.strip())
            continue
    
    # deep copy data dict to a MetaDict
    out: MetaDict = {}
    for k, vs in data.items():
        if not vs:
            out[k] = ""
        elif len(vs) == 1:
            out[k] = vs[0]
        else:
            out[k] = vs
    return out

def extract_main_text(soup: BeautifulSoup) -> str:
    main = soup.find(id="COPYRIGHTED_TEXT_CHUNK")
    if not main:
        # Fallback: try common content containers
        main = soup.select_one("main, article, #content, #main") or soup.body or soup
    # Drop boilerplate inside that region
    for tag in main.select("nav, aside, header, footer, script, style, noscript, form, iframe"):
        tag.decompose()
    text = main.get_text(separator=" ", strip=True)
    # Normalize whitespace
    return re.sub(r"\s+", " ", text)
     

def doc_from_file(path: Path) -> Doc:
    html = path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")
    meta = parse_metadata(soup)
    text = extract_main_text(soup)

    # Derive identifiers from filename and/or metadata
    # Examples: mn.010.than.html → nikaya="mn", number="010"
    stem = path.stem  # "mn.010.than"
    parts = stem.split(".")
    number = parts[1].lstrip("0") if len(parts) > 1 else ""
    alternative_translations = meta.get("ALT_TRANS")
    if isinstance(alternative_translations, list):
        alternative_translations = ','.join(alternative_translations)

    # remember, the keys are ALL UPPERCASE
    return Doc(
        identifier=meta.get("PATH_FETCHDOC"),
        nikaya=meta.get("NIKAYA"),
        doc_type=meta.get("TYPE"),
        number=number or "",
        title=meta.get("MY_TITLE"),
        subtitle=meta.get("SUBTITLE"),
        translator=meta.get("AUTHOR"),
        license=meta.get("LICENSE"),
        source=meta.get("SOURCE"),
        copyright_owner=meta.get("SOURCE_COPYRIGHT_OWNER"),
        copyright_year=meta.get("SOURCE_COPYRIGHT_YEAR"),
        alternate_translations=alternative_translations,
        raw_text=text,
        raw_path=str(path),
    )

def iter_docs(glob_pattern: str) -> Iterator[Doc]:
    """
    Iterate all files matching the glob pattern. Supports patterns like:
    '/Users/alee/Downloads/ati/tipitaka/mn/mn.*.than.html'
    '/Users/alee/Downloads/ati/tipitaka/mn/**/mn.*.than.html'
    """
    p = Path(glob_pattern).expanduser()

    parent, pat = p.parent, p.name
    for path in parent.glob(pat):
        if path.is_file():
            yield doc_from_file(path)

if __name__ == "__main__":
    patterns = [
        # "mn/mn.*..html"
        # "mn/mn.*.*.html", 
        "sn/*/sn*.html",
        # "an/an*/an*.html",
        # "an/an01/an01*.html",
        # "an/an03/an03*.html",
        # "an/an05/an05*.html",
        # "an/an07/an07*.html",
        # "an/an09/an09*.html",
        # "an/an11/an11*.html",
        # "an/an02/an02*.html",
		# "an/an04/an04*.html",		
        # "an/an06/an06*.hml",
		# "an/an08/an08*.html",
		# "an/an10/an10*.html"
        # "dn/dn*.html"
        # "kn/dhp/dhp*.html",
        # "kn/iti/iti*.html",
        # "kn/khp/khp*.html",
        # "kn/miln/miln*.html",
        # "kn/ud/ud*.html",
        # "kn/snp/snp*.html",
        # "kn/thag/thag*.html",
        # "kn/thig/thig*.html"
    ]

    ROOT = Path("/Users/alee/Downloads/ati/tipitaka/")
    CONN = "postgresql://alee:postgres@localhost:5432/tipitaka"

    INSERT = """
        INSERT INTO suttas (
            identifier,
            nikaya, 
            doc_type,
            number, 
            title, 
            subtitle,
            translator, 
            license, 
            source, 
            copyright_owner, 
            copyright_year,
            alternate_translations,
            raw_text, 
            raw_path)
        VALUES (%(identifier)s,
            %(nikaya)s,
            %(doc_type)s, 
            %(number)s, 
            %(title)s,
            %(subtitle)s, 
            %(translator)s, 
            %(license)s, 
            %(source)s, 
            %(copyright_owner)s,
            %(copyright_year)s,
            %(alternate_translations)s,  
            %(raw_text)s, %(raw_path)s)
    """

    # for pat in patterns:
    #     glob = ROOT.joinpath(pat)
    #     print(glob)
    #     load_to_postgres(CONN, glob)
    BASE = Path("/Users/alee/Downloads/ati/tipitaka/")  # <- set this correctly

    paths = list(BASE.glob("sn/sn[0-5][0-9]/sn[0-5][0-9].*.html"))
    print(len(paths), "files")
    for p in paths:
        load_to_postgres(CONN, p)
