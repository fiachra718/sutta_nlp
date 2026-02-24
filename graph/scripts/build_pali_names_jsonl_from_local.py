#!/usr/bin/env python3
"""Build pages.jsonl for Pali Names from a local DPPN folder.

This avoids web crawling entirely and outputs records compatible with
load_pali_names_to_db.py.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from bs4 import BeautifulSoup


def build_url(path: Path, root: Path, base_url: str | None) -> str:
    rel = path.relative_to(root).as_posix()
    if base_url:
        return f"{base_url.rstrip('/')}/{quote(rel)}"
    return f"file://{path.resolve().as_posix()}"


def parse_html(path: Path) -> tuple[str, str]:
    raw = path.read_bytes()
    soup = BeautifulSoup(raw, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else path.stem
    text = soup.get_text(" ", strip=True)
    return title, text


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root-dir", default="~/Downloads/DPPN")
    ap.add_argument("--out-jsonl", default="ne-data/work/pali_names_local/pages.jsonl")
    ap.add_argument(
        "--base-url",
        default="https://www.palikanon.com/english/pali_names",
        help="Optional synthetic base URL for relative paths. Use empty string for file:// URLs.",
    )
    ap.add_argument("--max-files", type=int, default=0, help="0 = all")
    args = ap.parse_args()

    root = Path(args.root_dir).expanduser().resolve()
    out = Path(args.out_jsonl).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    html_files = sorted(p for p in root.rglob("*.html") if p.is_file())
    if args.max_files > 0:
        html_files = html_files[: args.max_files]

    if not html_files:
        raise SystemExit(f"No html files found under {root}")

    written = 0
    with out.open("w", encoding="utf-8") as f:
        for path in html_files:
            title, text = parse_html(path)
            rec = {
                "url": build_url(path, root, args.base_url or None),
                "path": "/" + path.relative_to(root).as_posix(),
                "title": title,
                "text": text,
                "text_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            written += 1

    print(f"Built {written} records -> {out}")


if __name__ == "__main__":
    main()

