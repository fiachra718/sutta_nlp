#!/usr/bin/env python3
"""Polite crawler for https://www.palikanon.com/english/pali_names/.

Outputs one JSON object per crawled page with extracted plain text.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup


DEFAULT_START_URL = "https://www.palikanon.com/english/pali_names/dic_idx.html"
ALLOWED_PREFIX = "/english/pali_names/"
DEFAULT_UA = "sutta-nlp-crawler/0.1 (+local research use)"


def canonicalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    path = parsed.path or "/"
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, "", "", ""))


def in_scope(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc.lower() == "www.palikanon.com" and parsed.path.startswith(ALLOWED_PREFIX)


def load_robots(session: requests.Session, base_url: str, user_agent: str) -> RobotFileParser:
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = RobotFileParser()
    rp.set_url(robots_url)
    resp = session.get(robots_url, timeout=20)
    resp.raise_for_status()
    rp.parse(resp.text.splitlines())
    if not rp.can_fetch(user_agent, base_url):
        raise RuntimeError(f"Robots disallow start URL for {user_agent}: {base_url}")
    return rp


def iter_links(url: str, soup: BeautifulSoup) -> Iterable[str]:
    for a in soup.find_all("a", href=True):
        candidate = canonicalize_url(urljoin(url, a["href"]))
        if in_scope(candidate):
            yield candidate


def fetch_with_retries(
    session: requests.Session,
    url: str,
    retries: int,
    timeout: int,
) -> requests.Response:
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            if attempt < retries:
                time.sleep(min(8.0, 0.8 * (2**attempt)))
    assert last_err is not None
    raise last_err


def crawl(args: argparse.Namespace) -> tuple[int, int]:
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    pages_jsonl = out_dir / "pages.jsonl"
    urls_tsv = out_dir / "visited_urls.tsv"

    session = requests.Session()
    session.headers.update({"User-Agent": args.user_agent})

    start = canonicalize_url(args.start_url)
    rp = load_robots(session, start, args.user_agent)

    queue: deque[str] = deque([start])
    seen: set[str] = set()
    written = 0
    failures = 0

    with pages_jsonl.open("w", encoding="utf-8") as page_out, urls_tsv.open(
        "w", encoding="utf-8"
    ) as url_out:
        url_out.write("url\tstatus\tfetched_at_utc\n")

        while queue:
            if args.max_pages and written >= args.max_pages:
                break

            url = queue.popleft()
            if url in seen:
                continue
            seen.add(url)

            if not rp.can_fetch(args.user_agent, url):
                url_out.write(f"{url}\trobots_blocked\t{datetime.now(timezone.utc).isoformat()}\n")
                continue

            try:
                resp = fetch_with_retries(
                    session=session, url=url, retries=args.retries, timeout=args.timeout
                )
            except Exception:  # noqa: BLE001
                failures += 1
                url_out.write(f"{url}\terror\t{datetime.now(timezone.utc).isoformat()}\n")
                continue

            content_type = (resp.headers.get("Content-Type") or "").lower()
            if "html" not in content_type:
                url_out.write(f"{url}\tskipped_non_html\t{datetime.now(timezone.utc).isoformat()}\n")
                continue

            # Parse raw bytes so BeautifulSoup can honor in-document charset meta.
            soup = BeautifulSoup(resp.content, "html.parser")
            title = soup.title.get_text(" ", strip=True) if soup.title else ""
            text = soup.get_text(" ", strip=True)

            record = {
                "url": url,
                "path": urlparse(url).path,
                "title": title,
                "text": text,
                "text_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            page_out.write(json.dumps(record, ensure_ascii=False) + "\n")
            url_out.write(f"{url}\tok\t{record['fetched_at_utc']}\n")
            written += 1

            for nxt in iter_links(url, soup):
                if nxt not in seen:
                    queue.append(nxt)

            if args.delay_seconds > 0:
                jitter = random.uniform(0, args.jitter_seconds)
                time.sleep(args.delay_seconds + jitter)

    return written, failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-url", default=DEFAULT_START_URL)
    parser.add_argument(
        "--out-dir",
        default=f"ne-data/work/pali_names_crawl_{datetime.now().strftime('%Y_%m_%d')}",
    )
    parser.add_argument("--max-pages", type=int, default=0, help="0 = no explicit limit")
    parser.add_argument("--delay-seconds", type=float, default=1.5)
    parser.add_argument("--jitter-seconds", type=float, default=0.4)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--user-agent", default=DEFAULT_UA)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pages, failures = crawl(args)
    print(f"Crawl complete. pages={pages} failures={failures} out_dir={Path(args.out_dir).resolve()}")


if __name__ == "__main__":
    main()
