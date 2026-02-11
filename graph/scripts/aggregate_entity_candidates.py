#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path


LABELS = {"PERSON", "GPE", "LOC", "NORP"}


def norm_text(value: str) -> str:
    value = unicodedata.normalize("NFC", value or "")
    value = value.strip()
    value = re.sub(r"\s+", " ", value)
    return value


def key_text(value: str) -> str:
    return norm_text(value).casefold()


def infer_label_from_name(name: str) -> str | None:
    n = name.lower()
    if "person" in n or "people" in n:
        return "PERSON"
    if "gpe" in n:
        return "GPE"
    if "loc" in n:
        return "LOC"
    if "norp" in n:
        return "NORP"
    return None


def add_row(store: list[dict], label: str | None, text: str, source_file: str, source_kind: str, count_hint: int = 1):
    if label not in LABELS:
        return
    text = norm_text(text)
    if not text:
        return
    if text.lower() in {"select", "from", "where", "group by", "order by"}:
        return
    store.append(
        {
            "label": label,
            "surface": text,
            "norm": key_text(text),
            "source_file": source_file,
            "source_kind": source_kind,
            "count_hint": int(count_hint or 1),
        }
    )


def parse_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            parse_json_obj(obj, path, rows)


def parse_json(path: Path, rows: list[dict]) -> None:
    text = path.read_text(encoding="utf-8")
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return
    parse_json_obj(obj, path, rows)


def parse_json_obj(obj, path: Path, rows: list[dict]) -> None:
    source_file = str(path)

    if isinstance(obj, list):
        for item in obj:
            parse_json_obj(item, path, rows)
        return

    if not isinstance(obj, dict):
        return

    # all_nodes.jsonl style
    label = obj.get("type")
    if isinstance(label, str) and label in LABELS:
        if obj.get("canonical_name"):
            add_row(rows, label, obj["canonical_name"], source_file, "json_canonical", 1)
        aliases = obj.get("aliases") or []
        if isinstance(aliases, list):
            for alias in aliases:
                if isinstance(alias, dict):
                    val = alias.get("canonical_alias") or alias.get("normalized_alias")
                    if val:
                        add_row(rows, label, val, source_file, "json_alias", 1)
                elif isinstance(alias, str):
                    add_row(rows, label, alias, source_file, "json_alias", 1)

    # persons.json / gpe.json / loc.json style
    inferred = infer_label_from_name(path.name)
    name_val = obj.get("name") if isinstance(obj.get("name"), str) else None
    if inferred and name_val:
        add_row(rows, inferred, name_val, source_file, "json_name", 1)
        aliases = obj.get("aliases") or []
        if isinstance(aliases, list):
            for alias in aliases:
                if isinstance(alias, str):
                    add_row(rows, inferred, alias, source_file, "json_alias", 1)


def parse_text(path: Path, rows: list[dict]) -> None:
    inferred = infer_label_from_name(path.name)
    source_file = str(path)
    with path.open("r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            lower = line.lower()
            if lower.startswith("select ") or lower.startswith("from ") or lower.startswith("where ") or lower.startswith("--"):
                continue
            if re.match(r"^-{3,}$", line):
                continue
            if line.startswith("(") and "rows" in lower:
                continue

            # Common "text | count" or "label | text | count" style.
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 3 and parts[0] in LABELS:
                maybe_text = parts[1]
                count_hint = int(parts[2]) if parts[2].isdigit() else 1
                add_row(rows, parts[0], maybe_text, source_file, "text_table", count_hint)
                continue
            if len(parts) >= 2 and inferred in LABELS:
                maybe_text = parts[0]
                maybe_count = parts[1]
                count_hint = int(maybe_count) if maybe_count.isdigit() else 1
                add_row(rows, inferred, maybe_text, source_file, "text_table", count_hint)
                continue

            if inferred in LABELS:
                cleaned = re.sub(r"^\*+", "", line).strip()
                cleaned = re.sub(r"^(no match for|in person but not gpe:|! in person but not gpe:|!|in person but NOT gpe:)\s*", "", cleaned, flags=re.IGNORECASE)
                add_row(rows, inferred, cleaned, source_file, "text_list", 1)


def write_outputs(rows: list[dict], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_path = out_dir / "entity_candidates_raw.tsv"
    with raw_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["label", "surface", "norm", "source_file", "source_kind", "count_hint"],
            delimiter="\t",
        )
        writer.writeheader()
        writer.writerows(rows)

    grouped: dict[tuple[str, str], dict] = {}
    for r in rows:
        key = (r["label"], r["norm"])
        rec = grouped.setdefault(
            key,
            {
                "label": r["label"],
                "norm": r["norm"],
                "preferred_surface": r["surface"],
                "count_hint_sum": 0,
                "n_sources": 0,
                "source_files": set(),
                "variants": set(),
            },
        )
        rec["count_hint_sum"] += int(r["count_hint"])
        rec["source_files"].add(r["source_file"])
        rec["variants"].add(r["surface"])
        if len(r["surface"]) > len(rec["preferred_surface"]):
            rec["preferred_surface"] = r["surface"]

    unique_rows = []
    for rec in grouped.values():
        unique_rows.append(
            {
                "label": rec["label"],
                "norm": rec["norm"],
                "preferred_surface": rec["preferred_surface"],
                "count_hint_sum": rec["count_hint_sum"],
                "n_sources": len(rec["source_files"]),
                "source_files": "; ".join(sorted(rec["source_files"])),
                "variants": "; ".join(sorted(rec["variants"])),
            }
        )
    unique_rows.sort(key=lambda x: (x["label"], -x["count_hint_sum"], x["norm"]))

    unique_path = out_dir / "entity_candidates_unique.tsv"
    with unique_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "label",
                "norm",
                "preferred_surface",
                "count_hint_sum",
                "n_sources",
                "source_files",
                "variants",
            ],
            delimiter="\t",
        )
        writer.writeheader()
        writer.writerows(unique_rows)

    by_label = defaultdict(list)
    for row in unique_rows:
        by_label[row["label"]].append(row["preferred_surface"])

    for label, vals in by_label.items():
        p = out_dir / f"{label.lower()}_candidates.txt"
        p.write_text("\n".join(vals) + "\n", encoding="utf-8")

    print(f"Wrote {raw_path} ({len(rows)} rows)")
    print(f"Wrote {unique_path} ({len(unique_rows)} rows)")
    for label in sorted(by_label):
        print(f"Wrote {(out_dir / f'{label.lower()}_candidates.txt')} ({len(by_label[label])} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate PERSON/GPE/LOC/NORP candidates from graph/entities JSONL + TEXT layers.")
    parser.add_argument("--entities-dir", default="graph/entities", help="Base entities directory")
    parser.add_argument("--out-dir", default="graph/entities/reports", help="Output directory")
    args = parser.parse_args()

    base = Path(args.entities_dir)
    json_dir = base / "JSONL"
    text_dir = base / "TEXT"

    rows: list[dict] = []

    if json_dir.exists():
        for path in sorted(json_dir.iterdir()):
            if path.suffix == ".jsonl":
                parse_jsonl(path, rows)
            elif path.suffix == ".json":
                parse_json(path, rows)

    if text_dir.exists():
        for path in sorted(text_dir.iterdir()):
            if path.suffix == ".txt":
                parse_text(path, rows)

    write_outputs(rows, Path(args.out_dir))


if __name__ == "__main__":
    main()
