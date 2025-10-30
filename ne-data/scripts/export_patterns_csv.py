import csv
import json
from pathlib import Path

# NOTE: This keeps the export dependency-free so it can run anywhere.
# We convert token dicts into a lightweight readable string so the CSV
# has something human-friendly in addition to the raw JSON.

PATTERNS_PATH = Path("ne-data/patterns/entity_ruler/patterns.jsonl")
OUTPUT_PATH = Path("ne-data/patterns/entity_ruler/patterns.csv")


def token_repr(token: dict) -> str:
    """Turn a token pattern dict into a readable snippet."""
    text_value = None
    for key in ("TEXT", "LOWER", "ORTH", "LEMMA"):
        if key in token:
            text_value = token[key]
            break
    if isinstance(text_value, dict):
        if "IN" in text_value:
            text_value = "/".join(map(str, text_value["IN"]))
        elif "REGEX" in text_value:
            text_value = f"/{text_value['REGEX']}/"
    if text_value is None and "REGEX" in token:
        text_value = f"/{token['REGEX']}/"
    if text_value is None:
        # Fallback to compact JSON for any tokens we can't simplify.
        text_value = json.dumps(token, ensure_ascii=False)
    op = token.get("OP")
    return f"{text_value}{{{op}}}" if op else str(text_value)


def pattern_to_text(pattern) -> str:
    """Render the pattern as a space-separated string for the CSV."""
    if isinstance(pattern, str):
        return pattern
    if isinstance(pattern, dict):
        return json.dumps(pattern, ensure_ascii=False)
    if isinstance(pattern, list):
        parts = []
        for item in pattern:
            if isinstance(item, dict):
                parts.append(token_repr(item))
            else:
                parts.append(str(item))
        return " ".join(parts)
    return str(pattern)


def main() -> None:
    if not PATTERNS_PATH.exists():
        raise SystemExit(f"Missing patterns file: {PATTERNS_PATH}")

    rows = []
    with PATTERNS_PATH.open(encoding="utf-8") as infile:
        for line in infile:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            rows.append(
                {
                    "label": data.get("label", ""),
                    "id": data.get("id", ""),
                    "pattern_text": pattern_to_text(data.get("pattern")),
                    "pattern_json": json.dumps(
                        data.get("pattern"), ensure_ascii=False
                    ),
                }
            )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(
            outfile,
            fieldnames=["label", "id", "pattern_text", "pattern_json"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
