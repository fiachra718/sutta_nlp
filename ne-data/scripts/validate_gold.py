import json
import sys
from typing import Any, Dict, List


def load_input() -> Dict[str, Any]:
    """Read a single JSON object from stdin."""
    data = sys.stdin.read()
    try:
        return json.loads(data)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON input: {exc}") from exc


def validate_spans(record: Dict[str, Any]) -> List[str]:
    """Verify that each span matches the provided text offsets."""
    errors = []
    text = record.get("text")
    spans = record.get("spans", [])

    if text is None:
        errors.append("Missing 'text' field.")
        return errors

    if not isinstance(spans, list):
        errors.append("'spans' must be a list.")
        return errors

    for idx, span in enumerate(spans):
        try:
            start = span["start"]
            end = span["end"]
        except (TypeError, KeyError):
            errors.append(f"Span {idx} is missing 'start' or 'end'.")
            continue

        if not (isinstance(start, int) and isinstance(end, int)):
            errors.append(f"Span {idx} has non-integer offsets.")
            continue

        if start < 0 or end > len(text) or start >= end:
            errors.append(
                f"Span {idx} has invalid offsets: start={start}, end={end}."
            )
            continue

        extracted = text[start:end]
        expected = span.get("text")
        if expected is not None and extracted != expected:
            errors.append(
                f"Span {idx} text mismatch: expected '{expected}', found '{extracted}'."
            )

    return errors


def main() -> None:
    record = load_input()
    errors = validate_spans(record)
    if errors:
        print("INVALID")
        for err in errors:
            print(f"- {err}")
        raise SystemExit(1)
    print("OK")


if __name__ == "__main__":
    main()
