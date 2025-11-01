import json, hashlib, unicodedata
from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json
from pathlib import Path

def nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)

def md5_hex(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()

def sorted_spans(spans):
    # keep only essentials for hashing; sort deterministically
    data = [{"start": int(s["start"]), "end": int(s["end"]), "label": str(s["label"])} for s in spans]
    data.sort(key=lambda s: (s["start"], s["end"], s["label"]))
    return data

def entities_to_spans(text, entities):
    """
    This is copypasta from ner_pipe.py in ne-data/scripts
    Accepts entities in a few shapes:
      - {"start": ..., "end": ..., "label": ...}
      - [start, end, label]
      - (label, surface)
      - (label, surface, meta)
      - {"label": ..., "text": ...}
    Returns canonical spans or None if conversion fails.
    """
    if not isinstance(entities, list):
        return None
    spans = []
    surface_positions: dict[str, list[int]] = {}
    surface_counts: dict[str, int] = {}

    for ent in entities:
        start = end = None
        label = None
        surface = None

        if isinstance(ent, dict):
            start = ent.get("start")
            end = ent.get("end")
            label = ent.get("label")
            surface = ent.get("text")
        elif isinstance(ent, (list, tuple)):
            if len(ent) == 3 and isinstance(ent[0], int) and isinstance(ent[1], int):
                start, end, label = ent
            elif len(ent) >= 2 and isinstance(ent[0], str) and isinstance(ent[1], str):
                label, surface = ent[0], ent[1]
        # offset based entities are authoritative
        if start is not None and end is not None:
            try:
                start = int(start)
                end = int(end)
            except (TypeError, ValueError):
                return None
            if start < 0 or start >= end or end > len(text) or not isinstance(label, str):
                return None
            spans.append({"start": start, "end": end, "label": label})
            continue

        # otherwise match the provided surface string sequentially
        if not (isinstance(label, str) and isinstance(surface, str) and surface):
            return None
        surface = nfc(surface)
        positions = surface_positions.get(surface)
        if positions is None:
            # precompute all offsets for this surface text
            positions = []
            idx = text.find(surface)
            while idx != -1:
                positions.append(idx)
                idx = text.find(surface, idx + len(surface))
            surface_positions[surface] = positions
        use_idx = surface_counts.get(surface, 0)
        if use_idx >= len(positions):
            return None
        start = positions[use_idx]
        end = start + len(surface)
        if text[start:end] != surface:
            return None
        spans.append({"start": start, "end": end, "label": label})
        surface_counts[surface] = use_idx + 1

    return spans

SQL = """
INSERT INTO gold_training (id, text, text_hash, spans, spans_hash, source, from_file)
VALUES (%(id)s, %(text)s, %(text_hash)s, %(spans)s, %(spans_hash)s, %(source)s, %(from_file)s)
ON CONFLICT (text_hash, spans_hash) DO NOTHING
"""

def load_gold_jsonl(path: str, dsn="dbname=tipitaka user=alee", source="import"):
    inserted = 0
    with connect(dsn, row_factory=dict_row) as cx, cx.cursor() as cur, open(path, "r", encoding="utf-8") as f:
        batch = []
        for line_no, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                snippet_start = max(0, exc.pos - 40)
                snippet_end = exc.pos + 40
                snippet = line[snippet_start:snippet_end]
                raise ValueError(
                    f"Malformed JSON in {path} line {line_no}: {exc.msg} (char {exc.pos}). Near: {snippet!r}"
                ) from None
            text = nfc(rec["text"])
            text_hash = md5_hex(text.encode("utf-8"))
            if "spans" in rec:
                spans = rec["spans"]
            elif "entities" in rec:
                spans = entities_to_spans(text, rec["entities"])
                if spans is None:
                    continue
            else:
                continue
            # validate and canonicalize
            s_span = sorted_spans(spans)
            # verify slice text; skip if not guaranteed yet
            # for s in canon:
            #     assert text[s["start"]:s["end"]] == rec_span_text

            spans_hash = hashlib.sha256(json.dumps(s_span, separators=(",", ":")).encode("utf-8")).hexdigest()
            record_id = rec.get("id")
            if not record_id:
                record_id = f"{Path(path).stem}:{line_no}"

            batch.append({
                "id": record_id,
                "text": text,
                "text_hash": text_hash,
                "spans": Json(s_span),        # store canonical spans (no embedded text needed)
                "spans_hash": spans_hash,
                "source": source,
                "from_file": path,
            })

            if len(batch) >= 1000:
                cur.executemany(SQL, batch); inserted += cur.rowcount; batch.clear()

        if batch:
            cur.executemany(SQL, batch); inserted += cur.rowcount

        cx.commit()
    return inserted

if __name__ == "__main__":
    WORK_DIR = Path("/Users/alee/sutta_nlp/ne-data/work")
    for filename in WORK_DIR.glob("*.jsonl"):
        print("processing: {}".format(filename))
        rows = load_gold_jsonl(path=str(filename))
        print("rows processed: {}".format(rows))
