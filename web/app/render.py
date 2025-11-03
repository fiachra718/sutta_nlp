import html

PALETTE = {
    "PERSON": "#b3d4ff",  # blue-ish
    "GPE":    "#c6f6d5",  # green-ish
    "LOC":    "#a7f3d0",  # teal/green
    "NORP":   "#fde68a",  # yellow
    "EVENT":  "#fca5a5",  # red-ish
}

def _as_span_dict(s):
    # accept Pydantic Span or plain dict
    if isinstance(s, dict):
        return s
    # Pydantic/BaseModel or simple object with attributes
    return {
        "start": int(getattr(s, "start")),
        "end":   int(getattr(s, "end")),
        "label": str(getattr(s, "label")),
    }

def render_highlighted(text: str, spans: list[dict]) -> str:
    # Sort and build non-overlapping slices
    spans = [_as_span_dict(s) for s in spans]
    s = sorted(spans, key=lambda x: (x["start"], x["end"]))
    out = []
    i = 0
    for sp in s:
        a, b, lab = sp["start"], sp["end"], sp["label"]
        if a > i:
            out.append(html.escape(text[i:a]))
        piece = html.escape(text[a:b])
        color = PALETTE.get(lab, "#e5e7eb")  # gray fallback
        out.append(f'<span class="tag tag-{lab}" style="background:{color}"><b>{piece}</b><small class="lbl">{lab}</small></span>')
        i = b
    if i < len(text):
        out.append(html.escape(text[i:]))
    return "".join(out)