import json
import logging
import os
from flask import Flask, render_template, abort, request, jsonify
from .models.models import CandidateDoc, TrainingDoc, SuttaVerse
from .api.ner import run_ner
from .render import render_highlighted
from pydantic import ValidationError
from .db.db import (
    list_nikayas,
    list_book_numbers,
    list_vaggas,
    get_ner_verse_spans,
    update_ner_verse_spans,
)

app = Flask(__name__)


def _configure_logger():
    logger = logging.getLogger("sutta_nlp.web")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    level_name = os.environ.get("APP_LOG_LEVEL", "DEBUG").upper()
    level = getattr(logging, level_name, logging.DEBUG)
    logger.setLevel(level)
    return logger


logger = _configure_logger()


def _parse_meta_value(value):
    if not value:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return None


def _build_predict_payload(text: str, meta=None):
    doc = run_ner(text)
    payload = {
        "text": doc.get("text", text),
        "spans": doc.get("spans", []),
    }
    if meta:
        payload["meta"] = meta
    return payload


def _build_training_doc_context(doc: TrainingDoc):
    text = doc.text
    spans = []
    for span in doc.spans or []:
        span_data = span.model_dump() if hasattr(span, "model_dump") else {**span}
        start = span_data.get("start", 0)
        end = span_data.get("end", start)
        span_data["text"] = text[start:end]
        spans.append(span_data)
    html = render_highlighted(text, spans)
    raw_json = json.dumps(
        {"id": doc.id, "text": text, "spans": spans},
        ensure_ascii=False,
        indent=2,
    )
    return {
        "doc": doc,
        "text": text,
        "spans": spans,
        "html": html,
        "raw_json": raw_json,
    }

@app.route("/")
def home():
    return render_template("base.html", title="Jinja and Flask")


@app.route("/candidate/<int:candidate_id>")
def candidate(candidate_id: int):
    return render_template("doc.html", record=CandidateDoc.objects.get(candidate_id))

@app.route("/training/<string:training_id>")
def training(training_id: str):
    doc = TrainingDoc.objects.get(training_id)
    logger.debug("Fetched training doc %s: %s", training_id, bool(doc))
    if not doc:
        abort(404)
    context = _build_training_doc_context(doc)
    return render_template("view_doc.html", html=context["html"], raw_json=context["raw_json"])


@app.route("/training-doc/<string:doc_id>")
def training_doc(doc_id: str):
    doc = TrainingDoc.objects.get(doc_id)
    logger.debug("Training doc view request %s found=%s", doc_id, bool(doc))
    if not doc:
        abort(404)
    context = _build_training_doc_context(doc)
    return render_template("training_doc.html", **context)

@app.route("/predict", methods=["GET", "POST"])
def predict_page():
    if request.method == "GET":
        return render_template("predict.html", initial_doc=None)

    data = request.get_json(force=True, silent=True) or {}
    text = (data.get("text") or request.form.get("text") or "").strip()
    logger.debug("Predict request text length=%d meta=%s (json=%s form=%s)",
                 len(text),
                 bool(data.get("meta") or request.form.get("meta")),
                 request.is_json,
                 bool(request.form))
    if not text:
        return jsonify({"ok": False, "error": "text required"}), 400
    meta = _parse_meta_value(data.get("meta"))
    if not meta:
        meta = _parse_meta_value(request.form.get("meta"))

    payload = _build_predict_payload(text, meta)
    logger.debug("Predict payload spans=%d meta=%s", len(payload.get("spans", [])), bool(meta))

    if request.is_json:
        return jsonify({"ok": True, **payload})

    return render_template("predict.html", initial_doc=payload)


@app.post("/api/training")
def save_training_doc():
    data = request.get_json(force=True, silent=True) or {}
    text = data.get("text", "")
    spans = data.get("spans", [])
    logger.debug(data)

    if not isinstance(text, str) or not text.strip():
        return jsonify({"ok": False, "message": "Text is required."}), 400

    try:
        doc = TrainingDoc.model_validate({
            "id": data.get("id"),
            "text": text,
            "spans": spans,
            "source": "manual",
        })
    except ValidationError as exc:
        messages = "; ".join(error.get("msg", "invalid value") for error in exc.errors())
        return jsonify({"ok": False, "message": f"Invalid training doc: {messages}"}), 400

    result = TrainingDoc.objects.save(doc, source="manual")

    if result.get("ok"):
        return jsonify({"ok": True, "id": result["id"], "message": "Saved to training data."}), 201

    return jsonify({
        "ok": False,
        "id": result.get("id"),
        "message": result.get("message", "Unable to save training doc."),
    }), 409

@app.route("/random")
def random_verse():
    verse = SuttaVerse.random_with_titlecase()
    if not verse:
        logger.warning("random_verse: no verse matched titlecase criteria")
        abort(404)

    initial_doc = {
        "text": verse.text,
        "spans": [],
        "meta": {
            "identifier": verse.identifier,
            "verse_num": verse.verse_num,
        },
    }
    return render_template("random.html", initial_doc=initial_doc)

@app.route("/verses/browse")
def browse_verses():
    nikaya = request.args.get("nikaya") or ""
    book_number = request.args.get("book_number") or ""
    vagga = request.args.get("vagga") or ""
    verse_num_input = request.args.get("verse_num") or ""
    limit = request.args.get("limit", default=25, type=int)
    verse_num = None
    if verse_num_input:
        try:
            parsed = int(verse_num_input)
            if parsed > 0:
                verse_num = parsed - 1
        except ValueError:
            verse_num_input = ""

    verses = SuttaVerse.objects.search_verses(
        nikaya=nikaya or None,
        book_number=book_number or None,
        vagga=vagga or None,
        verse_num=verse_num,
        limit=limit or 25,
    )
    facets = {
        "nikayas": list_nikayas(),
        "book_numbers": list_book_numbers(),
        "vaggas": list_vaggas(),
    }
    return render_template(
        "verse_browser.html",
        filters={
            "nikaya": nikaya,
            "book_number": book_number,
            "vagga": vagga,
            "verse_num": verse_num_input,
            "limit": limit or 25,
        },
        facets=facets,
        verses=verses,
    )


@app.route("/api/sutta/<string:identifier>/<int:verse_num>")
def sutta_verse(identifier: str, verse_num: int):
    verse = SuttaVerse.objects.fetch_sutta_verse(identifier, verse_num)
    if not verse:
        abort(404)

    payload = {
        "identifier": verse.identifier,
        "verse_num": verse.verse_num,
        "text": verse.text,
        "text_hash": verse.text_hash,
    }
    for key in ("nikaya", "vagga", "book_number", "translator", "title", "subtitle"):
        value = getattr(verse, key, None)
        if value not in (None, ""):
            payload[key] = value
    return jsonify(payload)


@app.route("/predict/verse/<string:identifier>/<int:verse_num>")
def predict_verse(identifier: str, verse_num: int):
    verse = SuttaVerse.objects.fetch_sutta_verse(identifier, verse_num)
    if not verse:
        abort(404)
    text_value = verse.text or ""
    existing = get_ner_verse_spans(identifier) or []
    spans = []
    for entry in existing:
        if not isinstance(entry, dict):
            continue
        if entry.get("verse_num") != verse_num:
            continue
        try:
            start = int(entry.get("start", 0))
            end = int(entry.get("end", start))
        except (TypeError, ValueError):
            continue
        start = max(0, min(start, len(text_value)))
        end = max(start, min(end, len(text_value)))
        label = entry.get("label") or ""
        text = entry.get("text") or text_value[start:end]
        spans.append({
            "start": start,
            "end": end,
            "label": label,
            "text": text,
        })

    meta = {
        "identifier": verse.identifier,
        "verse_num": verse.verse_num,
    }
    for key in ("nikaya", "vagga", "book_number", "translator", "title", "subtitle"):
        value = getattr(verse, key, None)
        if value not in (None, ""):
            meta[key] = value

    initial_doc = {
        "text": text_value,
        "spans": spans,
        "meta": meta,
    }
    return render_template("predict.html", initial_doc=initial_doc)


@app.post("/api/verse/<string:identifier>/<int:verse_num>/ner")
def save_verse_spans(identifier: str, verse_num: int):
    verse = SuttaVerse.objects.fetch_sutta_verse(identifier, verse_num)
    if not verse:
        abort(404)

    data = request.get_json(force=True, silent=True) or {}
    spans = data.get("spans", [])
    if not isinstance(spans, list):
        return jsonify({"ok": False, "message": "Spans must be a list."}), 400

    text = verse.text or ""
    length = len(text)
    normalized = []
    for idx, span in enumerate(spans, start=1):
        if not isinstance(span, dict):
            return jsonify({"ok": False, "message": f"Span {idx} must be an object."}), 400
        try:
            start = int(span.get("start"))
            end = int(span.get("end"))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "message": f"Span {idx} has invalid offsets."}), 400
        if start < 0 or end < start or end > length:
            return jsonify({"ok": False, "message": f"Span {idx} has out-of-range offsets."}), 400
        label = span.get("label")
        if not isinstance(label, str) or not label.strip():
            return jsonify({"ok": False, "message": f"Span {idx} requires a label."}), 400
        snippet = span.get("text")
        if not isinstance(snippet, str) or not snippet:
            snippet = text[start:end]
        normalized.append({
            "verse_num": verse_num,
            "start": start,
            "end": end,
            "label": label.strip().upper(),
            "text": snippet,
        })

    success = update_ner_verse_spans(identifier, verse_num, normalized)
    if not success:
        abort(404)
    return jsonify({"ok": True, "saved": len(normalized), "message": "Saved verse annotations."})
    
if __name__ == "__main__":
    app.run(debug=True)
