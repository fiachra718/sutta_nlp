from flask import Flask, render_template, abort, request, jsonify
from .models.models import CandidateDoc, TrainingDoc
from .api.ner import run_ner
from .render import render_highlighted
from flask_restful import Resource, Api
import json
from pydantic import ValidationError

app = Flask(__name__)
api = Api(app)

@app.route("/")
def home():
    return render_template("base.html", title="Jinja and Flask")


@app.route("/candidate/<int:candidate_id>")
def candidate(candidate_id: int):
    return render_template("doc.html", record=CandidateDoc.objects.get(candidate_id))

@app.route("/training/<string:training_id>")
def training(training_id: str):
    doc = TrainingDoc.objects.get(training_id)
    print(doc)
    if not doc:
        abort(404)
    text = doc.text
    spans = [s.model_dump() if hasattr(s, "model_dump") else s for s in doc.spans]
    html = render_highlighted(text, spans)
    # html = render_highlighted(doc.text, [s.model_dump() for s in doc.spans])
    raw_json = json.dumps(
        {"id": doc.id, "text": text, "spans": spans},
        ensure_ascii=False, indent=2 )
    return render_template("view_doc.html", html=html, raw_json=raw_json)

@app.route("/predict", methods=["GET", "POST"])
def predict_page():
    if request.method == "GET":
        return render_template("predict.html")

    data = request.get_json(force=True, silent=True) or {}
    text = (data.get("text") or "").strip()
    if not text:
        return jsonify({"ok": False, "error": "text required"}), 400

    # run through trained SpaCy NLP
    doc = run_ner(text)
    return jsonify({"ok": True, "text": doc.get("text", text), "spans": doc.get("spans", [])})


@app.post("/api/training")
def save_training_doc():
    data = request.get_json(force=True, silent=True) or {}
    text = data.get("text", "")
    spans = data.get("spans", [])

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


if __name__ == "__main__":
    app.run(debug=True)
