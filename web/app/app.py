from flask import Flask, render_template
from .models.models import CandidateDoc, TrainingDoc

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("base.html", title="Jinja and Flask")

@app.route("/candidate/<int:candidate_id>")
def candidate(candidate_id: int):
    return render_template("doc.html", record=CandidateDoc.objects.get(candidate_id))

@app.route("/training/<string:training_id>")
def training(training_id: str):
    return render_template("doc.html", record=TrainingDoc.objects.get(training_id))
