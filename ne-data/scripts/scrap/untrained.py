import spacy
import json
from json import JSONDecodeError

nlp = spacy.load("en_core_web_md")
candidate_file = "./ne-data/work/candidates/cleaned_candidates.jsonl"


f = open("./ne-data/work/candidates/cleaned_candidates.jsonl", "r", encoding="utf-8")
for line in f.readlines()[0:50]:
    try:
        data = json.loads(line.strip())
    except JSONDecodeError as e:
        print(e)
        continue

    doc = nlp(data.get("text"))
    print( [ ( ent.text, ent.label_ ) for ent in doc.ents])
f.close()


from local_settings import load_model

nlp = load_model()
f = open("./ne-data/work/candidates/cleaned_candidates.jsonl", "r", encoding="utf-8")
for line in f.readlines()[0:50]:
    try:
        data = json.loads(line.strip())
    except JSONDecodeError as e:
        print(e)
        continue

    doc = nlp(data.get("text"))
    print( [ ( ent.text, ent.label_ ) for ent in doc.ents])
