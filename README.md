# Sutta NLP pipeline

- Build corpus from Postgres
- TF-IDF vectorizer wrapper
- Optional SVD + clustering

## Setup
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # edit DB_URL

## Run
python run_base.py
