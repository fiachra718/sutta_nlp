```
sutta_nlp/
├─ ne-data/                     
└─ web/
   ├─ app/                       # Python application code (Flask)
   │  ├─ __init__.py             # create_app(), register blueprints, CORS, logging
   │  ├─ api/                    # REST endpoints (pure JSON)
   │  │  ├─ __init__.py          # api blueprint (url_prefix="/api/v1")
   │  │  ├─ tag.py               # POST /tag, POST /search
   │  │  ├─ examples.py          # CRUD for examples/annotations
   │  │  ├─ eval.py              # POST /evaluate, GET /metrics
   │  │  └─ plans.py             # POST /plans/train (queues), POST /exports/*
   │  ├─ services/               # business logic (no Flask here)
   │  │  ├─ tagging.py           # wraps your NER/ruler; db search helpers
   │  │  ├─ examples.py          # create/update/list annotations
   │  │  ├─ eval.py              # computes P/R/F1 per label
   │  │  └─ queues.py            # RQ/Celery enqueue helpers (optional)
   │  ├─ models/                 # pydantic schemas (request/response)
   │  │  ├─ tagging.py
   │  │  ├─ examples.py
   │  │  └─ common.py
   │  ├─ db/                     # DB access (SQLAlchemy/psycopg helpers)
   │  │  ├─ __init__.py
   │  │  └─ queries.sql          # raw SQL you already use
   │  ├─ config.py               # Settings (loads .env)
   │  └─ cli.py                  # manage tasks (seed, snapshot, eval)
   |  |
   │  ├─ templates/               # Jinja
   │  └─ index.html              # simple UI shell (or a docs landing)
   ├─ static/                    # served at /static
   │  ├─ css/                    # styles (vanilla or Tailwind build output)
   │  ├─ js/                     # tiny UI scripts
   │  └─ img/                    # icons, logos
   │
   │
   ├─ tests/                     # pytest
   │  ├─ test_api_tag.py
   │  └─ test_api_examples.py
   │
   ├─ commands/                   # dev helpers
   │  ├─ run_dev.sh              # FLASK_DEBUG=1, auto-reload
   │  └─ seed_demo_data.py
   │
   ├─ .env.example               # DB_URL, SECRET_KEY, CORS_ORIGIN, etc.
   ├─ requirements.txt           # or pyproject.toml
   ├─ wsgi.py                    # gunicorn entrypoint
   ├─ Dockerfile                 # (optional) prod image
   ├─ nginx.conf                 # (optional) if you front it
   └─ README.md                  # how to run/dev/test
   ```