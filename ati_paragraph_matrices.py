from __future__ import annotations
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from sklearn.decomposition import TruncatedSVD
from pathlib import Path
import psycopg
from psycopg.rows import dict_row
import json
from base import CorpusBuilder, Vectorizer
from sklearn.pipeline import Pipeline
import numpy as np


conn = psycopg.connect("user=alee dbname=tipitaka")

params = {
    "strip_accents": None,
    "lowercase": True,
    "ngram_range": (1, 2),
    "stop_words": "english",
    "sublinear_tf": True,
    "min_df": 10,
    "max_df": 0.85,
    "dtype": np.float64,
}

sql = """
    SELECT
    s.id,
    s.identifier,
    s.title,
    (e.elem->>'text') AS paragraph_text
    FROM ati_suttas s
    CROSS JOIN LATERAL jsonb_array_elements(s.verses) WITH ORDINALITY AS e(elem, ord)
    WHERE s.nikaya IN ('MN', 'SN', 'AN')
    AND s.translator = 'Thanissaro Bhikkhu'
    AND LENGTH(e.elem->>'text') > 300
    ORDER BY s.identifier
"""

# sanity
# with conn.cursor(row_factory=dict_row) as cur:
#     cur = conn.execute(sql)
#     rows: list[dict] = cur.fetchall()
#     print(json.dumps(rows, indent=2, ensure_ascii=False))

# builder = CorpusBuilder(conn, sql)
# bundle_dir = Path("ati_para_run")
# v = Vectorizer(default_dir=bundle_dir, **params)
# X = v.fit_transform(builder)
# v.save(bundle_dir, X=X, docs=builder.doc_ids) 

def show_top_terms_per_topic(pipeline, n_top=15):
    # Grab pieces
    vectorizer = pipeline.named_steps["tfidf"]
    model = pipeline.named_steps["nmf"]  # or "svd" if you're inspecting SVD components
    H = model.components_               # shape: (n_topics, n_terms)
    terms = vectorizer.get_feature_names_out()

    for k, row in enumerate(H):
        top_idx = np.argsort(row)[::-1][:n_top]     # indices of largest weights
        top_terms = [terms[i] for i in top_idx]
        top_vals  = row[top_idx]
        print(f"Topic {k:02d}:")
        print("  " + ", ".join(f"{t} ({v:.3f})" for t, v in zip(top_terms, top_vals)))
        print()

if __name__ == "__main__":
    texts = list(CorpusBuilder(conn, sql))  # list of strings
    pipe = Pipeline([
        ("tfidf", TfidfVectorizer(**params)),
        ("nmf", NMF(
            n_components=200, init="nndsvd", random_state=0, max_iter=800, tol=1e-5, alpha_H=0.2, l1_ratio=0.5
            # n_components=200, init="nndsvd", random_state=0, max_iter=400
            ))
    ])
    W = pipe.fit_transform(texts)  # document-topic matrix
    # H = pipe.named_steps["nmf"].components_


    show_top_terms_per_topic(pipe)

    # vec = TfidfVectorizer(**params)              # your params
    # X = vec.fit_transform(list(CorpusBuilder(conn, sql)))            # rows=paragraphs

    # svd = TruncatedSVD(n_components=200, random_state=0).fit(X)
    # nmf = NMF(n_components=200, init="nndsvd", random_state=0, max_iter=800, tol=1e-5,
    #       alpha_H=0.2, l1_ratio=0.5).fit(X)

    # W = nmf.transform(X)                         # docs × topics
    # H = nmf.components_