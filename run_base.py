from sklearn.preprocessing import Normalizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD

from sklearn.pipeline import Pipeline
from datetime import datetime, timezone

import psycopg

from base import Vectorizer
from base import CorpusBuilder
from base import fit_lsa, top_docs_for_component, top_terms_for_component

from local_settings import settings

import numpy as np
from pathlib import Path

conn = psycopg.connect("dbname=tipitaka user=alee")

def build_small_matrix():
    sql = """
        select doc_id, identifier, title, raw_text 
        from suttas
        where LENGTH(raw_text) > 1000
        LIMIT 200 
        """
    builder = CorpusBuilder(conn, sql)

    bundle_dir = Path("testme")
    v = Vectorizer(default_dir=bundle_dir, **settings["TFIDF"])
    X = v.fit_transform(builder)
    doc_index = builder.doc_ids
    v.save(bundle_dir, X=X, docs=doc_index)             # writes vectorizer.joblib / X.npz / doc_index.json

def some_queries():
    bundle_dir = "test_data"
    v = Vectorizer.load(bundle_dir, strict=True, require_matrix=True, require_index=True)

    X = v._x_csr                                # (N x V) doc-term matrix
    docs = v.get_doc_index()
    assert X.shape[0] == len(docs), "Row mismatch"

    for q in ["bhikkhu Ambalaṭṭhika wholesome", "five aggregates"]:
        qv = v.transform([q])  # (1 x V) query vector
        scores = (X @ qv.T).toarray().ravel()        # (N,) one score per doc
        k = min(5, scores.size)
        idx = np.argpartition(-scores, k-1)[:k]
        idx = idx[np.argsort(-scores[idx], kind="stable")]

        print("\n{}\n\n".format(q))
        for i in idx:
            rec = docs[i]
            print(f"{rec['title'][:60]:60}  {rec['identifier']:<24}  doc_id={rec['doc_id']}  score={scores[i]:.3f}")

def try_lsa():
    bundle_dir = "test_data"
    v = Vectorizer.load(bundle_dir, strict=True, require_matrix=False, require_index=True)
    docs = v.get_doc_index()
    # X = Normalizer(copy=False).fit_transform(v._x_csr)
    Z, components, evr, svd = fit_lsa(v._x_csr)
    terms = v.terms
    pos, neg = top_terms_for_component(components, terms, j=0, n=12)
    return pos, neg

def run_pipeline():
    sql = """
         select doc_id, identifier, title, raw_text from suttas where LENGTH(raw_text) > 1000 AND translator = 'Thanissaro Bhikkhu';
        """
    
    texts = list(CorpusBuilder(conn, sql))  # list of strings
    pipe = Pipeline([("tfidf", TfidfVectorizer(**settings["TFIDF"])),
                 ("svd", TruncatedSVD(n_components=200, random_state=0))])
    Z = pipe.fit_transform(texts)

    return Z

if __name__ == "__main__":
    build_small_matrix()
    some_queries()
    pos, neg = try_lsa()
    print("Comp 0 ++", ", ".join(pos))
    print("Comp 0 --", ", ".join(neg))

    Z = run_pipeline()
    print(Z)
