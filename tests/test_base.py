from sklearn.pipeline import Pipeline

from sutta_nlp.base import Vectorizer
from sutta_nlp.base import CorpusBuilder
from sutta_nlp.base import fit_lsa
import psycopg
from sutta_nlp.local_settings import settings
import numpy as np
from pathlib import Path

conn = psycopg.connect("dbname=tipitaka user=alee")

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

assert X.shape[0] == len(doc_index)

bundle_dir = "test_data"
# Load everything from the SAME bundle (and fail fast if missing)
v2 = Vectorizer.load(bundle_dir, strict=True, require_matrix=True, require_index=True)

X2 = v2._x_csr                                # (N x V) doc-term matrix
docs = v2.get_doc_index()
assert X2.shape[0] == len(docs), "Row mismatch"

qv = v2.transform(["bhikkhu Ambalaṭṭhika wholesome"])  # (1 x V) query vector
scores = (X2 @ qv.T).toarray().ravel()        # (N,) one score per doc

assert X2.shape[0] == len(docs)
k = min(5, scores.size)
idx = np.argpartition(-scores, k-1)[:k]
idx = idx[np.argsort(-scores[idx], kind="stable")]


for i in idx:
    rec = docs[i]
    print(f"{rec['title'][:60]:60}  {rec['identifier']:<24}  doc_id={rec['doc_id']}  score={scores[i]:.3f}")

n_docs, n_terms = X2.shape

Z, components, evr, svd = fit_lsa(X2)
k = svd.n_components
assert Z.shape == (n_docs, k)
assert svd.components_.shape == (k, n_terms)

terms = v2.terms
assert terms is not None
