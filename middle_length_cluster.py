# middle_length_cluster_refactor.py
from __future__ import annotations
import json
from pathlib import Path
from typing import List, Dict, Any
import numpy as np
from scipy import sparse
from sklearn.decomposition import NMF
import spacy, unicodedata
from spacy.pipeline import EntityRuler

from base import Vectorizer, CorpusBuilder
from local_settings import settings


def undiacritic(s):
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

nlp = spacy.load("en_core_web_md")
ruler = nlp.add_pipe("entity_ruler", before="ner")

names = [
    ("PERSON", "Sāriputta"), ("PERSON", "Sariputta"),
    ("PERSON", "Ānanda"), ("PERSON", "Ananda"),
    ("PERSON", "Moggallāna"),("PERSON", "Moggallana"),
    ("PERSON", "Master Gotama"), ("PERSON", "Gotama"),
    ("PERSON", "King Pasenadi"), ("PERSON", "Bimbisara"),
    ("PERSON", "Malunkyaputta"), ("PERSON", "Maha Kotthita"),
    ("PERSON", "Ratthapala"), ("PERSON", "Mara"),
    ("PERSON", "Rahula"),     ("PERSON", "Anathapindika"),
    ("PERSON", "Dhanañjanin"),  ("PERSON", "Angulimala"),
    ("PERSON", "Assalayana"), ("PERSON", "Sāti"),
    ("GPE", "Rajagaha"),
    ("GPE", "Rājagaha"), ("GPE", "Rajagaha"),
    ("GPE", "Deer Park"), ("GPE", "Udañña"),
    ("GPE", "Salavatika"),
    ("GPE", "Sāvatthī"), ("GPE", "Savatthi"),
    ("ORG", "Sakyan"), ("GPE","Kosala"), ("GPE","Magadha"),

]
patterns = [{"label": lbl, "pattern": pat} for lbl, pat in names]
ruler.add_patterns(patterns)


def ensure_bundle(v: Vectorizer):
    X = v._x_csr
    docs = v.get_doc_index()
    terms = v.feature_names()
    n_docs, n_terms = X.shape
    assert n_docs == len(docs), f"Row mismatch: X={n_docs} docs={len(docs)}"
    assert n_terms == len(terms), f"Col mismatch: X={n_terms} terms={len(terms)}"
    return X, docs, terms

def fit_nmf(X, k=20, max_iter=400, random_state=0, sparsity=False):
    nmf = NMF(
        n_components=k,
        init="nndsvd",
        max_iter=max_iter,
        random_state=random_state,
        **({"alpha_W":0.1,"alpha_H":0.1,"l1_ratio":0.5} if sparsity else {})
    )
    W = nmf.fit_transform(X)     # (n_docs, k)
    H = nmf.components_          # (k, n_terms)
    # sanity checks
    n_docs, n_terms = X.shape
    assert W.shape == (n_docs, nmf.n_components)
    assert H.shape == (nmf.n_components, n_terms)
    return nmf, W, H

def top_terms(H: np.ndarray, terms: List[str], j: int, n: int = 12):
    """Return [(term, weight), ...] for topic j (descending)."""
    w = H[j]
    idx = np.argpartition(w, -n)[-n:]             # pick top-n (unordered)
    idx = idx[np.argsort(w[idx])[::-1]]           # sort those by weight desc
    return [(terms[i], float(w[i])) for i in idx]

def top_docs(W: np.ndarray, docs: List[Dict[str,Any]], j: int, k: int = 5):
    """Return [{doc..., weight}, ...] for topic j (descending)."""
    col = W[:, j]
    idx = np.argpartition(-col, min(k-1, col.size-1))[:k]
    idx = idx[np.argsort(-col[idx], kind="stable")]
    return [{**docs[i], "weight": float(col[i])} for i in idx]

def run_from_bundle(bundle_dir: Path, k_topics=20, sparsity=False):
    v = Vectorizer.load(bundle_dir, strict=True, require_matrix=True, require_index=True)
    X, docs, terms = ensure_bundle(v)
    nmf, W, H = fit_nmf(X, k=k_topics, sparsity=sparsity)

    for j in range(min(k_topics, H.shape[0])):
        tt = top_terms(H, terms, j, 12)
        print(f"\nTopic {j:02d} terms:", [t for t,_ in tt])
        for row in top_docs(W, docs, j, 5):
            print(f"{row['identifier']:<22} {row['title'][:48]:48}  w={row['weight']:.3f}")

def build_then_run(conn, sql: str, out_dir: Path, k_topics=25, sparsity=False):
    # Build corpus once (materialize to avoid exhausted iterators)
    builder = CorpusBuilder(conn, sql)
    texts = list(builder)
    docs = builder.doc_index if hasattr(builder, "doc_index") else builder.doc_ids

    # Fit TF-IDF
    v = Vectorizer(default_dir=out_dir, **settings["TFIDF"])
    X = v.fit_transform(texts)
    v.set_doc_index(docs)
    v.save(X=X, docs=docs)  # persist bundle for reuse

    # Topic modeling
    nmf, W, H = fit_nmf(X, k=k_topics, sparsity=sparsity)
    terms = v.feature_names()

    for j in range(min(k_topics, H.shape[0])):
        tt = top_terms(H, terms, j, 12)
        print(f"\nTopic {j:02d} terms:", [t for t,_ in tt])
        for row in top_docs(W, docs, j, 5):
            print(f"{row['identifier']:<22} {row['title'][:48]:48}  w={row['weight']:.3f}")

if __name__ == "__main__":
    # --- choose ONE path below ---

    # A) from existing bundle
    # run_from_bundle(Path("mn"), k_topics=20, sparsity=False)

    # B) or build fresh from SQL, then run
    import psycopg
    with psycopg.connect("dbname=tipitaka user=alee") as conn:
        sql = """
          SELECT doc_id, identifier, title, raw_text
          FROM suttas
          WHERE nikaya = 'Majjhima'
            AND translator = 'Thanissaro Bhikkhu'
        """
        build_then_run(conn, sql, Path("tfidf_run"), k_topics=45, sparsity=False)
