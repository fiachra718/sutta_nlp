from __future__ import annotations
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

from sklearn.decomposition import TruncatedSVD
from scipy import sparse
from pathlib import Path
import numpy as np
import json, joblib
from datetime import datetime, timezone

from typing import Dict, List, Iterator, Any, Iterable, Optional
from html import unescape
from local_settings import settings


class CorpusBuilder:
    """ 
    The corpus is stored in a postgres database so we pass the db engine
    and a select statement 
    We return a list of records -> list[dict]
    where the doc_ids has keys: 
        doc_id, doc_identifier, doc_title
    """
    def __init__(self, conn, select):
        self.conn = conn
        self.sql = select
        self.doc_ids: list[dict[str]] = []
        self._pass = 0
    
    def __iter__(self) -> Iterator[str]:
        '''
        yield text, append metadata to self.doc_ids
        '''
        self._pass += 1
        first_pass = (self._pass == 1)

        cur = self.conn.execute(self.sql)   # returns a cursor in psycopg3
        while True:
            rows = cur.fetchmany(1000)
            if not rows:
                break
            for doc_id, identifier, title, text in rows:
                if first_pass:
                    self.doc_ids.append({
                        "doc_id": int(doc_id),
                        "identifier": identifier,
                        "title": unescape(title or ""),
                    })
                yield text 


class Vectorizer:
    """Thin wrapper around TfidfVectorizer with explicit save/load."""
    def __init__(self, default_dir: Optional[Path] = None, **params):
        self.params: Dict[str, Any] = params or settings["TFIDF"]
        self.default_dir: Optional[Path] = Path(default_dir) if default_dir else None
        self._sk = None          # fitted sklearn TfidfVectorizer
        self._x_csr = None       # csr_matrix of TF-IDF rows
        self.doc_index: List[Dict[str, Any]] = []  # [{"doc_id", "identifier", "title"}, ...]

    # ---------- doc index helpers ----------
    def set_doc_index(self, doc_index: Iterable[Dict[str, Any]]) -> None:
        self.doc_index = list(doc_index)

    def get_doc_index(self) -> List[Dict[str, Any]]:
        return self.doc_index

    def titles(self) -> List[str]:
        return [r["title"] for r in self.doc_index]

    def doc_ids(self) -> List[int]:
        return [r["doc_id"] for r in self.doc_index]

    def identifiers(self) -> List[str]:
        return [r["identifier"] for r in self.doc_index]

    # ---------- construction ----------
    @classmethod
    def from_corpus(cls, corpus: Iterable[str], *, out_dir: Optional[Path] = None, docs: Optional[List[Dict[str, Any]]] = None, **params):
        """
        Fit on corpus and return an instance. Does not perform any I/O.
        Use .save(...) to persist artifacts.
        """
        self = cls(default_dir=out_dir, **params)
        self._fit_transform_inplace(corpus)
        if docs is not None:
            self.set_doc_index(docs)
        return self

    @classmethod
    def load(cls, bundle_dir: Path, *, strict: bool = True,
             require_matrix: bool = False, require_index: bool = False):
        """
        strict=True  -> raise if vectorizer missing (recommended).
        require_matrix/index -> also require X.npz / doc_index.json.
        """
        bundle_dir = Path(bundle_dir)
        names = settings["BUNDLE"]
        vec_path = bundle_dir / names["vectorizer"]
        x_path   = bundle_dir / names["x_csr"]
        idx_path = bundle_dir / names["doc_index"]

        if strict and not vec_path.exists():
            raise FileNotFoundError(
                f"Missing vectorizer at {vec_path}. "
                f"Expected bundle layout: {names}"
            )

        self = cls(default_dir=bundle_dir)

        # Vectorizer is required (strict) or optional
        if vec_path.exists():
            self._sk = joblib.load(vec_path)

        # Matrix: required/optional based on flags
        if require_matrix and not x_path.exists():
            raise FileNotFoundError(f"Missing matrix at {x_path}")
        if x_path.exists():
            self._x_csr = sparse.load_npz(x_path)

        # Doc index: required/optional based on flags
        if require_index and not idx_path.exists():
            raise FileNotFoundError(f"Missing doc index at {idx_path}")
        if idx_path.exists():
            self.doc_index = json.loads(idx_path.read_text(encoding="utf-8"))

        # Sanity: if both present, rows must match
        if self._x_csr is not None and self.doc_index:
            nX, nI = self._x_csr.shape[0], len(self.doc_index)
            if nX != nI:
                raise ValueError(f"Row mismatch: X has {nX} rows, doc_index has {nI}")

        return self

    # ---------- core ops ----------
    def fit(self, corpus: Iterable[str]):
        from sklearn.feature_extraction.text import TfidfVectorizer
        self._sk = TfidfVectorizer(**self.params).fit(corpus)
        return self

    def transform(self, texts: Iterable[str]):
        assert self._sk is not None, "Vectorizer not fitted/loaded"
        return self._sk.transform(texts)

    def fit_transform(self, corpus: Iterable[str]):
        # single-pass fit+transform; does NOT store _x_csr unless you want to
        from sklearn.feature_extraction.text import TfidfVectorizer
        self._sk = TfidfVectorizer(**self.params)
        X = self._sk.fit_transform(corpus)
        return X

    def _fit_transform_inplace(self, corpus: Iterable[str]) -> None:
        """Fit + store matrix in self._x_csr (one pass)."""
        from sklearn.feature_extraction.text import TfidfVectorizer
        self._sk = TfidfVectorizer(**self.params)
        self._x_csr = self._sk.fit_transform(corpus)

    # ---------- persistence ----------
    def save(self, dirpath: Optional[Path] = None, X=None, docs: Optional[List[Dict[str, Any]]] = None, manifest: Optional[Dict[str, Any]] = None) -> Path:
        """
        Write vectorizer + (optionally) X and docs to dirpath.
        If X is None but self._x_csr is set, save that.
        """
        names = settings["BUNDLE"]
        out = Path(dirpath or self._timestamped_dir())
        out.mkdir(parents=True, exist_ok=True)

        # vectorizer
        assert self._sk is not None, "Nothing to save: vectorizer not fitted/loaded"
        joblib.dump(self._sk, out / names["vectorizer"])

        # matrix
        X_to_save = X if X is not None else self._x_csr
        if X_to_save is not None:
            sparse.save_npz(out / names["x_csr"], X_to_save)

        # doc index
        if docs is not None:
            self.doc_index = list(docs)
        if self.doc_index:
            (out / names["doc_index"]).write_text(
                json.dumps(self.doc_index, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )

        # manifest (always write a minimal one)
        mf = self._manifest_defaults(manifest)
        (out / names["manifest"]).write_text(json.dumps(mf, ensure_ascii=False, indent=2), encoding="utf-8")

        return out

    # ---------- utilities ----------
    def _timestamped_dir(self) -> Path:
        stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return self.default_dir or Path(f"tfidf_{stamp}")

    def _manifest_defaults(self, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        m = {
            "created_at": datetime.now(tz=timezone.utc).isoformat(),
            "params": self.params,
            "has_matrix": self._x_csr is not None,
            "n_docs": (self._x_csr.shape[0] if self._x_csr is not None else (len(self.doc_index) or None)),
            "vocab_size": (len(getattr(self._sk, "vocabulary_", {})) if self._sk is not None else None),
        }
        if extra:
            m.update(extra)
        return m


class Reducer:
    def __init__(self, n_components=200, random_state=0):
        self.n_components = n_components
        self.random_state = random_state

    def fit_lsa(self, X_csr: sparse.csr_matrix):
        svd = TruncatedSVD(n_components=self.n_components, random_state=self.random_state)
        Z = svd.fit_transform(X_csr)
        
        return Z, svd.components, svd.explained_variance_ratio_



class Clusterer:
    def __init__(self, k):
        pass

    def k_means_on(self, random_state=0):
        pass
