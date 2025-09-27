settings = {
    "DB": {
        "NAME": "tipitaka",
        "USER": "alee",
        "HOST": "localhost",
        "PORT": "5432",
        "URL": "postgresql+psycopg2://alee:postgres@localhost:5432/tipitaka"
    },
    "TFIDF": {
        "strip_accents": "unicode",
        "lowercase": True,
        "ngram_range": (1, 2),
        "stop_words": "english",
        "sublinear_tf": True,
        "min_df": 10,
        "max_df": 0.85,
        "dtype": "float32",
    },
    "BUNDLE":{
        "vectorizer": "vectorizer.joblib",
        "x_csr": "X.npz",
        "doc_index": "doc_index.json",
        "manifest": "manifest.json"
    }
}