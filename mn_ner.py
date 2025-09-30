from typing import Iterable
import spacy
from base import CorpusBuilder
from collections import Counter, defaultdict
import psycopg
from local_settings import settings


conn = psycopg.connect("dbname=tipitaka user=alee")

KEEP = {"PERSON", "GPE", "ORG"}  # adjust to your domain
counts = Counter()
examples_by_ent = defaultdict(list)

nlp = spacy.load("en_core_web_md")


def iter_docs(corpus_iterable):  # yields raw texts
    for text in corpus_iterable:
        # yield whole doc if short, else split into chunks
        if len(text) <= 2000:
            yield (0, text)
        else:
            yield from chunk_text(text)

def batched_ner(corpus_iterable, batch_size=32, n_process=2):
    # pipe takes iterable, batch size, n_processes, as_tuples flag
    # iter_docs does the trick for iterable 
    for docs in nlp.pipe(
            (t for _, t in iter_docs(corpus_iterable)), batch_size=batch_size, n_process=n_process, as_tuples=False):
        yield docs

def chunk_text(text: str, max_len: int = 1500, overlap: int = 100):
    assert 0 <= overlap < max_len
    n = len(text)
    start = 0
    while start < n:
        end = min(start + max_len, n)
        yield start, text[start:end]
        if end == n:                       # <-- IMPORTANT break condition
            break
        start = end - overlap   


if __name__ == "__main__":
    sql = """SELECT doc_id, identifier, title, raw_text
        FROM suttas WHERE nikaya = 'Majjhima' and translator = 'Thanissaro Bhikkhu'"""

    corpus = CorpusBuilder(conn, sql)
    for doc in batched_ner(corpus):
        for ent in doc.ents:
            if ent.label_ in KEEP and ent.text.strip():
                norm = ent.text.strip()
                counts[norm] += 1
                if len(examples_by_ent[norm]) < 3:
                    examples_by_ent[norm].append(doc[ent.sent.start:ent.sent.end].text)

    # Look at the top candidates
    for ent, freq in counts.most_common(50):
        print(freq, ent, "— e.g.", examples_by_ent[ent][0])