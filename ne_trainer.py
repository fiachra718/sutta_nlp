# ne_bootstrap.py
from pathlib import Path
import random
import spacy
from spacy.pipeline import EntityRuler
from spacy.training.example import Example
from spacy.util import minibatch


from base import CorpusBuilder

TRAIN_FROM_SILVER = True   # set True if you want to fit a tiny NER on ruler labels
N_ITER = 20
BATCH_SIZE = 32
RANDOM_SEED = 0

MODEL_DIR = Path("models/sutta_ner-v1")

# Gazetteer seeds: (LABEL, PHRASE)
PERSON_GAZETTEER = [
    ("PERSON", "Sāriputta"), ("PERSON", "Sariputta"),
    ("PERSON", "Ānanda"), ("PERSON", "Ananda"),
    ("PERSON", "Mahā-Moggallāna"), ("PERSON", "Mahā Moggallāna"), ("PERSON", "Moggallāna"), ("PERSON", "Moggallana"),
    ("PERSON", "Rāhula"), ("PERSON", "Rahula"),
    ("PERSON", "Mahā-Kaccāna"), ("PERSON", "Mahā Kaccāna"), ("PERSON", "Mahakaccana"), ("PERSON", "Kaccāna"), ("PERSON", "Kaccana"),
    ("PERSON", "Mahā-Kotthita"), ("PERSON", "Mahā Kotthita"), ("PERSON", "Maha Kotthita"), ("PERSON", "Kotthita"),
    ("PERSON", "Anuruddha"),
    ("PERSON", "Upāli"), ("PERSON", "Upali"),
    ("PERSON", "Saccaka"),
    ("PERSON", "Assalāyana"), ("PERSON", "Assalayana"),
    ("PERSON", "Vacchagotta"),
    ("PERSON", "Aggivessana"),  # epithet used in dialogues with Vacchagotta
    ("PERSON", "Bāhiya"), ("PERSON", "Bahiya"),
    ("PERSON", "Devadatta"),
    ("PERSON", "King Pasenadi"), ("PERSON", "Pasenadi"),
    ("PERSON", "Bimbisāra"), ("PERSON", "Bimbisara"),
    ("PERSON", "Queen Mallikā"), ("PERSON", "Mallikā"), ("PERSON", "Mallika"),
    ("PERSON", "Sāti"), ("PERSON", "Sati"),
    ("PERSON", "Dhanañjānī"), ("PERSON", "Dhanañjani"),
    ("PERSON", "Anāthapiṇḍika"), ("PERSON", "Anathapindika"),
    ("PERSON", "Visākhā"), ("PERSON", "Visakha"),
    ("PERSON", "Citta"),  # householder
    ("PERSON", "Hatthaka Āḷavaka"), ("PERSON", "Hatthaka of Ālavi"), ("PERSON", "Hatthaka of Alavi"),
    ("PERSON", "Aṅgulimāla"), ("PERSON", "Angulimala"),
    ("PERSON", "Ratthapāla"), ("PERSON", "Ratthapala"),
    ("PERSON", "Cunda"),
    ("PERSON", "Sela"),
    ("PERSON", "Potaliya"),
    ("PERSON", "Kandaraka"),
    ("PERSON", "Soṇa Koḷivisa"), ("PERSON", "Sona Kolivisa"),
    ("PERSON", "Sunakkhatta"),
    ("PERSON", "Udayin"), ("PERSON", "Udāyin"),
    ("PERSON", "Sahampati Brahmā"), ("PERSON", "Brahmā Sahampati"), ("PERSON", "Brahma Sahampati"),
    ("PERSON", "Māra"), ("PERSON", "Mara"),
    ("PERSON", "Sakka"),
    ("PERSON", "Gotama"), ("PERSON", "Master Gotama"),
]

GPE_GAZETTEER = [
    ("GPE", "Sāvatthī"), ("GPE", "Savatthi"),
    ("GPE", "Rājagaha"), ("GPE", "Rajagaha"), ("GPE", "Rajgir"),
    ("GPE", "Vesālī"), ("GPE", "Vesali"),
    ("GPE", "Kosambī"), ("GPE", "Kosambi"),
    ("GPE", "Kapilavatthu"), ("GPE", "Kapilavatthu"),  # duplicate forms often appear; keep as-is if in your texts
    ("GPE", "Campā"), ("GPE", "Campa"),
    ("GPE", "Mithilā"), ("GPE", "Mithila"),
    ("GPE", "Bārāṇasī"), ("GPE", "Benares"), ("GPE", "Vārāṇasī"), ("GPE", "Varanasi"),
    ("GPE", "Ukkatthā"), ("GPE", "Ukkattha"),
    ("GPE", "Sāketa"), ("GPE", "Saketa"),
    ("GPE", "Pātaliputta"), ("GPE", "Pataliputta"),
    ("GPE", "Nālandā"), ("GPE", "Nalanda"),
    ("GPE", "Avantī"), ("GPE", "Avanti"),
    ("GPE", "Magadha"),
    ("GPE", "Kosala"),
    ("GPE", "Videha"),
    ("GPE", "Kuru"),
    ("GPE", "Malla"),
    ("GPE", "Vajjī"), ("GPE", "Vajji"),
    ("GPE", "Aṅga"), ("GPE", "Anga"),
    ("GPE", "Kāsi"), ("GPE", "Kasi"),
    ("GPE", "Āḷavī"), ("GPE", "Alavi"),
    ("GPE", "Pāvā"), ("GPE", "Pava"),
    ("GPE", "Kusinārā"), ("GPE", "Kusinara"),
    ("GPE", "Uruvelā"), ("GPE", "Uruvela"),
    ("GPE", "Gayā"), ("GPE", "Gaya"),
]

LOC_GAZETTEER = [
    # Parks/monasteries/compounds
    ("LOC", "Jetavana"), ("LOC", "Jeta's Grove"), ("LOC", "Anāthapiṇḍika’s Park"), ("LOC", "Anathapindika's Park"),
    ("LOC", "Veḷuvana"), ("LOC", "Veluvana"), ("LOC", "Bamboo Grove"),
    ("LOC", "Pubbārāma"), ("LOC", "Pubbarama"), ("LOC", "Eastern Park"), ("LOC", "East Park"),
    ("LOC", "Nigrodhārāma"), ("LOC", "Nigrodharama"), ("LOC", "Banyan Grove"),
    ("LOC", "Jīvakambavana"), ("LOC", "Jivaka’s Mango Grove"), ("LOC", "Jivaka's Mango Grove"),
    ("LOC", "Ambapālivana"), ("LOC", "Ambapali's Mango Grove"),
    ("LOC", "Gosinga Sālavana"), ("LOC", "Gosinga Sal Tree Wood"), ("LOC", "Gosinga Sal Grove"),
    ("LOC", "Migāramātu’s Monastery"), ("LOC", "Migaramatu’s Monastery"), ("LOC", "Migāramātupāsāda"), ("LOC", "Migaramatupasada"),
    ("LOC", "Mahāvana"), ("LOC", "Great Wood"),
    ("LOC", "Kūṭāgārasālā"), ("LOC", "Kutagārasālā"), ("LOC", "Kutagarasala"), ("LOC", "Hall with the Peaked Roof"),

    # Mountains & sites
    ("LOC", "Gijjhakūṭa"), ("LOC", "Gijjhakuta"), ("LOC", "Vulture Peak"),
    ("LOC", "Isipatana"), ("LOC", "Deer Park"),
    ("LOC", "Cālikapabbata"), ("LOC", "Calika Rock"), ("LOC", "Calika Mountain"),

    # Rivers
    ("LOC", "Nerañjarā"), ("LOC", "Neranjara"),
    ("LOC", "Aciravatī"), ("LOC", "Ajiravati"),
    ("LOC", "Gaṅgā"), ("LOC", "Ganga"), ("LOC", "Ganges"),
    ("LOC", "Rohiṇī"), ("LOC", "Rohini"),
]


def load_base_nlp():
    # defensive boilerplate
    try:
        return spacy.load("en_core_web_sm")
    except Exception:
        nlp = spacy.blank("en")
        if "sentencizer" not in nlp.pipe_names:
            nlp.add_pipe("sentencizer")
        return nlp


def add_entity_ruler(nlp, pairs):
    # add the gazetteer datato this ruler
    if "entity_ruler" in nlp.pipe_names:
        nlp.remove_pipe("entity_ruler")
    ruler = nlp.add_pipe("entity_ruler", config={"overwrite_ents": True})
    pats = []
    for label, phrase in pairs:
        if not phrase:
            continue
        pats.append({"label": label, "pattern": phrase})
        # also add a case-insensitive-ish token pattern
        toks = phrase.split()
        if any(ch.isupper() for ch in phrase):
            pats.append({"label": label, "pattern": [{"LOWER": t.lower()} for t in toks]})
    ruler.add_patterns(pats)
    return ruler


def to_examples_for_training(nlp, ruler, texts):
    """Create silver Examples using ruler hits (skip docs with no ents)."""
    examples = []
    for t in texts:
        doc = nlp.make_doc(t)
        doc = ruler(doc)  # run only the ruler
        if not doc.ents:
            continue
        ents = [(e.start_char, e.end_char, e.label_) for e in doc.ents]
        examples.append(Example.from_dict(nlp.make_doc(t), {"entities": ents}))
    return examples


def train_ner_from_silver(examples, n_iter=N_ITER, batch_size=BATCH_SIZE, seed=RANDOM_SEED):
    if not examples:
        return None
    random.seed(seed)
    nlp = spacy.blank("en")
    nlp.add_pipe("sentencizer")
    ner = nlp.add_pipe("ner")
    # collect labels
    for eg in examples:
        for ent in eg.reference.ents:
            ner.add_label(ent.label_)
    nlp.initialize(lambda: examples)
    for i in range(n_iter):
        random.shuffle(examples)
        losses = {}
        for batch in minibatch(examples, size=batch_size):
            nlp.update(batch, losses=losses)
        if (i + 1) % max(1, n_iter // 5) == 0 or i == 0:
            print(f"[train] iter {i+1}/{n_iter} losses={losses}")
    return nlp


def extract_entities(nlp, texts):
    """Return per-doc list of (LABEL, TEXT)."""
    out = []
    for t in texts:
        doc = nlp(t)
        out.append([(ent.label_, ent.text) for ent in doc.ents])
    return out


def main(conn, sql):
    # 1) build from DB
    builder = CorpusBuilder(conn, sql)

    # try to get aligned meta + texts regardless of builder’s internals
    # - texts: list of raw_text
    # - meta:  list of dicts with doc_id/identifier/title
    texts = list(builder)  # CorpusBuilder is usually iterable over raw_text
    meta = []

    # best-effort metadata extraction
    try:
        # many builders keep rows with these fields
        for row in builder.rows:  # type: ignore[attr-defined]
            meta.append({
                "doc_id": row.get("doc_id"),
                "identifier": row.get("identifier"),
                "title": row.get("title"),
            })
    except Exception:
        # fallback: just use indices / doc_ids if available
        try:
            doc_ids = list(builder.doc_ids)  # type: ignore[attr-defined]
            for i, did in enumerate(doc_ids):
                meta.append({"doc_id": did, "identifier": str(did), "title": None})
        except Exception:
            # final fallback
            for i in range(len(texts)):
                meta.append({"doc_id": i, "identifier": f"doc-{i}", "title": None})

    # 2) base nlp + ruler
    # just defensive code for loading small en model
    base_nlp = load_base_nlp()
    ruler = add_entity_ruler(base_nlp, PERSON_GAZETTEER + LOC_GAZETTEER + GPE_GAZETTEER)


    # 3) (if set) train small NER from silver labels
    # our silver examples are oh-so-bespoke
    if TRAIN_FROM_SILVER:
        silver = to_examples_for_training(base_nlp, ruler, texts)
        if silver:
            ner_nlp = train_ner_from_silver(silver)

            # put the ruler back in front for high-precision gazetteer hits
            add_entity_ruler(ner_nlp, PERSON_GAZETTEER + LOC_GAZETTEER + GPE_GAZETTEER)
            # and, voila, we have a new nlp
            nlp = ner_nlp

            # --- SAVE THE MODEL ---
            MODEL_DIR.mkdir(parents=True, exist_ok=True)
            # Saving the entire pipeline (includes the entity_ruler patterns)
            nlp.to_disk(MODEL_DIR)
            # also save just the ruler patterns as a separate artifact
            nlp.get_pipe("entity_ruler").to_disk(MODEL_DIR / "ruler_patterns")

            # save config for reproducibility
            nlp.config.to_disk(MODEL_DIR / "config.cfg")
        
        else:
            print("No silver labels found; using base pipeline.")
            nlp = base_nlp
    else:
        # no training step
        nlp = base_nlp

    # 4) inference
    ents_per_doc = extract_entities(nlp, texts)

    # 5) print tuples (LABEL, TEXT) per doc — ready for clustering use
    for i, ents in enumerate(ents_per_doc):
        m = meta[i] if i < len(meta) else {"identifier": f"doc-{i}", "title": None}
        ident = m.get("identifier") or f"doc-{i}"
        title = m.get("title") or ""
        print(f"\n=== {ident} {title}".strip())
        if not ents:
            print("(no entities)")
        else:
            # EXACT format you requested: (entity type, named entity)
            for lbl, txt in ents:
                print((lbl, txt))


if __name__ == "__main__":
    '''
    created a view --
        CREATE OR REPLACE VIEW ati_paragraphs_long AS
            SELECT
            s.id AS sutta_id,
            s.identifier,
            s.title,
            e.elem->>'text' AS text,
            e.ord::int AS para_seq
            FROM ati_suttas s
            CROSS JOIN LATERAL jsonb_array_elements(s.verses) WITH ORDINALITY AS e(elem, ord)
            WHERE s.nikaya IN ('MN','SN','AN')
            AND char_length(e.elem->>'text') >= 300
            AND lower(e.elem->>'text') NOT LIKE 'see also:%';

        So, ati_paragraphs_long is JUST paragraphs from MN, SN, AN (4857 of them) that are at least
        300 characters long
    '''

    sql = """
        SELECT sutta_id AS id, identifier, title, text FROM ati_paragraphs_long;
    """
    import psycopg
    conn = psycopg.connect("dbname=tipitaka user=alee")
    # main will create a corpus via CorpusBuilder
    # then call train_ner_from_silver (if TRAIN_FROM_SILVER is set)
    # we then save tne trained nlp in main
    main(conn, sql)
