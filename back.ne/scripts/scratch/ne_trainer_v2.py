import spacy
from spacy.pipeline import EntityRuler
from pathlib import Path

MODEL_IN  = Path("ne-data/models/archive/sutta_ner-v1")
MODEL_OUT = Path("ne-data/models/archive/sutta_ner-v2")


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

def add_literal_and_ci_patterns(ruler, pairs):
    pats = []
    for label, phrase in pairs:
        if not phrase:
            continue
        # exact literal
        pats.append({"label": label, "pattern": phrase})
        # case-insensitive token pattern
        toks = phrase.split()
        pats.append({"label": label, "pattern": [{"LOWER": t.lower()} for t in toks]})
    ruler.add_patterns(pats)


def add_honorific_patterns(ruler):
    honorifics = [
        ["ven", "."], ["venerable"], ["bhikkhu"], ["thera"], ["master"]
    ]
    king_titles = [["king"], ["queen"]]

    pats = []

    # Generic honorific + Name (one or more Title-cased tokens), optional possessive "'s"
    for hon in honorifics:
        tokpat = [{"LOWER": hon[0]}]
        if len(hon) == 2 and hon[1] == ".":
            tokpat.append({"ORTH": "."})
        tokpat.append({"IS_TITLE": True, "OP": "+"})
        tokpat.append({"ORTH": "'s", "OP": "?"})
        pats.append({"label": "PERSON", "pattern": tokpat})

    # Specific “Master Gotama” (catch “Master Gotama’s” too)
    pats.append({"label": "PERSON", "pattern": [{"LOWER": "master"}, {"LOWER": "gotama"}, {"ORTH": "'s", "OP": "?"}]})

    # Kings/Queens + Name (often followed by ‘of …’, but we just capture the name)
    for kt in king_titles:
        tokpat = [{"LOWER": kt[0]}, {"IS_TITLE": True, "OP": "+"}, {"ORTH": "'s", "OP": "?"}]
        pats.append({"label": "PERSON", "pattern": tokpat})

    ruler.add_patterns(pats)


def add_opener_patterns(ruler):
    pats = []

    # "At Sāvatthī …" → label the place after "at" as GPE
    pats.append({
        "label": "GPE",
        "pattern": [
            {"LOWER": "at"},
            {"IS_TITLE": True, "OP": "+"}  # capture "Sāvatthī", "Rājagaha", possibly multi-token
        ]
    })

    # “dwelling / staying / residing at Jetavana …” → LOC
    for verb in ["dwelling", "staying", "residing", "living", "abiding"]:
        pats.append({
            "label": "LOC",
            "pattern": [
                {"LOWER": verb},
                {"LOWER": {"IN": ["at", "in"]}},
                {"IS_TITLE": True, "OP": "+"}  # e.g., Jetavana, Jeta's Grove, Bamboo Grove
            ]
        })

    # “… in Jeta’s Grove / Bamboo Grove …” (generic in/at + TitleCase+)
    pats.append({
        "label": "LOC",
        "pattern": [
            {"LOWER": {"IN": ["in", "at"]}},
            {"IS_TITLE": True, "OP": "+"}
        ]
    })

    # “of Kosala” often follows "King Pasenadi"
    pats.append({
        "label": "GPE",
        "pattern": [{"LOWER": "of"}, {"IS_TITLE": True, "OP": "+"}]
    })

    ruler.add_patterns(pats)

if __name__ == "__main__":
    # nab the model we ran with last time
    nlp = spacy.load(MODEL_IN)

    # ensure ruler is first so it can “protect” precise spans
    if "entity_ruler" in nlp.pipe_names:
        nlp.remove_pipe("entity_ruler")
    ruler = nlp.add_pipe("entity_ruler", first=True, config={"overwrite_ents": True})
    
    # add in some new rules
    add_opener_patterns(ruler)
    add_honorific_patterns(ruler)
    add_literal_and_ci_patterns(ruler, PERSON_GAZETTEER + GPE_GAZETTEER + LOC_GAZETTEER)
    
    # save it
    MODEL_OUT.mkdir(parents=True, exist_ok=True)
    nlp.to_disk(MODEL_OUT)  # saves the entire pipeline incl. the new ruler

    # also keep the ruler patterns as a standalone artifact for version control
    ruler.to_disk(MODEL_OUT / "ruler_patterns")
    nlp.config.to_disk(MODEL_OUT / "config.cfg")

    print(f"Saved updated pipeline to {MODEL_OUT}")
