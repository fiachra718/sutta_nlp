import spacy
from spacy.pipeline import EntityRuler

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


def make_nlp_with_ruler(seed_tuples, model="en_core_web_sm"):
    # seed_tuples: list of (label, text), e.g. ("PERSON","Sariputta")
    try:
        nlp = spacy.load(model)
    except OSError:
        # minimal fallback if the medium model isn’t available
        nlp = spacy.blank("en")
        nlp.add_pipe("tok2vec", config={"model": {"@architectures":"spacy.Tok2Vec.v2"}})  # optional

    ruler = nlp.add_pipe("entity_ruler", config={"overwrite_ents": False})
    patterns = [{"label": lab, "pattern": txt} for lab, txt in seed_tuples]
    ruler.add_patterns(patterns)
    return nlp, ruler

if __name__ == '__main__':
    nlp, ruler = make_nlp_with_ruler(PERSON_GAZETTEER + LOC_GAZETTEER, model="en_core_web_md")
    