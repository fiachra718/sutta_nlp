#!/usr/bin/env python3
import psycopg, re, json, unicodedata
from psycopg.rows import dict_row

# --- built-in gazetteer ---
GAZETTEER = {
    "PERSON": [
        "Ānanda","Sāriputta","Sariputta","Moggallāna","Moggallana","Mahākassapa","Mahākaccāna",
        "Mahākaccana","Mahākotthita","Kaccāna","Māra","Buddha","Gotama",
        "King Pasenadi","Visākhā","Pajāpati","Subrahma","Paramatta","Tissa",
        "Sanankumara","Cunda","Anuruddha","Devadatta","Brahma","Sakka",
        "Blessed One","Acela-Kassapa","Naked Kassapa","King Pasenadi Kosala",
        "Pasenadi","Ven. Sariputta","Dhanañjanin","King Seniya Bimbisāra",
        "Bimbisāra","Tathāgata","Purana Kassapa","Aggivessana",
        "Saccaka the Nigaṇṭha-son","Lohicca","Ambapali","Ajatasattu",
        "Dhatarattha","Virulhaka","Virupakkha","Kuvera","Sunidha",
        "Vassakara","Tathagata","Anathapindika","Rahula",
        "Sāti the Fisherman's Son","Dhammadinna","Visakha",
        "Potthapada","Queen Mallika","Makkhali Gosāla","Ajita Kesakambalin",
        "Pakudha Kaccāyana","Sañjaya Velaṭṭhaputta","Nigaṇṭha Nāṭaputta",
        "Kasi Bharadvaja","Queen Videha","Ven. Bhumija","Punna Mantaniputta",
    ],
    "GPE": [
        "Sāvatthī","Rājagaha","Vesālī","Kosambī","Kapilavatthu",
        "Avanti","Nālandā","Kāsī","Kosala","Kesaputta","Veluvana",
        "kingdom of Magadha","Magadha","Saketa","Upavattana",
        "Kusinara","Bhandagama","Pataligama","Benares","Vesali",
        "Salavatika"
    ],
    "LOC": [
        "Jeta’s Grove","Veluvana","Gosinga Sālavana",
        "Sword-leaf Forest","Bamboo Grove","Pubbārāma","Migāramātu’s Monastery",
        "Hiraññavati River","Vultures' Peak","Banyan Grove","Robbers' Cliff",
        "Sattapanni Cave","Vebhara Mountain","Squirrels' Feeding-ground","Jivaka's Mango Grove",
        "Small Nook","Deer Park","Anathapindika's Monastery","Ghosita's monastery",
        "Southern Mountains","Squirrels' Sanctuary","Crocodile Haunt"
    ],
    "NORP": [
        "Maras", "Brahmas", "Yama gods", "gods of the Thirty-three","Thirty-three gods",
        "Four Great Kings","Licchavis","Ajivakas"
    ]
}

def strip_diacritics(s):
    return "".join(ch for ch in unicodedata.normalize("NFD", s)
                   if not unicodedata.combining(ch))

def variants(term):
    plain = strip_diacritics(term)
    return {term, plain, term.replace("’","'"), plain.replace("’","'")}

def build_regex(term):
    pats = [re.escape(v) for v in variants(term)]
    return re.compile(rf"\b({'|'.join(pats)})\b", flags=re.UNICODE)

def find_spans(text, gaz):
    spans = []
    for label, terms in gaz.items():
        for t in terms:
            for m in build_regex(t).finditer(text):
                spans.append((m.start(), m.end(), label))
    spans = sorted(set(spans), key=lambda x:(x[0],-(x[1]-x[0])))
    keep=[]
    for s in spans:
        if not any(not (s[1]<=k[0] or s[0]>=k[1]) for k in keep):
            keep.append(s)
    return keep

def main():
    dsn = "dbname=tipitaka user=alee"
    out_path = "candidates.jsonl"
    nikayas = ["DN","MN","AN","SN"]

    sql = """
      SELECT nikaya, identifier, title,
             (j.elem->>'text') AS text,
             (j.elem->>'seq')::int AS para_seq
        FROM ati_suttas s,
             LATERAL jsonb_array_elements(s.verses) AS j(elem)
       WHERE s.nikaya IN ('DN','MN','AN','SN')
         AND length(j.elem->>'text') > 200
         AND translator = 'Thanissaro Bhikkhu'
       ORDER BY identifier, para_seq;
    """

    wrote = 0
    with psycopg.connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur, open(out_path,"w",encoding="utf-8") as fout:
        cur.execute(sql)
        for row in cur:
            text = row["text"]
            ents = find_spans(text, GAZETTEER)
            if not ents:
                continue
            rec = {
                "text": text,
                "entities": [[s,e,lbl] for s,e,lbl in ents],
                "meta": {
                    "nikaya": row["nikaya"],
                    "identifier": row["identifier"],
                    "title": row["title"],
                    "para_seq": row["para_seq"]
                }
            }
            fout.write(json.dumps(rec, ensure_ascii=False)+"\n")
            wrote += 1
    print(f"!! wrote {wrote} candidate paragraphs → {out_path}")

if __name__ == "__main__":
    main()