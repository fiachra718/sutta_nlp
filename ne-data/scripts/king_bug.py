import spacy
from local_settings import load_model
# text = """Yes, it is, great king. But first, with regard to that, I will ask you a counter-question. Answer however you please. Suppose there were a man of yours: your slave, your workman, rising in the morning before you, going to bed in the evening only after you, doing whatever you order, always acting to please you, speaking politely to you, always watching for the look on your face. The thought would occur to him: 'Isn't it amazing? Isn't it astounding? — the destination, the results, of meritorious deeds. For this King Ajatasattu is a human being, and I, too, am a human being, yet King Ajatasattu enjoys himself supplied and replete with the five strings of sensuality — like a deva, as it were — while I am his slave, his workman... always watching for the look on his face. I, too, should do meritorious deeds. What if I were to shave off my hair and beard, put on the ochre robes, and go forth from the household life into homelessness?"""
text = """Thus the Blessed One answered, having been asked by Sakka the deva-king. Gratified, Sakka was delighted in & expressed his approval of the Blessed One's words: "So it is, O Blessed One. So it is, O One Well-gone. Hearing the Blessed One's answer to my question, my doubt is now cut off, my perplexity is overcome."""

# model from ./dist
# nlp = spacy.load("en_sutta_ner")

# from ./ne-data/work/models/DATE/
nlp = load_model()
print(nlp.pipe_names)

doc = nlp(text)
print("WITH full pipeline:")
for ent in doc.ents:
    print(repr(ent.text), ent.label_)

# Remove entity_ruler (if present) and re-run

# nlp_wo_ruler = spacy.load("en_sutta_ner")
# nlp_wo_ruler = load_model()
nlp.remove_pipe("entity_ruler")
doc2 = nlp(text)
print("\nWITHOUT entity_ruler:")
for ent in doc2.ents:
    print(repr(ent.text), ent.label_)

print("\nWITHOUT entity_ruler AND norp_head_ruler:")
with nlp.disable_pipes("norp_head_ruler"):
    doc3 = nlp(text)
    for ent in doc3.ents:
        print(repr(ent.text), ent.label_)
