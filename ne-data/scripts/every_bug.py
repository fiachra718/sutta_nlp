import spacy
from local_settings import load_model

nlp = spacy.load("en_sutta_ner")
# nlp = load_model()
print("META:", nlp.meta["name"], nlp.meta["version"], "PATH:", nlp.path)
print("PIPES:", nlp.pipe_names)

text1 = "\"Any consciousness whatsoever that is past, future, or present; internal or external; blatant or subtle; common or sublime; far or near: Every consciousness is to be seen with right discernment as it has come to be: 'This is not mine. This is not my self. This is not what I am.'"

doc1 = nlp(text1)
print("ENTS 1:", [(ent.text, ent.start_char, ent.end_char, ent.label_) for ent in doc1.ents])

text2 = "\"Furthermore, when going forward & returning, he makes himself fully alert; when looking toward & looking away... when bending & extending his limbs... when carrying his outer cloak, his upper robe & his bowl... when eating, drinking, chewing, & savoring... when urinating & defecating... when walking, standing, sitting, falling asleep, waking up, talking, & remaining silent, he makes himself fully alert. And as he remains thus heedful, ardent, & resolute, any memories & resolves related to the household life are abandoned, and with their abandoning his mind gathers & settles inwardly, grows unified & centered. This is how a monk develops mindfulness immersed in the body."

doc2 = nlp(text2)
print("ENTS 2:", [(ent.text, ent.start_char, ent.end_char, ent.label_) for ent in doc2.ents])

text3 = "Trivial thoughts, subtle thoughts, Mental jerkings that follow one along: Not understanding these mental thoughts, One runs back and forth with wandering mind. But having known these mental thoughts, The ardent and mindful one restrains them. An awakened one has entirely abandoned them, These mental jerkings that follow one along."

doc3 = nlp(text3)
print("ENTS 3:", [(ent.text, ent.start_char, ent.end_char, ent.label_) for ent in doc3.ents])