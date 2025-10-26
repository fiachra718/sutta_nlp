from top2vec import Top2Vec
import psycopg

# connect to your DB
conn = psycopg.connect("postgresql://localhost/tipitaka?user=alee")

# grab some MN paragraphs (Thanissaro translations only, just a few to start)
rows = conn.execute("""
    SELECT b.body
    FROM ati_suttas s
    JOIN ati_sutta_body b ON b.identifier = s.identifier
    WHERE s.nikaya IN ('MN', 'AN', 'SN')
      AND s.translator = 'Thanissaro Bhikkhu'
""")

docs = [r[0] for r in rows]
print(f"Loaded {len(docs)} paragraphs.")

model = Top2Vec(
    docs,
    speed="learn",
    workers=4,
    min_count=1,
    umap_args={'n_neighbors': 2}
)

topic_words, _, topic_nums = model.get_topics()
for i, words in enumerate(topic_words[:5]):
    print(f"\nTopic {topic_nums[i]}: {' | '.join(words[:10])}")