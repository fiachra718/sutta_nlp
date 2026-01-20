from collections import Counter
import psycopg
from psycopg.rows import dict_row
import string
import re
import numpy as np

import matplotlib.pyplot as plt
plt.style.use('_mpl-gallery')


HAS_LETTER = re.compile(r'[^\W\d_]', re.UNICODE)

sql = ''' SELECT identifier, nikaya, book_number, vagga, text from ati_verses ORDER BY identifier'''

word_counter = Counter()

connection = psycopg.connect("dbname=tipitaka user=alee", row_factory=dict_row)
with connection.cursor() as cur:
    cur.execute(sql)
    for row in cur.fetchall():
        for word in row.get('text').split():
            word = word.strip(string.punctuation)
            word_counter[word.lower()] +=1


# print(word_counter.total())
#for w in word_counter.most_common()[:-5000:-1]:
# for c, w in enumerate(word_counter.most_common()):
#     print(c, w)

words_freqs = sorted(
    word_counter.items(),
    key=lambda x: x[1],
    reverse=True
)
# update to squash nums and punct-only
words_freqs = [
    (w, f) for w, f in words_freqs
    if HAS_LETTER.search(w)
]
# print(words_freqs[0:10])      
N = len(words_freqs)


ranks = range(1, len(words_freqs) + 1)
freqs = [freq for _, freq in words_freqs]
words = [word for word, _ in words_freqs]
import matplotlib.pyplot as plt

plt.figure(figsize=(16, 10))
plt.loglog(ranks, freqs, marker='.', linestyle='none')
# for i in range(0, len(ranks), 100):
#     plt.annotate(
#         words[i],
#         (ranks[i], freqs[i]),
#         textcoords="offset points",
#         xytext=(3, 3),
#         fontsize=7,
#         alpha=0.7
#     )
plt.xlabel("Rank")
plt.ylabel("Frequency")
plt.title("Zipf's Law – ATI Corpus")
label_idx = np.unique(np.round(np.geomspace(1, len(words_freqs), num=60)).astype(int)) - 1
for i in label_idx:
    plt.annotate(words[i], (ranks[i], freqs[i]),
                 textcoords="offset points", xytext=(3, 3),
                 fontsize=7, alpha=0.7)


plt.tight_layout()

plt.show()
plt.savefig("ati_zipf.svg")