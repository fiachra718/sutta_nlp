import re
from collections import Counter
import numpy as np
import plotly.graph_objects as go
import psycopg
from psycopg.rows import dict_row
import string
# for png
import matplotlib.pyplot as plt

plt.style.use('_mpl-gallery')
HAS_LETTER = re.compile(r"[^\W\d_]", re.UNICODE)
connection = psycopg.connect("dbname=tipitaka user=alee", row_factory=dict_row)


def canon_count():
    SQL = """SELECT text from ati_verses ORDER BY identifier """
    counter = Counter()

    with connection.cursor() as cur:
        cur.execute(SQL)
        for row in cur.fetchall():
            text = row.get('text')
            text.strip(string.punctuation)
            words = text.split()
            for w in words:
                w = w.strip(string.punctuation)
                if HAS_LETTER.search(w):
                    counter[w] += 1

    freqs = sorted(counter.items(), key=lambda x: x[1], reverse=True)
    N = len(freqs)
    w_ranks = np.arange(1, N + 1)
    w_freqs = np.array([f for _, f in freqs])
    words = [w for w, _ in freqs]

    return w_ranks, w_freqs, words


def ner_counts():
    SQL = '''SELECT
    ner_span::jsonb AS ner_span
    FROM ati_verses
    WHERE ner_span IS NOT NULL
    AND jsonb_array_length(ner_span::jsonb) > 0;'''

    ner_counter = Counter()

    with connection.cursor() as cur:
        cur.execute(SQL)
        for row in cur.fetchall():
            for span in row.get('ner_span'):
                # token = json.loads(word)
                if span['label'] in ('PERSON', 'GPE', 'LOC'):
                    # word = span['text'].strip(string.punctuation)
                    # ner_counter[word.lower()] +=1
                    ner_counter[span['text']] += 1
                        
    ner_freqs = sorted(ner_counter.items(), key=lambda x: x[1], reverse=True)
    ner_freqs = [
        (w, f) for w, f in ner_freqs
        if HAS_LETTER.search(w)
    ]

    # --- vectors ---
    N = len(ner_freqs)
    ranks = np.arange(1, N + 1)
    freqs = np.array([f for _, f in ner_freqs])
    words = [w for w, _ in ner_freqs]

    return ranks, freqs, words


def plotly_figure(ranks, freqs, words):
    fig = go.Figure()
    colors = np.log10(freqs)

    fig.add_trace(
        go.Scatter(
            x=ranks,
            y=freqs,
            mode="markers",
            marker=dict(
                size=4,
                color=colors,
                colorscale="Viridis",
                showscale=True,
                colorbar=dict(title="log10(freq)")
            ),
            text=words,
            hovertemplate="Rank %{x}<br>Freq %{y}<br>%{text}<extra></extra>"
        )
    )

    fig.update_layout(
        title="Zipf's Law - ATI Corpus",
        xaxis_title="Word rank",
        yaxis_title="Word frequency",
        xaxis_type="log",
        yaxis_type="log",
        width=1000,
        height=650
    )


    fig.show()
    fig.write_html("ati_zipf_interactive.html")


def matplotlib_figure(ranks, freqs, words):
    plt.figure(figsize=(16, 10))
    plt.loglog(ranks, freqs, marker='.', linestyle='none')
    for i in range(0, len(ranks), 100):
        plt.annotate(
            words[i],
            (ranks[i], freqs[i]),
            textcoords="offset points",
            xytext=(3, 3),
            fontsize=7,
            alpha=0.7
        )

    plt.xlabel("Rank")
    plt.ylabel("Frequency")
    plt.title("Zipf's Law – ATI Corpus")
    label_idx = np.unique(np.round(np.geomspace(1, len(freqs), num=60)).astype(int)) - 1
    for i in label_idx:
        plt.annotate(words[i], (ranks[i], freqs[i]),
                    textcoords="offset points", xytext=(3, 3),
                    fontsize=7, alpha=0.7)


    plt.tight_layout()

    # plt.show()
    plt.savefig("ati_zipf.svg")
    plt.savefig("ati_zipf.png", dpi=200)

def overlay_plotly(w_ranks, w_freqs, w_words, ner_ranks, ner_freqs, ner_words):
    fig = go.Figure()

    # Whole corpus tokens (background)
    fig.add_trace(go.Scatter(
        x=w_ranks, y=w_freqs,
        mode="markers",
        marker=dict(size=3),
        opacity=0.25,
        text=w_words,
        hovertemplate="TOKEN<br>Rank %{x}<br>Freq %{y}<br>%{text}<extra></extra>",
        name="All tokens"
    ))

    # Named-entity phrases (foreground)
    fig.add_trace(go.Scatter(
        x=ner_ranks, y=ner_freqs,
        mode="markers",
        marker=dict(size=7, symbol="diamond"),
        opacity=0.95,
        text=ner_words,
        hovertemplate="ENTITY PHRASE<br>Rank %{x}<br>Freq %{y}<br>%{text}<extra></extra>",
        name="Named entities (phrases)"
    ))

    fig.update_layout(
        title="Zipf comparison: Whole-corpus tokens vs Named-entity phrases",
        xaxis_title="Rank",
        yaxis_title="Frequency",
        xaxis_type="log",
        yaxis_type="log",
        width=1100,
        height=700
    )

    fig.show()

if __name__ == '__main__':
    ner_ranks, ner_freqs, ner_words = ner_counts()
    matplotlib_figure(ner_ranks, ner_freqs, ner_words)
    plotly_figure(ner_ranks, ner_freqs, ner_words)
    
    w_ranks, w_freqs, w_words = canon_count()
    matplotlib_figure(w_ranks, w_freqs, w_words)
    plotly_figure(w_ranks, w_freqs, w_words)

    # grand daddy
    overlay_plotly(w_ranks, w_freqs, w_words, 
                   ner_ranks, ner_freqs, ner_words)
    

