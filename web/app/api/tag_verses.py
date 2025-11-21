# conceptually -- nab all the verses in
# the text field of ati_verses if the text length is > 1000
#  clean that shit up!
# tag it!
# take the tagged spans, cleaned text,  MD5 of the cleaned text, 
# write that back to the DB (update ati_verses and set
#   cleaned_text, cleaned_text_hash, ner_spans, updated_at
#
# THEN, present the results via a web view

import psycopg
import re
from hashlib import md5
from ner import run_ner
import json

def decruft_text(text):
    pattern = r'''
        ^\s*                             # leading whitespace
        (?:\{[^}]*\}\s*)?                # optional { II,iv,2 }-style citation
        (?:\[\s*\d+\s*\]\s*)?            # optional [ 1 ] footnote marker
        (?:\d+\s*\.\s*|\.\s*)?           # optional "7 . " or just ". "
        ["“”']*                          # optional leading quotes
    '''

    cleaned = re.sub(pattern, '', text, flags=re.VERBOSE)
    cleaned = re.sub(r'\s+', ' ', cleaned)   # collapse runs of whitespace
    cleaned = cleaned.strip()
    return cleaned

SQL = '''select id, text from ati_verses'''

CONN = psycopg.connect("dbname=tipitaka user=alee")
with CONN:
    with CONN.cursor() as cur:
        cur.execute(SQL)
        rows = cur.fetchall()
        count = 0 
        for verse_id, verse_text in rows:
            # decruft this
            cleaned = decruft_text(verse_text)
            cleaned_hash = md5(cleaned.encode()).hexdigest()
            ner_data = run_ner(cleaned)
            spans = ner_data.get("spans", [])  
            s_spans = json.dumps(spans)
            ner_span_hash = md5(s_spans.encode()).hexdigest()
            try:
                cur.execute(
                    """UPDATE 
                        ati_verses
                    SET 
                        ner_span = %s, 
                        ner_span_hash = %s, 
                        cleaned_text = %s, 
                        cleaned_text_hash = %s 
                    WHERE 
                        id = %s""",
                        (s_spans, ner_span_hash, cleaned, cleaned_hash, verse_id),
                )
            except psycopg.errors.UniqueViolation as e:
                print(e)
                continue
            except psycopg.DatabaseError as e:
                print(e)
                break

            assert(cur.rowcount == 1)
            count += 1
        print(f"{count} records processed")
