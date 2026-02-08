import psycopg
from psycopg.rows import dict_row
from collections import defaultdict
from collections import Counter


alias_index = build_alias_index(load_alias_rows_from_pg())

for verse_id, gid, sutta_ref, verse_num, text, ner_span in verse_generator():
    for span in ner_span:
        entity_type = span["label"]      # or however your JSON stores it
        surface = span["text"]

        span_norm = norm(surface)

        status, entity_norm, candidates = resolve_span(alias_index, entity_type, span_norm)

        if status == "resolved":
            # MERGE entity node using (entity_type, entity_norm)
            # MERGE mention edge from verse(gid) to entity(...)
            pass
        else:
            # don't create junk nodes; log it for later cleanup
            pass