import psycopg

connection = psycopg.connect("user=alee dbname=tipitaka")

with open("./graph/entities/not_norp.txt") as f, connection.cursor() as cur:
    for line in f:
        bad_norp = line.strip()
        print(bad_norp)
    
        u_SQL = """
                UPDATE ati_verses av
                    SET ner_span = (
                    SELECT COALESCE(jsonb_agg(s), '[]'::jsonb)
                    FROM jsonb_array_elements(av.ner_span) AS s
                    WHERE NOT (s->>'label' = 'NORP' AND s->>'text' ILIKE %(bad)s)
                    )
                WHERE EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(av.ner_span) AS s
                    WHERE s->>'label' = 'NORP' AND s->>'text' ILIKE  %(bad)s
                );
        """
        SQL = '''
            SELECT DISTINCT s->>'text'
            FROM ati_verses av
            CROSS JOIN LATERAL jsonb_array_elements(av.ner_span) AS s
            WHERE s->>'label' = 'NORP'
            AND s->>'text' ILIKE %(bad)s ;
        '''
        cur.execute(SQL, {"bad": bad_norp})
        for res in cur.fetchall():
            print(res)
            cur.execute(u_SQL, {"bad": bad_norp})
            if cur.rowcount:
                print(f"{bad_norp!r}: updated {cur.rowcount} verses")
    connection.commit()
