import psycopg

conn = psycopg.connect("dbname=tipitaka user=alee")

def normalized(s):
    s = s.strip()
    return s.lower()

path = "./graph/entities/locs.txt"
insert = """INSERT INTO ati_entities (entity_type, canonical, normalized) VALUES (%s, %s, %s) """
with conn.cursor() as cur, open(path, 'r', encoding='utf-8') as f:
    for line in f:
        loc_name = line.strip()
        try:
            cur.execute(insert, ("LOC", loc_name, normalized(loc_name)))
        except psycopg.errors.UniqueViolation as e:
            print(f'got an {e}, continuing')
            continue
conn.commit()