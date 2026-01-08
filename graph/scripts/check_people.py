import psycopg

conn = psycopg.connect("dbname=tipitaka user=alee")

other_names = set()
SQL = ''' select e.canonical, ea.alias from 
    ati_entities as e LEFT JOIN ati_entity_aliases as ea 
    ON ea.entity_id = e.id 
    WHERE e.canonical = %s
    OR ea.alias = %s;''' 
with open("./graph/entities/persons.txt") as f, conn.cursor() as cur:
    for line in f.readlines():
        name = line.strip()
        try:
            cur.execute(SQL, (name, name))
        except psycopg.DatabaseError as e:
            print(e)
            continue
        rows = cur.fetchall()
        if not rows:
            print(f"no match for {name}")
            other_names.add(name)
        else:
            for canonical, alias in rows:
                print(f"{canonical}")
                print(f"\t{alias}")
    
for name in sorted(other_names):
    print(f"What's this?  {name}")
