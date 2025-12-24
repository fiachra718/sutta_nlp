# generate CSVs
import csv
import psycopg

CONN = psycopg.connect("dbname=tipitaka user=alee")

# with CONN.cursor as cur:
#     sql = ''' '''

##### convert suttas.psv to suttas.csv
def suttas_to_csv(filename):
    with open(filename, "r", encoding="utf-8") as infile:
        reader = csv.DictReader(infile, delimiter="|")
        for row in reader:
            print(row)


if __name__ == "__main__":
    suttas_to_csv("./graph/entities/suttas.psv")