# generate CSVs
import csv
import psycopg

CONN = psycopg.connect("dbname=tipitaka user=alee")

# with CONN.cursor as cur:
#     sql = ''' '''

##### convert suttas.psv to suttas.csv
def suttas_to_csv(infilename):
    with open(infilename, "r", encoding="utf-8") as infile:
        reader = csv.DictReader(infile, delimiter=",")
        for row in reader:
            csv_line = ""
            if row["nikaya"] in ["AN", "SN"]:
                csv_line = f'{row["identifier"]},{row["nikaya"]} {row["book_number"]}.{row["vagga"]}, {row["title"]}, {row["subtitle"]}'
            elif row["nikaya"] in ["MN", "DN"]:
                csv_line = f'{row["identifier"]},{row["nikaya"]} {row["book_number"]}, {row["title"]}, {row["subtitle"]}'
            else:
                csv_line = f'{row["identifier"]},{row["vagga"]} {row["book_number"]}, {row["title"]}, {row["subtitle"]}'
            print(csv_line)

if __name__ == "__main__":
    suttas_to_csv("./graph/entities/kn_suttas.csv")