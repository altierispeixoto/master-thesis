from argparse import ArgumentParser
from sparketl import ETLSpark
etlspark = ETLSpark()

parser = ArgumentParser()
parser.add_argument("-q", "--query", dest="query",
                    help="sql query", metavar="QUERY")

parser.add_argument("-f", "--folder", dest="folder",
                    help="folder", metavar="FOLDER")

args = parser.parse_args()
print(args.query)

target_path = "/data/processed/{}".format(args.folder)

df = etlspark.load_from_database(args.query)
etlspark.save(df, target_path, coalesce=1, format="csv")
