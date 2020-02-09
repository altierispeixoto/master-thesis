from argparse import ArgumentParser
from sparketl import ETLSpark
etlspark = ETLSpark()

parser = ArgumentParser()
parser.add_argument("-q", "--query", dest="query",
                    help="sql query", metavar="QUERY")

parser.add_argument("-f", "--folder", dest="folder",
                    help="folder", metavar="FOLDER")

parser.add_argument("-d", "--datareferencia", dest="datareferencia",
                    help="datareferencia", metavar="DATAREFERENCIA")


args = parser.parse_args()

query = args.query
folder = args.folder
datareferencia = args.datareferencia

target_path = "/data/processed/{}/{}/".format(folder,datareferencia)

df = etlspark.load_from_database(query)
etlspark.save(df, target_path, coalesce=1, format="csv")
