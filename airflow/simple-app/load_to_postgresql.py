from argparse import ArgumentParser
from sparketl import ETLSpark
etlspark = ETLSpark()

parser = ArgumentParser()
parser.add_argument("-f", "--file", dest="table",
                    help="write report to table", metavar="FILE")

args = parser.parse_args()
print(args.table)

source_path = "/data/raw/{}".format(args.table)
raw_data = etlspark.extract(source_path)
etlspark.load_to_database(raw_data, args.table)