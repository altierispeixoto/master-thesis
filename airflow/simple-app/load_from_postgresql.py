from argparse import ArgumentParser
from sparketl import ETLSpark
etlspark = ETLSpark()

parser = ArgumentParser()
parser.add_argument("-f", "--file", dest="table",
                    help="write report to table", metavar="FILE")

args = parser.parse_args()
print(args.table)

target_path = "/data/processed/{}".format(args.table)

tbl = '{}_stg'.format(args.table)
df = etlspark.load_from_database(tbl)
etlspark.save(df, target_path, coalesce=1, format="csv")
