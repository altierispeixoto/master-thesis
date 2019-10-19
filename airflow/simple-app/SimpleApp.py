import os

#directories = os.listdir("/home/altieris/master-thesis/airflow/data/raw/")

directories = os.listdir("/data/raw/")


from argparse import ArgumentParser
from sparketl import ETLSpark
etlspark = ETLSpark()

parser = ArgumentParser()
parser.add_argument("-f", "--file", dest="table",
                    help="write report to table", metavar="FILE")

args = parser.parse_args()
print(args.table)

source_path = "/data/raw/{}".format(args.table)
target_path = "/data/processed/{}".format(args.table)

raw_data = etlspark.extract(source_path)
etlspark.load_to_database(raw_data, args.table)
etlspark.transform(raw_data, target_path)