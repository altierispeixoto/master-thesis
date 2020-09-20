# Path hack.
import os
import sys
from argparse import ArgumentParser
from datetime import datetime

sys.path.insert(0, os.path.abspath('..'))
from dataprocessing.processors.neo4j_ingestion import Neo4JDataProcess

parser = ArgumentParser()
parser.add_argument("-d", "--date", dest="date", help="date", metavar="DATE")

args = parser.parse_args()

dt = datetime.strptime(args.date, '%Y-%m-%d')

year = dt.year
month = dt.month
day = dt.day

Neo4JDataProcess(year, month, day)()
