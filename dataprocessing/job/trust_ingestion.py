# Path hack.
import sys, os
from argparse import ArgumentParser
from datetime import datetime

sys.path.insert(0, os.path.abspath('..'))

from dataprocessing.processors.trust_ingestion import TrustProcessing

parser = ArgumentParser()
parser.add_argument("-d", "--date", dest="date", help="date", metavar="DATE")

args = parser.parse_args()

dt = datetime.strptime(args.date, '%Y-%m')

dt = dt.strftime("%Y-%m")

TrustProcessing(dt)()
