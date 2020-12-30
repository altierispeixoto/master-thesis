# Path hack.

from argparse import ArgumentParser
from datetime import datetime

import sys
import os

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from dataprocessing.processors.refined_ingestion import LineRefinedProcess, BusStopRefinedProcess, TimetableRefinedProcess, TrackingDataRefinedProcess


parser = ArgumentParser()
parser.add_argument("-d", "--date", dest="date", help="date", metavar="DATE")
parser.add_argument("-j", "--job", dest="job", help="job", metavar="JOB")

args = parser.parse_args()

dt = datetime.strptime(args.date, '%Y-%m-%d')

year = dt.year
month = dt.month
day = dt.day

job = args.job

if job == 'line':
    LineRefinedProcess(year, month, day)()
elif job == 'timetable':
    TimetableRefinedProcess(year, month, day)()
elif job == 'bus-stop':
    BusStopRefinedProcess(year, month, day)()
elif job == 'tracking':
    TrackingDataRefinedProcess(year, month, day)()
else:
    raise NotImplementedError("Job not implemented yet...")
