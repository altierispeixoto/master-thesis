# Path hack.
import os
import sys
from argparse import ArgumentParser

sys.path.insert(0, os.path.abspath('..'))
from dataprocessing.processors.refined_ingestion import LineRefinedProcess, BusStopRefinedProcess, \
    TimetableRefinedProcess, TrackingDataRefinedProcess
from datetime import datetime

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
