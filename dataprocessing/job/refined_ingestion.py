# Path hack.
import sys, os

sys.path.insert(0, os.path.abspath('..'))

from dataprocessing.processors.refined_ingestion import LineRefinedProcess, BusStopRefinedProcess, \
    TimetableRefinedProcess, TrackingDataRefinedProcess

# LineRefinedProcess()()
# BusStopRefinedProcess()()
# TimetableRefinedProcess()()
TrackingDataRefinedProcess()()
