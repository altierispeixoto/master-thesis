# Path hack.
import sys, os
sys.path.insert(0, os.path.abspath('..'))

from dataprocessing.processors.trust_ingestion import TrustProcessing

TrustProcessing()()
