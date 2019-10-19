"""SimpleApp.py"""
import os

directories = os.listdir("/data/raw/")

import psycopg2

conn = psycopg2.connect(
        host = "172.18.0.4",
        database = "dw",
        user = "airflow",
        password = "airflow")

from sparketl import ETLSpark
etlspark = ETLSpark()

for dir in directories:
    source_path = "/data/raw/{}".format(dir)
    target_path = "/data/processed/{}".format(dir)

    raw_data = etlspark.extract(source_path)
    etlspark.transform(raw_data, target_path)