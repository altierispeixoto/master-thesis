from pyspark.sql.functions import *

from sparketl import ETLSpark
etlspark = ETLSpark()

events_processed = etlspark.sqlContext.read.parquet('/data/processed/eventsprocessed/')

evt = events_processed.select('cod_linha', col('veic').alias('vehicle'), col('event_timestamp').alias('stop_timestamp'),'year','month','day', 'hour','minute','second',
                        date_format('event_timestamp', 'hh:mm:ss').alias('event_time'), col('lat').alias('latitude'), col('lon').alias('longitude')) \
    .filter(col('moving_status') == 'STOPPED').orderBy('vehicle','event_timestamp')

target_path = "/data/processed/{}".format("stopevents")
etlspark.save(evt, target_path, coalesce=1, format="csv")