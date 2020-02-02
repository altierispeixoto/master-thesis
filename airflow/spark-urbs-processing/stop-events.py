from pyspark.sql.functions import *

from sparketl import ETLSpark
etlspark = ETLSpark()


query = """(
            select v.cod_linha, v.veic, v.event_timestamp, v.year, v.month, v.day, v.hour, v.minute, v.second , v.lat, v.lon  
            from veiculos v 
                where v.moving_status = 'STOPPED' 
                  and DATE(v.event_timestamp) = '2019-01-01' 
            ) q1"""

events_processed = etlspark.load_from_database(query = query)

#events_processed = etlspark.sqlContext.read.parquet('/data/processed/eventsprocessed/')

evt = events_processed.select('cod_linha', col('veic').alias('vehicle'), col('event_timestamp').alias('stop_timestamp'),'year','month','day', 'hour','minute','second',
                        date_format('event_timestamp', 'hh:mm:ss').alias('event_time'), col('lat').alias('latitude'), col('lon').alias('longitude')) \
    .orderBy('vehicle','event_timestamp')


target_path = "/data/processed/{}".format("stopevents")
etlspark.save(evt, target_path, coalesce=1, format="csv")