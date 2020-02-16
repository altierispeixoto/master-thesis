from pyspark.sql.functions import *
from argparse import ArgumentParser
from sparketl import ETLSpark
etlspark = ETLSpark()


parser = ArgumentParser()
parser.add_argument("-d", "--date", dest="date",
                    help="date", metavar="DATE")


args = parser.parse_args()
datareferencia = args.date


query = """(
            select v.cod_linha, v.veic, v.event_timestamp, v.year, v.month, v.day, v.hour, v.minute, v.second , v.lat, v.lon  
            from veiculos v 
            where v.moving_status = 'STOPPED' 
              and v.year = cast(extract( YEAR from date '{datareferencia}') as varchar)
              and v.month= cast(extract( MONTH from date '{datareferencia}') as varchar)
              and v.day = cast(extract( DAY from date '{datareferencia}')  as varchar)
            ) q1""".format(datareferencia = datareferencia)

events_processed = etlspark.load_from_presto(query = query)

evt = events_processed.select('cod_linha', col('veic').alias('vehicle'), col('event_timestamp').alias('stop_timestamp'),'year','month','day', 'hour','minute','second',
                        date_format('event_timestamp', 'hh:mm:ss').alias('event_time'), col('lat').alias('latitude'), col('lon').alias('longitude')) \
    .orderBy('vehicle','event_timestamp')


target_path = "/data/neo4j/{folder}/{datareferencia}".format(folder = "stopevents",datareferencia =  datareferencia)
etlspark.save(evt, target_path, coalesce=1, format="csv")