from pyspark.sql.functions import *
from argparse import ArgumentParser
from sparketl import ETLSpark
etlspark = ETLSpark()


parser = ArgumentParser()
parser.add_argument("-d", "--date", dest="date",
                    help="date", metavar="DATE")


args = parser.parse_args()
datareferencia = args.date

query = f"""(
            select v.cod_linha,
                   v.veic as vehicle,
                   v.event_timestamp as stop_timestamp, 
                   v.year, 
                   v.month, 
                   v.day, 
                   v.hour, 
                   v.lat as latitude, 
                   v.lon as longitude, 
                   date_format(cast(v.event_timestamp as timestamp), '%T') as event_time
            from veiculos v 
            where v.moving_status = 'STOPPED' 
              and v.year = cast(extract( YEAR from date '{datareferencia}') as varchar)
              and v.month= cast(extract( MONTH from date '{datareferencia}') as varchar)
              and v.day = cast(extract( DAY from date '{datareferencia}')  as varchar)
          order by vehicle, event_timestamp 
         ) q1"""

events_processed = etlspark.load_from_presto(query = query)
target_path = f"/data/neo4j/stopevents/{datareferencia}"
etlspark.save(events_processed, target_path, coalesce=1, format="csv")