from sparketl import ETLSpark
etlspark = ETLSpark()
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-d", "--date", dest="date",
                    help="date", metavar="DATE")

args = parser.parse_args()
datareferencia = args.date



query = """
(
with 
events_processed as (
	 select cod_linha, veic, event_timestamp, delta_time , delta_distance, delta_velocity , moving_status
   		from veiculos v 
     	where v.year = cast(extract( YEAR from date '{datareferencia}') as varchar)
       		and v.month= cast(extract( MONTH from date '{datareferencia}') as varchar)
       		and v.day = cast(extract( DAY from date '{datareferencia}')  as varchar)
),
stops as (
    select cod_linha
          ,veic
          ,event_timestamp as last_stop
          ,lead(event_timestamp) over (partition by veic, moving_status order by event_timestamp asc )  as current_stop
     from events_processed v
       where 
       moving_status = 'STOPPED' --and cod_linha = '666'
),
trips as (
    select sum( if(evp.delta_time is null, 0, evp.delta_distance)) as delta_distance
          ,round(avg(evp.delta_velocity), 2) as delta_velocity
          ,evp.veic
          ,evp.cod_linha
          ,st.last_stop
          ,st.current_stop
     from events_processed evp, stops st
     where
            (evp.event_timestamp between st.last_stop and st.current_stop)
            and (evp.veic = st.veic)
            and (evp.cod_linha = st.cod_linha)
    group by evp.cod_linha,evp.veic, st.last_stop, st.current_stop
    order by evp.cod_linha,evp.veic, st.last_stop, st.current_stop
)
select cod_linha
      ,veic
      ,date_diff('second', cast(last_stop as timestamp),cast(current_stop as timestamp) ) as delta_time
      ,delta_distance
      ,delta_velocity
      ,last_stop
      ,current_stop
from trips
) as q
""".format(datareferencia=datareferencia)

target_path = "/data/neo4j/{folder}/{datareferencia}".format(folder = "trackingdata",datareferencia = datareferencia)

evt = etlspark.load_from_presto(query = query)

etlspark.save(evt, target_path, coalesce=5, format="csv")