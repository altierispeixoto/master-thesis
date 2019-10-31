from sparketl import ETLSpark
etlspark = ETLSpark()


events_processed = etlspark.sqlContext.read.parquet('/data/processed/eventsprocessed/')

events_processed.registerTempTable("events_processed")

query = """

with stops as (
    select cod_linha
          ,veic
          ,event_timestamp as last_stop
          ,lead(event_timestamp) over (partition by veic, moving_status order by event_timestamp asc )  as current_stop
     from events_processed
     where moving_status = 'STOPPED' and cod_linha = '666'
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
      ,unix_timestamp(current_stop) - unix_timestamp(last_stop) as delta_time
      ,delta_distance
      ,delta_velocity
      ,last_stop
      ,current_stop
from trips
where cod_linha = '666'
"""

target_path = "/data/processed/{}".format("trackingdata")
etlspark.save(events_processed, target_path, coalesce=1, format="csv")