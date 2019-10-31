from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, StringType
from sparketl import ETLSpark
etlspark = ETLSpark()


etlspark.load_from_database("pontoslinha_stg").registerTempTable("pontos_linha")
etlspark.load_from_database("tabelaveiculo_stg").registerTempTable("tabela_veiculo")

etlspark.sqlContext.read.csv("/data/processed/stopevents", header='true').registerTempTable("stop_events")

query = """
with
query_1 as (
    select cod_linha     as line_code
          ,cod_ponto     as start_point
          ,horario       as start_time
          ,tabela        as time_table
          ,veiculo       as vehicle
          ,lead(horario) over(partition by cod_linha,tabela,veiculo order by cod_linha, horario)   as end_time
          ,lead(cod_ponto) over(partition by cod_linha,tabela,veiculo order by cod_linha, horario) as end_point
          from tabela_veiculo
          order by cod_linha,horario
),
start_end as (
    select  cod
           ,sentido
           ,min(int(seq)) as start_trip
           ,max(int(seq)) as end_trip
         from pontos_linha
    group by cod,sentido
),
itinerary as (
 select ps.cod     as line_code
       ,ps.sentido as line_way
       ,ps.num     as start_point
       ,ps.nome    as ponto_origem
       ,pe.num     as end_point
       ,pe.nome    as destination
  from start_end  ss
     inner join pontos_linha ps on (ps.cod = ss.cod  and ps.sentido = ss.sentido and ps.seq = ss.start_trip)
     inner join pontos_linha pe on (pe.cod = ss.cod  and pe.sentido = ss.sentido and pe.seq = ss.end_trip)
),
line_way_events_stop as (
    select svt.cod_linha as line_code
          ,svt.vehicle
          ,svt.stop_timestamp
          ,svt.event_time
          ,svt.latitude
          ,svt.longitude
          ,tv.start_time
          ,tv.end_time
          ,tv.time_table
          ,tv.start_point
          ,tv.end_point
          ,it.line_way
      from stop_events svt
      inner join query_1 tv on (svt.cod_linha = tv.line_code and svt.vehicle = tv.vehicle)
      inner join itinerary it on (svt.cod_linha = it.line_code and tv.start_point = it.start_point and tv.end_point = it.end_point)
      where tv.line_code = '666'  and tv.vehicle = 'GN606'
         and svt.event_time between tv.start_time and tv.end_time
  )
  select line_code
        ,vehicle
        ,stop_timestamp
        ,event_time
        ,latitude
        ,longitude
        ,start_time
        ,end_time
        ,time_table
        ,start_point
        ,end_point
        ,line_way
        ,pl.lat    as busstop_latitude
        ,pl.lon    as busstop_longitude
        ,pl.nome   as bus_stop_name
        ,pl.num    as bus_stop_number
  from line_way_events_stop lwep
     inner join pontos_linha  pl on (lwep.line_code = pl.cod and lwep.line_way = pl.sentido )
     where line_code = '666'
"""

def haversine(lon1, lat1, lon2, lat2):
    import math

    lon1, lat1 = lon1, lat1
    lon2, lat2 = lon2, lat2

    R = 6371000                               # radius of Earth in meters
    phi_1 = math.radians(lat1)
    phi_2 = math.radians(lat2)

    delta_phi = math.radians(lat2-lat1)
    delta_lambda = math.radians(lon2-lon1)

    a = math.sin(delta_phi/2.0)**2 + math.cos(phi_1) * \
        math.cos(phi_2) * math.sin(delta_lambda/2.0)**2

    c = 2*math.atan2(math.sqrt(a), math.sqrt(1-a))

    return R*c  # output distance in meters


apply_haversine = udf(lambda lon0, lat0, lon1, lat1, : haversine(lon0, lat0, lon1, lat1), DoubleType())


events =  etlspark.sqlContext.sql(query).withColumn("distance",
            apply_haversine(col('longitude').cast('double'), col('latitude').cast('double'), col('busstop_longitude').cast('double'), col('busstop_latitude').cast('double'))) \
            .filter("distance < 60")
            #.orderBy('event_time')

evt = events.select(["line_code", "latitude", "longitude", "vehicle", "event_time", "line_way", "bus_stop_number"])

target_path = "/data/processed/{}".format("event-stop-edges")
etlspark.save(evt, target_path, coalesce=1, format="csv")