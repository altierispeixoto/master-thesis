import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from processors.sparketl import ETLSpark
from argparse import ArgumentParser
etlspark = ETLSpark()

parser = ArgumentParser()
parser.add_argument("-d", "--date", dest="date",
                    help="date", metavar="DATE")


args = parser.parse_args()
datareferencia = args.date

query = f"""
  with
  pontos_linha as (
     select * 
       from pontoslBuinha 
       where  year = year('{datareferencia}')
         and  month= month('{datareferencia}')
         and  day = dayofmonth('{datareferencia}')
     ),
  tabela_veiculo as (
   select * 
      from tabelaveiculo 
      where  year = year('{datareferencia}')
         and  month= month('{datareferencia}')
         and  day = dayofmonth('{datareferencia}')
  ),  
  stop_events as (
   select v.cod_linha,
          v.veic as vehicle,
          v.event_timestamp as stop_timestamp,
          v.year,
          v.month,
          v.day, 
          v.hour,
          v.minute,
          v.second,
          v.lat as latitude,
          v.lon as longitude,
          date_format(event_timestamp, 'HH:mm:ss') as event_time
        from veiculos v 
            where v.moving_status = 'STOPPED' 
              and year = year('{datareferencia}')
              and month= month('{datareferencia}')
              and day = dayofmonth('{datareferencia}')
),
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
           ,min(cast(seq as integer)) as start_trip
           ,max(cast(seq as integer)) as end_trip
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
     inner join pontos_linha ps on (ps.cod = ss.cod  and ps.sentido = ss.sentido and cast(ps.seq as integer) = ss.start_trip)
     inner join pontos_linha pe on (pe.cod = ss.cod  and pe.sentido = ss.sentido and cast(pe.seq as integer) = ss.end_trip)
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
      where svt.event_time between tv.start_time and tv.end_time
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


apply_haversine = F.udf(lambda lon0, lat0, lon1, lat1, : haversine(lon0, lat0, lon1, lat1), DoubleType())


events = etlspark.load_spark_sql(query = query)


events = events.withColumn("distance",
            apply_haversine(F.col('longitude').cast('double'), F.col('latitude').cast('double'), F.col('busstop_longitude').cast('double'), F.col('busstop_latitude').cast('double'))) \
            .filter("distance < 60")

evt = events.select(
    ["line_code", "latitude", "longitude", "vehicle", "event_time", "line_way", "bus_stop_number"])

target_path = f"/data/neo4j/event-stop-edges/{datareferencia}"
etlspark.save(evt, target_path, coalesce=1, format="csv")
