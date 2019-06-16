import findspark
findspark.init('/usr/local/spark')


import sys
import os
from pyspark.sql.window import Window
from pyspark.sql import functions
from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.types import DoubleType, StringType


conf = SparkConf().setAppName("App")
conf = (conf.setMaster('local[*]')
        .set('spark.executor.memory', '4G')
        .set('spark.driver.memory', '4G')
        .set('spark.driver.maxResultSize', '3G'))

sc = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext(sc)


def show(query, n=10):
    sqlContext.sql(query).show(n)


def save(query, target_path):
    sqlContext.sql(query).coalesce(1) \
        .write.mode('overwrite')      \
        .option("header", "true")     \
        .format("csv")                \
        .save(target_path)

processed_path = '/home/altieris/datascience/data/urbs/processed/'

position_events = sqlContext.read.parquet(processed_path+'veiculos/')
position_events.registerTempTable("veiculos")

tabelaVeiculo = sqlContext.read.parquet(processed_path+'tabelaveiculo/')
tabelaVeiculo.registerTempTable("tabela_veiculo")

sqlContext.read.parquet('/home/altieris/datascience/data/urbs/processed/pontoslinha/').registerTempTable("pontos_linha")

events_filtered = position_events.select('cod_linha', 'veic', 'lat', 'lon', functions.date_format(functions.unix_timestamp('dthr', 'dd/MM/yyyy HH:mm:ss')
                                                                                             .cast('timestamp'), "yyyy-MM-dd HH:mm:ss").alias('event_timestamp')) \
            .withColumn("year",  functions.year(functions.col('event_timestamp')))  \
            .withColumn("month",  functions.month(functions.col('event_timestamp')))  \
            .withColumn("day",  functions.dayofmonth(functions.col('event_timestamp')))  \
            .withColumn("hour",  functions.hour(functions.col('event_timestamp')))  \
            .withColumn("minute",  functions.minute(functions.col('event_timestamp')))  \
            .withColumn("second",  functions.second(functions.col('event_timestamp')))  \
            .sort(functions.asc("event_timestamp"))
            #.filter("cod_linha in (666,507) and veic in ('GN606','EL309')")  \



windowSpec = Window.partitionBy('cod_linha', 'veic').orderBy('event_timestamp')


scriptpath = "/home/altieris/master-thesis/source/fromnotebooks/src/"

# Add the directory containing your module to the Python path (wants absolute paths)
sys.path.append(os.path.abspath(scriptpath))


events = events_filtered.withColumn("last_timestamp", functions.lag("event_timestamp", 1, 0).over(windowSpec))\
    .withColumn("last_latitude", functions.lag("lat", 1, 0).over(windowSpec))\
    .withColumn("last_longitude", functions.lag("lon", 1, 0).over(windowSpec))


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


def create_flag_status(delta_velocity):
    if delta_velocity is not None and delta_velocity > 15:
        return 'MOVING'
    else:
        return 'STOPPED'


apply_haversine = functions.udf(lambda lon0, lat0, lon1, lat1, : haversine(
    lon0, lat0, lon1, lat1), DoubleType())

apply_moving = functions.udf(
    lambda velocity, : create_flag_status(velocity), StringType())


events_processed = events.withColumn("delta_time", functions.unix_timestamp('event_timestamp') - functions.unix_timestamp('last_timestamp')) \
    .withColumn("delta_distance",
                apply_haversine(functions.col('lon').cast('double'), functions.col('lat').cast('double'), functions.col('last_longitude').cast('double'), functions.col('last_latitude').cast('double')))\
    .withColumn("delta_velocity", (functions.col('delta_distance').cast('double') / functions.col('delta_time').cast('double'))*3.6) \
    .withColumn("moving_status", apply_moving(functions.col('delta_velocity').cast('double'))) \
    .orderBy('event_timestamp')


events_processed.registerTempTable("events_processed")


query = """
    select evt.cod_linha
          ,evt.veic as vehicle
          ,evt.event_timestamp as stop_timestamp
          ,evt.year
          ,evt.month
          ,evt.day
          ,evt.hour
          ,evt.minute
          ,evt.second
          ,concat(
                  if( length(evt.hour) < 2 , concat(0,evt.hour), evt.hour)
                  ,':',
                  if( length(evt.minute) < 2 ,concat(0,evt.minute),evt.minute)
                  ,':',
                  if( length(evt.second) < 2 ,concat(0,evt.second),evt.second)
                  ) as event_time
          ,evt.lat as latitude
          ,evt.lon as longitude
     from events_processed evt
     where evt.moving_status = 'STOPPED' and evt.cod_linha = '666'
     order by cod_linha
"""


save(query, target_path='/home/altieris/datascience/data/urbs/processed/stopevents/')

target_path='/home/altieris/datascience/data/urbs/processed/stopevents-parquet/'
sqlContext.sql(query).coalesce(1) \
    .write.mode('overwrite')      \
    .format("parquet")                \
    .save(target_path)



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

show(query,n=30)

save(query, target_path='/home/altieris/datascience/data/urbs/processed/trackingdata/')


stop_events = sqlContext.read.parquet('/home/altieris/datascience/data/urbs/processed/stopevents-parquet/')
stop_events.registerTempTable('stop_events')


query = """
select * from stop_events
where cod_linha = 666 and vehicle = 'GN606'
order by stop_timestamp asc
limit 10
"""
show(query)


sqlContext.read.parquet('/home/altieris/datascience/data/urbs/processed/tabelaveiculo/').registerTempTable("tabela_veiculo")

query = """
select * from tabela_veiculo
where cod_linha = '666'  and veiculo = 'GN606'
 limit 10
"""
show(query)


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

events =  sqlContext.sql(query).withColumn("distance",
            apply_haversine(functions.col('longitude').cast('double'), functions.col('latitude').cast('double'), functions.col('busstop_longitude').cast('double'), functions.col('busstop_latitude').cast('double'))) \
            .filter("distance < 60")
            #.orderBy('event_time')


target_path='/home/altieris/datascience/data/urbs/processed/event-stop-edges/'
events.select(["line_code","latitude","longitude","vehicle","event_time","line_way","bus_stop_number"])\
    .coalesce(1) \
    .write.mode('overwrite')      \
    .option("header", "true")     \
    .format("csv")                \
    .save(target_path)
