# %% markdown
# #### **INIT SPARK CONTEXT AND SET CONFIGURATIONS**
# %%
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


# %%

conf = SparkConf().setAppName("App")
conf = (conf.setMaster('local[*]')
        .set('spark.executor.memory', '4G')
        .set('spark.driver.memory', '4G')
        .set('spark.driver.maxResultSize', '3G'))

sc = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext(sc)

# %% markdown
# #### **LOAD DATA FILES**
# %%
processed_path = '/home/altieris/datascience/data/urbs/processed/'

position_events = sqlContext.read.parquet(processed_path+'veiculos/')
position_events.registerTempTable("veiculos")

tabelaVeiculo = sqlContext.read.parquet(processed_path+'tabelaveiculo/')
tabelaVeiculo.registerTempTable("tabela_veiculo")

# %% markdown
# #### **SHOW DATA**
# %%


def executeQuery(table_name):
    query = 'select * from {} limit 10'.format(table_name)

    return sqlContext.sql(query)


# %% markdown
# #### **Linhas de ônibus**
# %% markdown
# Todas as linhas da Rede Integrada do Transporte Coletivo de Curitiba.
# %%
#executeQuery('veiculos').toPandas().head(2)


# tabelaVeiculo.select(['cod_ponto', 'horario']).filter(
#     "cod_linha == 507 ").show()

 #and hour in (6,7)
 #.filter("cod_linha == 507 ")  \
events_507 = position_events.select('cod_linha', 'veic', 'lat', 'lon', functions.date_format(functions.unix_timestamp('dthr', 'dd/MM/yyyy HH:mm:ss')
                                                                                             .cast('timestamp'), "yyyy-MM-dd HH:mm:ss").alias('event_timestamp')) \
            .withColumn("year",  functions.year(functions.col('event_timestamp')))  \
            .withColumn("month",  functions.month(functions.col('event_timestamp')))  \
            .withColumn("day",  functions.dayofmonth(functions.col('event_timestamp')))  \
            .withColumn("hour",  functions.hour(functions.col('event_timestamp')))  \
            .withColumn("minute",  functions.minute(functions.col('event_timestamp')))  \
            .withColumn("second",  functions.second(functions.col('event_timestamp')))  \
            .filter("cod_linha == 507 and veic == 'EL309' and hour in (7,8)")  \
            .sort(functions.asc("event_timestamp"))


windowSpec = Window.partitionBy('cod_linha', 'veic').orderBy('event_timestamp')


scriptpath = "/home/altieris/master-thesis/source/fromnotebooks/src/"

# Add the directory containing your module to the Python path (wants absolute paths)
sys.path.append(os.path.abspath(scriptpath))


events = events_507.withColumn("last_timestamp", functions.lag("event_timestamp", 1, 0).over(windowSpec))\
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
    if delta_velocity is not None and delta_velocity > 5:
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
          ,evt.lat as latitude
          ,evt.lon as longitude
     from events_processed evt
     where evt.moving_status = 'STOPPED' and cod_linha='507' and evt.veic ='EL309'
     order by cod_linha
"""

target_path = '/home/altieris/datascience/data/urbs/processed/stopevents/'

#sqlContext.sql(query).show(10)

sqlContext.sql(query).coalesce(1).write.mode('overwrite').option(
    "header", "true").format("csv").save(target_path)


query = """

with stops as (
    select cod_linha
          ,veic
          ,event_timestamp as last_stop
          ,lead(event_timestamp) over (partition by veic,moving_status order by event_timestamp asc )  as current_stop
     from events_processed
     where moving_status = 'STOPPED' and cod_linha='507' and  veic ='EL309'
),
trips as (
    select round(sum( evp.delta_time )/60,2)     as delta_time
          ,round(sum( if(evp.delta_time is null, 0,evp.delta_distance) )/1000,2) as delta_distance
          ,round(avg(evp.delta_velocity),2) as delta_velocity
          ,evp.veic
          ,evp.cod_linha
          ,st.last_stop
          ,st.current_stop
     from events_processed evp,stops st
     where
            (evp.event_timestamp between st.last_stop and st.current_stop)
            and (evp.veic = st.veic)
            and (evp.cod_linha = st.cod_linha)
    group by evp.cod_linha,evp.veic, st.last_stop, st.current_stop
    order by evp.cod_linha,evp.veic, st.last_stop, st.current_stop
)
select cod_linha
      ,veic
      ,delta_time
      ,delta_distance
      ,delta_velocity
      ,last_stop
      ,current_stop
from trips
"""
# where delta_time <= 10
target_path = '/home/altieris/datascience/data/urbs/processed/trackingdata/'
sqlContext.sql(query).coalesce(1).write.mode('overwrite').option(
    "header", "true").format("csv").save(target_path)
