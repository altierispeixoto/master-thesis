# %% markdown
# #### **INIT SPARK CONTEXT AND SET CONFIGURATIONS**
# %%
import findspark
findspark.init('/usr/local/spark')
# %%
import pyspark
import random
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import  functions

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
executeQuery('veiculos').toPandas().head(2)


tabelaVeiculo.select(['cod_ponto','horario']).filter("cod_linha == 507 and veiculo = 'EL309'").show()


events_507 = position_events.select('lat','lon','veic', functions.date_format(functions.unix_timestamp('dthr', 'dd/MM/yyyy HH:mm:ss').cast('timestamp'),"yyyy-MM-dd HH:mm:ss").alias('event_timestamp')).filter("cod_linha == 507 and veic = 'EL309'").sort(functions.asc("event_timestamp"))

from pyspark.sql.window import Window

windowSpec = Window.partitionBy('veic').orderBy('event_timestamp')


import os
import sys

scriptpath = "/home/altieris/master-thesis/source/fromnotebooks/src/"

# Add the directory containing your module to the Python path (wants absolute paths)
sys.path.append(os.path.abspath(scriptpath))


events = events_507.withColumn("last_timestamp", functions.lag("event_timestamp", 1, 0).over(windowSpec))\
.withColumn("last_latitude", functions.lag("lat", 1, 0).over(windowSpec))\
.withColumn("last_longitude", functions.lag("lon", 1, 0).over(windowSpec))


from pyspark.sql.types import DoubleType


def haversine(lon1,lat1,lon2,lat2):
    import math

    lon1,lat1=lon1,lat1
    lon2,lat2=lon2,lat2

    R=6371000                               # radius of Earth in meters
    phi_1=math.radians(lat1)
    phi_2=math.radians(lat2)

    delta_phi=math.radians(lat2-lat1)
    delta_lambda=math.radians(lon2-lon1)

    a = math.sin(delta_phi/2.0)**2+ math.cos(phi_1)*math.cos(phi_2) * math.sin(delta_lambda/2.0)**2

    c=2*math.atan2(math.sqrt(a),math.sqrt(1-a))

    return R*c # output distance in meters



apply_test = functions.udf(lambda lon0,lat0,lon1,lat1,: haversine(lon0,lat0,lon1,lat1), DoubleType())


events.withColumn("delta_time", functions.unix_timestamp('event_timestamp') - functions.unix_timestamp('last_timestamp'))\
.withColumn("delta_distance",
 apply_test(functions.col('lon').cast('double'),functions.col('lat').cast('double'),functions.col('last_longitude').cast('double'),functions.col('last_latitude').cast('double')))\
.show()
