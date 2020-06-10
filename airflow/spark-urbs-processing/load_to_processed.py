from argparse import ArgumentParser
from sparketl import ETLSpark
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, StringType
import math

apply_haversine = udf(lambda lon0, lat0, lon1, lat1,: haversine(lon0, lat0, lon1, lat1), DoubleType())

apply_moving = udf(lambda velocity,: create_flag_status(velocity), StringType())


def haversine(lon1, lat1, lon2, lat2):
    try:
        lon1, lat1 = lon1, lat1
        lon2, lat2 = lon2, lat2

        R = 6371000  # radius of Earth in meters
        phi_1 = math.radians(lat1)
        phi_2 = math.radians(lat2)

        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi_1) * \
            math.cos(phi_2) * math.sin(delta_lambda / 2.0) ** 2

        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c  # output distance in meters
    except:
        print("lon1: {} lat1: {} lon2: {} lat2: {}".format(lon1, lat1, lon2, lat2))


def create_flag_status(delta_velocity):
    if delta_velocity is not None and delta_velocity > 15:
        return 'MOVING'
    else:
        return 'STOPPED'


def process_raw_events(position_events):
    events_filtered = position_events.select('cod_linha', 'veic', 'lat', 'lon',
                                             date_format(unix_timestamp('dthr', 'dd/MM/yyyy HH:mm:ss')
                                                         .cast('timestamp'), "yyyy-MM-dd HH:mm:ss").alias(
                                                 'event_timestamp')) \
        .withColumn("year", year(col('event_timestamp'))) \
        .withColumn("month", month(col('event_timestamp'))) \
        .withColumn("day", dayofmonth(col('event_timestamp'))) \
        .withColumn("hour", hour(col('event_timestamp'))) \
        .withColumn("minute", minute(col('event_timestamp'))) \
        .withColumn("second", second(col('event_timestamp'))) \
        .sort(asc("event_timestamp"))

    windowSpec = Window.partitionBy('cod_linha', 'veic', 'year', 'month', 'day').orderBy('event_timestamp')

    events = events_filtered.withColumn("last_timestamp", lag("event_timestamp", 1, 0).over(windowSpec)) \
        .withColumn("last_latitude", lag("lat", 1, 0).over(windowSpec)) \
        .withColumn("last_longitude", lag("lon", 1, 0).over(windowSpec))

    events_processed = events.withColumn("delta_time",
                                         unix_timestamp('event_timestamp') - unix_timestamp('last_timestamp')) \
        .withColumn("delta_distance",
                    apply_haversine(col('lon').cast('double'), col('lat').cast('double'),
                                    col('last_longitude').cast('double'), col('last_latitude').cast('double'))) \
        .withColumn("delta_velocity", (col('delta_distance').cast('double') / col('delta_time').cast('double')) * 3.6) \
        .withColumn("moving_status", apply_moving(col('delta_velocity').cast('double'))) \
        .orderBy('event_timestamp')

    return events_processed


# ----------------------------------------------------------

etlspark = ETLSpark()

parser = ArgumentParser()
parser.add_argument("-f", "--file", dest="file",
                    help="write report to table", metavar="FILE")

parser.add_argument("-t", "--table", dest="table",
                    help="write report to table", metavar="FILE")


args = parser.parse_args()
print(args.file)

source_path = args.file
table = args.table

spark_df = etlspark.extract(source_path)

target_path = "/data/processed/{}".format(table)

if args.table == 'veiculos':
    events_processed = process_raw_events(spark_df)
    etlspark.save_partitioned(events_processed, target_path, coalesce=1)
    #etlspark.load_to_database(events_processed, args.table)
else:
    spark_df = spark_df.withColumn("year", year("datareferencia")).withColumn("month", month("datareferencia")).withColumn("day",dayofmonth("datareferencia"))
    etlspark.save_partitioned(spark_df, target_path, coalesce=1)
    #etlspark.load_to_database(spark_df, args.table)
