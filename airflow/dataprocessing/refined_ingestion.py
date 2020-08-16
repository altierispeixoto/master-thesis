import math

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from sparketl import ETLSpark


@F.udf(returnType=T.StringType())
def create_flag_status(delta_velocity):
    if delta_velocity is not None and delta_velocity > 15:
        return 'MOVING'
    else:
        return 'STOPPED'


@F.udf(returnType=T.DoubleType())
def delta_velocity(delta_distance, delta_time):
    return (delta_distance / delta_time) * 3.6


@F.udf(returnType=T.DoubleType())
def haversine(lon1, lat1, lon2, lat2):
    try:
        lon1, lat1 = lon1, lat1
        lon2, lat2 = lon2, lat2

        R: int = 6371000  # radius of Earth in meters
        phi_1 = math.radians(lat1)
        phi_2 = math.radians(lat2)

        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi_1) * \
            math.cos(phi_2) * math.sin(delta_lambda / 2.0) ** 2

        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c  # output distance in meters
    except:
        print(f"lon1: {lon1} lat1: {lat1} lon2: {lon2} lat2: {lat2}")


class LineRefinedProcess:

    def __init__(self):
        self.etlspark = ETLSpark()
        self.df = self.filter_data("2020", "5", "3")

    def perform(self):
        service_categories = self.service_category()
        self.persist(service_categories, "/data/refined/service_categories")

        colors = self.color()
        self.persist(colors, "/data/refined/colors")

        lines = self.lines()
        self.persist(lines, "/data/refined/lines")

    def __call__(self, *args, **kwargs):
        self.perform()

    def filter_data(self, year: str, month: str, day: str) -> DataFrame:
        return (self.etlspark.sqlContext.read.parquet("/data/trusted/lines")
                .filter(f"year =='{year}' and month=='{month}' and day=='{day}'"))

    def service_category(self) -> DataFrame:
        return (self.df.select("service_category", "year", "month", "day")
                .distinct())

    def color(self) -> DataFrame:
        return (self.df.select("color", "year", "month", "day")
                .distinct())

    def lines(self) -> DataFrame:
        return self.df.distinct()

    def persist(self, df: DataFrame, output: str):
        (df.coalesce(1).write.mode('overwrite').option("header", True)
         .partitionBy("year", "month", "day")
         .format("csv").save(output))


class TimetableRefinedProcess:

    def __init__(self):
        self.etl_spark = ETLSpark()
        self.df = self.filter_data("2020", "5", "3")

    def __call__(self, *args, **kwargs):
        self.perform()

    def perform(self):
        trips = self.trips()
        self.save(trips, "/data/refined/trips")

    def filter_data(self, year: str, month: str, day: str) -> DataFrame:
        return (self.etl_spark.sqlContext.read.parquet("/data/trusted/timetable")
                .filter(f"year =='{year}' and month=='{month}' and day=='{day}'"))

    def timetable(self) -> DataFrame:
        return (self.df.withColumn('end_time', F.lead('time').over(
            Window.partitionBy('line_code', 'timetable', 'vehicle').orderBy('line_code', 'time')))
                .withColumn('end_point', F.lead('busstop_number').over(
            Window.partitionBy('line_code', 'timetable', 'vehicle').orderBy('line_code', 'time')))
                .select('line_code', F.col('busstop_number').alias('start_point'), F.col('time').alias('start_time'),
                        'timetable', 'vehicle', 'end_time', 'end_point', "year", "month", "day")
                .orderBy('line_code', 'time'))

    def trips(self):
        bs = BusStopRefinedProcess()
        trip_endpoints = bs.trip_endpoints().drop("year", "month", "day")
        print(trip_endpoints.show(3))

        return (self.timetable()
                .join(trip_endpoints, ['line_code', 'start_point', 'end_point'], how='left')
                .filter('line_way is not null'))

    @staticmethod
    def save(df: DataFrame, output: str):
        (df.coalesce(1).write.mode('overwrite').option("header", True)
         .partitionBy("year", "month", "day")
         .format("csv").save(output))


class BusStopRefinedProcess:

    def __init__(self):
        self.etl_spark = ETLSpark()
        self.df = self.filter_data("2020", "5", "3")

    def perform(self):
        bus_stop_type = self.bus_stop_type()
        self.save(bus_stop_type, "/data/refined/bus_stop_type")

        bus_stops = self.bus_stops()
        self.save(bus_stops, "/data/refined/bus_stops")

        line_routes = self.line_routes()
        self.save(line_routes, "/data/refined/line_routes")

        trip_endpoints = self.trip_endpoints()
        self.save(trip_endpoints, "/data/refined/trip_endpoints")

    def __call__(self, *args, **kwargs):
        self.perform()

    def filter_data(self, year: str, month: str, day: str) -> DataFrame:
        return (self.etl_spark.sqlContext.read.parquet("/data/trusted/busstops")
                .filter(f"year =='{year}' and month=='{month}' and day=='{day}'"))

    def bus_stop_type(self) -> DataFrame:
        return self.df.select("type", "year", "month", "day").distinct()

    def bus_stops(self, ) -> DataFrame:
        return (self.df
                .select("line_code", "line_way", "number", "name", F.col("seq").cast(T.IntegerType()).alias("seq"),
                        "latitude", "longitude", "type", "year", "month", "day")
                .distinct())

    def line_routes(self) -> DataFrame:
        bus_stops = self.bus_stops().select("line_code", "line_way", "number", "name", "seq", "year", "month", "day")

        return (bus_stops.alias("ps")
                .join(bus_stops.alias("pe"),
                      (F.col("ps.line_code") == F.col("pe.line_code")) & (
                              F.col("ps.line_way") == F.col("pe.line_way")) & (
                              F.col("ps.seq") + 1 == F.col("pe.seq")))
                .select("ps.line_code", "ps.line_way", F.col("ps.seq").alias("start_seq"),
                        F.col("pe.seq").alias("end_seq"),
                        F.col("ps.number").alias("start_point"), F.col("pe.number").alias("end_point"), "ps.year",
                        "ps.month", "ps.day")
                .orderBy("line_code", F.asc("start_seq")))

    def line_start_end(self) -> DataFrame:
        return (self.bus_stops().groupby("line_code", "line_way", "year", "month", "day")
                .agg(F.min('seq').alias("start_trip"), F.max('seq').alias("end_trip"))
                .select("line_code", "line_way", "start_trip", "end_trip", "year", "month", "day"))

    def trip_endpoints(self) -> DataFrame:
        bus_stops = self.bus_stops()
        line_start_end = self.line_start_end()

        return (line_start_end.alias("ss")
                .join(bus_stops.alias("ps"),
                      (F.col("ss.line_code") == F.col("ps.line_code")) & (
                              F.col("ss.line_way") == F.col("ps.line_way")) & (
                              F.col("ss.start_trip") == F.col("ps.seq")))
                .join(bus_stops.alias("pe"),
                      (F.col("ss.line_code") == F.col("pe.line_code")) & (
                              F.col("ss.line_way") == F.col("pe.line_way")) & (
                              F.col("ss.end_trip") == F.col("pe.seq")))
                .select("ps.line_code", "ps.line_way", F.col("ps.number").alias("start_point"),
                        F.col("pe.number").alias("end_point"), "ss.year",
                        "ss.month", "ss.day"))

    @staticmethod
    def save(df: DataFrame, output: str):
        (df.coalesce(1).write.mode('overwrite').option("header", True)
         .partitionBy("year", "month", "day")
         .format("csv").save(output))


class TrackingDataRefinedProcess:

    def __init__(self):
        self.etl_spark = ETLSpark()
        self.df = self.filter_data("2020", "5", "3")
        self.df = self.vehicles()

    def filter_data(self, year: str, month: str, day: str) -> DataFrame:
        return (self.etl_spark.sqlContext.read.parquet("/data/trusted/vehicles")
                .filter(f"year =='{year}' and month=='{month}' and day=='{day}'"))

    def perform(self):
        vehicles = self.compute_metrics()
        self.save(vehicles, "/data/refined/vehicles")

    def __call__(self, *args, **kwargs):
        self.perform()

    def vehicles(self) -> DataFrame:
        return (self.df.withColumn("year", F.year(self.df.event_timestamp))
                .withColumn("month", F.month(self.df.event_timestamp))
                .withColumn("day", F.dayofmonth(self.df.event_timestamp))
                .withColumn("hour", F.hour(self.df.event_timestamp)).sort(F.asc("event_timestamp")))

    def compute_metrics(self) -> DataFrame:
        window_spec = (
            Window.partitionBy(self.df.line_code, self.df.vehicle, self.df.year, self.df.month, self.df.day).orderBy(
                self.df.event_timestamp))

        events = (self.df.withColumn("last_timestamp", F.lag(F.col('event_timestamp'), 1, 0).over(window_spec))
                  .withColumn("last_latitude", F.lag(F.col('latitude'), 1, 0).over(window_spec))
                  .withColumn("last_longitude", F.lag(F.col('longitude'), 1, 0).over(window_spec)))

        events_processed = (events.withColumn("delta_time",
                                              F.unix_timestamp(F.col('event_timestamp')) - F.unix_timestamp(
                                                  F.col('last_timestamp')))
                            .withColumn("delta_distance",
                                        haversine(F.col('longitude'), F.col('latitude'),
                                                  F.col('last_longitude'), F.col('last_latitude')))
                            .withColumn("delta_velocity",
                                        delta_velocity(F.col('delta_distance'), F.col('delta_time')))
                            .withColumn("moving_status", create_flag_status(F.col('delta_velocity')))
                            .orderBy('event_timestamp'))

        return events_processed

    @staticmethod
    def save(df: DataFrame, output: str):
        (df.coalesce(1).write.mode('overwrite').option("header", True)
         .partitionBy("year", "month", "day")
         .format("csv").save(output))


# LineRefinedProcess()()
# BusStopRefinedProcess()()
# TimetableRefinedProcess()()
TrackingDataRefinedProcess()()