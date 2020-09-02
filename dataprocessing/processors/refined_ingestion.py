import math

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from .sparketl import ETLSpark


@F.udf(returnType=T.StringType())
def create_flag_status(dv: float) -> str:
    return 'MOVING' if dv is not None and dv > 15 else 'STOPPED'


@F.udf(returnType=T.DoubleType())
def delta_velocity(delta_distance: float, delta_time: float) -> float:
    try:
        return round((delta_distance / delta_time) * 3.6, 2) if (
                delta_distance is not None and delta_time is not None and delta_time > 0) else 0
    except TypeError:
        print(f"delta_distance: {delta_distance} , delta_time: {delta_time}")


@F.udf(returnType=T.DoubleType())
def haversine(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    try:

        R: int = 6371000  # radius of Earth in meters
        phi_1 = math.radians(lat1)
        phi_2 = math.radians(lat2)

        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi_1) * \
            math.cos(phi_2) * math.sin(delta_lambda / 2.0) ** 2

        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return round(R * c, 2)  # output distance in meters
    except Exception as err:
        print(f"Exception has been occurred :{err}")
        print(f"lon1: {lon1} lat1: {lat1} lon2: {lon2} lat2: {lat2}")


class LineRefinedProcess:

    def __init__(self, year, month, day):
        self.etlspark = ETLSpark()
        self.df = self.filter_data(year, month, day)

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

    def __init__(self, year, month, day):
        self.year = year
        self.month = month
        self.day = day
        self.etlspark = ETLSpark()
        self.df = self.filter_data(year, month, day)

    def __call__(self, *args, **kwargs):
        self.perform()

    def perform(self):
        trips = self.trips()
        self.save(trips, "/data/refined/trips")

    def filter_data(self, year: str, month: str, day: str) -> DataFrame:
        return (self.etlspark.sqlContext.read.parquet("/data/trusted/timetable")
                .filter(f"year ='{year}' and month='{month}' and day='{day}'"))

    def timetable(self) -> DataFrame:
        return (self.df.withColumn('end_time', F.lead('time')
                                   .over(
            Window.partitionBy('line_code', 'timetable', 'vehicle')
                .orderBy('line_code', 'time')))
                .withColumn('end_point', F.lead('busstop_number')
                            .over(
            Window.partitionBy('line_code', 'timetable', 'vehicle')
                .orderBy('line_code', 'time')))
                .select(
            'line_code',
            F.col('busstop_number').alias('start_point'),
            F.col('time').alias('start_time'),
            'timetable',
            'vehicle',
            'end_time',
            'end_point',
            "year",
            "month",
            "day"
        )
                .orderBy('line_code', 'time'))

    def trips(self):
        bs = BusStopRefinedProcess(self.year, self.month, self.day)
        trip_endpoints = bs.trip_endpoints().drop("year", "month", "day")

        return (self.timetable()
                .join(trip_endpoints, ['line_code', 'start_point', 'end_point'], how='left')
                .filter('line_way is not null'))

    @staticmethod
    def save(df: DataFrame, output: str):
        (df.coalesce(1).write.mode('overwrite').option("header", True)
         .partitionBy("year", "month", "day")
         .format("csv").save(output))


class BusStopRefinedProcess:

    def __init__(self, year, month, day):
        self.etlspark = ETLSpark()
        self.df = self.filter_data(year, month, day)

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
        return (self.etlspark.sqlContext.read.parquet("/data/trusted/busstops")
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

    def __init__(self, year, month, day):
        self.year = year
        self.month = month
        self.day = day
        self.etlspark = ETLSpark()
        self.df = self.filter_data(year, month, day)

    def filter_data(self, year: str, month: str, day: str) -> DataFrame:
        tracking_data = (self.etlspark.sqlContext.read.parquet("/data/trusted/vehicles")
                         .filter(f"year ='{year}' and month ='{month}' and day ='{day}'")
                         .withColumn("hour", F.hour(F.col("event_timestamp")))
                         .withColumn("minute", F.minute(F.col("event_timestamp"))).sort(F.asc("event_timestamp"))
                         )

        scheduled_vehicles = TimetableRefinedProcess(year, month, day).trips().select("line_code", "vehicle").distinct()
        tracking_vehicles = tracking_data.select("line_code", "vehicle").distinct()
        tracking_scheduled_vehicles = tracking_vehicles.join(scheduled_vehicles, ["line_code", "vehicle"], 'inner')

        return tracking_data.join(tracking_scheduled_vehicles, ["line_code", "vehicle"], 'inner')

    def perform(self):
        vehicles = self.compute_metrics()
        # self.save(vehicles, "/data/refined/vehicles")

        stop_events = self.stop_events(vehicles)

        # self.save(stop_events, "/data/refined/stop_events")

        event_stop_edges = self.event_stop_edges(stop_events)
        self.save(event_stop_edges, "/data/refined/event_stop_edges")

    def __call__(self, *args, **kwargs):
        self.perform()

    def compute_metrics(self) -> DataFrame:
        window_spec = (
            Window.partitionBy(self.df.line_code, self.df.vehicle, self.df.year, self.df.month, self.df.day)
                .orderBy(self.df.event_timestamp)
        )

        events = (self.df.withColumn("last_timestamp", F.lag(F.col('event_timestamp'), 1, 0).over(window_spec))
                  .withColumn("last_latitude", F.lag(F.col('latitude'), 1, 0).over(window_spec))
                  .withColumn("last_longitude", F.lag(F.col('longitude'), 1, 0).over(window_spec)))

        events_processed = (events.withColumn("delta_time",
                                              F.round(F.unix_timestamp(F.col('event_timestamp')) - F.unix_timestamp(
                                                  F.col('last_timestamp')), 2))
                            .withColumn("delta_distance",
                                        haversine(F.col('longitude'), F.col('latitude'),
                                                  F.col('last_longitude'), F.col('last_latitude')))
                            .withColumn("delta_velocity",
                                        delta_velocity(F.col('delta_distance'), F.col('delta_time')))
                            .withColumn("moving_status", create_flag_status(F.col('delta_velocity')))
                            .orderBy('event_timestamp'))

        return events_processed

    def stop_events(self, df):
        window_spec = (
            Window.partitionBy(df.moving_status, df.line_code, df.vehicle, df.year, df.month,
                               df.day, df.hour, df.minute)
                .orderBy(df.event_timestamp)
        )

        stop_events = (df.filter(F.col("moving_status") == 'STOPPED')
                       .withColumn("rn", F.row_number().over(window_spec))
                       .where(F.col("rn") == 1).drop("rn"))

        stop_events = stop_events.select(F.col("line_code"),
                                         F.col("vehicle"),
                                         F.col("event_timestamp").alias("stop_timestamp"),
                                         F.col("year"),
                                         F.col("month"),
                                         F.col("day"),
                                         F.col("hour"),
                                         F.col("minute"),
                                         F.col("latitude"),
                                         F.col("longitude")
                                         ).sort(F.col("vehicle"), F.col("event_timestamp"))

        window_spec2 = (
            Window.partitionBy(stop_events.line_code, stop_events.vehicle)
                .orderBy(stop_events.stop_timestamp)
        )

        stop_events_windowed = stop_events.withColumn("last_stop", F.lag("stop_timestamp").over(window_spec2)).filter(
            "last_stop is not null").select("line_code", "vehicle", "last_stop",
                                            F.col("stop_timestamp").alias("actual_stop"))

        events_computed = (
            df.filter("delta_time is not null").join(stop_events_windowed, ['line_code', 'vehicle'], 'inner').filter(
                F.col("event_timestamp").between(F.col("last_stop"), F.col("actual_stop")))
                .groupBy("year", "month", "day", "hour", "line_code", "vehicle", "actual_stop").agg(
                F.round(F.mean('delta_velocity'), 2).alias("avg_velocity"),
                F.round(F.sum('delta_distance'), 2).alias("distance")).
                withColumnRenamed("actual_stop", "stop_timestamp"))

        stop_events = stop_events.select([F.col(col).alias(col) for col in stop_events.columns])

        events_computed = events_computed.select([F.col(col).alias(col) for col in events_computed.columns])

        return (stop_events.join(events_computed, ['line_code', 'vehicle', "stop_timestamp"], 'inner'))

    def event_edges(self, events):
        trips = TimetableRefinedProcess(self.year, self.month, self.day).trips().drop("year", "month", "day")
        bus_stops = BusStopRefinedProcess(self.year, self.month, self.day).bus_stops().drop("year", "month", "day")
        bus_stops = bus_stops.withColumnRenamed("latitude", "bus_stop_latitude").withColumnRenamed("longitude",
                                                                                                   "bus_stop_longitude")

        events_computed = (
            events.withColumn("event_time", F.date_format(F.col("event_timestamp"), 'HH:mm:ss')).alias("se")
                .join(trips.alias("tr"), ["line_code", "vehicle"])
                .filter(F.col("event_time").between(F.col("start_time"), F.col("end_time")))
        )

        return (events_computed.alias("se").join(bus_stops.alias("bs"), ["line_code", "line_way"], 'inner')
                .withColumn("distance",
                            haversine(F.col('se.longitude').cast(T.DoubleType()),
                                      F.col('se.latitude').cast(T.DoubleType()),
                                      F.col('bs.bus_stop_longitude').cast(T.DoubleType()),
                                      F.col('bs.bus_stop_latitude').cast(T.DoubleType())))
                .filter(F.col("distance") < 30))

    @staticmethod
    def save(df: DataFrame, output: str):
        (df.write.mode('overwrite').option("header", True)
         .partitionBy("year", "month", "day")
         .format("csv").save(output))
