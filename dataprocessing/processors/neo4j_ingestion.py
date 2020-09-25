from pyspark.sql import DataFrame

from .sparketl import ETLSpark
import pyspark.sql.functions as F


class Neo4JDataProcess:

    def __init__(self, year, month, day):
        self.etlspark = ETLSpark()
        self.year = year
        self.month = month
        self.day = day

    def __call__(self, *args, **kwargs):
        self.perform()

    def perform(self):
        color = self.color()
        self.save(color, "/data/neo4j/color")

        service_category = self.service_category()
        self.save(service_category, "/data/neo4j/service_category")

        lines = self.lines()
        self.save(lines, "/data/neo4j/lines")

        trips = self.trips()
        self.save(trips, "data/neo4j/trips")

        bus_stop_type = self.bus_stop_type()
        self.save(bus_stop_type, "/data/neo4j/bus_stop_type")

        bus_stops = self.bus_stops()
        self.save(bus_stops, "/data/neo4j/bus_stops")

        line_routes = self.line_routes()
        self.save(line_routes, "/data/neo4j/line_routes")

        trip_endpoints = self.trip_endpoints()
        self.save(trip_endpoints, "/data/neo4j/trip_endpoints")

        bus_event_edges = self.bus_event_edges()
        self.save(bus_event_edges, "/data/neo4j/bus_event_edges")

        events = self.events()
        self.save(events, "/data/neo4j/events")

    def color(self) -> DataFrame:
        """
        STATIC DATA
        :return:
        """
        return self.etlspark.sqlContext.read.parquet("/data/refined/colors").distinct()

    def service_category(self) -> DataFrame:
        return self.etlspark.sqlContext.read.parquet("/data/refined/service_categories").distinct()

    def lines(self) -> DataFrame:
        return (self.etlspark.sqlContext.read.parquet("/data/refined/lines")
                .filter(f"year =='{self.year}' and month=='{self.month}' and day=='{self.day}'")
                .distinct())

    def trips(self) -> DataFrame:
        return (self.etlspark.sqlContext.read.parquet("/data/refined/trips")
                .filter(f"year =='{self.year}' and month=='{self.month}' and day=='{self.day}'")
                .distinct())

    def bus_stop_type(self):
        return self.etlspark.sqlContext.read.parquet("/data/refined/bus_stop_type").distinct()

    def bus_stops(self) -> DataFrame:
        return (self.etlspark.sqlContext.read.parquet("/data/refined/bus_stops")
                .filter(f"year =='{self.year}' and month=='{self.month}' and day=='{self.day}'")
                .distinct())

    def line_routes(self) -> DataFrame:
        return (self.etlspark.sqlContext.read.parquet("/data/refined/line_routes")
                .filter(f"year =='{self.year}' and month=='{self.month}' and day=='{self.day}'")
                .distinct())

    def trip_endpoints(self) -> DataFrame:
        return (self.etlspark.sqlContext.read.parquet("/data/refined/trip_endpoints")
                .filter(f"year =='{self.year}' and month=='{self.month}' and day=='{self.day}'")
                .distinct())

    def bus_event_edges(self) -> DataFrame:
        return (self.etlspark.sqlContext.read.parquet("/data/refined/bus_event_edges")
                .filter(f"year =='{self.year}' and month=='{self.month}' and day=='{self.day}'")
                .distinct())

    def events(self) -> DataFrame:
        return (self.etlspark.sqlContext.read.parquet("/data/refined/events")
                .filter(f"year =='{self.year}' and month=='{self.month}' and day=='{self.day}'")
                .distinct().sort(F.col("line_code"), F.col("line_way"), F.col("vehicle"), F.col("last_timestamp"),
                                 F.col("event_timestamp")))

    @staticmethod
    def save(df: DataFrame, output: str):
        (df.coalesce(1).write.mode('overwrite').option("header", True)
         .format("csv").save(output))
