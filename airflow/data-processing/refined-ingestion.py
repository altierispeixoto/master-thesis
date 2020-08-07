from pyspark.sql import DataFrame
from sparketl import ETLSpark


class LineRefinedProcess:

    def __init__(self):
        self.etlspark = ETLSpark()
        self.df = self.filter_data("2020", "5", "3")

    def perform(self):
        service_categories = self.service_category()
        self.save(service_categories, "/data/refined/service_categories")

        colors = self.color()
        self.save(colors, "/data/refined/colors")

        lines = self.lines()
        self.save(lines, "/data/refined/lines")

    def filter_data(self, year: str, month: str, day: str) -> DataFrame:
        return self.etlspark.sqlContext.read.parquet("/data/trusted/lines") \
            .filter(f"year =='{year}' and month=='{month}' and day=='{day}'")

    def service_category(self) -> DataFrame:
        return self.df.select("service_category", "year", "month", "day") \
            .distinct()

    def color(self) -> DataFrame:
        return self.df.select("color", "year", "month", "day") \
            .distinct()

    def lines(self) -> DataFrame:
        return self.df.distinct()

    @staticmethod
    def save(df: DataFrame, output: str):
        df.coalesce(1).write.mode('overwrite').option("header", True) \
            .partitionBy("year", "month", "day") \
            .format("csv").save(output)


class BusStopRefinedProcess:

    def __init__(self):
        self.etl_spark = ETLSpark()
        self.df = self.filter_data("2020", "5", "3")

    def perform(self):
        bus_stop_type = self.bus_stop_type()
        self.save(bus_stop_type, "/data/refined/bus_stop_type")

        bus_stops = self.bus_stops()
        self.save(bus_stops, "/data/refined/bus_stops")

    def filter_data(self, year: str, month: str, day: str) -> DataFrame:
        return self.etl_spark.sqlContext.read.parquet("/data/trusted/busstops") \
            .filter(f"year =='{year}' and month=='{month}' and day=='{day}'")

    def bus_stop_type(self) -> DataFrame:
        return self.df.select("type", "year", "month", "day").distinct()

    def bus_stops(self, ) -> DataFrame:
        return self.df \
            .select("number", "name", "latitude", "longitude", "line_way", "type", "year", "month", "day") \
            .distinct()

    @staticmethod
    def save(df: DataFrame, output: str):
        df.coalesce(1).write.mode('overwrite').option("header", True) \
            .partitionBy("year", "month", "day") \
            .format("csv").save(output)


lines_refined = LineRefinedProcess()
lines_refined.perform()

bus_stop_refined = BusStopRefinedProcess()
bus_stop_refined.perform()
