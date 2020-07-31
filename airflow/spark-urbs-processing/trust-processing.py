import pyspark.sql.functions as F
from sparketl import ETLSpark


class TrustProcessing:

    def __init__(self):
        self.etlspark = ETLSpark()

    def perform(self):

        self.etlspark.sqlContext.read.json("/data/raw/2020-05/veiculos") \
            .select(F.col("COD_LINHA").alias("line_code"),
                    F.date_format(F.unix_timestamp('dthr', 'dd/MM/yyyy HH:mm:ss').cast('timestamp'),
                                  "yyyy-MM-dd HH:mm:ss").alias('event_timestamp'),
                    F.col("LAT").cast('double').alias("latitude"),
                    F.col("LON").cast('double').alias("longitude"),
                    F.col("VEIC").alias("vehicle")
                    ) \
            .withColumn("year", F.year("event_timestamp")) \
            .withColumn("month", F.month("event_timestamp")) \
            .withColumn("day", F.dayofmonth("event_timestamp")).dropDuplicates().coalesce(10) \
            .write.mode('overwrite') \
            .partitionBy("year", "month", "day") \
            .format("parquet") \
            .save("/data/trusted/veiculos")

        self.etlspark.extract("/data/raw/2020-05/linhas") \
            .dropDuplicates().coalesce(1).write.mode('overwrite') \
            .partitionBy("year", "month", "day") \
            .format("parquet").save("/data/trusted/linhas")

        self.etlspark.extract("/data/raw/2020-05/pontoslinha") \
            .drop("GRUPO").dropDuplicates().coalesce(1).write.mode('overwrite') \
            .partitionBy("year", "month", "day") \
            .format("parquet").save("/data/trusted/pontoslinha")

        self.etlspark.extract("/data/raw/2020-05/tabelaveiculo") \
            .dropDuplicates().coalesce(1).write.mode('overwrite') \
            .partitionBy("year", "month", "day") \
            .format("parquet").save("/data/trusted/tabelaveiculo")


job = TrustProcessing()
job.perform()
