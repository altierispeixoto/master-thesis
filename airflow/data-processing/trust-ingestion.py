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
            .save("/data/trusted/vehicles")

        self.etlspark.extract("/data/raw/2020-05/linhas") \
            .withColumn("service_category", F.col("categoria_servico")) \
            .withColumn("line_name", F.col("nome")) \
            .withColumn("line_code", F.col("cod")) \
            .withColumn("color", F.col("nome_cor")) \
            .withColumn("card_only", F.col("somente_cartao")) \
            .drop("categoria_servico", "cod", "nome", "nome_cor", "somente_cartao") \
            .dropDuplicates().coalesce(1).write.mode('overwrite') \
            .partitionBy("year", "month", "day") \
            .format("parquet").save("/data/trusted/lines")

        self.etlspark.extract("/data/raw/2020-05/pontoslinha") \
            .withColumn("line_code", F.col("cod")) \
            .withColumn("latitude", F.col("lat")) \
            .withColumn("longitude", F.col("lon")) \
            .withColumn("name", F.col("nome")) \
            .withColumn("number", F.col("num")) \
            .withColumn("line_way", F.col("sentido")) \
            .withColumn("seq", F.col("seq")) \
            .withColumn("type", F.col("tipo")) \
            .drop("GRUPO", "cod", "lat", "lon", "nome", "num", "sentido", "tipo") \
            .dropDuplicates().coalesce(1).write.mode('overwrite') \
            .partitionBy("year", "month", "day") \
            .format("parquet").save("/data/trusted/busstops")

        self.etlspark.extract("/data/raw/2020-05/tabelaveiculo") \
            .withColumn("line_code", F.col("cod_linha")) \
            .withColumn("line_name", F.col("nome_linha")) \
            .withColumn("busstop_number", F.col("cod_ponto")) \
            .withColumn("time", F.col("horario")) \
            .withColumn("timetable", F.col("tabela")) \
            .withColumn("vehicle", F.col("veiculo")) \
            .drop("cod_linha", "cod_ponto", "horario", "nome_linha", "tabela", "veiculo") \
            .dropDuplicates().coalesce(1).write.mode('overwrite') \
            .partitionBy("year", "month", "day") \
            .format("parquet").save("/data/trusted/timetable")


job = TrustProcessing()
job.perform()
