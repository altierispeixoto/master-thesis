import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext


class ETLSpark:

    def __init__(self):
        self.conf = SparkConf().setAppName("App")
        self.conf = (self.conf.setMaster('local[*]')
                     .set('spark.executor.memory', '12G')
                     .set('spark.driver.memory', '12G')
                     .set('spark.driver.maxResultSize', '5G')
                     .set('spark.sql.autoBroadcastJoinThreshold', '-1')
                     .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
                     )

        self.sc = SparkContext.getOrCreate(conf=self.conf)
        self.sqlContext = SQLContext(self.sc)

    # def extract(sqlContext, src):
    #     df = sqlContext.read.json(src).withColumn("filepath", F.input_file_name())
    #
    #     split_col = F.split(df['filepath'], '/')
    #     df = df.withColumn('filename', split_col.getItem(9))
    #
    #     split = F.split(df['filename'], '_')
    #
    #     df = df.withColumn('year', split.getItem(0))
    #     df = df.withColumn('month', split.getItem(1))
    #     df = df.withColumn('day', split.getItem(2))
    #     return df
    #

    def extract(self, src):
        print(f"FILE: {F.input_file_name()}")
        df = self.sqlContext.read.json(src).withColumn("filepath", F.input_file_name())

        split_col = F.split(df['filepath'], '/')

        df = df.withColumn('filename', split_col.getItem(7))

        split = F.split(df['filename'], '_')

        # df = df.withColumn('datareferencia', F.to_date(
        #     F.concat(split.getItem(0), F.lit("-"), split.getItem(1), F.lit("-"),
        #              split.getItem(2)), 'yyyy-MM-dd'))

        df = df.withColumn('year', split.getItem(0))
        df = df.withColumn('month', split.getItem(1))
        df = df.withColumn('day', split.getItem(2))

        dropcolumns = ["filepath", "filename"]
        df = df.toDF(*[c.lower() for c in df.columns]).drop(*dropcolumns)

        return df

    # def load_spark_sql(self, query):
    #     self.sqlContext.read.parquet("/data/processed/veiculos").registerTempTable("veiculos")
    #     self.sqlContext.read.parquet("/data/processed/linhas").registerTempTable("linhas")
    #     self.sqlContext.read.parquet("/data/processed/pontoslinha").registerTempTable("pontoslinha")
    #     self.sqlContext.read.parquet("/data/processed/tabelaveiculo").registerTempTable("tabelaveiculo")
    #
    #     df = self.sqlContext.sql(sqlQuery=query)
    #
    #     return df

    # def load_from_presto(self, query):
    #     db_properties = {}
    #     db_url = "jdbc:presto://localhost:8585/hive/default"
    #     db_properties['user'] = "hive"
    #     db_properties['password'] = ""
    #     db_properties['driver'] = "com.facebook.presto.jdbc.PrestoDriver"
    #
    #     df = self.sqlContext.read.jdbc(url=db_url, table=query, properties=db_properties)
    #
    #     return df

    # def save(self, src_data, target_path, coalesce=1, format="parquet"):
    #     src_data.repartition(coalesce).write.mode('overwrite').option("header", "true").format(format).save(target_path)
    #     del src_data
    #     gc.collect()
    #
    # def save_partitioned(self, src_data, target_path, coalesce=1, format="parquet"):
    #     src_data.repartition(coalesce).write.partitionBy("year", "month", "day").mode('overwrite').option("header",
    #                                                                                                       "true").format(
    #         format).save(target_path)
    #     del src_data
    #     gc.collect()
