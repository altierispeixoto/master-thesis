from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

import gc


class ETLSpark:

    def __init__(self):
        self.conf = SparkConf().setAppName("App")
        self.conf = (self.conf.setMaster('local[*]')
                     # .set('spark.executor.memory', '8G')
                     # .set('spark.driver.memory', '20G')
                     # .set('spark.driver.maxResultSize', '10G')
                     .set('spark.sql.autoBroadcastJoinThreshold', '-1')
                     .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
                     )

        self.sc = SparkContext.getOrCreate(conf=self.conf)
        self.sqlContext = SQLContext(self.sc)

    def extract(self, src):
        print(f"FILE: {input_file_name()}")
        df = self.sqlContext.read.json(src).withColumn("filepath", input_file_name())

        split_col = split(df['filepath'], '/')

        df = df.withColumn('filename', split_col.getItem(7))

        split = split(df['filename'], '_')

        df = df.withColumn('datareferencia', to_date(
             concat(split.getItem(0), lit("-"), split.getItem(1), lit("-"),
                             split.getItem(2)), 'yyyy-MM-dd'))

        dropcolumns = ["filepath", "filename"]
        df = df.toDF(*[c.lower() for c in df.columns]).drop(*dropcolumns)
        return df

    # @staticmethod
    # def load_to_database(df, tablename):
    #     print(df.show(5))
    #
    #     db_properties = {}
    #     db_url = "jdbc:postgresql://10.5.0.3:5432/dw?user=airflow&password=airflow"
    #     db_properties['username'] = "airflow"
    #     db_properties['password'] = "airflow"
    #     db_properties['driver'] = "org.postgresql.Driver"
    #
    #     tabletarget = 'public.{}_stg'.format(tablename)
    #     # Save the dataframe to the table.
    #     df.write.jdbc(url=db_url, table=tabletarget, mode='overwrite', properties=db_properties)

    # def load_from_database(self, query):
    #     db_properties = {}
    #     db_url = "jdbc:postgresql://10.5.0.3:5432/dw?user=airflow&password=airflow"
    #     db_properties['username'] = "airflow"
    #     db_properties['password'] = "airflow"
    #     db_properties['driver'] = "org.postgresql.Driver"

    #     df = self.sqlContext.read.jdbc(url=db_url, table=query, properties=db_properties)

    #     #print(df.show(5))
    #     return df

    def load_from_presto(self, query):
        db_properties = {}
        db_url = "jdbc:presto://localhost:8585/hive/default"
        db_properties['user'] = "hive"
        db_properties['password'] = ""
        db_properties['driver'] = "com.facebook.presto.jdbc.PrestoDriver"

        df = self.sqlContext.read.jdbc(url=db_url, table=query, properties=db_properties)

        return df

    def save(self, src_data, target_path, coalesce=1, format="parquet"):
        src_data.repartition(coalesce).write.mode('overwrite').option("header", "true").format(format).save(target_path)
        del src_data
        gc.collect()

    def save_partitioned(self, src_data, target_path, coalesce=1, format="parquet"):
        src_data.repartition(coalesce).write.partitionBy("year", "month", "day").mode('overwrite').option("header", "true").format(format).save(target_path)
        del src_data
        gc.collect()

