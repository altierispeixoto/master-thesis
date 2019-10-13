"""SimpleApp.py"""
from sparketl import ETLSpark
etlspark = ETLSpark()

source_path = "/data/raw/"
target_path = "/data/processed/"

raw_data = etlspark.extract(source_path)
etlspark.transform(raw_data, target_path)

# spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
# logData = spark.read.json(logFile).cache()
#
# numAs = logData.filter(logData.value.contains('a')).count()
# numBs = logData.filter(logData.value.contains('b')).count()
#
# print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
#
# spark.stop()