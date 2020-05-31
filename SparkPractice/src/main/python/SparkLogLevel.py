
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

spark=SparkSession.builder.appName("Practice1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

path = "/Users/anantarora/Downloads/udacity/sparkify_log_small.json"

logs =  spark.read.json(path)
print(logs.collect())