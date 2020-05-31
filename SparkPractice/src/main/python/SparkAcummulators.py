# As the name hints, accumulators are variables that accumulate. Because Spark runs in distributed mode,
# the workers are running in parallel, but asynchronously.
# For example, worker 1 will not be able to know how far worker 2 and worker 3 are done with their tasks.
# With the same analogy, the variables that are local to workers are not going to be shared to another worker unless you accumulate them.
# Accumulators are used for mostly sum operations, like in Hadoop MapReduce, but you can implement it to do otherwise.


from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from pyspark.sql.functions import udf

spark=SparkSession.builder.appName("Practice1").getOrCreate()

path = "/Users/anantarora/Downloads/udacity/sparkify_log_small.json"

logs =  spark.read.json(path)

# define accumulator
incorrect_records =SparkContext.accumulator(0,0)

print (incorrect_records.value)

# create function for incrementing accumulator
def add_incorrect_record ():
    global incorrect_records
    incorrect_records += 1

# create user defined functions to check is ts column is digit or not
correct_ts = udf(lambda x: 1 if x.isdigit() else add_incorrect_record())


logs2 = logs.where(logs.userId == "1046")
print (logs2.show())

logs3 = logs2.withColumn("ts_digit",correct_ts(logs2.ts))
print (logs3.show())
print (incorrect_records.value)


# print (logs3.show())
