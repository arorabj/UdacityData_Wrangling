from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql import functions as F

spark=SparkSession.builder.appName("Practice1").getOrCreate()

path = "/Users/anantarora/Downloads/udacity/sparkify_log_small.json"

logs =  spark.read.json(path)

# 1st error in where clause by misspell where
#
# logs = logs.select(["userId","firstName","lastName","page","song"]).wher(logs.userId== "1046")

# invalid column name
#
# logs = logs.select(["userId","firstName","lastName","pag","song"]).wher(logs.userId== "1046")


# Where in capital W
#
# logs = logs.select(["userId","firstName","lastName","pag","song"]).Where(logs.userId== "1046")


# Now correction
log = logs.select(["userId","firstName","lastName","page","song"]).where(logs.userId== "1046")

#group by
userid = logs.groupBy("userID").count()
print(userid.take(1))

# add new column to original DataFrame
logs2 = logs.withColumn("artist", logs.artist + "x")
print(logs2.take(1))

# example of join
logs.crossJoin(logs).take(45)

songs = logs.where (logs.page == "NextSong")
print(songs.head(5))

sumLength = songs.groupBy("userId").agg(F.sum(songs.length))
print (sumLength.show())