from pyspark.sql import SparkSession
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import desc


pd.set_option('max_colwidth',200)

spark = SparkSession.builder.config("spark.jars.packages","JohnSnowLabs:spark-nlp:1.8.2").getOrCreate()
print(spark)
path = "/Users/anantarora/Downloads/DataSets/SparkPracticeUdemy/reddit-worldnews.json"
df_data = spark.read.json(path)

print (df_data.count())

print (df_data.printSchema())

title = 'data.title'
author = 'data.author'

df_AuthorTitle = df_data.select(title, author)
print (df_AuthorTitle.limit(5).toPandas())

df_WordCount = df_data.select(F.explode(F.split(title,"\\s+")).alias("word")).groupBy("word").count().orderBy(F.desc("count"))
print (df_WordCount.limit(15).toPandas())

from com.johnsnowlabs.nlp.pretrained.pipeline.en import BasicPipeline as bp

df_TitleAnnotate =  bp.annotate (df_AuthorTitle,"title")
print(df_TitleAnnotate.printSchema())

df_Pos= df_TitleAnnotate.select("text","pos.metadata","pos.result")
print (df_Pos.limit(15).toPandas())

df_Pos= df_TitleAnnotate.select(F.explode("pos").alias("pos"))
print (df_Pos.printSchema())
print (df_Pos.toPandas())

nlpFilter = "pos.result = 'NNP' or pos.result = 'NNPS' "
df_PosFiltered = df_Pos.where(nlpFilter)
print (df_PosFiltered.limit(10).toPandas())

df_WordTag = df_PosFiltered.selectExpr("pos.metadata['word'] as word", "pos.result as tag")
print (df_WordTag.limit(10).toPandas())

print (df_WordTag.groupBy("word").count().orderBy(desc("count")).limit(10).toPandas())