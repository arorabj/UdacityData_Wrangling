from pyspark.sql import SparkSession
import os
import configparser as cp
import pyspark.sql.functions as F
env="AWS"
confPath="../../../resources/application.properties"
conf =  cp.RawConfigParser()
conf.read(confPath)

os.environ['AWS_ACCESS_KEY_ID']=conf.get(env,'AWS_KEY')
os.environ['AWS_ACCESS_KEY']=conf.get(env,'AWS_SECRET')

spark = SparkSession.builder.config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()

df = spark.read.csv("s3a://")
print(df.printSchema())
print(df.show(5))

df = spark.read.csv("s3a://",sep=';',inferSchema=True,header=True)
print(df.printSchema())
print(df.show(5))


df_payment = df.withColumn("payment_date", F.to_timestamp('payment_date'))
print(df_payment.printSchema())
print(df_payment.show(5))


