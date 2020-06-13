from pyspark.sql import SparkSession
import os
import configparser as cp
import pyspark.sql.functions as F
from pyspark.sql.types import *
env="AWS"
confPath="../../../resources/application.properties"
conf =  cp.RawConfigParser()
conf.read(confPath)

os.environ['AWS_ACCESS_KEY_ID']=conf.get(env,'AWS_KEY')
os.environ['AWS_ACCESS_KEY']=conf.get(env,'AWS_SECRET')

spark = SparkSession.builder.config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()

df = spark.read.csv("s3a://udacity-dend/pagila/payment/payment.csv")
print(df.printSchema())
print(df.show(5))

df = spark.read.csv("s3a://udacity-dend/pagila/payment/payment.csv",sep=';',inferSchema=True,header=True)
print(df.printSchema())
print(df.show(5))


df_payment = df.withColumn("payment_date", F.to_timestamp('payment_date'))
print(df_payment.printSchema())
print(df_payment.show(5))

df_payment = df_payment.withColumn("month", F.month('payment_date'))
print(df_payment.show(5))

df_payment.createOrReplaceTempView("payment")
print(spark.sql("select month, sum(amount) as revenue from payment group by month order by revenue desc").show())


#fix schema
paymentSchema = StructType([
    StructField("payment_id",IntegerType()),
    StructField("customer_id",IntegerType()),
    StructField("staff_id",IntegerType()),
    StructField("rental_id",IntegerType()),
    StructField("amount",DoubleType()),
    StructField("payment_date",DateType())
])

df = spark.read.csv("s3a://udacity-dend/pagila/payment/payment.csv",sep=';',schema=paymentSchema,header=True)

df_payment.createOrReplaceTempView("payment")
print(spark.sql("select month(payment_date) as mnth, sum(amount) as revenue from payment group by mnth order by revenue desc").show())
