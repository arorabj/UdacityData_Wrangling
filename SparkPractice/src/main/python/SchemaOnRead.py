from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import split, udf, desc, expr
from pyspark.sql.types import MapType, StringType


spark = SparkSession.builder.appName("SchemaOnRead").getOrCreate()

path = "/Users/anantarora/Downloads/DataSets/SparkPracticeUdemy/NASA_access_log_Jul95.gz"
df_log = spark.read.text(path)

print ( df_log.printSchema())
print (df_log.count())
print(df_log.show(5))
print(df_log.show(5,truncate=False))



# convert to Dataframe
pd.set_option('max_colwidth', 200)
print(df_log.limit(5).toPandas())

df_array = df_log.withColumn("tokenized",split('value'," "))
print(df_array.limit(5).toPandas())


#In Below UDF return type is not define so it will return string

@udf
def parseUDF(line):
    import re
    PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] (\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'
    match = re.search(PATTERN,line)
    if match is None:
        return (line,0)
    size_field= match.group(9)
    if size_field == '_':
        size = 0
    else:
        size = match.group(9)
    return {"host"          : match.group(1),
            "client_identd" : match.group(2),
            "user_id"       : match.group(3),
            "date_time"     : match.group(4),
            "method"        : match.group(5),
            "endpoint"      : match.group(6),
            "protocol"      : match.group(7),
            "response_code" : int(match.group(8)),
            "content_size"  : size
            }

df_parsed = df_log.withColumn("parsed",parseUDF("value"))

print(df_parsed.limit(10).toPandas())


print(df_parsed.printSchema())



#In Below UDF return type is nis defined

@udf(MapType(StringType(),StringType()))
def parseUDFbetter(line):
    import re
    PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] (\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'
    match = re.search(PATTERN,line)
    if match is None:
        return (line,0)
    size_field= match.group(9)
    if size_field == '_':
        size = 0
    else:
        size = match.group(9)
    return {"host"          : match.group(1),
            "client_identd" : match.group(2),
            "user_id"       : match.group(3),
            "date_time"     : match.group(4),
            "method"        : match.group(5),
            "endpoint"      : match.group(6),
            "protocol"      : match.group(7),
            "response_code" : int(match.group(8)),
            "content_size"  : size
            }

df_parsed = df_log.withColumn("parsed",parseUDFbetter("value"))
print(df_parsed.limit(10).toPandas())

print(df_parsed.printSchema())

print (df_parsed.select("parsed").limit(5).toPandas())

# select dict values as column
print (df_parsed.selectExpr("parsed['host'] as host").limit(5).toPandas())


# now doing the above for all columns
fields = ['host','client_identd','user_id','date_time','method','endpoint','protocol','response_code','content_size']
exprs = ["parsed['{}'] as {}".format(i,i) for i in fields]
print (exprs)


df_clean = df_parsed.selectExpr(*exprs)

print(df_clean.limit(10).toPandas())


print(df_clean.groupBy("host").count().orderBy(desc("count")).limit(10).toPandas())

# popular endpoint
print(df_clean.groupBy("endpoint").count().orderBy(desc("count")).limit(10).toPandas())

df_clean.createOrReplaceTempView("cleanlog")
print (spark.sql("select endpoint,content_size from cleanlog order by content_size desc").limit(10).toPandas())

df_cleanType= df_clean.withColumn("content_size_bytes", expr('cast(content_size as int)'))
print(df_cleanType.limit(10).toPandas())

df_cleanType.createOrReplaceTempView("cleantypelog")
print (spark.sql("select endpoint,content_size_bytes from cleantypelog order by content_size_bytes desc").limit(10).toPandas())
