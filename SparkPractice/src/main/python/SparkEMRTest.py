from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark=SparkSession.builder.appName("FirstSparkProgramForEMRCluster").getOrCreate()

    log_of_songs=["Despacito",
                  "Nice for what",
                  "No tears left to cry",
                  "Despacito",
                  "Havana",
                  "In my feeling"
                  "Nice for what",
                  "despacito",
                  "All the stars"
                  ]

    distributes_song_log = spark\
        .sparkContext\
        .parallelize(log_of_songs)

    print (distributes_song_log.map(lambda x: x.lower()).collect())

    spark.stop()