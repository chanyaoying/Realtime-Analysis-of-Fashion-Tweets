#!! command to run this script line by line in pyspark shell

####
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2
# OR
# $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 ./bin/consumeTweets.py 
####

from pyspark.sql import SparkSession
from pyspark.sql.functions import window

spark = SparkSession \
    .builder \
    .appName("is459") \
    .getOrCreate()

# Subscribe to 1 topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test_tweets") \
    .option("includeTimestamp", "true") \
    .option("startingOffsets", "earliest") \
    .load()

windowedCounts = df.withWatermark("timestamp", "30 seconds") \
    .groupBy(
        window("timestamp", "30 seconds", "15 seconds"), df.value
    ) \
    .count()

# pipes real-time stream into console (for testing)
query = windowedCounts.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

# TODO: create batch queries
# TODO: pipe real-time stream and batch queries into analyses (sentiment scoring, topic modeling)