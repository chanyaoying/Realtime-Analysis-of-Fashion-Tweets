#!! command to run this script line by line in pyspark shell

####
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2
####

from pyspark.sql import SparkSession

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
    .load()


# pipes real-time stream into console (for testing)
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .start() 

# TODO: create batch queries
# TODO: pipe real-time stream and batch queries into analyses (sentiment scoring, topic modeling)