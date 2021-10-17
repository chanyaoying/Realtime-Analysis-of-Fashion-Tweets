#!! command to run this script line by line in pyspark shell

####
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2
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
    .load()

# windowedCounts = df.groupBy(
#     window(df.timestamp, "2 minutes", "1 minutes"),
#     df.value).count()

windowed = df.withWatermark("timestamp", "1 year")
    
windowed.groupBy(window(df.timestamp, "1 year", "2years"), df.value).count()

# pipes real-time stream into console (for testing)
windowed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

windowed.awaitTermination()

# TODO: create batch queries
# TODO: pipe real-time stream and batch queries into analyses (sentiment scoring, topic modeling)