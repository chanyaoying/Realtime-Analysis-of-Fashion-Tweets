
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
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").collect()
