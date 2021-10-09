
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

# for fun
spark.sparkContext.setCheckpointDir('/is459-project/spark-checkpoint')

#!! WARNING !! THIS RUNS FOREVER
# supposedly creates a sink for the stream  
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("checkpointLocation", "/is459-project/spark-checkpoint") \
    .option("topic", "test_tweets") \
    .start() 
