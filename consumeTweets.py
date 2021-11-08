####
# TO RUN:
#
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2
# OR
# $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 consumeTweets.py 
####

from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col
from pyspark.sql.types import StringType
from pymongo import MongoClient


#####################################
# GLOBAL CONSTANTS
#####################################

# Set window and trigger interval
window_interval = "2 minutes"
trigger_interval = "1 minutes"

# Set watermark
watermark_time = "2 minutes"

# Spark Session
spark = SparkSession \
    .builder \
    .appName("is459") \
    .getOrCreate()

client = MongoClient('localhost', 27017)
db = client.realtime_tweets_analysis

#####################################
# FOR EACH BATCH FUNCTION
#####################################

def insert_to_DB(batchDF, epochID):
    del batchDF
    test_data = {"name": "Zhang Zhenjie", "yes?": True, "epochID": epochID}
    collection = db.topic_modelling
    collection.insert_one(test_data)
    

#####################################
# GLOBAL CONSTANTS
#####################################

# Subscribe to 1 kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test_tweets") \
    .option("includeTimestamp", "true") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)", "timestamp")

# TODO: Parse tweets to get only the text


#####################################
# SENTIMENT SCORING
#####################################
"""
No batching needed.
1. Pass the text through the sentiment scoring model
2. For each batch, 
    a. Convert the batch into a dictionary
    b. Save it in MongoDB, under the collection sentimentScoring
"""

# windowedCounts = df.withWatermark("timestamp", "30 seconds") \
#     .groupBy(
#         window("timestamp", "30 seconds", "15 seconds"), 'value'
#     ) \
#     .count()

# # pipes real-time stream into console (for testing)
# query = windowedCounts.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()


#####################################
# TOPIC MODELLING
#####################################
"""
1. Batching according to the global constants defined above.
2. Add to topic modelling model
3. 
"""

topic_modelling_output = df.writeStream \
    .outputMode('append') \
    .foreachBatch(insert_to_DB) \
    .start()


####################################
# Await termination for both queries
####################################

# query.awaitTermination()
topic_modelling_output.awaitTermination()