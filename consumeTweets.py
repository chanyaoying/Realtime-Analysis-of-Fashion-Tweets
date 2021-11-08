####
# TO RUN:
#
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2
# OR
# $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 consumeTweets.py 
####

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pymongo import MongoClient
from pyspark.sql.functions import udf, from_json, col, window
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from datetime import datetime
import re


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
# HELPER FUNCTIONS
#####################################

# Changing datetime format
date_process = udf( \
    lambda x: datetime.strftime( \
        datetime.strptime(x,'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S'))

# Text Cleaning
pre_process = udf( \
    lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', \
        x.lower().strip()).split(), ArrayType(StringType()))


#####################################
# FOR EACH BATCH FUNCTION
#####################################

def insert_to_DB(batchDF, epochID):
    del batchDF
    test_data = {"name": "Zhang Zhenjie", "yes?": True, "epochID": epochID}
    collection = db.topic_modelling
    collection.insert_one(test_data)
    

#####################################
# PREPROCESSING
#####################################

# Subscribe to 1 kafka topic

schema = StructType( \
    [StructField("created_at", StringType()), \
    StructField("text", StringType())] \
    )


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test_tweets") \
    .option("includeTimestamp", "true") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(timestamp AS TIMESTAMP) as timestamp", "CAST(value AS STRING) as message") \
    .withColumn("value", from_json("message", schema)) \
    .select('timestamp', 'value.*') \
    .withColumn("created_at", date_process("created_at")) \
    .withColumn("cleaned_data", pre_process("text")) \
    .dropna()

# remove text from other languages

# lemmatise


# # remove stopwords
# stopwords_cleaner = StopWordsCleaner()\
#       .setInputCols("normalized")\
#       .setOutputCol("cleanTokens")\
#       .setCaseSensitive(False)


#####################################
# SENTIMENT SCORING
#####################################
"""
1. Batch them.
2. Pass the text through the sentiment scoring model
                                   [ text | score | hastag1 ]
3. [ text | score | hastags ] -->  [ text | score | hastag2 ]
                                   [ text | score | hastag3 ]
4. group by hastags, mean the scores
5. get top 10 hastags per batch
6. For each batch, 
    a. Convert the batch into a dictionary
    b. Save it in MongoDB, under the collection sentimentScoring
"""

#!! Write to console (for now)
sentiment_scoring = df \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()


#####################################
# TOPIC MODELLING
#####################################
"""
1. Batching according to the global constants defined above.
2. Add to topic modelling model
3. 
"""

# topic_modelling_output = df.writeStream \
#     .outputMode('append') \
#     .foreachBatch(insert_to_DB) \
#     .start()


# ####################################
# # Await termination for both queries
# ####################################

sentiment_scoring.awaitTermination()
# topic_modelling_output.awaitTermination()