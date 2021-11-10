####
# TO RUN:
#
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2
# OR
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.2 consumeTweets.py
####
import json
from datetime import datetime

from pymongo import MongoClient

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, from_json, col, window, explode, split, transform
from pyspark.sql.types import StringType, StructType, StructField, ArrayType

from textblob import TextBlob

from pyspark.ml import Pipeline
# from sparknlp.base import *
# from sparknlp.annotator import *


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
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.2") \
    .getOrCreate()

client = MongoClient('localhost', 27017)
db = client.realtime_tweets_analysis


#####################################
# HELPER FUNCTIONS
#####################################

# Changing datetime format
data_process = udf(
    lambda x: datetime.strftime(
        datetime.strptime(x, '%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S'))

# Text Cleaning
# pre_process = udf( \
#     lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)|(@\w+)|(#)|(rt)|(:)', '', \
#         x.lower().strip()).split(), ArrayType(StringType()))


def preprocessing(lines):
    words = lines.select(
        explode(split(lines.text, "t_end")).alias("word"), "created_at")
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    return words


# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity


def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity


def sentiment_detection(text):
    return TextBlob(text).sentiment


def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn(
        "subjectivity", subjectivity_detection_udf("word"))
    # sentiment score
    sentiment_detection_udf = udf(sentiment_detection, StringType())
    words = words.withColumn("sentiment", sentiment_detection_udf("word"))

    return words


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

# Function to convert JSON array string to a list
import json

def parse_json(array_str):
    t = type(array_str)
    if array_str:
        if t == str:
            json_obj = json.loads(array_str)
        else:
            json_obj = array_str
        
        hashtags_array = []
        for item in json_obj:
            item = json.loads(item)
            hashtags_array.append(item["text"])
        return hashtags_array

# json_schema = ArrayType(StructType([StructField('text', StringType(
# ), nullable=False), StructField('indices', ArrayType(StringType()), nullable=False)]))

json_schema = ArrayType(StringType())

# Define udf
from pyspark.sql.functions import udf

udf_parse_json = udf(lambda str: parse_json(str), json_schema)

# Subscribe to 1 kafka topic

schema = StructType(
    [
        StructField("created_at", StringType()),
        StructField("text", StringType()),
        StructField("extended_tweet", StringType())
    ]
)

extended_tweet_schema = StructType(
    [
        StructField("entities", StringType())
    ]
)

extended_extended_tweet_schema = StructType(
    [
        StructField("hashtags", ArrayType(StringType()))
    ]
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
    .withColumn("created_at", data_process("created_at")) \
    .withColumn('entities', from_json('extended_tweet', extended_tweet_schema)) \
    .select('timestamp', 'created_at', 'text', 'entities.*') \
    .withColumn('hashtags', from_json('entities', extended_extended_tweet_schema)) \
    .select('timestamp', 'created_at', 'text', 'hashtags.*') \
    .select('timestamp', 'created_at', 'text', udf_parse_json('hashtags').alias("hashtags")) \
    .dropna()

    # .withColumn('hashtags', from_json('entities', extended_extended_tweet_schema)) \
    # .select('timestamp', 'created_at', 'text', 'hashtags.*') \
    # .select('timestamp', 'created_at', 'text', transform('hashtags', lambda n: from_json(n, extended_tweet_schema))) \
    # .withColumn('hashtags', from_json('hashtags', extended_extended_extended_tweet_schema)) \
    # .select('timestamp', 'created_at', 'text', 'hashtags') \

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

# words = preprocessing(df)
# words = text_classification(words)
# words = words.repartition(1)


#!! Write to console (for now)
words_query = df \
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

# documentAssembler = DocumentAssembler() \
#     .setInputCol('text') \
#     .setOutputCol('document')

# sentenceDetector = SentenceDetector() \
#     .setInputCols(["document"]) \
#     .setOutputCol("sentence")

# regexTokenizer = Tokenizer() \
#     .setInputCols(["sentence"]) \
#     .setOutputCol("token")

# finisher = Finisher() \
#     .setInputCols(["token"]) \
#     .setCleanAnnotations(False)

# pipeline = Pipeline() \
#     .setStages([
#         documentAssembler,
#         sentenceDetector,
#         regexTokenizer,
#         finisher
#     ])

# lemmatise

# # remove stopwords
# stopwords_cleaner = StopWordsCleaner()\
#       .setInputCols("normalized")\
#       .setOutputCol("cleanTokens")\
#       .setCaseSensitive(False)

# topic_modelling_output = df.writeStream \
#     .outputMode('append') \
#     .foreachBatch(insert_to_DB) \
#     .start()


# ####################################
# # Await termination for both queries
# ####################################

words_query.awaitTermination()
# topic_modelling_output.awaitTermination()
