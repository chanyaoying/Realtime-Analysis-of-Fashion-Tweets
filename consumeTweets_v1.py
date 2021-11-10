####
# TO RUN:
#
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2
# OR
# $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 consumeTweets.py
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.2,com.github.fommil.netlib:all:1.1.2 consumeTweets_v1.py
####

from pyspark.sql.functions import udf
import json
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.clustering import LDA

from sparknlp.base import *
from sparknlp.annotator import *

from pymongo import MongoClient
from textblob import TextBlob


#####################################
# GLOBAL CONSTANTS
#####################################

# Set window and trigger interval
# window_interval = "2 minutes"
window_interval = {'sentiment_analysis': '2 minutes',
                   'topic_modelling': '2 minutes'}
trigger_interval = {'sentiment_analysis': '1 minutes',
                    'topic_modelling': '1 minutes'}

# Set watermark
watermark_time = {'sentiment_analysis': '2 minutes',
                  'topic_modelling': '2 minutes'}

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

# define process function
my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'


def remove_links(tweet):
    tweet = re.sub(r'http\S+', '', tweet)
    tweet = re.sub(r'bit.ly/\S+', '', tweet)
    tweet = tweet.strip('[link]')
    return tweet


def remove_users(tweet):
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet)
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet)
    return tweet


def remove_punctuation(tweet):
    tweet = re.sub('['+my_punctuation + ']+', ' ', tweet)
    return tweet


def remove_number(tweet):
    tweet = re.sub('([0-9]+)', '', tweet)
    return tweet


def remove_hashtag(tweet):
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet)
    return tweet


# split lines into words
def split_lines(lines):
    words = lines.select(
        explode(split(lines.text_cleaned, "t_end")).alias("word"), "created_at", "hashtag", "timestamp")
    return words


# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity


def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity


def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn(
        "subjectivity", subjectivity_detection_udf("word"))

    return words


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


# register user defined function
remove_links = udf(remove_links)
remove_users = udf(remove_users)
remove_punctuation = udf(remove_punctuation)
remove_number = udf(remove_number)
remove_hashtag = udf(remove_hashtag)


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

json_schema = ArrayType(StringType())
udf_parse_json = udf(lambda str: parse_json(str), json_schema)

schema = StructType([StructField("created_at", StringType()), StructField(
    "text", StringType()), StructField("extended_tweet", StringType())])
extended_tweet_schema = StructType([StructField("entities", StringType())])
extended_extended_tweet_schema = StructType(
    [StructField("hashtags", ArrayType(StringType()))])

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
    .withColumn('entities', from_json('extended_tweet', extended_tweet_schema)) \
    .select('timestamp', 'created_at', 'text', 'entities.*') \
    .withColumn('hashtags', from_json('entities', extended_extended_tweet_schema)) \
    .select('timestamp', 'created_at', 'text', 'hashtags.*') \
    .select('timestamp', 'created_at', 'text', udf_parse_json('hashtags').alias("hashtags"))


#####################################
# SENTIMENT SCORING
#####################################
"""
                                   [ text | score | hastag1 ]
1. [ text | score | hastags ] -->  [ text | score | hastag2 ]
                                   [ text | score | hastag3 ]
2. Pass the text through the sentiment scoring model
3. Batch them.
4. group by hastags, mean the scores
5. get top 10 hastags per batch
6. For each batch, 
    a. Convert the batch into a dictionary
    b. Save it in MongoDB, under the collection sentimentScoring
"""

SA_cleaned_df = df.dropna()\
    .withColumn('text_cleaned', remove_links(df['text']))
SA_cleaned_df = SA_cleaned_df.withColumn(
    'text_cleaned', remove_users(SA_cleaned_df['text_cleaned']))
SA_cleaned_df = SA_cleaned_df.withColumn(
    'text_cleaned', remove_punctuation(SA_cleaned_df['text_cleaned']))
SA_cleaned_df = SA_cleaned_df.withColumn(
    'text_cleaned', remove_number(SA_cleaned_df['text_cleaned']))
SA_cleaned_df = SA_cleaned_df.select("text_cleaned", "created_at", "hashtags", "timestamp")

SA_cleaned_df = SA_cleaned_df.select(
    "text_cleaned", "created_at", explode(SA_cleaned_df.hashtags).alias("hashtag"), "timestamp")

words = split_lines(SA_cleaned_df)
words = text_classification(words)
words = words.repartition(1)

SA_batches = words.withWatermark('timestamp', watermark_time['sentiment_analysis']) \
    .groupBy(
        window("timestamp", window_interval['sentiment_analysis'],
               trigger_interval['sentiment_analysis']), "hashtag"
) \
    .agg(F.mean('polarity'))

words_query = SA_batches \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()


#####################################
# TOPIC MODELLING
#####################################
"""
TODO
1. Batching according to the global constants defined above.
2. Add to topic modelling model
3. 
"""

LDA_batches = df.withWatermark('timestamp', watermark_time['topic_modelling']) \
    .groupBy(
        window("timestamp", window_interval['topic_modelling'],
               trigger_interval['topic_modelling']), "text", "created_at"
) \
    .count()

LDA_cleaned_df = LDA_batches.withColumn(
    'text_cleaned', remove_links(LDA_batches['text']))
LDA_cleaned_df = LDA_cleaned_df.withColumn(
    'text_cleaned', remove_users(LDA_cleaned_df['text_cleaned']))
LDA_cleaned_df = LDA_cleaned_df.withColumn(
    'text_cleaned', remove_punctuation(LDA_cleaned_df['text_cleaned']))
LDA_cleaned_df = LDA_cleaned_df.withColumn(
    'text_cleaned', remove_number(LDA_cleaned_df['text_cleaned']))
LDA_cleaned_df = LDA_cleaned_df.select("text_cleaned", "created_at")

# STEP 1: DATA PREPARATION --------------------------------------------------------------------------------

# Spark NLP requires the input dataframe or column to be converted to document.
document_assembler = DocumentAssembler() \
    .setInputCol("text_cleaned") \
    .setOutputCol("document") \
    .setCleanupMode("shrink")
# Split sentence to tokens(array)
tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")
# clean unwanted characters and garbage
normalizer = Normalizer() \
    .setInputCols(["token"]) \
    .setOutputCol("normalized")
# remove stopwords
stopwords_cleaner = StopWordsCleaner()\
    .setInputCols("normalized")\
    .setOutputCol("cleanTokens")\
    .setCaseSensitive(False)
# stem the words to bring them to the root form.
stemmer = Stemmer() \
    .setInputCols(["cleanTokens"]) \
    .setOutputCol("stem")
# Finisher is the most important annotator. Spark NLP adds its own structure when we convert each row in the dataframe to document. Finisher helps us to bring back the expected structure viz. array of tokens.
finisher = Finisher() \
    .setInputCols(["stem"]) \
    .setOutputCols(["tokens"]) \
    .setOutputAsArray(True) \
    .setCleanAnnotations(False)


# We build a ml pipeline so that each phase can be executed in sequence. This pipeline can also be used to test the model.
nlp_pipeline = Pipeline(
    stages=[document_assembler,
            tokenizer,
            normalizer,
            stopwords_cleaner,
            stemmer,
            finisher])

# train the pipeline
nlp_model = nlp_pipeline.fit(LDA_cleaned_df)

# apply the pipeline to transform dataframe.
processed_df = nlp_model.transform(LDA_cleaned_df)

tokens_df = processed_df.select('created_at', 'tokens')

# STEP 2: FEATURE ENGINEERING

cv = CountVectorizer() \
    .setInputCol("tokens") \
    .setOutputCol("features") \
    .setVocabSize(500) \
    .setMinDF(3.0)


def build_LDA_model(batchDF, epochID):
    cv_model = cv.fit(batchDF)
    vectorized_tokens = cv_model.transform(batchDF)
    vocab = cv_model.vocabulary

    # STEP 3: BUILD THE LDA MODEL

    if vocab:
        num_topics = 3
        lda = LDA(k=num_topics, maxIter=10)
        model = lda.fit(vectorized_tokens)
        topics = model.describeTopics()
        topics_rdd = topics.rdd
        topics_words = topics_rdd \
            .map(lambda row: row['termIndices']) \
            .map(lambda idx_list: [vocab[idx] for idx in idx_list]) \
            .collect()

        print(topics_words)

    # TODO:
    # STEP 4: PUSH TO MONGODB


topic_modelling_query = tokens_df.writeStream \
    .outputMode("append") \
    .foreachBatch(build_LDA_model) \
    .start()


# ####################################
# # Await termination for both queries
# ####################################

words_query.awaitTermination()
topic_modelling_query.awaitTermination()
