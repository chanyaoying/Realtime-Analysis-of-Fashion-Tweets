####
# TO RUN:
#
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2
# OR
# $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 consumeTweets.py 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.2 consumeTweets.py
####

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pymongo import MongoClient
# from pyspark.sql.functions import udf, from_json, col, window
# from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from datetime import datetime
from textblob import TextBlob
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import re

from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, IDF

# importing some libraries
import pandas as pd
import pyspark

# stuff we'll need for text processing
from nltk.corpus import stopwords
# stuff we'll need for building the model
from pyspark.mllib.linalg import Vector, Vectors
from pyspark.ml.clustering import LDA



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
# pre_process = udf( \
#     lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)|(@\w+)|(#)|(rt)|(:)', '', \
#         x.lower().strip()), ArrayType(StringType()))

def line_preprocessing(lines):
    lines = regexp_replace(lines, r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '')
    lines = regexp_replace(lines, '@\w+', '')
    lines = regexp_replace(lines, '#', '')
    lines = regexp_replace(lines, 'RT', '')
    lines = regexp_replace(lines, ':', '')
    # lines = regexp_replace(lines, 'content', '')
    # lines = regexp_replace(lines, 'author', '')

    return lines


def preprocessing(lines):
    words = lines.select(explode(split(lines.text, "t_end")).alias("word"), "created_at")
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

def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))

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

# words = preprocessing(df)
# words = text_classification(words)
# words = words.repartition(1)


# #!! Write to console (for now)
# words_query = words \
#     .writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()


#####################################
# TOPIC MODELLING
#####################################
"""
1. Batching according to the global constants defined above.
2. Add to topic modelling model
3. 
"""
### STEP 1: DATA PREPARATION --------------------------------------------------------------------------------

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

# data preprocessing of twitter text to remove symbols and special characters
cleaned_df = df.select(line_preprocessing(col("text")).alias("text_cleaned"), "created_at")

# train the pipeline
nlp_model = nlp_pipeline.fit(cleaned_df)

# apply the pipeline to transform dataframe.
processed_df  = nlp_model.transform(cleaned_df)

tokens_df = processed_df.select('created_at','tokens')


cv = CountVectorizer() \
    .setInputCol("tokens") \
    .setOutputCol("features") \
    .setVocabSize(500) \
    .setMinDF(3.0)

def build_LDA_model(batchDF, epochID):
    ### STEP 2: FEATURE ENGINEERING ------------------------------------------------------------
    cv_model = cv.fit(batchDF)
    vectorized_tokens = cv_model.transform(batchDF)

    num_topics = 3
    # lda = LDA(k=num_topics, maxIter=10)
    lda = LDA(k=num_topics, maxIter=10)
    model = lda.fit(vectorized_tokens)
    ll = model.logLikelihood(vectorized_tokens)
    lp = model.logPerplexity(vectorized_tokens)

    # extract vocabulary from CountVectorizer
    vocab = cv_model.vocabulary
    topics = model.describeTopics()   
    topics_rdd = topics.rdd
    topics_words = topics_rdd \
        .map(lambda row: row['termIndices']) \
        .map(lambda idx_list: [vocab[idx] for idx in idx_list])\
        .take(10)



topic_modelling_query = tokens_df.writeStream \
    .outputMode("append") \
    .foreachBatch(build_LDA_model) \
    .start()

# train the model

# transform the data. Output column name will be features.


# # IDF
# idf = IDF(inputCol="raw_features", outputCol="features")
# idfModel = idf.fit(vectorized_tokens)
# result_tfidf = idfModel.transform(vectorized_tokens) 

### STEP 3: BUILD THE LDA MODEL -------------------------------------------------------------------------------------------------
# num_topics = 3
# lda = LDA(k=num_topics, maxIter=10)
# model = lda.fit(vectorized_tokens)
# ll = model.logLikelihood(vectorized_tokens)
# lp = model.logPerplexity(vectorized_tokens)

# num_topics = 10
# max_iterations = 100
# lda_model = LDA.train(result_tfidf[['index','features']].map(list), k=num_topics, maxIterations=max_iterations)

### STEP 4: VISUALIZE TOPICS ------------------------------------------------------------------------------------
# extract vocabulary from CountVectorizer
# vocab = cv_model.vocabulary
# topics = model.describeTopics()   
# topics_rdd = topics.rdd
# topics_words = topics_rdd\
#        .map(lambda row: row['termIndices'])\
#        .map(lambda idx_list: [vocab[idx] for idx in idx_list])\
#        .collect()




# topic_modelling_output = df.writeStream \
#     .outputMode('append') \
#     .foreachBatch(insert_to_DB) \
#     .start()

# for testing purposes
# topic_modelling_query = vectorized_tokens \
#     .writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()


# ####################################
# # Await termination for both queries
# ####################################

# words_query.awaitTermination()
topic_modelling_query.awaitTermination()