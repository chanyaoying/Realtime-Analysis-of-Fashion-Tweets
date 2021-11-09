import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lower, regexp_replace, split, col, explode, window, from_json
from pyspark.ml.feature import Tokenizer, StopWordsRemover

#  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2
##############################
# HELPER FUNCTIONS
##############################

 
def parse_data_from_kafka_message(sdf, schema):
    assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
    sdf = sdf.withColumn('value', from_json(col('value'), schema))
    for idx, field in enumerate(schema):
        sdf = sdf.withColumn(field.name, sdf.value[field.name].cast(field.dataType))
    return sdf


def clean_text(string: str) -> str:
    """
    Cleans text for further processing
    """
    # remove all links
    string = regexp_replace(string, "(https?\://)\S+", "")

    # remove all punctuations and symbols
    string = regexp_replace(string, "[^a-zA-Z0-9\\s]", "")

    # replace all multiple whitespaces with a single whitespace
    string = regexp_replace(string, "/  +/g", " ")

    return string


def sortByWordCount_limit10(batchDF, epochID):
    batchDF \
        .orderBy(col("window").desc(), col("count").desc()) \
        .filter(col('word').rlike("[^ +]")) \
        .limit(10) \
        .show(truncate=False)


def sortByAuthorCount_limit10(batchDF, epochID):
    batchDF \
        .orderBy(col("window").desc(), col("count").desc()) \
        .filter(col('author').rlike("[^ +]")) \
        .limit(10) \
        .show(truncate=False)
    


#####################################
# MAIN
#####################################

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("KafkaWordCount") \
        .getOrCreate()

    # Specify the schema of the fields
    hardwarezoneSchema = StructType([
        StructField("topic", StringType()),
        StructField("author", StringType()),
        StructField("content", StringType())
    ])

    # Read kafka topic
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "scrapy-output") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)", "timestamp")

    # Set window and trigger interval
    window_interval = "2 minutes"
    trigger_interval = "1 minutes"

    # Set watermark
    watermark_time = "2 minutes"

    #####################################
    # Getting top 10 authors per window
    #####################################

    """
    1. watermark 2 minutes
    2. group by window and author, aggregate by count
    3. write stream
    4. for each batch, order by the counts, then take the top 10
    """
    authors_query = parse_data_from_kafka_message(df, hardwarezoneSchema) \
        .select("author", "timestamp") \
        .withWatermark("timestamp", watermark_time) \
        .groupBy(
            window("timestamp", window_interval, trigger_interval), "author"
    ) \
        .count() \
        .writeStream \
        .outputMode('append') \
        .foreachBatch(sortByAuthorCount_limit10) \
        .start()

    ####################################
    # Getting top 10 words per window
    ####################################

    # Use the function to parse the fields
    word_lines = parse_data_from_kafka_message(df, hardwarezoneSchema) \
        .select("content", "timestamp") \

    # Clean content
    clean_df = word_lines.select(clean_text(
        col("content")).alias("content"), "timestamp")

    # Tokenizing content
    tokenizer = Tokenizer(inputCol="content", outputCol="vector")
    tokenized_df = tokenizer.transform(clean_df).select("vector", "timestamp")

    # Remove stopwords
    remover = StopWordsRemover()
    remover.setInputCol("vector")
    remover.setOutputCol("vector_no_stopwords")

    """
    1. Remove stop words
    2. explode the array of words in each words into new rows
    3. watermark 2 minutes
    4. group by window and word, aggregate by count
    5. write stream
    6. for each batch, order by the counts, then take the top 10
    """
    words_query = remover.transform(tokenized_df) \
        .select("vector_no_stopwords", "timestamp") \
        .select(explode("vector_no_stopwords").alias('word'), "timestamp") \
        .withWatermark("timestamp", watermark_time) \
        .groupBy(
            window("timestamp", window_interval, trigger_interval), "word"
    ) \
        .count() \
        .writeStream \
        .outputMode('append') \
        .foreachBatch(sortByWordCount_limit10) \
        .start()

    ####################################
    # Await termination for both queries
    ####################################

    authors_query.awaitTermination()
    words_query.awaitTermination()
