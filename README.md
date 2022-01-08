# Realtime Analysis of Fashion Trends via Tweets

_AY2021-2022 Semester 1, IS459 G1, Group 7_

Real-time Analysis of Fast Fashion Trends from Twitter

This solution provides an end-to-end pipeline for real time streaming of tweets for text analysis, with quick visualisations. This provides real time tracking of twitter user behavior and sentiments. Our architecture is easily integrable, and it is domain-agnostic as the list of topics can be specified to any target field.

## Note
1. The Tweepy API allows us to listen to 1% of all tweets in real time.
2. There will be 1000+ tweets per minute based on our queries.
3. For Sentiment Analysis, only tweets with hashtags are kept. As a result, we drop ~50% of them.
4. For Topic Modelling, there are only 30 words. 10 words represent a topic.
5. Since the window time is big, output for Topic Modelling output is relatively stable in each batch.
6. Any analysis derived is time-specific, not location specific.

## Getting started
Clone this repository and change to this assignment's directory
```sh
git clone https://github.com/chanyaoying/IS459-Project.git
```

### Prerequisites

1. Ubuntu 20.04 (never tried 18.04)
2. Python 3
3. Hadoop File System (hadoop-3.3.0)
4. Spark (spark-3.1.2-bin-hadoop3.2)
5. Kafka and Zookeeper (kafka_2.12-2.8.0)
6. Mongo Database (v3.6.8)
7. Tableau (Tableau 2020.3)
8. (optional) Docker, Docker-compose

### Installation
#### Packages

All required packages will be installed automatically. For Python packages, we will be using a virtual environment.

Create and activate the virtual environment
```sh
python3 -m venv venv
source venv/bin/activate
```

Install Python packages via pip
```sh
pip install -r requirements.txt
```
#### Kafka + Zookeeper + MongoDB
We are assuming that you have MongoDB, Kafka and Zookeeper installed.

Alternatively, you can run them on docker containers. A `docker-compose` file is provided. Docker needs to be installed and started before running the command below:
```sh
docker-compose up
```
#### Apache Drill
Regardless of whether docker is used, the Drill ODBC driver will need to be installed for the Apache Drill-Tableau connector.

[Install Drill ODBC driver](https://drill.apache.org/docs/installing-the-driver-on-windows/)

If docker-compose is not being used, we have to manually install it.

[Install Apache Drill](https://drill.apache.org/docs/embedded-mode-prerequisites/)
## Usage
Make sure Hadoop FS, Zookeeper, Kafka, Apache Drill and MongoDB are running.

### Kafka Producer
Open a new WSL Terminal and run the kafka producer.
```sh
python3 produceTweets.py test_tweets
```

### Spark Consumer
On a separate terminal, run `consumeTweets.py` with spark-submit coupled with a few packages. Assuming that `$SPARK_HOME` is where Spark is installed on your machine.
```sh
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.2,com.github.fommil.netlib:all:1.1.2 consumeTweets.py
```

### Apache Drill
We have to add the mongo storage plugin on drill.
1. Navigate to http://localhost:8047
2. Go to Storage
3. Enable the mongo plugin.
4. If you are using the Drill Docker Image, update the connection of mongoDB to `mongodb://MongoDB:27017/`.

Make sure that both ports 8047 and 31010 are being used by Apache Drill.
### Tableau
[You may refer to this guide.](https://help.tableau.com/current/pro/desktop/en-us/examples_apachedrill.htm)

Connect using the data connector
1. Start `Fashion Viz.twb` and under Connect, select Apache Drill. For a complete list of data connections, select More under To a Server.
2. Select Direct connection (to http://localhost:31010)
3. Select No Authentication
4. For the initial SQL statement you can put `show schemas` (without semicolons)
5. Sign In

Data source page
1. On the data source page, under Schema, select `mongodb.realtime_tweets_analysis`. This is the name of our MongoDB database.
2. Drag the 2 tables (MongoDB collections) `sentiment_analysis` and `topic_modelling` to the middle of the page.
3. Link them by id_.
4. Make sure they are live connections (as opposed to data extracts)
4. Select the sheets at the bottom of the page to see the visualisations.

## Notes

### Tweepy API
This API exposes 1% of all tweets being tweeted in real time. `produceTweets.py` creates a listener that receives all tweets according to our query terms. For further information, see this [tutorial](https://developer.twitter.com/en/docs/tutorials/consuming-streaming-data).

The credentials are available in plain text in the python script. There is no credit card attached to this, we are unlikely to hit the rate limit as long as we do not leave it running the whole day.

Please note that we are using [`v3.10.0`](https://docs.tweepy.org/en/v3.10.0/). The latest version is [`v4.3.0`](https://docs.tweepy.org/en/v4.3.0/).
### Query Terms
For our current project, we are focusing on the fashion industry, thus, the query terms set to draw tweets are terms related to fashion. These factors can be easily modified in the `produceTweets.py` file under `query_terms`.
### Throughput Analysis
Window is the time interval that tweets are being aggregated and trigger is the time interval the aggregation updates from the stream. These factors can be easily modified in the `consumeTweets.py` file under `#Global Constants`.

We have determined that smallest window/trigger intervals based on the number of tweets coming in vs the number of outputs. This is because for the LDA model for topic modelling, there is a required number of tokens per window in order to generate the vocabularies table.

These are the current settings:
```python
# Set window and trigger interval
window_interval = {'sentiment_analysis': '5 seconds',
                   'topic_modelling': '1 minutes'}
trigger_interval = {'sentiment_analysis': '5 seconds',
                    'topic_modelling': '20 seconds'}

# Set watermark
watermark_time = {'sentiment_analysis': '5 seconds',
                  'topic_modelling': '1 minutes'}
```
### Apache Drill Custom SQL statements
As our database gets filled with the outputs, we use SQL statements to query the amount of data that we want in our visualisation. In our workbook we are sorting them by ID, then taking the latest 60. This would not cause Tableau to lag.

```SQL
SELECT `topic_modelling`.`_id` AS `_id (topic_modelling)`,
  `topic_modelling`.`token` AS `token`,
  `topic_modelling`.`count` AS `count`,
  `topic_modelling`.`topic` AS `topic`
FROM `mongo.realtime_tweets_analysis`.`topic_modelling` `topic_modelling`
order by `_id (topic_modelling)` desc
limit 60
```

### Data Cleaning
Before moving on to data preparation for respective model building, we performed a series of data cleaning using user defined funcions for every tweet, which includes a series of steps to remove links, user names, punctuation, numbers and hashtags from twitter content.

### Sentiment Analysis Pipeline
The model takes in text_cleaned and created_at columns and then applies sentiment analysis using textblob to generate polarity and subjectivity scores of each tweet within the range [0,1]. This is being done by user defined function text_classification. 
At the end of the model, we group all the tweets by hashtags and generate the aggregate mean polarity for every hashtag in each window out of all the windows we have batched.

### Topic Modelling Pipeline
Topic modeling takes in text_cleaned and created_at columns and pass the data through a pre-processing pipeline using sparkNLP. This pipeline consists of document assembler, tokenizer, normalizer, stopwords cleaner, stemmer and finisher. preprocessed data in token format will then be passed through feature engineering using spark MLlib's CountVectorizer to generate features specific to Latent Dirichlet Allocation (LDA) model vacabulary to perform topic modeling. 
When building up topic modeling, we specify the number of topics we want to get as 3 due to the length of window for batching data. This can be adjusted under variable num_topics. We set the number of maximum iterations to be 10 in order to fit the case for real time analysis.
At the end of the model, we find out the count of each unique token in all batched windows grouped by each topic. The dataframe is then sent to mongoDB for further data visualizations using Tableau.

## License
Distributed under the MIT License. See `LICENSE` for more information.

## Acknowledgements
* [Starting code on Prof Zhang's IS459 repository](https://github.com/ZhangZhenjie/SMU-IS459)
* [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
* [Spark Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)

!! Acknowledgements will be updated

## Group Members:

- CHAN Yao Ying, yychan.2019@scis.smu.edu.sg
- Bryan HUI Hon Yu, bryan.hui.2019@scis.smu.edu.sg
- Kenny GOH Ju Lin, kenny.goh.2019@scis.smu.edu.sg
- YANG Zhouyifan, zyfyang.2018@scis.smu.edu.sg
- Yash GADODIA, yashgadodia.2019@scis.smu.edu.sg




