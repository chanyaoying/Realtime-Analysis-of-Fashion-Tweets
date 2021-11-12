# IS459-Project
Real-time Analysis of Fast Fashion Trends from Twitter

TODO: update drill & tableau
This solution provides a end to end pipeline for real time streaming tweets analysis with quick visualizations generated which provides real time tracking of twitter user behavior and sentiment. It is easily integrable and domain agnostic to other use cases as list of topics can be specified to any target field.

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
#### Kafka + Zookeeper + Apache Drill + MongoDB
We are assuming that you have MongoDB, Kafka and Zookeeper installed.

Alternatively, you can run them on docker containers. A `docker-compose` file is provided.
```sh
docker-compose up
```

To install Apache drill manually,
refer to this official installation guide.
## Usage (edit from here @bbhui)
Make sure Hadoop, Zookeeper, Kafka, Apache Drill and MongoDB are running

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
TODO: update (@yao)

### Tableau
TODO: update (@yao)

### Window and Trigger Interval
Window is the time interval that tweets are being aggregated and trigger is the time interval the aggregation updates from the stream. These factors can be easily modified in the `consumeTweets.py` file under `Global Constants`.
Currently, window interval is set to 2 minutes, while trigger interval is set to 1 minute.

### Query Terms
For our current project, we are focusing on the fashion industry, thus, the query terms set to draw tweets are terms related to fashion. These factors can be easily modified in the `produceTweets.py` file.

## License
Distributed under the MIT License. See `LICENSE` for more information.

## Acknowledgements
* [Starting code on Prof Zhang's IS459 repository](https://github.com/ZhangZhenjie/SMU-IS459)
* [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
* [Spark Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)






