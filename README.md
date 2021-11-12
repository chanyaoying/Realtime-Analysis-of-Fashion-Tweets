# IS459-Project
Real-time Analysis of Fast Fashion Trends from Twitter

TODO: insert description of project here (@bbhui)

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
Make sure Hadoop, Zookeeper and Kafka are running
On a separate terminal, run `kafka_wordcount.py` with spark-submit. Assuming that `$SPARK_HOME` is where Spark is installed on your machine.
```sh
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 kafka_wordcount.py
```
Once the consumer is up, spin up the producer on a separate terminal
```sh
cd hardwarezone
scrapy crawl hardwarezone
```

## License
Distributed under the MIT License. See `LICENSE` for more information.

## Acknowledgements
* [Starting code on Prof Zhang's IS459 repository](https://github.com/ZhangZhenjie/SMU-IS459)
* [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
* [Spark Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)






