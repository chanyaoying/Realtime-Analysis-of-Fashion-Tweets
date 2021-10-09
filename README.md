# IS459-Project
Real-time Analysis of Fast Fashion Trends from Twitter

Usage
--------
1. Clone repo and cd into directory.

```
git clone https://github.com/chanyaoying/IS459-Project.git
cd IS459-Project
```

2. Ensure you have env vars for tweitter api in a .env folder 

3. **Start the Kafka + Zookeeper server**

```
docker compose up --build
```

4. **Start the Twitter producer stream**


```
python bin/produceTweets.py test_tweets
```

5. **Start a consumer**

To start a consumer for printing all messages in real-time to the console:

Enter PySpark shell
```
pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2
```
Run each command in `bin/consumerTweets.py` line-by-line into the shell.






