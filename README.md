# IS459-Project
Real-time Analysis of Fast Fashion Trends from Twitter

Usage
--------
1. Clone repo and cd into directory.

```
git clone https://github.com/chanyaoying/IS459-Project.git
cd time-series-kafka-demo
```

2. Ensure you have env vars for tweitter api in a .env folder 

3. **Start the Kafka + Zookeeper broker**

```
docker compose up --build
```

4. **Start a producer**


```
python bin/sendStream.py data/data.csv my-stream
```

5. **Start a consumer**

To start a consumer for printing all messages in real-time from the stream "my-stream":

```
python bin/processStream.py my-stream
```






