"""Generates a twitter stream to Kafka"""

import os
import tweepy as tw
import pandas as pd
import argparse
import json
import sys
import time
import socket
from dotenv import load_dotenv
from confluent_kafka import Producer, KafkaError, KafkaException
from tweepy.streaming import StreamListener
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler

load_dotenv("/.env")

#############################
## HELPER FUNCTIONS
#############################

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))

class TweetListener(StreamListener):

    def __init__(self, conf, topic):
        # initialise the kafka producer 
        self.producer = Producer(conf)
        self.topic = topic
        
    def on_data(self, data):
        try:
            # write to kafka producer
            result = json.dumps(data)
            self.producer.produce(self.topic, key="key", value=result, callback=acked)
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True

def main():

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')
    args = parser.parse_args()

    topic = args.topic

    conf = {'bootstrap.servers': "localhost:9092",
            'client.id': socket.gethostname()}
            
    ###################################

    consumer_key= 'xsoYzI8TDAGyZQBIhaE6gY5ZI'
    consumer_secret= 'iOYYy64CazvdHiGZZgGpUhguowpPjVzGa59XGlCTF8MaA0xAoI'
    access_token= '2365205522-hkfTmRYn8qXxBnUqWFE6MV0XQPwr7rYqXDWcFLy'
    access_token_secret= 'hNfRlQrXmgcC2mIURYTinCZCGuZPrmKNCBRCtxm49inFC'

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # Define the search term and the date_since date as variables
    twitter_stream = Stream(auth, TweetListener(conf, topic))
    query_terms = ['covid', 'corona', '19']
    twitter_stream.filter(track=query_terms)

    #####################################################

if __name__ == "__main__":
    main()