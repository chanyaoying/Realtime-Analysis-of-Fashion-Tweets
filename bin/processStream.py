"""Consumes stream for printing all messages to the console."""

import os
import tweepy as tw
import pandas as pd
import argparse
import json
import sys
import time
import socket
from confluent_kafka import Consumer, KafkaError, KafkaException
from tweepy.streaming import StreamListener
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler


#############################
## HELPER FUNCTIONS
#############################

def msg_process(msg):
    # Print the current time and the message.
    time_start = time.strftime("%Y-%m-%d %H:%M:%S")
    val = msg.value()
    dval = json.loads(val)
    print(time_start, dval)


output_file = 'Tweets_covid.txt'
class MyListener(StreamListener):
    def on_data(self, data):
        try:
            # IMPORTANT: 'a' for appending to the existing file content
            # write to kafka consumer 
            with open(output_file, 'a') as f:
                f.write(data)
                return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


#############################
## MAIN FUNCTION
#############################

def main():

    #TODO: create env file for keys/tokens
    consumer_key= 'xsoYzI8TDAGyZQBIhaE6gY5ZI'
    consumer_secret= 'iOYYy64CazvdHiGZZgGpUhguowpPjVzGa59XGlCTF8MaA0xAoI'
    access_token= '2365205522-hkfTmRYn8qXxBnUqWFE6MV0XQPwr7rYqXDWcFLy'
    access_token_secret= 'hNfRlQrXmgcC2mIURYTinCZCGuZPrmKNCBRCtxm49inFC'

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # Define the search term and the date_since date as variables
    twitter_stream = Stream(auth, MyListener())
    query_terms = ['covid', 'covid19', 'coronavirus', 'wuhanvirus']
    twitter_stream.filter(track=query_terms)

    #####################################################

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream: ')

    args = parser.parse_args()

    conf = {'bootstrap.servers': 'localhost:9092',
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'group.id': socket.gethostname()}

    consumer = Consumer(conf)

    running = True

    try:
        while running:
            consumer.subscribe([args.topic])

            msg = consumer.poll(1)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    sys.stderr.write('Topic unknown, creating %s topic\n' %
                                     (args.topic))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)

    except KeyboardInterrupt:
        pass

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == "__main__":
    main()