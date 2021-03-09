# TWITTER TO KAFKA PRODUCER
#
# This program will read a tweeter stream and create a Kafka producer from it.
#

import tweepy, json
from kafka import KafkaProducer
from time import sleep

# twitter credentials
consumer_key = "CONSUMER_KEY"
consumer_secret = "CONSUMER_SECRET"
access_token = "ACCESS_TOKEN"
access_token_secret = "ACCESS_SECRET"

# topic to track
TOPIC_NAME = 'covid'

# server config
KAFKA_SERVER = 'localhost:9092'

class StreamListener(tweepy.StreamListener):

    def on_status(self, status):
        print(status.text)

    def on_error(self, status_code):
        if status_code == 420:
            return False

    def on_data(self, data):
        try:
            tweet = json.loads(data)
            producer.send(TOPIC_NAME, tweet['text'].encode('utf-8'))
            print(tweet['text'])
            sleep(3)
        except Exception as e:
            print(e)
            return False

        return True

    def on_timeout(self):
        return True

# Twitter authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

# create producer
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

# stream listener
stream_listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=20, retry_delay=5, retry_count=10, retry_errors=set([401, 404, 500, 503])))
stream = tweepy.Stream(auth = api.auth, listener=stream_listener)

# start the stream
print("Tracking: " + str(TOPIC_NAME))
stream.filter(track=[TOPIC_NAME], languages = ['en'])