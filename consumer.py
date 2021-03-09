# Kafka Consumer

from kafka import KafkaConsumer

TOPIC_NAME = 'soccer'

consumer = KafkaConsumer(TOPIC_NAME)

for message in consumer:
    print(message)
