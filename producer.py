# Kafka producer

from kafka import KafkaProducer

TOPIC_NAME = 'soccer'
KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

producer.send(TOPIC_NAME, b'Goal!')

#send message in batch
producer.flush()
