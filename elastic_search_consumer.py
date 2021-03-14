# kafka consumer to elastic search
#
# This program will consume the data from kafka and send it to Bonsai Elastic Search

import json
from kafka import KafkaConsumer
from datetime import datetime
from elasticsearch import Elasticsearch

TOPIC_NAME = 'covid'

consumer = KafkaConsumer(TOPIC_NAME)

host = "es-bonsai-hostname" #replace with hostname
user = "es-bonsai-user" # replace with real user name
password = "es-bonsai-password" # replace with password
port = 443

es_header = [{
 'host': host,
 'port': port,
 'use_ssl': True,
 'http_auth': (user,password)
}]

es = Elasticsearch(es_header)

for message in consumer:
    doc = {
        'text': message.value.decode()
    }

    json_string = json.dumps(doc)
    res = es.index(index="twitter-feed", body=json_string)