from pykafka import KafkaClient
from time import sleep

client = KafkaClient(hosts="127.0.0.1:9092")
topic = client.topics[b'test']
batch_size = 1000
limit = 100000

with open('tweets.txt') as f:
    tweets = f.readlines()

with topic.get_sync_producer() as producer:
    i = 0
    while i < limit:
        for tweet in tweets[i : i + batch_size]:
            producer.produce(tweet.encode('utf-8'))
        i += batch_size
        print(i)
        sleep(1)
