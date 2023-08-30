from kafka import KafkaConsumer
from json import loads
import json
from s3fs import S3FileSystem

consumer = KafkaConsumer(
    'demo_test',
    bootstrap_servers=['18.197.87.240:9092'],  # add your IP here
    value_deserializer=lambda x: loads(x.decode('utf-8')))

s3 = S3FileSystem()
s3 = S3FileSystem(key='AKIAXGT3DVLU6XJ24ROE',
                  secret='YQJkR5y7Tyn2nkoZn4XUpcH8fMh5nQCC5+oueoa1')


for count, i in enumerate(consumer):
    s3_key = "data-center921{}.json".format(count)
    s3_path = "s3://data-center921/{}".format(s3_key)

    with s3.open(s3_path, 'w') as file:
        json.dump(i.value, file)
