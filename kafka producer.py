import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps

# Create a KafkaProducer instance
producer = KafkaProducer(
    bootstrap_servers=['18.197.87.240:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

# Send a single message
producer.send('demo_testing3', value={'surname': 'paramar'})

# Read CSV file using pandas
csv_file_path = r'C:\Users\curse\Desktop\kafka\stock-market-kafka-data-engineering-project\indexProcessed.csv'
df = pd.read_csv(csv_file_path)

# Print the first few rows of the DataFrame
print(df.head())

# Continuously send sample data from the DataFrame to Kafka
while True:
    dict_stock = df.sample(1).to_dict(orient="records")[0]
    producer.send('demo_test', value=dict_stock)
    sleep(1)
