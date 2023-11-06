from kafka import KafkaConsumer
import csv

kafka_bootstrap_servers = "localhost:9092" 
kafka_topic = "sparkKafka"

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    value_deserializer=lambda v: v.decode('utf-8')
)
print(consumer)

for i in consumer:
    print(i)
    data1=i.value()
    print(data1)

csv_output_path = "output2.csv"

with open(csv_output_path, 'w', newline='') as csv_file:
    csv_writer = csv.DictWriter(csv_file, fieldnames=["CustomerID", "Gender","Age", "Annual Income (k$)","Spending Score (1-100)"])
    csv_writer.writeheader()

    for message in consumer:
        print(message)
        data = message.value()
        print(data)
        csv_writer.writerow(data)

consumer.close()