from kafka import KafkaProducer
import json
from data import get_customer
import time

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(bootstrap_servers=["localhost:9092"],value_serializer=json_serializer)

if __name__=="__main__":
    customer_list=get_customer()
    for record in customer_list:
        customer={
            "Id":record[0],
            "age":record[1],
            "gender":record[2]
        } 
        print(customer)
        producer.send("topic_63",customer)
        time.sleep(1)