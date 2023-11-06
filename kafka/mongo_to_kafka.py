# from confluent_kafka import Producer
from pymongo import MongoClient
from kafka import KafkaProducer
from json import dumps
import os
from bson import ObjectId
import json

# MongoDB configuration
mongo_uri = "mongodb+srv://nifi:Nifi12345678@cluster0.nnyrzav.mongodb.net/"
mongo_db_name = "datalake"
mongo_collection_name = "nifi"
batch_size = 100  # Adjust the batch size as needed

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[mongo_db_name]
collection = db[mongo_collection_name]

# Shared state file on the network-attached file system
state_file = "shared-data\state.txt"

def get_last_processed_id():
    try:
        if os.path.exists(state_file):
            with open(state_file, "r") as file:
                last_id = file.read()
                if last_id:
                    return last_id
                else:
                    cursor = list(collection.find({}).sort([("_id", 1)]).limit(1))
                    return cursor[0]['_id']
        else:
            cursor = list(collection.find({}).sort([("_id", 1)]).limit(1))
            return cursor[0]['_id']
    except Exception as e:
        print(e)
        cursor = list(collection.find({}).sort([("_id", 1)]).limit(1))
        return cursor[0]['_id']

def save_last_processed_id(last_id):
    with open(state_file, "w") as file:
        file.write(str(last_id))
        
def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(bootstrap_servers=["localhost:9092"],value_serializer=json_serializer)

def process_data(cursor, last_processed_id):
    documents_exist = False
    for document in cursor:
        
        # data = {"merchant_details":[{"merchant_id":"gopi","start_date":"111","end_date":"222"}]}

        # producer.send("gopi",value=data)
        documents_exist = True
        if document["_id"] > last_processed_id:
            # Your data processing logic here
            last_processed_id = document["_id"]
            # print(last_processed_id)
        del document["_id"]
        del document['Timestamp']
        data = document
        print(type(data))
        print(data)
        producer.send("testing",value=data)
        # print(data)
    if not documents_exist:
        return False

    # Save the last processed ID to the shared state file
    
    save_last_processed_id(last_processed_id)
    return True

def main():

    last_processed_id = get_last_processed_id()
    print(last_processed_id,"last_processed_id")
    last_processed_id = ObjectId(last_processed_id)
    while True:
        cursor = list(collection.find({"_id": {"$gt": last_processed_id}}).limit(batch_size))
        
        if not process_data(cursor, last_processed_id):
            break
    

if __name__ == "__main__":
    main()