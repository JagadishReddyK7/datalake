from pymongo import MongoClient
import requests

def validate_mongoDB(connection_url,collection_name):
    try:
        client=MongoClient(connection_url)
        db=client.get_database()
        collection_name=db.get_collection(collection_name)
        result=collection_name.find_one()
        if result:
            return True
    except Exception as e:
        print("MongoError:"+e)

def validate_nifi(nifi_url,processor_url,queue_url):
    check_nifi_webui(nifi_url)
    check_nifi_processor(processor_url)
    check_flowfile_queue(queue_url)
    

def validate_s3_bucket(access_key,secret_key,bucket_name):
    pass

def validate_kafka_topic(topic_name):
    pass

def check_nifi_webui(nifi_url):
    response = requests.get(nifi_url)

    if response.status_code == 200:
        print("NiFi Web UI is accessible.")
        return True
    else:
        print(f"NiFi Web UI is not accessible. Status code: {response.status_code}")
        return False
    
def check_nifi_processor(processor_url):
    response = requests.get(processor_url)

    if response.status_code == 200:
        print("Processor is running.")
        return True
    else:
        print(f"Processor is not running. Status code: {response.status_code}")
        return False
    
def check_flowfile_queue(queue_url):
    response = requests.get(queue_url)

    if response.status_code == 200:
        queue_info = response.json()
        print(f"Queue size: {queue_info['queueSize']}")
    else:
        print(f"Unable to get queue information. Status code: {response.status_code}")

