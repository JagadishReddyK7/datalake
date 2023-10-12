import asyncio
from pymongo import MongoClient
import requests
import boto3
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.errors import KafkaError
from retrying import retry

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
async def validate_mongoDB(connection_uri, collection_name):
    try:
        client = MongoClient(connection_uri)
        db = client.get_database()
        collection_name = db.get_collection(collection_name)
        result = await loop.run_in_executor(None, collection_name.find_one)
        if result:
            return True
    except Exception as e:
        print(e)
        return False

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
async def validate_nifi(nifi_url, processor_url, queue_url):
    tasks = [check_nifi_webui(nifi_url), check_nifi_processor(processor_url), check_flowfile_queue(queue_url)]
    await asyncio.gather(*tasks)

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
async def validate_s3_bucket(access_key, secret_key, bucket_name):
    tasks = [check_s3_connection(access_key, secret_key), check_s3_upload(access_key, secret_key, bucket_name)]
    await asyncio.gather(*tasks)

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
async def validate_kafka_topic(broker_address,topic_name):
    tasks=[check_kafka_broker(broker_address), check_topic(broker_address,topic_name)]
    await asyncio.gather(*tasks)

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
async def check_nifi_webui(nifi_url):
    response = await loop.run_in_executor(None, requests.get, nifi_url)

    if response.status_code == 200:
        print("NiFi Web UI is accessible.")
        return True
    else:
        print(f"NiFi Web UI is not accessible. Status code: {response.status_code}")
        return False

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
async def check_nifi_processor(processor_url):
    response = await loop.run_in_executor(None, requests.get, processor_url)

    if response.status_code == 200:
        print("Processor is running.")
        return True
    else:
        print(f"Processor is not running. Status code: {response.status_code}")
        return False

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
async def check_flowfile_queue(queue_url):
    response = await loop.run_in_executor(None, requests.get, queue_url)

    if response.status_code == 200:
        queue_info = response.json()
        print(f"Queue size: {queue_info['queueSize']}")
        return True
    else:
        print(f"Unable to get queue information. Status code: {response.status_code}")
        return False

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
async def check_s3_connection(aws_access_key, aws_secret_key):
    s3 = boto3.client('s3', aws_access_key, aws_secret_key)
    try:
        await loop.run_in_executor(None, s3.list_buckets)
        print("Connected to S3.")
        return True
    except Exception as e:
        print(f"Unable to connect to S3: {e}")
        return False

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
async def check_s3_upload(aws_access_key, aws_secret_key, bucket_name):
    s3 = boto3.client('s3', aws_access_key, aws_secret_key)
    file_path = ''
    try:
        await loop.run_in_executor(None, s3.upload_file, file_path, bucket_name, '')
        print("File uploaded to S3.")
        return True
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        return False

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)    
async def check_kafka_broker(broker_address):
    try:
        producer=await loop.run_in_executor(None, KafkaProducer,broker_address)
        producer.close()
        print("Kafka producer connected successfully.")

        consumer = await loop.run_in_executor(None, KafkaConsumer,broker_address)
        consumer.close()
        print("Kafka consumer connected successfully.")

        admin_client = await loop.run_in_executor(None, KafkaAdminClient,broker_address)
        metadata = await loop.run_in_executor(None, admin_client.list_topics, 5000)
        admin_client.close()
        print("Kafka metadata retrieved successfully.")
        return True

    except KafkaError as e:
        print(f"Error connecting to Kafka: {e}")
        return False

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
async def check_topic(broker_address, topic_name):
    try:
        consumer = await loop.run_in_executor(None, KafkaConsumer,broker_address)
        topic_metadata = await loop.run_in_executor(None,consumer.list_topics,topic_name, 5000)
        consumer.close()

        if topic_metadata and topic_metadata.topics.get(topic_name):
            print(f"Kafka topic '{topic_name}' exists.")
            return True
        else:
            print(f"Kafka topic '{topic_name}' does not exist.")
            return False

    except KafkaError as e:
        print(f"Error checking topic existence: {e}")
        return False

async def main_func(connection_uri,collection_name,nifi_url,processor_url,queue_url,access_key,secret_key,bucket_name,broker_address,topic_name):

    await validate_mongoDB(connection_uri, collection_name)
    await validate_nifi(nifi_url, processor_url, queue_url)
    await validate_s3_bucket(access_key, secret_key, bucket_name)
    await validate_kafka_topic(broker_address,topic_name)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main_func())
    loop.close()
