import asyncio
from pymongo import MongoClient
import requests
import boto3
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
from retrying import retry
import logging
import yaml
# from stream_kafka.kafka_consumer_ray import kafka_consumer
# from stream_kafka.kafka_producer_ray import kafka_producer
# Configure logging
log_filename = "validation.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create a logger instance
logger = logging.getLogger("Validation")

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
async def validate_mongoDB(connection_uri, collection_name):
    """
    Validate MongoDB connection and check if a specified collection exists.
    """
    try:
        client = MongoClient(connection_uri)
        db = client.get_database()
        collection_name = db.get_collection(collection_name)
        result = await loop.run_in_executor(None, collection_name.find_one)
        if result:
            logger.info("MongoDB connection and collection validation successful.")
            return True
    except Exception as e:
        logger.error(f"MongoDB validation failed: {e}")
        return False

# @retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
# async def validate_nifi(nifi_url, processor_url, queue_url):
#     """
#     Validate NiFi components, including NiFi Web UI, processor, and flowfile queue.
#     """
#     tasks = [check_nifi_webui(nifi_url), check_nifi_processor(processor_url), check_flowfile_queue(queue_url)]
#     await asyncio.gather(*tasks)

# @retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
# async def validate_s3_bucket(access_key, secret_key, bucket_name):
#     """
#     Validate S3 bucket connection and check if the bucket is accessible.
#     """
#     tasks = [check_s3_connection(access_key, secret_key), check_s3_upload(access_key, secret_key, bucket_name)]
#     await asyncio.gather(*tasks)

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
async def validate_kafka_topic(broker_address, topic_list):
    """
    Validate Kafka connection and check if a specified topic exists.
    """
    tasks = [check_kafka_broker(broker_address)]
    for topic in topic_list:
        tasks.append(check_and_create_topic(broker_address,topic))
    await asyncio.gather(*tasks)

# @retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
# async def check_nifi_webui(nifi_url):
#     """
#     Check if NiFi Web UI is accessible.
#     """
#     response = await loop.run_in_executor(None, requests.get, nifi_url)

#     if response.status_code == 200:
#         logger.info("NiFi Web UI is accessible.")
#         return True
#     else:
#         logger.error(f"NiFi Web UI is not accessible. Status code: {response.status_code}")
#         return False

# @retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
# async def check_nifi_processor(processor_url):
#     """
#     Check if the NiFi processor is running.
#     """
#     response = await loop.run_in_executor(None, requests.get, processor_url)

#     if response.status_code == 200:
#         logger.info("Processor is running.")
#         return True
#     else:
#         logger.error(f"Processor is not running. Status code: {response.status_code}")
#         return False

# @retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
# async def check_flowfile_queue(queue_url):
#     """
#     Check the status of the NiFi flowfile queue.
#     """
#     response = await loop.run_in_executor(None, requests.get, queue_url)

#     if response.status_code == 200:
#         queue_info = response.json()
#         logger.info(f"Queue size: {queue_info['queueSize']}")
#         return True
#     else:
#         logger.error(f"Unable to get queue information. Status code: {response.status_code}")
#         return False

# @retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
# async def check_s3_connection(aws_access_key, aws_secret_key, bucket_name):
#     """
#     Check the existence of an S3 bucket.
#     """
#     s3 = boto3.client('s3', aws_access_key, aws_secret_key)
#     try:
#         await loop.run_in_executor(None, lambda: s3.head_bucket(bucket_name))
#         logger.info("S3 bucket exists.")
#         return True
#     except Exception as e:
#         logger.error(f"The S3 bucket does not exist or is not accessible: {e}")
#         return False

# @retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
# async def check_s3_upload(aws_access_key, aws_secret_key, bucket_name):
#     """
#     Check if a file can be uploaded to the specified S3 bucket.
#     """
#     s3 = boto3.client('s3', aws_access_key, aws_secret_key)
#     file_path = ''
#     try:
#         await loop.run_in_executor(None, s3.upload_file, file_path, bucket_name, '')
#         logger.info("File uploaded to S3.")
#         return True
#     except Exception as e:
#         logger.error(f"Error uploading file to S3: {e}")
#         return False

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
async def check_kafka_broker(broker_address):
    """
    Check the connection to the Kafka broker and its metadata.
    """
    try:
        producer = await loop.run_in_executor(None, KafkaProducer, broker_address)
        producer.close()
        logger.info("Kafka producer connected successfully.")

        consumer = await loop.run_in_executor(None, KafkaConsumer, broker_address)
        consumer.close()
        logger.info("Kafka consumer connected successfully.")

        admin_client = await loop.run_in_executor(None, KafkaAdminClient, broker_address)
        metadata = await loop.run_in_executor(None, admin_client.list_topics, 5000)
        admin_client.close()
        logger.info("Kafka metadata retrieved successfully.")
        return True

    except KafkaError as e:
        logger.error(f"Error connecting to Kafka: {e}")
        return False

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
async def check_and_create_topic(broker_address, topic_name):
    """
    Check if a specified Kafka topic exists. If not, create the topic.
    """
    try:
        consumer = await loop.run_in_executor(None, KafkaConsumer, broker_address)
        topic_metadata = await loop.run_in_executor(None, consumer.list_topics, topic_name, 5000)
        consumer.close()

        if topic_metadata and topic_metadata.topics.get(topic_name):
            logger.info(f"Kafka topic '{topic_name}' exists.")
            return True
        else:
            logger.warning(f"Kafka topic '{topic_name}' does not exist. Creating the topic.")
            admin_client = await loop.run_in_executor(None, KafkaAdminClient, broker_address)
            await create_kafka_topic(admin_client, topic_name)
            return False

    except KafkaError as e:
        logger.error(f"Error checking/creating topic '{topic_name}': {e}")
        return False

@retry(wait_exponential_multiplier=10000, wait_exponential_max=40000, stop_max_attempt_number=3)
async def create_kafka_topic(admin_client, topic_name, num_partitions=1, replication_factor=1):
    """
    Create a Kafka topic with the specified parameters.
    """
    try:
        new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        logger.info(f"Kafka topic '{topic_name}' created.")
        return True

    except KafkaError as e:
        logger.error(f"Error creating Kafka topic '{topic_name}': {e}")
        return False

async def main_func(connection_uri, collection_name, broker_address, topic_list):
    """
    Main function to run validation checks for MongoDB, NiFi, S3, and Kafka components.
    """
    await validate_mongoDB(connection_uri, collection_name)
    await validate_kafka_topic(broker_address, topic_list)
    # await validate_nifi(nifi_url, processor_url, queue_url)
    # await validate_s3_bucket(access_key, secret_key, bucket_name)

if __name__ == "__main__":
    with open("scheduler/config.yaml", "r") as yamlfile:
        data = yaml.load(yamlfile, Loader=yaml.FullLoader)
        mongo_uri = data[0]['source']['mongoDB']['connection_uri']
        collection = data[0]['source']['mongoDB']['collection_name']
        broker_address = data[1]['processor']['kafka']['kafka_broker']
        source_list=list(data[0]['source'].keys())
        processor_list=list(data[1]['processor'].keys())
        destination_list=list(data[2]['destination'].keys())

def get_kafka_topic_names(source_list,processor_list,destination_list):
    topics_list=[]
    for source in source_list:
        for processor in processor_list:
            for destination in destination_list:
                topics_list.append(source+processor+destination)
    return topics_list

topic_names=get_kafka_topic_names(source_list,processor_list,destination_list)

loop = asyncio.get_event_loop()
loop.run_until_complete(main_func(mongo_uri,collection,broker_address,topic_names))
loop.close()