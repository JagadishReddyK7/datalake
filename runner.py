import yaml
from concurrent.futures import ThreadPoolExecutor
from validation import *
from job_scheduler import main_func
from time import sleep
import asyncio

with open("config.yaml", "r") as yamlfile:
    data = yaml.load(yamlfile, Loader=yaml.FullLoader)
    mongo_uri = data[0]['mongoDB']['connection_uri']
    collection = data[0]['mongoDB']['collection_name']
    nifi_web_url = data[2]['nifi']['web_url']
    nifi_processor_url = data[2]['nifi']['processor_url']
    nifi_queue_url = data[2]['nifi']['queue_url']
    s3_bucket_name = data[1]['aws_s3']['bucket_name']
    s3_access_key = data[1]['aws_s3']['access_key']
    s3_secret_key = data[1]['aws_s3']['secret_key']
    broker_address = data[3]['kafka']['kafka_broker']
    topic_name = data[3]['kafka']['kafka_topic']

loop = asyncio.get_event_loop()
loop.run_until_complete(main_func(mongo_uri,collection,nifi_web_url,nifi_processor_url,nifi_queue_url,s3_access_key,s3_secret_key,s3_bucket_name,broker_address,topic_name))
loop.close()
