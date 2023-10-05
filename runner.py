import yaml
from concurrent.futures import ThreadPoolExecutor
from validation import *
from time import sleep

with open("config.yaml", "r") as yamlfile:
    data = yaml.load(yamlfile, Loader=yaml.FullLoader)
    mongo_uri=data[0]['mongoDB']['connection_uri']
    collection=data[0]['mongoDB']['collection_name']
    nifi_web_url=data[2]['nifi']['web_url']
    nifi_processor_url=data[2]['nifi']['processor_url']
    nifi_queue_url=data[2]['nifi']['queue_url']
    s3_bucket_name=data[1]['aws_s3']['bucket_name']
    s3_access_key=data[1]['aws_s3']['access_key']
    s3_secret_key=data[1]['aws_s3']['secret_key']

print(data)

with ThreadPoolExecutor(max_workers=10) as ex:
    mongo_check=ex.submit(validate_mongoDB,mongo_uri,collection)
    # print(mongo_check.result())
    nifi_check=ex.submit(validate_nifi,nifi_web_url,nifi_processor_url,nifi_queue_url)
    s3_check=ex.submit(validate_s3_bucket)

    