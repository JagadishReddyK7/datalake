import yaml
from concurrent.futures import ThreadPoolExecutor
from validation import *

with open("config.yaml", "r") as yamlfile:
    data = yaml.load(yamlfile, Loader=yaml.FullLoader)
    print("Read successful")
print(data)



