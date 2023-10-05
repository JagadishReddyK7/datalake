import yaml

config=[
    {
        'kafka': {
            'kafka_topic':'',
            'kafka_broker':''
        }
    }
]

with open('config.yaml','a') as configfile:
    data=yaml.dump(config,configfile)
    print("configuration added successfully")