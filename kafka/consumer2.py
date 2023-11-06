from kafka import KafkaConsumer,TopicPartition
import csv

def create_consumer(broker, group_id, topic,partitions):
    consumer = KafkaConsumer(
        group_id=group_id,
        bootstrap_servers=broker,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        # group_subscribe=True,
        value_deserializer=lambda v: v.decode('utf-8')
    )
    partitions_for_consumer = [TopicPartition(topic, partition) for partition in partitions]
    consumer.assign(partitions_for_consumer)

    print(consumer)
    return consumer

def consume_messages(consumer):
    try:
        for msg in consumer:
            print(f"Received message: {msg.value} from partition {msg.partition}")
    except Exception as e:
        print(e)
    finally:
        consumer.close()

def write_messages_to_csv(consumer,csv_output_path):
    try:
        with open(csv_output_path,'w',newline='') as csv_file:
            csv_writer=csv.DictWriter(csv_file)
            for message in consumer:
                data=message.value()
                csv_writer.writerow(data)
    except Exception as e:
        print(e)
    finally:
        consumer.close()

if __name__ == "__main__":
    broker_address = 'localhost:9092'
    group_id = '1707'
    topic = 'topic_65'
    csv_output_path='multiConsumerData2.csv'

    consumer = create_consumer(broker_address, group_id, topic,[2,3])
    consume_messages(consumer)
    # write_messages_to_csv(consumer,csv_output_path)
