from kafka import KafkaConsumer,TopicPartition
import csv
import asyncio

def create_consumer(broker, group_id, topic,partitions):
    consumer = KafkaConsumer(
        # topic,
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

async def consume_messages(consumer):
    try:
        for msg in consumer:
            await loop.run_in_executor(None,print,f"Received message: {msg.value} from partition {msg.partition}")
    except Exception as e:
        print(e)
    finally:
        consumer.close()

async def write_messages_to_csv(consumer,csv_output_path):
    try:
        with open(csv_output_path,'a',newline='') as csv_file:
            csv_writer=csv.DictWriter(csv_file)
            for message in consumer:
                data=message.value()
                await loop.run_in_executor(None,csv_writer.writerow,data)
    except Exception as e:
        print(e)
    finally:
        consumer.close()

async def main(consumer1,consumer2,csv_output_path):
    await consume_messages(consumer1)
    await consume_messages(consumer2)
    await write_messages_to_csv(consumer1,csv_output_path)
    await write_messages_to_csv(consumer2,csv_output_path)


if __name__ == "__main__":
    broker_address = 'localhost:9092'
    group_id = '1707'
    topic = 'testing'
    csv_output_path='multiConsumerData1.csv'

    consumer1 = create_consumer(broker_address, group_id, topic,[0,1])
    consumer2 = create_consumer(broker_address, group_id, topic,[2,3])

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(consumer1,consumer2,csv_output_path))
    loop.close()