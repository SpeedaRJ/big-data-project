import os
import json
import argparse
from multiprocessing import Process

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:29092"


def parse_args():
    parser = argparse.ArgumentParser(description="Consume data from Kafka")
    parser.add_argument("--topics", nargs="+", help="Topics to consume")
    parser.add_argument("--save_path", type=str, default="./tasks/04/results", help="Path to save the results")
    return parser.parse_args()


def consumer(topic, save_path):
    # start the consumer
    consumer = KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP_SERVERS, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    
    # delete the file with results if it exists
    if save_path and os.path.exists(f"{save_path}/{topic}.txt"):
        os.remove(f"{save_path}/{topic}.txt")
    
    # consume messages
    print(f"Consumer started. Waiting for messages on topic {topic}...")
    for message in consumer:
        if save_path:
            with open(f"{save_path}/{topic}.txt", "a") as f:
                f.write(f"{message.topic}:{message.partition}:{message.offset} - key: {message.key}, value: {message.value}\n")
        else:
            print(f"{message.topic}:{message.partition}:{message.offset} - key: {message.key}, value: {message.value}")


def main():
    args = parse_args()
    # start a separate process for each topic
    for topic in args.topics:
        p = Process(target=consumer, args=(topic, args.save_path))
        p.start()


if __name__ == "__main__":
    main()
