import os
import json
import argparse
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "raw-data"

def parse_args():
    parser = argparse.ArgumentParser(description="Produce data to Kafka")
    parser.add_argument("--data-dir", default="./data/raw", help="Directory with data files relative to root of the project")
    parser.add_argument("--years", nargs="+", help="Years to produce") # we assume that each year has an associated <year>.csv file
    parser.add_argument("--n-lines", type=int, default=100, help="Number of lines to produce") # so we can limit the number of lines produced when testing (if -1 then all lines are produced)
    return parser.parse_args()



def producer(file_path, n_lines):
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    with open(file_path, "r") as f:
        count = 0
        header = f.readline()
        columns = header.strip().split(",")
        for line in f:
            fields = line.strip().split(",")
            
            # TODO: send only fields that we will use in the stream processing
            line_dict = {column: field for column, field in zip(columns, fields)}
            
            producer.send(topic=TOPIC, value=line_dict)
            count += 1
            if count >= n_lines:
                break
        producer.flush()


def main():
    args = parse_args()
    for year in args.years:
        file_path = f"{args.data_dir}/{year}.csv"
        
        if not os.path.exists(file_path):
            print(f"File {file_path} does not exist.")
            continue
        
        producer(file_path, args.n_lines)


if __name__ == "__main__":
    main()
