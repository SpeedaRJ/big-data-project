import os
import json
import argparse
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "raw-data"
# all columns contained in the csv files
ALL_COLUMNS = [
    "Summons Number",
    "Plate ID",
    "Registration State",
    "Plate Type",
    "Issue Date",
    "Violation Code",
    "Vehicle Body Type",
    "Vehicle Make",
    "Issuing Agency",
    "Street Code1",
    "Street Code2",
    "Street Code3",
    "Vehicle Expiration Date",
    "Violation Location",
    "Violation Precinct",
    "Issuer Precinct",
    "Issuer Code",
    "Issuer Command",
    "Issuer Squad",
    "Violation Time",
    "Time First Observed",
    "Violation County",
    "Violation In Front Of Or Opposite",
    "House Number", # 2014 has just "Number" instead of "House Number"
    "Street Name", # 2014 has just "Street" instead of "Street Name"
    "Intersecting Street",
    "Date First Observed",
    "Law Section",
    "Sub Division",
    "Violation Legal Code",
    "Days Parking In Effect",
    "From Hours In Effect",
    "To Hours In Effect",
    "Vehicle Color",
    "Unregistered Vehicle?",
    "Vehicle Year",
    "Meter Number",
    "Feet From Curb",
    "Violation Post Code",
    "Violation Description",
    "No Standing or Stopping Violation",
    "Hydrant Violation",
    "Double Parking Violation"
]
COLUMN_MAPPING = {
    "Number": "House Number", # 2014
    "Street": "Street Name", # 2014
    "Days Parking In Effect    ": "Days Parking In Effect" # until 2019
}


def parse_args():
    parser = argparse.ArgumentParser(description="Produce data to Kafka")
    parser.add_argument("--data-dir", default="./data/raw", help="Directory with data files relative to root of the project")
    parser.add_argument("--years", nargs="+", help="Years to produce") # we assume that each year has an associated <year>.csv file
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
            line_dict = {}
            for col, field in zip(columns, fields):
                # map mistakes in the column names
                if col in COLUMN_MAPPING.keys():
                    col = COLUMN_MAPPING[col]
                
                # notify if column is not in the list of all columns
                if col not in ALL_COLUMNS:
                    print(f"Column {col} not in ALL_COLUMNS")
                    continue
                
                # notify if we are missing a column
                if len(columns) != len(ALL_COLUMNS):
                    print(f"Missing columns in the file")
                    continue
                
                col = col.strip().lower().replace(" ", "_").replace("?", "")
                line_dict[col] = field
            
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
