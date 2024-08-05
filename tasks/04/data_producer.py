import os
import json
import argparse

import pandas as pd
import dask.dataframe as dd
from tqdm import tqdm
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "raw-data"

COLUMNS = [
    'Summons Number', 'Plate ID', 'Registration State', 'Plate Type',
    'Issue Date', 'Violation Code', 'Vehicle Body Type', 'Vehicle Make',
    'Issuing Agency', 'Street Code1', 'Street Code2', 'Street Code3',
    'Vehicle Expiration Date', 'Violation Location', 'Violation Precinct',
    'Issuer Precinct', 'Issuer Code', 'Issuer Command', 'Issuer Squad',
    'Violation Time', 'Violation County',
    'Violation In Front Of Or Opposite', 'House Number', 'Street Name',
    'Intersecting Street', 'Date First Observed', 'Law Section',
    'Sub Division', 'Violation Legal Code', 'Days Parking In Effect',
    'From Hours In Effect', 'To Hours In Effect', 'Vehicle Color',
    'Vehicle Year', 'Feet From Curb', 'Violation Post Code',
    'Violation Description', 'Latitude', 'Longitude', 'datetime', 'tempmax',
    'tempmin', 'temp', 'conditions', 'humidity', 'windspeed', 'visibility'
]
# TODO: decide which columns to keep to send for processing
# KEEP_COLUMNS = []
KEEP_COLUMNS = COLUMNS.copy()


def parse_args():
    parser = argparse.ArgumentParser(description="Produce data to Kafka")
    parser.add_argument("--tickets_file", type=str, help="Path to the tickets file")
    parser.add_argument("--weather_file", type=str, help="Path to the weather file")
    parser.add_argument("--fiscal_years", nargs="+", help="Years to produce")
    parser.add_argument("--limit", type=int, default=100, help="Limit the number of rows to produce")
    return parser.parse_args()



def producer(fiscal_year_ddf, limit):
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    n_produced = 0
    npart = 1
    for partition in fiscal_year_ddf.partitions:
        if n_produced >= limit and limit != -1:
            break
        partition = partition.compute()
        pbar = tqdm(partition.iterrows(), desc=f"Partition {npart}/{fiscal_year_ddf.npartitions}", total=len(partition))
        for index, content in pbar:
            if n_produced >= limit and limit != -1:
                break
            value = content[KEEP_COLUMNS].to_dict()
            # convert datetime to string for serialization
            if "datetime" in value:
                try:
                    value["datetime"] = value["datetime"].strftime("%Y-%m-%d")
                except Exception as e:
                    print(e)
                    value["datetime"] = ""
            if "Issue Date" in value:
                try:
                    value["Issue Date"] = value["Issue Date"].strftime("%Y-%m-%d")
                except Exception as e:
                    print(e)
                    value["Issue Date"] = ""
            # send the message
            producer.send(topic=TOPIC, value=value)
            n_produced += 1
        npart += 1
    producer.flush()


def main():
    args = parse_args()
    # weather data is fairly small so read it with pandas
    weather = pd.read_csv(args.weather_file)
    weather["datetime"] = pd.to_datetime(weather["datetime"]).dt.floor("D")
    
    # tickets are a lot bigger so read them with dask so it doesn't load in memory
    tickets = dd.read_parquet(args.tickets_file)
    tickets["Issue Date"] = dd.to_datetime(tickets["Issue Date"], unit="ms").dt.floor("D")
    
    # lazy merge
    merged = tickets.merge(weather, how="left", left_on="Issue Date", right_on="datetime")
    
    for year in args.fiscal_years:
        # start and end date of the fiscal year
        start_date, end_date = f"{int(year)-1}-07-01", f"{year}-06-30"
        
        fiscal_year_ddf = merged[(merged["Issue Date"] >= start_date) & (merged["Issue Date"] <= end_date)]
        producer(fiscal_year_ddf, args.limit)


if __name__ == "__main__":
    main()
