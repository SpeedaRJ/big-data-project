import os
import argparse

import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar


def parse_args():
    parser = argparse.ArgumentParser(description='Add weather data to the dataset')
    parser.add_argument('--tickets_location', type=str, help='The location of the ticket data')
    parser.add_argument('--weather_location', type=str, help='The location of the weather data')
    parser.add_argument('--output_location', type=str, help='The location to save the output data')
    parser.add_argument('--output_name', type=str, help='The name of the output file')
    parser.add_argument('--data_format', type=str, default='parquet', help='The format of the data')
    return parser.parse_args()


def main():
    args = parse_args()

    # small dataset so read in memory with pandas    
    weather = pd.read_csv(args.weather_location)
    weather["datetime"] = pd.to_datetime(weather["datetime"]).dt.floor("D")
    
    if args.data_format == 'parquet':
        tickets = dd.read_parquet(args.tickets_location)
        tickets["Issue Date"] = dd.to_datetime(tickets["Issue Date"], unit="ms").dt.floor("D")
    elif args.data_format == 'hdf5':
        # TODO: add hdf5 support
        pass
    else:
        raise ValueError('Invalid data format')
    
    print("Merging datasets")
    with ProgressBar():
        merged = tickets.merge(weather, how="left", left_on="Issue Date", right_on="datetime")
    
    print("Saving output")
    os.makedirs(args.output_location, exist_ok=True)
    if args.data_format == 'parquet':
        merged.to_parquet(os.path.join(args.output_location, f"{args.output_name}.parquet"))
    elif args.data_format == 'hdf5':
        # TODO
        pass
    else:
        raise ValueError('Invalid data format')


if __name__ == '__main__':
    main()
