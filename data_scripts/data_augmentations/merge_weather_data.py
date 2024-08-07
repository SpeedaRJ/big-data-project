import argparse
import os
import sys

import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.diagnostics import ProgressBar

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir)
    ),
)

from to_hdf5 import read_hdf5, save_to_hdf5

ProgressBar().register()


def parse_args():
    parser = argparse.ArgumentParser(description="Add weather data to the dataset")
    parser.add_argument(
        "--tickets_location",
        type=str,
        help="The location of the ticket data",
        required=True,
    )
    parser.add_argument(
        "--weather_location",
        type=str,
        help="The location of the weather data",
        required=True,
    )
    parser.add_argument(
        "--output_location",
        type=str,
        help="The location to save the output data",
        required=True,
    )
    parser.add_argument(
        "--output_name", type=str, help="The name of the output file", default="dataset"
    )
    parser.add_argument(
        "--data_format", type=str, default="parquet", help="The format of the data"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print("Reading datasets")
    weather = pd.read_csv(args.weather_location, sep=";")
    weather["datetime"] = pd.to_datetime(weather["datetime"]).astype(np.int64) // 10**6

    if args.data_format == "parquet":
        tickets = dd.read_parquet(args.tickets_location)
    elif args.data_format == "hdf5":
        tickets = dd.from_pandas(read_hdf5(args.tickets_location))
    elif args.data_format == "duckdb":
        tickets = dd.read_parquet(args.tickets_location)
    else:
        raise ValueError("Invalid data format")

    print("Merging datasets")
    if args.data_format in ["parquet", "hdf5"]:
        merged = tickets.merge(
            weather, how="left", left_on="Issue Date", right_on="datetime"
        )
        with ProgressBar():
            merged = merged.compute()

    print("Saving output")
    os.makedirs(args.output_location, exist_ok=True)
    if args.data_format == "parquet":
        merged.to_parquet(
            os.path.join(args.output_location, f"{args.output_name}.parquet")
        )
    elif args.data_format == "hdf5":
        save_to_hdf5(merged, args.output_location, args.output_name)


if __name__ == "__main__":
    main()
