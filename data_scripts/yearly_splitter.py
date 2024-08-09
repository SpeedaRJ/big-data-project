import argparse

import pandas as pd
from data_cleaning import filter_rows_by_date
from to_hdf5 import read_hdf5, save_to_hdf5
from to_parquet import read_parquet, save_to_parquet
from tqdm import tqdm


def parse_args():
    parser = argparse.ArgumentParser(description="Split data by fiscal year.")
    parser.add_argument(
        "--tickets_location",
        type=str,
        help="The location of the input data file (parquet or hdf5).",
    )
    parser.add_argument(
        "--output_location",
        type=str,
        help="The location to save the filtered output files.",
    )
    parser.add_argument(
        "--data_format",
        type=str,
        choices=["parquet", "hdf5"],
        help="The format of the input data file (parquet or hdf5).",
    )
    parser.add_argument(
        "--start_year", type=int, help="The start year for the fiscal year split."
    )
    parser.add_argument(
        "--end_year", type=int, help="The end year for the fiscal year split."
    )
    return parser.parse_args()


def run():
    args = parse_args()

    print("Reading Data...")
    if args.data_format == "parquet":
        data_frame = read_parquet(args.tickets_location)
    elif args.data_format == "hdf5":
        data_frame = read_hdf5(args.tickets_location)
    else:
        raise ValueError("Invalid data format")

    for year in tqdm(
        range(args.start_year, args.end_year + 1), desc="Splitting into years"
    ):
        yearly_data = data_frame[filter_rows_by_date(data_frame, year)]
        if args.data_format == "parquet":
            save_to_parquet(yearly_data, args.output_location, f"{year}_filtered")
        elif args.data_format == "hdf5":
            save_to_hdf5(yearly_data, args.output_location, f"{year}_filtered")


if __name__ == "__main__":
    run()
