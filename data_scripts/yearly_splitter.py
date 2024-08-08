import argparse

import pandas as pd

from to_hdf5 import read_hdf5, save_to_hdf5
from to_parquet import read_parquet, save_to_parquet

fiscal_year = lambda year: (
    pd.Timestamp(year=year - 1, month=7, day=1),
    pd.Timestamp(year=year, month=6, day=30),
)

def parse_args():
    parser = argparse.ArgumentParser(description="Split data by fiscal year.")
    parser.add_argument(
        "tickets_location",
        type=str,
        help="The location of the input data file (parquet or hdf5)."
    )
    parser.add_argument(
        "output_location",
        type=str,
        help="The location to save the filtered output files."
    )
    parser.add_argument(
        "data_format",
        type=str,
        choices=["parquet", "hdf5"],
        help="The format of the input data file (parquet or hdf5)."
    )
    parser.add_argument(
        "start_year",
        type=int,
        help="The start year for the fiscal year split."
    )
    parser.add_argument(
        "end_year",
        type=int,
        help="The end year for the fiscal year split."
    )
    return parser.parse_args()

def run():
    args = parse_args()

    if args.data_format == "parquet":
        data_frame = read_parquet(args.tickets_location)
    elif args.data_format == "hdf5":
        data_frame = read_hdf5(args.tickets_location)
    else:
        raise ValueError("Invalid data format")
    
    for year in range(args.start_year, args.end_year + 1):
        start_date, end_date = fiscal_year(year)
        mask = (data_frame["Issue Date"] >= start_date) & (
            data_frame["Issue Date"] <= end_date
        )
        if args.data_format == "parquet":
            save_to_parquet(data_frame[mask], args.output_location, f"{year}_filtered")
        elif args.data_format == "hdf5":
            save_to_hdf5(data_frame[mask], args.output_location, f"{year}_filtered")
        


if __name__ == "__main__":
    run()