import argparse
import os
import sys

import dask.dataframe as dd
import pandas as pd
from dask.diagnostics import ProgressBar
from haversine import haversine
from rtree.index import Index
from tqdm import tqdm

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir)
    ),
)

from to_hdf5 import read_hdf5, save_to_hdf5

ProgressBar().register()


def parse_args():
    parser = argparse.ArgumentParser(description="Joining lat long data")

    parser.add_argument(
        "--tickets_location",
        type=str,
        help="The location of the main dataframe",
        required=True,
    )
    parser.add_argument(
        "--df2_location",
        type=str,
        help="The location of the secondary dataframe",
        required=True,
    )
    parser.add_argument(
        "--df2_name_parameter",
        type=str,
        help="Name of the column that holds the name of the location",
        required=False,
        default="name",
    )
    parser.add_argument(
        "--output_name_column",
        type=str,
        help="Name of the output name column",
        required=False,
        default="Nearest Location Name",
    )
    parser.add_argument(
        "--output_distance_column",
        type=str,
        help="Name of the output distance column",
        required=False,
        default="Distance to Nearest Location",
    )
    parser.add_argument(
        "--output_location",
        type=str,
        help="The location of the output dataframe",
        required=True,
    )
    parser.add_argument(
        "--output_name",
        type=str,
        help="The name of the output dataframe",
        required=True,
    )
    parser.add_argument(
        "--data_format", type=str, default="parquet", help="The format of the data"
    )

    return parser.parse_args()


def create_rtree_index(df, name):
    idx = Index()
    for i, row in enumerate(df.iterrows()):
        row = row[1]
        idx.insert(
            i,
            (row["Latitude"], row["Longitude"], row["Latitude"], row["Longitude"]),
            obj={
                "name": row[name],
                "industry": row["Industry"],
                "lat": row["Latitude"],
                "long": row["Longitude"],
                "active_from": row["License Creation Date"],
                "active_to": row["License Expiration Date"],
            },
        )
    return idx


def get_nearest_locations(idx, lat, lang, n):
    return [
        item.object
        for item in list(idx.nearest((lat, lang, lat, lang), n, objects=True))
    ]


def computational_wrapper(row, lat_i, long_i, idx, time_i, n=16):
    locations = pd.DataFrame(get_nearest_locations(idx, row[lat_i], row[long_i], n))
    locations = locations[
        (locations["active_from"] <= row[time_i])
        & (locations["active_to"] >= row[time_i])
    ]
    if locations.empty:
        return computational_wrapper(row, lat_i, long_i, idx, time_i, n * 2)
    return (
        locations.iloc[0]["name"],
        locations.iloc[0]["industry"],
        haversine(
            (row[lat_i], row[long_i]),
            (locations.iloc[0]["lat"], locations.iloc[0]["long"]),
        ),
    )


def main():
    args = parse_args()

    print("Reading data")
    secondary_df = pd.read_csv(args.df2_location)

    if args.data_format == "parquet":
        tickets = dd.read_parquet(args.tickets_location)
    elif args.data_format == "hdf5":
        tickets = dd.from_pandas(read_hdf5(args.tickets_location))
    elif args.data_format == "duckdb":
        tickets = dd.read_parquet(args.tickets_location)
    else:
        raise ValueError("Invalid data format")

    tickets_subset = tickets[["Issue Date", "Longitude", "Latitude"]].repartition(
        npartitions=1
    )

    print("Builing location index")
    idx = create_rtree_index(secondary_df, args.df2_name_parameter)

    print("Generating reference dataframe")
    lat_i = tickets_subset.columns.tolist().index("Latitude") + 1
    long_i = tickets_subset.columns.tolist().index("Longitude") + 1
    time_i = tickets_subset.columns.tolist().index("Issue Date") + 1
    res = []

    for row in tickets_subset.itertuples():
        res.append(computational_wrapper(row, lat_i, long_i, idx, time_i))

    location_reference = pd.DataFrame(
        res,
        columns=[
            args.output_name_column,
            "Industry of Business",
            args.output_distance_column,
        ],
        index=tickets_subset.index,
    )

    print("Merging datasets")
    if args.data_format in ["parquet", "hdf5"]:
        tickets = tickets.merge(location_reference, left_index=True, right_index=True)
        with ProgressBar():
            tickets = tickets.compute()

    print("Saving output")
    os.makedirs(args.output_location, exist_ok=True)
    if args.data_format == "parquet":
        tickets.to_parquet(
            os.path.join(args.output_location, f"{args.output_name}.parquet")
        )
    elif args.data_format == "hdf5":
        save_to_hdf5(tickets, args.output_location, args.output_name)


if __name__ == "__main__":
    main()
