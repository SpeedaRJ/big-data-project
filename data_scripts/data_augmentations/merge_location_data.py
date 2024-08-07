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
            obj={"name": row[name], "lat": row["Latitude"], "long": row["Longitude"]},
        )
    return idx


def get_nearest_location(idx, lat, lang):
    hit = list(idx.nearest((lat, lang, lat, lang), 1, objects=True))[0].object
    return (hit["name"], haversine((lat, lang), (hit["lat"], hit["long"])))


def computational_wrapper(row, lat_i, long_i, idx):
    return get_nearest_location(idx, row[lat_i], row[long_i])


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

    tickets_subset = tickets[["Longitude", "Latitude"]]

    print("Builing location index")
    idx = create_rtree_index(secondary_df, args.df2_name_parameter)

    print("Generating reference dataframe")
    lat_i = tickets_subset.columns.tolist().index("Latitude") + 1
    long_i = tickets_subset.columns.tolist().index("Longitude") + 1
    res = []

    for row in tqdm(
        tickets_subset.itertuples(), total=tickets_subset.shape[0].compute()
    ):
        res.append(computational_wrapper(row, lat_i, long_i, idx))

    location_reference = pd.DataFrame(
        res,
        columns=[args.output_name_column, args.output_distance_column],
        index=tickets_subset.index,
    )

    print("Merging datasets")
    if args.data_format in ["parquet", "hdf5"]:
        merged = tickets.merge(location_reference, left_index=True, right_index=True)
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
