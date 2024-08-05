import argparse
import os

import dask
import dask.dataframe as dd
import numpy as np
from haversine import haversine
from rtree import index
from tqdm import tqdm


def parse_args():
    parser = argparse.ArgumentParser(description="Joining lat long data")

    parser.add_argument(
        "--df1_location", type=str, help="The location of the main dataframe"
    )
    parser.add_argument("--df1_key", type=str, help="The key of the main dataframe")

    parser.add_argument(
        "--df2_location", type=str, help="The location of the secondary dataframe"
    )
    parser.add_argument(
        "--df2_key", type=str, help="The key of the secondary dataframe"
    )

    parser.add_argument(
        "--output_location", type=str, help="The location of the output dataframe"
    )
    parser.add_argument(
        "--output_name", type=str, help="The name of the output dataframe"
    )

    return parser.parse_args()


def create_rtree_index(df, name=1, lat=2, long=3):
    idx = index.Index()
    for i, row in tqdm(
        enumerate(df.itertuples()), total=df.shape[0].compute(), desc="Building index"
    ):
        idx.insert(
            i,
            (row[lat], row[long], row[lat], row[long]),
            obj={"name": row[name], "lat": row[lat], "long": row[long]},
        )
    return idx


def get_nearest_from_index(row, idx, lat=2, long=3):
    nearest = list(
        idx.nearest((row[lat], row[long], row[lat], row[long]), 1, objects=True)
    )
    return nearest[0].object


# TODO: slurm cluster setup
def cluster_setup():
    # global CLUSTER
    # global CLIENT
    # CLUSTER = SLURMCluster(cores=64, processes=1, memory="128GB", walltime="24:00:00")
    # CLIENT = Client(CLUSTER)
    # CLUSTER.scale(jobs=4)
    # print(CLUSTER.job_script())
    pass


def lat_lon_join(df1_location, df1_key, df2_location, df2_key, lat=0, long=0):
    # global CLIENT
    # read in the very large main df with dask
    df1 = dd.read_parquet(df1_location).repartition(npartitions=2**18)
    # read in the smaller secondary df
    df2 = dd.read_csv(df2_location)

    print("Making RTree index...")
    idx = create_rtree_index(df2)

    if not lat and not long:
        lat = list(df1.columns).index("Latitude") + 1
        long = list(df1.columns).index("Longitude") + 1

    print("Processing file...")
    results = []
    for row in tqdm(
        df1.itertuples(), total=df1.shape[0].compute(), desc="Computing rows"
    ):
        nearest = get_nearest_from_index(row, idx, lat, long)
        distance = haversine((row[lat], row[long]), (nearest["lat"], nearest["long"]))
        results.append((nearest["name"], distance))
    results = np.array(results)

    # TODO: change column names - if we chain call with different secondary dfs, we will have the same column names atm
    # df1 = df1.assign(closest=dask.array.from_array(results[:, 0]))
    # df1 = df1.assign(distance_between=dask.array.from_array(results[:, 1]))
    # df1 = df1.reset_index()
    print("Adding new data...")
    final_df = df1.assign(
        nearest_middle_school=dask.array.from_array(results[:, 0])
    ).assign(distance_ms=dask.array.from_array(results[:, 1]))
    return final_df.compute()


def save_to_parquet(df, location, name):
    os.makedirs(location, exist_ok=True)
    df.to_parquet(os.path.join(location, f"{name}.parquet"))


def main():
    args = parse_args()
    # print("Creating cluster...")
    # cluster_setup()
    df = lat_lon_join(
        args.df1_location, args.df1_key, args.df2_location, args.df2_key
    )
    print("Saving processed data...")
    save_to_parquet(df, args.output_location, args.output_name)


if __name__ == "__main__":
    print("Starting processing...")
    main()
