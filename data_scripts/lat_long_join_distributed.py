import argparse
import os
import warnings

import dask.dataframe as dd
from haversine import haversine
from rtree import index

warnings.simplefilter(action="ignore", category=FutureWarning)
import platform
from contextlib import suppress

import dask_jobqueue
from distributed import Client, progress

print(platform.python_version())


def parse_args():
    parser = argparse.ArgumentParser(description="Joining lat long data")

    parser.add_argument(
        "--df1_location",
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

    return parser.parse_args()


def create_rtree_index(df, name):
    idx = index.Index()
    for i, row in enumerate(df.iterrows()):
        idx.insert(
            i,
            (row["Latitude"], row["Longitude"], row["Latitude"], row["Longitude"]),
            obj={"name": row[name], "lat": row["Latitude"], "long": row["Longitude"]},
        )
    return idx


def get_nearest_location(idx, lat, lang):
    hit = list(idx.nearest((lat, lang, lat, lang), 1, objects=True))[0].object
    return (hit["name"], haversine((lat, lang), (hit["lat"], hit["long"])))


def computational_wrapper(row, df, name):
    idx = create_rtree_index(df, name)
    return get_nearest_location(idx, row["Latitude"], row["Longitude"])


def run():
    workers=32
    print("Creating cluster...")
    cluster = dask_jobqueue.SLURMCluster(
        queue="all",
        processes=1,
        cores=workers,
        memory="64G",
        death_timeout=360,
        walltime="24:00:00"
    )
    client = Client(cluster, timeout="360s")
    client.cluster.scale(workers)
    print(client.cluster)
    print(cluster.job_script())

    client.wait_for_workers(n_workers=workers // 16)

    args = parse_args()

    print("Reading data...")
    main_df = dd.read_parquet(args.df1_location).repartition(npartitions=2**18)
    secondary_df = dd.read_csv(args.df2_location).persist()

    print("Computing partitions...")
    res = main_df.map_partitions(
        lambda df: df.apply(
            computational_wrapper,
            axis=1,
            df=secondary_df,
            name=args.df2_name_parameter,
            result_type="expand",
        ),
        meta={0: str, 1: float},
    )
    res.columns = [args.output_name_column, args.output_distance_column]

    computation = client.submit(
        lambda df1, df2: df1.merge(df2, left_index=True, right_index=True).compute(),
        client.scatter(main_df),
        client.scatter(res),
    )

    progress(computation)

    print("Saving file...")
    os.makedirs(args.output_location, exist_ok=True)
    computation.result().to_parquet(
        os.path.join(args.output_location, f"{args.output_name}.parquet")
    )


if __name__ == "__main__":
    run()
