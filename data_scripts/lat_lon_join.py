import os
import argparse
import numpy as np
from tqdm import tqdm
import dask
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from haversine import haversine


def parse_args():
    parser = argparse.ArgumentParser(description="Joining lat long data")
    
    parser.add_argument("--df1_location", type=str, help="The location of the main dataframe")
    parser.add_argument("--df1_key", type=str, help="The key of the main dataframe")
    
    parser.add_argument("--df2_location", type=str, help="The location of the secondary dataframe")
    parser.add_argument("--df2_key", type=str, help="The key of the secondary dataframe")
    
    parser.add_argument("--output_location", type=str, help="The location of the output dataframe")
    parser.add_argument("--output_name", type=str, help="The name of the output dataframe")
    
    return parser.parse_args()


# TODO: slurm cluster setup
def cluster_setup():
    # cluster = LocalCluster(n_workers=1, threads_per_worker=16, memory_target_fraction=0.95, memory_limit='64GB')
    # client = Client(cluster)
    pass


def lat_lon_join(df1_location, df1_key, df2_location, df2_key):
    # read in the very large main df with dask 
    df1 = dd.read_parquet(df1_location).repartition(npartitions=1)
    # read in the smaller secondary df and compute it
    df2 = dd.read_csv(df2_location).compute()
    
    results = []
    for index, row in tqdm(df1.iterrows()):
        tmp = row.to_frame().T.assign(key=0).set_index("key")
        tmp = tmp.merge(df2.assign(key=0).set_index("key"), how="inner", left_index=True, right_index=True)
        tmp = tmp.assign(distance=tmp.apply(lambda row: haversine((row['Latitude_x'], row['Longitude_x']), (row['Latitude_y'], row['Longitude_y'])), axis=1))
        tmp = tmp.sort_values("distance").groupby([df1_key]).first().reset_index()
        results.append((tmp[df2_key].values[0], tmp["distance"].values[0]))
    results = np.array(results)

    # TODO: change column names - if we chain call with different secondary dfs, we will have the same column names atm
    df1 = df1.assign(closest=dask.array.from_array(results[:, 0]))
    df1 = df1.assign(distance_between=dask.array.from_array(results[:, 1]))
    df1 = df1.reset_index()
    return df1.compute()


def save_to_parquet(df, location, name):
    os.makedirs(location, exist_ok=True)
    df.to_parquet(os.path.join(location, f"{name}.parquet"))


def main():
    args = parse_args()
    cluster_setup()
    df = lat_lon_join(args.df1_location, args.df1_key, args.df2_location, args.df2_key)
    save_to_parquet(df, args.output_location, args.output_name)


if __name__ == "__main__":
    main()
