import argparse
import os
import time

import dask.dataframe as dd
from dask.diagnostics import ProgressBar


def data_to_parquet(file_path, target_path):
    data_frame = dd.read_csv(
        file_path, dtype=str, blocksize=64_000_000, sample=80_000, low_memory=False
    )
    with ProgressBar():
        data_frame.to_parquet(target_path, engine="pyarrow")
    del data_frame


def data_to_hdf5(file_path, target_path):
    def name_function(i):
        return f"partition_{i}"

    data_frame = dd.read_csv(
        file_path, dtype=str, blocksize=640_000, sample=8_000, low_memory=True
    )
    with ProgressBar():
        data_frame.to_hdf(
            target_path, key="/data", name_function=name_function, scheduler="processes"
        )
    del data_frame


if __name__ == "__main__":
    def tic():
        return time.time()
    
    def tac(tic):
        return time.time() - tic

    parser = argparse.ArgumentParser()
    parser.add_argument("data_location", type=str)
    parser.add_argument("data_dropoff", type=str)
    args = parser.parse_args()

    s_time = tic()
    data_to_parquet(
        os.path.join(args.data_location, "*.csv"),
        os.path.join(args.data_dropoff, "parquet"),
    )
    print(f"To Parquet file conversion took: {tac(s_time):.4f}s")

    s_time = tic()
    data_to_hdf5(
        os.path.join(args.data_location, "*.csv"),
        os.path.join(args.data_dropoff, "hdf", "*.hdf"),
    )
    print(f"To HDF5 file conversion took: {tac(s_time):.4f}s")
