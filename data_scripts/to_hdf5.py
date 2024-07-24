import argparse
import os
import time
from pathlib import Path

import h5py
import numpy as np
import pandas as pd
from data_schema import DataSchema
from tqdm import tqdm


def process_type(type):
    if type == np.int64:
        return "int"
    if type == np.dtype("O"):
        return "S1"
    raise ValueError(f"Unknown type {type}")


def save_to_hdf5(location, dropoff):
    for file in tqdm(os.listdir(location)):
        path = os.path.join(location, file)
        year = int(Path(path).stem)
        schema = DataSchema(year)
        data = pd.read_csv(
            path, dtype=schema.get_schema(path), parse_dates=schema.get_dates()
        )
        data_processed = DataSchema.to_primitive_dtypes(DataSchema.fill_na(data), year)
        data_types = [
            (name, process_type(type)) for name, type in data_processed.dtypes.items()
        ]
        array = np.empty(len(data_processed), dtype=data_types)
        for column in data_processed.columns:
            array[column] = data_processed[column]
        with h5py.File(os.path.join(dropoff, f"{Path(file).stem}.h5"), "w") as h5df:
            h5df.create_dataset(
                "data", data=array, compression="gzip", compression_opts=9
            )


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
    save_to_hdf5(
        args.data_location,
        args.data_dropoff,
    )
    print(f"To HDF5 conversion took: {tac(s_time):.4f}s")
