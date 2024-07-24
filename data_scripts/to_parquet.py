import argparse
import os
import time
from pathlib import Path

import pandas as pd
from data_schema import DataSchema
from tqdm import tqdm


def save_to_parquet(location, dropoff):
    for file in tqdm(os.listdir(location)):
        path = os.path.join(location, file)
        year = int(Path(path).stem)
        schema = DataSchema(year)
        data = pd.read_csv(
            path, dtype=schema.get_schema(path), parse_dates=schema.get_dates()
        )
        data_processed = DataSchema.to_primitive_dtypes(DataSchema.fill_na(data), year)
        data_processed.to_parquet(os.path.join(dropoff, f"{Path(file).stem}.parquet"))


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
    save_to_parquet(
        args.data_location,
        args.data_dropoff,
    )
    print(f"To Parquet file conversion took: {tac(s_time):.4f}s")
