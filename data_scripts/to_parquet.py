import argparse
import os
import time
from pathlib import Path

import pandas as pd
from data_schema import DataSchema


def save_to_parquet(location, dropoff):
    for file in os.listdir(location):
        data = pd.read_csv(
            os.path.join(location, file),
            dtype=DataSchema.schema,
            parse_dates=DataSchema.dates,
        )
        data_processed = DataSchema.to_primitive_dtypes(DataSchema.fill_na(data))
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
