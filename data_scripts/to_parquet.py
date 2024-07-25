import argparse
import os
import time
from pathlib import Path

import pandas as pd
from data_schema import DataSchema
from tqdm import tqdm


def save_to_parquet(location, dropoff):
    """
    Reads CSV files from a specified directory, processes the data, and saves it to Parquet format.

    Args:
        location (str): The directory path containing the CSV files to be processed.
        dropoff (str): The directory path where the Parquet files will be saved.

    This function performs the following steps:
    1. Iterates over each file in the specified directory.
    2. Reads the CSV file and applies the schema for the corresponding year.
    3. Processes the data by filling missing values and converting data types to primitive types.
    4. Saves the processed DataFrame to a Parquet file.

    Note:
        The schema and data processing are handled by the `DataSchema` class and its methods.
    """
    for file in tqdm(os.listdir(location)):
        path = os.path.join(location, file)
        year = int(Path(path).stem)
        schema = DataSchema(year)
        data = pd.read_csv(
            path, dtype=schema.get_schema(path), parse_dates=schema.get_dates()
        )
        data_processed = DataSchema.to_primitive_dtypes(DataSchema.fill_na(data), year)
        data_processed.to_parquet(os.path.join(dropoff, f"{Path(file).stem}.parquet"))


def read_parquet(path):
    """
    Reads data from a Parquet file and converts it to a pandas DataFrame.

    Args:
        path (str): The path to the Parquet file to read.

    Returns:
        pd.DataFrame: A DataFrame containing the data read from the Parquet file.

    This function performs the following steps:
    1. Reads the Parquet file from the specified path.
    2. Converts the data to a pandas DataFrame.
    """
    return pd.read_parquet(path)


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
