import argparse
import os
from pathlib import Path

import pandas as pd
from data_schema import DataSchema
from tqdm import tqdm


def csv_to_parquet(location, dropoff):
    """
    Convert CSV files in the specified location to Parquet files.

    This function reads CSV files from the given location, processes the data
    according to the schema for each year, and saves the processed data to Parquet files
    in the specified dropoff directory.

    Parameters:
    location (str): The directory containing the CSV files to be processed.
    dropoff (str): The directory where the Parquet files will be saved.

    Returns:
    None
    """
    for file in tqdm(os.listdir(location)):
        path = os.path.join(location, file)
        year = int(Path(path).stem)
        schema = DataSchema(year)
        data = pd.read_csv(
            path, dtype=schema.get_schema(path), parse_dates=schema.get_dates()
        )
        data_processed = DataSchema.to_primitive_dtypes(DataSchema.fill_na(data), year)
        save_to_parquet(data_processed, dropoff, year)


def save_to_parquet(data, dropoff, filename):
    """
    Save the given data to a Parquet file.

    Parameters:
    data (DataFrame): The data to be saved.
    dropoff (str): The directory where the Parquet file will be saved.
    filename (int): The filename to be used.

    Returns:
    None
    """
    data.to_parquet(os.path.join(dropoff, f"{filename}.parquet"))


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

    parser = argparse.ArgumentParser()
    parser.add_argument("data_location", type=str)
    parser.add_argument("data_dropoff", type=str)
    args = parser.parse_args()

    csv_to_parquet(
        args.data_location,
        args.data_dropoff,
    )
    print(f"To Parquet file conversion finished")
