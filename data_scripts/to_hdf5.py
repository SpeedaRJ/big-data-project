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
    """
    Converts a NumPy data type to a corresponding string representation.

    Args:
        type (np.dtype): The NumPy data type to convert.

    Returns:
        str: The string representation of the data type. Returns "int" for `np.int64` and "S1" for object type.

    Raises:
        ValueError: If the data type is not recognized.

    Example:
        >>> process_type(np.int64)
        'int'
        >>> process_type(np.dtype("O"))
        'S1'
    """
    if type == np.int64:
        return "int"
    if type == np.dtype("O"):
        return "S1"
    raise ValueError(f"Unknown type {type}")


def save_to_hdf5(location, dropoff):
    """
    Reads CSV files from a specified directory, processes the data, and saves it to HDF5 format.

    Args:
        location (str): The directory path containing the CSV files to be processed.
        dropoff (str): The directory path where the HDF5 files will be saved.

    This function performs the following steps:
    1. Iterates over each file in the specified directory.
    2. Reads the CSV file and applies the schema for the corresponding year.
    3. Processes the data by filling missing values and converting data types to primitive types.
    4. Converts the processed DataFrame to a structured NumPy array.
    5. Saves the structured array to an HDF5 file with gzip compression.

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


def read_hdf5(path):
    """
    Reads data from an HDF5 file and converts it to a pandas DataFrame.

    Args:
        path (str): The path to the HDF5 file to read.

    Returns:
        pd.DataFrame: A DataFrame containing the data read from the HDF5 file.

    This function performs the following steps:
    1. Opens the HDF5 file in read mode.
    2. Reads the dataset named "data" from the HDF5 file.
    3. Converts the dataset to a pandas DataFrame.
    4. Sets the DataFrame columns using the schema for the year 2014.

    Note:
        The schema is retrieved using the `get_schema` method of the `DataSchema` class.
    """
    data = []
    with h5py.File(path, "r") as f:
        data = f["data"][()]

    return pd.DataFrame(
        data,
        columns=DataSchema(2014).get_schema(f"{os.path.splitext(path)[0]}.csv").keys(),
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
