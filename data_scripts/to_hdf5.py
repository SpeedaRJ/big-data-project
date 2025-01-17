import argparse
import os
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
        str: The string representation of the data type. Returns "int" for `np.int64`, "S1" for object type, "float" for np.float64.

    Raises:
        ValueError: If the data type is not recognized.

    Example:
        >>> process_type(np.int64)
        'int'
        >>> process_type(np.dtype("O"))
        'S1'
    """
    if type == np.int64 or type == np.int32:
        return "<i8"
    if type == np.dtype("O") or type == "string":
        return h5py.string_dtype(encoding="utf-8")
    if type == np.float64:
        return "float"
    raise ValueError(f"Unknown type {type}")


def csv_to_hdf5(location, dropoff):
    """
    Convert CSV files in the specified location to HDF5 files.

    This function reads CSV files from the given location, processes the data
    according to the schema for each year, and saves the processed data to HDF5 files
    in the specified dropoff directory.

    Parameters:
    location (str): The directory containing the CSV files to be processed.
    dropoff (str): The directory where the HDF5 files will be saved.

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
        save_to_hdf5(data_processed, dropoff, year)


def save_to_hdf5(data_processed, dropoff, filename):
    """
    Save the processed data to an HDF5 file.

    Parameters:
    data_processed (DataFrame): The processed data to be saved.
    dropoff (str): The directory where the HDF5 file will be saved.
    filename (str): The filename to be used.

    Returns:
    None
    """
    data_types = [
        (name, process_type(type)) for name, type in data_processed.dtypes.items()
    ]
    array = np.empty(len(data_processed), dtype=data_types)
    for i, column in enumerate(data_processed.columns):
        array[column] = data_processed[column]
    with h5py.File(os.path.join(dropoff, f"{filename}.h5"), "w") as h5df:
        h5df.create_dataset("data", data=array, compression="gzip", compression_opts=9)


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
    """
    data = []
    with h5py.File(path, "r") as f:
        data = f["data"][()]

    return pd.DataFrame(data)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("data_location", type=str)
    parser.add_argument("data_dropoff", type=str)
    args = parser.parse_args()

    csv_to_hdf5(
        args.data_location,
        args.data_dropoff,
    )
    print(f"To HDF5 conversion finished")
