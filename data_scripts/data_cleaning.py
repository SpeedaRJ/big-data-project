import argparse
import os
from pathlib import Path

import h5py
import numpy as np
import pandas as pd
from to_hdf5 import read_hdf5, process_type
from to_parquet import read_parquet
from tqdm import tqdm

fiscal_year = lambda year: (
    pd.Timestamp(year=year - 1, month=7, day=1),
    pd.Timestamp(year=year, month=6, day=30),
)

filter_row = lambda beginning, ending: lambda row: (
    beginning < pd.Timestamp(row["Issue Date"], unit="ms")
) and (pd.Timestamp(row["Issue Date"], unit="ms") < ending)


def filter_rows_by_date(dataframe, year):
    """
    Filters rows in the given DataFrame based on the specified fiscal year.

    Args:
        dataframe (pd.DataFrame): The DataFrame to filter.
        year (int): The fiscal year to filter rows by.

    Returns:
        pd.DataFrame: A DataFrame with rows filtered by the specified fiscal year.
    """
    fiscal_row_filter = filter_row(*fiscal_year(year))
    return dataframe.apply(fiscal_row_filter, axis=1)


def get_data_by_year(path, parquet=True):
    """
    Reads data files from the specified directory and organizes them by year.

    Args:
        path (str): The directory path containing the data files.
        parquet (bool, optional): If True, read files as Parquet format. If False, read files as HDF5 format. Defaults to True.

    Returns:
        dict: A dictionary where keys are years (int) and values are the corresponding data read from the files.
    """
    data = {}
    for file in tqdm(os.listdir(path)):
        file_path = os.path.join(path, file)
        year = int(Path(file_path).stem)
        data[year] = read_parquet(file_path) if parquet else read_hdf5(file_path)
    return data


def require_fiscal_year(data):
    """
    Filters the data for each fiscal year in the given dictionary.

    Args:
        data (dict): A dictionary where keys are years (int) and values are DataFrames (pd.DataFrame).

    Returns:
        dict: A dictionary with the same structure, but with DataFrames filtered by the corresponding fiscal year.
    """
    for year in tqdm(data.keys()):
        data[year] = data[year][filter_rows_by_date(data[year], year)]
    return data


def hand_curated_type_definitions(data, column):
    """
    Applies specific type transformations to the given column in the DataFrame.

    Args:
        data (pd.DataFrame): The DataFrame containing the data to be transformed.
        column (str): The column name to apply the transformations to. Supported columns are:
                      - "Vehicle Expiration Date"
                      - "Date First Observed"
                      - "Vehicle Year"
                      - "Violation Location"

    Returns:
        pd.DataFrame: The DataFrame with the specified column transformed.

    Raises:
        ValueError: If the column is not supported.
    """
    if column in ["Vehicle Expiration Date", "Date First Observed"]:
        data[column] = (
            pd.to_numeric(data[column].replace("01/05/0001 12:00:00 PM", ""))
            .fillna(-1)
            .astype(np.int64)
        )
    elif column in ["Vehicle Year", "Violation Location"]:
        data[column] = pd.to_numeric(data[column]).fillna(-1).astype(np.int64)
    else:
        raise ValueError(f"Column {column} is not supported")
    return data


def unify_column_names_and_dtypes(data):
    """
    Unifies column names and data types across multiple DataFrames in a dictionary.

    Args:
        data (dict): A dictionary where keys are years (int) and values are DataFrames (pd.DataFrame).

    Returns:
        dict: A dictionary with DataFrames that have unified column names and data types.

    This function performs the following steps:
    1. Collects column names and data types for each DataFrame.
    2. Determines the most common column name and data type for each column index.
    3. Renames columns and converts data types in each DataFrame to the most common ones.
    4. Handles any exceptions by applying specific type transformations using `hand_curated_type_definitions`.

    Raises:
        Exception: If there is an error in converting data types, it attempts to handle it by calling `hand_curated_type_definitions`.
    """
    naming_convention = {}
    typing_convention = {}

    for year in data.keys():
        for i, (column, dtype) in enumerate(zip(data[year].columns, data[year].dtypes)):
            if i not in naming_convention:
                naming_convention[i] = []
            naming_convention[i].append(column)

            if i not in typing_convention:
                typing_convention[i] = []
            typing_convention[i].append(dtype)

    for column_n, column_t in zip(naming_convention, typing_convention):
        values, counts = np.unique(naming_convention[column_n], return_counts=True)
        naming_convention[column_n] = (values, counts)

        values, counts = np.unique(typing_convention[column_t], return_counts=True)
        typing_convention[column_t] = (values, counts)

    column_names = [
        counting_data[0][np.argmax(counting_data[1])].strip()
        for _, counting_data in naming_convention.items()
    ]

    column_types = [
        counting_data[0][np.argmax(counting_data[1])]
        for _, counting_data in typing_convention.items()
    ]

    type_dict = {column: dtype for column, dtype in zip(data[-1].columns, column_types)}
    type_dict["Unregistered Vehicle?"] = np.dtype("O")

    for year in data.keys():
        data[year].columns = column_names
        parsed = False
        while not parsed:
            try:
                data[year] = data[year].astype(type_dict)
                parsed = True
            except Exception as e:
                error_column = str(e).split("column")[-1].strip().replace("'", "")
                data[year] = hand_curated_type_definitions(data[year], error_column)

    return data


def remove_mostly_null_files(data):
    """
    Removes columns from the DataFrame that have more than 75% of their values as null or placeholder values.

    Args:
        data (pd.DataFrame): The DataFrame from which mostly null columns will be removed.

    Returns:
        pd.DataFrame: The DataFrame with mostly null columns removed.

    This function performs the following steps:
    1. Calculates the frequency of each value in each column.
    2. Identifies columns where more than 75% of the values are null or placeholder values (e.g., -1, "").
    3. Removes these columns from the DataFrame.

    Note:
        The placeholder values are defined as -1 and "".
    """
    column_value_frequencies = {}

    for column in data.columns:
        counts = data[column].value_counts()
        column_value_frequencies[column] = (counts.index, counts.values / len(data))

    nullable_values = [
        -1,
        "",
    ]  # Comes from the DataSchema.to_primitive_dtypes() static method and hand_curated_type_definitions() function
    columns_to_remove = []

    for column in column_value_frequencies:
        values = column_value_frequencies[column][0]
        frequencies = column_value_frequencies[column][1]
        frequencies_of_nullable_values = np.array(
            [
                (values[i], frequencies[i])
                for i in range(len(values))
                if values[i] in nullable_values
            ]
        )
        if (
            frequencies_of_nullable_values.size > 0
            and (frequencies_of_nullable_values)[:, 1].astype(np.float64).sum() > 0.75
        ):
            columns_to_remove.append(column)

    return data.drop(columns=columns_to_remove)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("parquet_location", type=str)
    parser.add_argument("hdf5_location", type=str)
    args = parser.parse_args()

    parquet_data = get_data_by_year(args.parquet_location)
    hdf5_data = get_data_by_year(args.hdf5_location, parquet=False)

    parquet_data = require_fiscal_year(parquet_data)
    hdf5_data = require_fiscal_year(hdf5_data)

    parquet_data = unify_column_names_and_dtypes(parquet_data)
    hdf5_data = unify_column_names_and_dtypes(hdf5_data)

    parquet_data_concat = pd.concat(
        [parquet_data[year] for year in parquet_data.keys()]
    )
    hdf5_data_concat = pd.concat([hdf5_data[year] for year in hdf5_data.keys()])

    parquet_data_concat = remove_mostly_null_files(parquet_data_concat)
    hdf5_data_concat = remove_mostly_null_files(hdf5_data_concat)

    parquet_data_concat.to_parquet(f"{args.parquet_location}/full_data.parquet")
    
    data_types = [
        (name, process_type(type)) for name, type in hdf5_data_concat.dtypes.items()
    ]
    array = np.empty(len(hdf5_data_concat), dtype=data_types)
    for column in hdf5_data_concat.columns:
        array[column] = hdf5_data_concat[column]
    with h5py.File(f"{args.hdf5_location}/full_data.h5", "w") as h5df:
        h5df.create_dataset(
            "data", data=array, compression="gzip", compression_opts=9
        )
