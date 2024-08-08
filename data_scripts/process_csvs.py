import argparse
import os
from pathlib import Path

import joblib as jl
import numpy as np
import pandas as pd
from data_cleaning import (
    remove_mostly_null_columns,
    require_fiscal_year,
    unify_column_names_and_dtypes,
)
from data_schema import DataSchema
from to_hdf5 import save_to_hdf5
from to_parquet import save_to_parquet
from tqdm import tqdm
from tqdm_joblib import tqdm_joblib as tjl


def read_csvs(location):
    """
    Reads CSV files from a specified location, processes them according to their schema,
    and returns a dictionary with the processed data for each year.

    Args:
        location (str): The directory path where the CSV files are located.

    Returns:
        dict: A dictionary where the keys are years (int) and the values are DataFrames
              containing the processed data for each year.

    The function performs the following steps:
    1. Iterates over all files in the specified location.
    2. Extracts the year from the file name (assuming the file name is the year).
    3. Reads the CSV file into a DataFrame using a schema specific to the year.
    4. Processes the data by filling missing values and converting data types.
    5. Stores the processed DataFrame in a dictionary with the year as the key.
    """
    yearly_data_dict = {}
    for file in tqdm(os.listdir(location), desc="Reading CSV files by year"):
        path = os.path.join(location, file)
        year = int(Path(path).stem)
        schema = DataSchema(year)
        data = pd.read_csv(
            path, dtype=schema.get_schema(path), parse_dates=schema.get_dates()
        )
        data_processed = DataSchema.to_primitive_dtypes(DataSchema.fill_na(data), year)
        yearly_data_dict[year] = data_processed
    return yearly_data_dict


def unify_borough_names(data):
    """
    Unifies the borough names in the 'Violation County' column of the given DataFrame.

    This function performs the following steps:
    1. Drops rows where 'Violation County' is NaN.
    2. Ensures that at least one street code is present in each row.
    3. Maps various aliases of borough names to their standardized form.
    4. Applies the mapping to the 'Violation County' column.
    5. Removes rows with unidentifiable borough names.

    Args:
        data (pd.DataFrame): The input DataFrame containing a 'Violation County' column
                             and 'Street Code1', 'Street Code2', 'Street Code3' columns.

    Returns:
        pd.DataFrame: The DataFrame with unified borough names in the 'Violation County' column.
    """
    data = data.dropna(subset=["Violation County"])
    # Ensure at least one street code is present
    data = data[
        (data["Street Code1"] != 0)
        | (data["Street Code2"] != 0)
        | (data["Street Code3"] != 0)
    ]

    borough_unification_map = {
        "Manhattan": ["NY", "MN", "MAN", "NEWY", "NEW Y", "NYC", "N", "MH"],
        "Bronx": ["BX", "Bronx", "BRONX"],
        "Brooklyn": ["K", "Kings", "BK", "KINGS", "KING"],
        "Queens": ["Q", "QN", "Qns", "QUEEN", "QNS", "QU"],
        "Staten Island": ["ST", "Rich", "R", "RICH", "RC"],
    }

    get_borough = lambda row: next(
        (
            borough
            for borough, borough_aliases in borough_unification_map.items()
            if row["Violation County"] in borough_aliases
        ),
        "Unknown",
    )

    data["Violation County"] = data.apply(get_borough, axis=1)

    # Remove a few rows with unknown boroughs, we saw a few examples of unidentifiable borough names
    data = data[data["Violation County"] != "Unknown"]
    return data


def _get_location_via_street_codes(map, row, borough_codes, street_codes):
    """
    Determines the latitude and longitude of a location based on street codes and borough codes.

    This function performs the following steps:
    1. Retrieves the borough name using the borough code from the row.
    2. Checks if the concatenation of the borough and each street code exists in the street_codes index.
    3. If a match is found, retrieves the latitude and longitude for that street code and appends to a list.
    4. Calculates the mean latitude and longitude from the list of coordinates.
    5. Updates the map with the calculated latitude and longitude for the given row.

    Args:
        row (pd.Series): A row from the DataFrame containing street codes and a borough code.
        borough_codes (dict): A dictionary mapping borough codes to borough names.
        street_codes (pd.DataFrame): A DataFrame with street codes as the index and columns for latitude and longitude.

    Returns:
        None: The function updates the global map dictionary with the calculated latitude and longitude.
    """
    lat_lang = {"Latitude": 0, "Longitude": 0}
    borough = borough_codes[row[4]]
    coords = []
    if f"{borough}{row[1]}" in street_codes.index:
        coord_data = street_codes.loc[f"{borough}{row[1]}"]
        coords.append((coord_data["lat"], coord_data["long"]))
    if f"{borough}{row[2]}" in street_codes.index:
        coord_data = street_codes.loc[f"{borough}{row[2]}"]
        coords.append((coord_data["lat"], coord_data["long"]))
    if f"{borough}{row[3]}" in street_codes.index:
        coord_data = street_codes.loc[f"{borough}{row[3]}"]
        coords.append((coord_data["lat"], coord_data["long"]))
    coords = np.array(coords)
    if len(coords) > 0:
        lat_lang["Latitude"] = np.mean(coords[:, 0])
        lat_lang["Longitude"] = np.mean(coords[:, 1])
    map[row[0]] = lat_lang


def compute_violation_coords(data):
    """
    Computes the latitude and longitude coordinates for each violation based on street codes and borough codes.

    This function performs the following steps:
    1. Reads and processes the street code mapping from a CSV file.
    2. Defines a dictionary mapping borough names to borough codes.
    3. Filters the input DataFrame to include only relevant columns.
    4. Initializes an empty dictionary to store the computed coordinates.
    5. Uses parallel processing to compute the coordinates for each row in the filtered DataFrame.
    6. Converts the computed coordinates dictionary to a DataFrame.
    7. Identifies and removes rows with missing coordinates.
    8. Merges the original data with the computed coordinates.

    Args:
        data (pd.DataFrame): The input DataFrame containing violation data, including street codes and borough codes.

    Returns:
        pd.DataFrame: The DataFrame with computed latitude and longitude coordinates merged with the original data.
    """
    street_codes = pd.read_csv("../data/meta_data/street_code_mapper.csv").T
    street_codes.columns = street_codes.iloc[0]
    street_codes = street_codes.iloc[1:]
    street_codes["street_code"] = street_codes["street_code"].astype(int)
    street_codes["lat"] = street_codes["lat"].astype(float)
    street_codes["long"] = street_codes["long"].astype(float)

    borough_codes = {
        "Manhattan": 1,
        "Bronx": 2,
        "Brooklyn": 3,
        "Queens": 4,
        "Staten Island": 5,
    }

    working_data = data[
        [
            "Summons Number",
            "Street Code1",
            "Street Code2",
            "Street Code3",
            "Violation County",
        ]
    ]

    map = {}

    with tjl(
        desc="Calculating violation coordinates", total=len(working_data)
    ) as progress_bar:
        jl.Parallel(n_jobs=32, require="sharedmem", prefer="threads")(
            jl.delayed(_get_location_via_street_codes)(
                map, row, borough_codes, street_codes
            )
            for row in working_data.to_numpy()
        )

    coords_data = pd.DataFrame(map).T

    missing_locations = coords_data[
        (coords_data["Latitude"] == 0) & (coords_data["Longitude"] == 0)
    ].index

    data = data[data["Summons Number"].isin(missing_locations) == False]

    return pd.merge(data, coords_data, left_on="Summons Number", right_index=True)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("csv_location", type=str)
    parser.add_argument("parquet_location", type=str)
    parser.add_argument("hdf5_location", type=str)
    args = parser.parse_args()

    data = read_csvs(args.csv_location)

    data = require_fiscal_year(data)

    data = unify_column_names_and_dtypes(data)

    data_concat = pd.concat([data[year] for year in data.keys()])

    data_concat = remove_mostly_null_columns(data_concat)

    data_concat = unify_borough_names(data_concat)

    data_concat = compute_violation_coords(data_concat)

    print("Saving data to parquet")
    save_to_parquet(data_concat, args.parquet_location, "full_data_cleaned")

    print("Saving data to hdf5")
    save_to_hdf5(data_concat, args.hdf5_location, "full_data_cleaned")

    print("Saving data to csv")
    data_concat.to_csv(
        os.path.join(args.csv_location, "full_data_cleaned.csv"), index=False
    )
