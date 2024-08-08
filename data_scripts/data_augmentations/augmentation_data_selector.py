import os
import re
from itertools import chain

import numpy as np
import pandas as pd

MAIN_PATH = ".\\data\\additional_data"


def load_additional_data(data, filename, sep=","):
    """
    Load additional data from a CSV file.

    Parameters:
    data (str): The directory or path where the data is located.
    filename (str): The name of the CSV file to load.
    sep (str, optional): The delimiter to use for separating values. Defaults to ",".

    Returns:
    DataFrame: The loaded data as a pandas DataFrame.
    """
    data = pd.read_csv(os.path.join(MAIN_PATH, data, filename), sep=sep)
    return data


def _parse_geom(geom_string, pattern):
    """
    Parse a geometry string and extract coordinates based on a regex pattern.

    Parameters:
    geom_string (str): The geometry string to be parsed.
    pattern (str): The regex pattern to find the coordinates in the geometry string.

    Returns:
    numpy.ndarray: A numpy array containing the mean coordinates.
    """
    geom_list = [
        geom.replace("(", "").replace(")", "")
        for geom in re.findall(pattern, str(geom_string))
    ]
    geom_points = [point.split(",") for point in geom_list]
    geom_locations = list(chain.from_iterable(geom_points))
    return np.array(
        [location.strip().split(" ") for location in geom_locations], dtype=np.float64
    ).mean(axis=0)


def get_center_location_from_polygon(data):
    """
    Calculate the center location (longitude and latitude) from polygon geometries in a DataFrame.

    Parameters:
    data (pandas.DataFrame): The DataFrame containing polygon geometries in the 'the_geom' column.

    Returns:
    pandas.DataFrame: The DataFrame with added 'long' and 'lat' columns representing the center locations.
    """
    pattern = r"(?<=\(\().*?(?=\)\))"
    for idx, row in data.iterrows():
        if "EMPTY" in row["the_geom"]:
            continue
        center_location = _parse_geom(row["the_geom"], pattern)
        data.loc[idx, "long"] = center_location[0]
        data.loc[idx, "lat"] = center_location[1]
    return data


if __name__ == "__main__":
    weather_data = load_additional_data("weather", "weather_NYC_2013_2024.csv", sep=";")
    bussiness_data = load_additional_data("businesses", "businesses_NYC_2023.csv")
    events_data = load_additional_data("events", "events_NYC_2008_2024.csv")
    middle_school_data = load_additional_data("schools", "middle_schools_NYC_2021.csv")
    high_school_data = load_additional_data("schools", "high_schools_NYC_2021.csv")
    landmarks_individual_data = load_additional_data(
        "landmarks", "lankmarks_NYC_individual.csv"
    )
    landmarks_scenic_data = load_additional_data(
        "landmarks", "lankmarks_NYC_scenic.csv"
    )

    landmarks_scenic_data = get_center_location_from_polygon(landmarks_scenic_data)
    lsd_names = ["SCEN_LM_NA", "lat", "long"]
    landmarks_scenic_data[lsd_names].dropna(subset=lsd_names).to_csv(
        os.path.join(MAIN_PATH, "landmarks", "landmarks_NYC_scenic_processed.csv"),
        index=False,
    )

    landmarks_individual_data = get_center_location_from_polygon(
        landmarks_individual_data
    )
    lid_names = ["LPC_NAME", "lat", "long"]
    landmarks_individual_data[lid_names].dropna(subset=lid_names).to_csv(
        os.path.join(MAIN_PATH, "landmarks", "landmarks_NYC_individual_processed.csv"),
        index=False,
    )

    msd_names = ["name", "Latitude", "Longitude"]
    middle_school_data[msd_names].dropna(subset=msd_names).to_csv(
        os.path.join(MAIN_PATH, "schools", "middle_schools_NYC_2021_processed.csv"),
        index=False,
    )

    hsd_names = ["school_name", "Latitude", "Longitude"]
    high_school_data[hsd_names].dropna(subset=hsd_names).to_csv(
        os.path.join(MAIN_PATH, "schools", "high_schools_NYC_2021_processed.csv"),
        index=False,
    )

    bd_names = [
        "License Expiration Date",
        "License Creation Date",
        "Industry",
        "Business Name",
        "Longitude",
        "Latitude",
    ]
    bussiness_data[bd_names].dropna(subset=bd_names).to_csv(
        os.path.join(MAIN_PATH, "businesses", "businesses_NYC_2023_processed.csv"),
        index=False,
    )

    wd_names = ['datetime', 'tempmax', 'tempmin', 'temp', 'conditions', 'humidity', 'windspeed', 'visibility']
    weather_data[wd_names].dropna(subset=wd_names).to_csv(
        os.path.join(MAIN_PATH, "weather", "weather_NYC_2013_2024_processed.csv"),
        index=False,
    )