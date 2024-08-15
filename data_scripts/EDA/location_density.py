import argparse
import glob
import os
import sys

import geopandas as gpd
import geoplot as gplt
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir)
    ),
)

from to_hdf5 import read_hdf5


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_location",
        type=str,
        help="The location of the input data file (parquet or hdf5).",
    )
    parser.add_argument(
        "--data_format",
        type=str,
        choices=["parquet", "h5"],
        help="The format of the input data file (parquet or hdf5).",
    )
    return parser.parse_args()


def read_data(location, format):
    files = glob.glob(f"{location}*.{format}")
    if format == "parquet":
        data = pd.concat([pd.read_parquet(file) for file in files[::2]]).sample(frac=0.01)
    elif format == "h5":
        data = pd.concat([read_hdf5(file) for file in files[::2]]).sample(frac=0.01)
    geo_df = gpd.GeoDataFrame(
        data,
        crs="EPSG:4326",
        geometry=gpd.points_from_xy(data["Longitude"], data["Latitude"]),
    )
    return geo_df


def make_plot(
    data, nyc_boroughs, save_path="../../tasks/03/figs/location_density.png"
):
    ax = gplt.polyplot(nyc_boroughs, figsize=(12,12), projection=gplt.crs.AlbersEqualArea(), zorder=1, edgecolor="k")
    gplt.kdeplot(
        data,
        cmap="mako",
        thresh=0.05,
        alpha=1,
        ax=ax,
        clip=nyc_boroughs.geometry,
        zorder=3,
    )
    gplt.pointplot(
        data,
        hue="Violation County",
        ax=ax,
        legend=True,
        alpha=0.25,
        zorder=2,
        cmap="Reds",
        legend_kwargs={"loc": "upper left"},
    )
    plt.tight_layout()
    # plt.title("Distribution of parking tickets in New York City (Sampled)")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.savefig(save_path, dpi=300)


if __name__ == "__main__":
    args = parse_args()
    print("Reading Data...")
    data = read_data(args.input_location, args.data_format)
    print(f"Have read {data.shape[0]} rows of data.")

    nyc_boroughs = gpd.read_file(gplt.datasets.get_path("nyc_boroughs"))

    print("Making Plot...")
    make_plot(
        data,
        nyc_boroughs,
        save_path=f"../../tasks/03/figs/location_density_{args.data_format}.png",
    )
