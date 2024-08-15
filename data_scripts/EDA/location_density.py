import argparse
import glob
import os
import sys

import geopandas as gpd
import geoplot as gplt
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

sns.set_theme(style="dark")

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
        data = pd.read_parquet(location).sample(frac=0.02)
    elif format == "h5":
        data = pd.concat([read_hdf5(file) for file in files]).sample(frac=0.02)
    geo_df = gpd.GeoDataFrame(
        data,
        crs="EPSG:4326",
        geometry=gpd.points_from_xy(data["Longitude"], data["Latitude"]),
    )
    return geo_df


def make_plot(
    data, nyc_boroughs, save_path
):
    fig, ax = plt.subplots(
        1, 1, figsize=(12, 8)
    )
    nyc_boroughs.plot(ax=ax, alpha=0.4, edgecolor="k", zorder=1)
    gplt.kdeplot(
        data,
        cmap="mako",
        levels=np.linspace(0, 1, 16),
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
    plt.title("Distribution of parking tickets in New York City (Sampled)")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.savefig(save_path, dpi=300)


if __name__ == "__main__":
    args = parse_args()
    data = read_data(args.input_location, args.data_format)
    geo_df = gpd.GeoDataFrame(
        data,
        crs="EPSG:4326",
        geometry=gpd.points_from_xy(data["Longitude"], data["Latitude"]),
    )

    nyc_boroughs = gpd.read_file(gplt.datasets.get_path("nyc_boroughs"))

    make_plot(
        data,
        nyc_boroughs,
        save_path=f"../../tasks/03/figs/location_density_{args.data_format}.png",
    )
