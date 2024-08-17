import argparse
import glob
import os
import sys
import time

import contextily as ctx
import dask.dataframe as dd
import geopandas as gpd
import geoplot as gplt
import matplotlib.pyplot as plt
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
        choices=["parquet", "h5", "duckdb"],
        help="The format of the input data file (parquet or hdf5).",
    )
    return parser.parse_args()


def read_data(location, format):
    files = glob.glob(f"{location}*.{format}")
    if format in ["duckdb", "parquet"]:
        data = dd.read_parquet(location).sample(frac=0.02)
    elif format == "h5":
        data = dd.concat([read_hdf5(file) for file in files]).sample(frac=0.02)
    return data


def compute_points(data, complementary_data, column, name):
    return (
        data.groupby(column)["Summons Number"]
        .count()
        .to_frame()
        .sort_values(by="Summons Number", ascending=False)
        .merge(complementary_data, left_index=True, right_on=name)
        .head(10, npartitions=-1)
    )


def make_plot_reg(ms_data, hs_data, li_data, ls_data, b_data, save_path):
    nyc_boroughs = gpd.read_file(gplt.datasets.get_path("nyc_boroughs"))

    fig, ax = plt.subplots(1, 1, figsize=(10, 10))

    nyc_boroughs.plot(ax=ax, alpha=0.4, edgecolor="k")

    pallete = sns.color_palette("deep", 5)

    ms_data.plot(
        kind="scatter",
        x="Longitude",
        y="Latitude",
        ax=ax,
        color=pallete[0],
        alpha=0.8,
        label="Middle Schools",
        s=ms_data["Summons Number"] / ms_data["Summons Number"].sum() * 500,
    )
    hs_data.plot(
        kind="scatter",
        x="Longitude",
        y="Latitude",
        ax=ax,
        color=pallete[1],
        alpha=0.8,
        label="High Schools",
        s=hs_data["Summons Number"] / hs_data["Summons Number"].sum() * 500,
    )
    li_data.plot(
        kind="scatter",
        x="Longitude",
        y="Latitude",
        ax=ax,
        color=pallete[2],
        alpha=0.8,
        label="Individual Landmarks",
        s=li_data["Summons Number"] / li_data["Summons Number"].sum() * 500,
    )
    ls_data.plot(
        kind="scatter",
        x="Longitude",
        y="Latitude",
        ax=ax,
        color=pallete[3],
        alpha=0.8,
        label="Scenic Landmarks",
        s=ls_data["Summons Number"] / ls_data["Summons Number"].sum() * 500,
    )
    b_data.plot(
        kind="scatter",
        x="Longitude",
        y="Latitude",
        ax=ax,
        color=pallete[4],
        alpha=0.8,
        label="Businesses",
        s=b_data["Summons Number"] / b_data["Summons Number"].sum() * 500,
    )

    # ctx.add_basemap(ax, crs=nyc_boroughs.crs.to_string(), zoom=12)

    # Fallback if OpenStreetMap is not available
    ctx.add_basemap(ax, crs=nyc_boroughs.crs.to_string(), source=ctx.providers.CartoDB.Positron)

    # plt.title("Top 10 Closest Points of Interest w.r.t. Number of Parking Tickets Issued")
    plt.legend(loc='upper left')
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)


if __name__ == "__main__":
    args = parse_args()

    tic = time.time()

    data = read_data(args.input_location, args.data_format)

    ms_data = dd.read_csv(
        "../../data/additional_data/schools/middle_schools_NYC_2021_processed.csv"
    )
    hs_data = dd.read_csv(
        "../../data/additional_data/schools/high_schools_NYC_2021_processed.csv"
    )
    li_data = dd.read_csv(
        "../../data/additional_data/landmarks/landmarks_NYC_individual_processed.csv"
    )
    ls_data = dd.read_csv(
        "../../data/additional_data/landmarks/landmarks_NYC_scenic_processed.csv"
    )
    try:
        b_data = dd.read_csv(
            "../../data/additional_data/businesses/businesses_NYC_2023_processed.csv"
        )
    except:
        b_data = dd.read_csv(
            "/d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/businesses/businesses_NYC_2023_processed.csv"
        )

    ms_freq = compute_points(data, ms_data, "Closest Middle School", "name")
    hs_freq = compute_points(data, hs_data, "Closest High School", "school_name")
    li_freq = compute_points(data, li_data, "Closest Individual Landmark", "LPC_NAME")
    ls_freq = compute_points(data, ls_data, "Closest Scenic Landmark", "SCEN_LM_NA")
    b_freq = compute_points(data, b_data, "Closest Business", "Business Name")

    if not args.data_format == "duckdb":
        make_plot_reg(
            ms_freq, hs_freq, li_freq, ls_freq, b_freq,
            save_path=f"../../tasks/03/figs/top_points_of_interest_{args.data_format}.png",
        )
    else:
        ...

    print(f"Done in {time.time() - tic:.2f} seconds.")
