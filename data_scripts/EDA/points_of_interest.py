import argparse
import glob
import os
import sys
import time

import contextily as ctx
import dask.dataframe as dd
import duckdb
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
    ctx.add_basemap(
        ax, crs=nyc_boroughs.crs.to_string(), source=ctx.providers.CartoDB.Positron
    )

    # plt.title("Top 10 Closest Points of Interest w.r.t. Number of Parking Tickets Issued")
    plt.legend(loc="upper left")
    plt.tight_layout()
    plt.savefig(save_path, dpi=300, bbox_inches="tight")


def make_plot_duckdb(ms_data, hs_data, li_data, ls_data, b_data, save_path):
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
        s=ms_data["Number_MS"] / ms_data["Number_MS"].sum() * 500,
    )
    hs_data.plot(
        kind="scatter",
        x="Longitude",
        y="Latitude",
        ax=ax,
        color=pallete[1],
        alpha=0.8,
        label="High Schools",
        s=hs_data["Number_HS"] / hs_data["Number_HS"].sum() * 500,
    )
    li_data.plot(
        kind="scatter",
        x="Longitude",
        y="Latitude",
        ax=ax,
        color=pallete[2],
        alpha=0.8,
        label="Individual Landmarks",
        s=li_data["Number_LI"] / li_data["Number_LI"].sum() * 500,
    )
    ls_data.plot(
        kind="scatter",
        x="Longitude",
        y="Latitude",
        ax=ax,
        color=pallete[3],
        alpha=0.8,
        label="Scenic Landmarks",
        s=ls_data["Number_LS"] / ls_data["Number_LS"].sum() * 500,
    )
    b_data.plot(
        kind="scatter",
        x="Longitude",
        y="Latitude",
        ax=ax,
        color=pallete[4],
        alpha=0.8,
        label="Businesses",
        s=b_data["Number_B"] / b_data["Number_B"].sum() * 500,
    )

    # ctx.add_basemap(ax, crs=nyc_boroughs.crs.to_string(), zoom=12)

    # Fallback if OpenStreetMap is not available
    ctx.add_basemap(
        ax, crs=nyc_boroughs.crs.to_string(), source=ctx.providers.CartoDB.Positron
    )

    # plt.title("Top 10 Closest Points of Interest w.r.t. Number of Parking Tickets Issued")
    plt.legend(loc="upper left")
    plt.tight_layout()
    plt.savefig(save_path, dpi=300, bbox_inches="tight")


if __name__ == "__main__":
    args = parse_args()

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

    if not args.data_format == "duckdb":
        tic = time.time()
        ms_freq = compute_points(data, ms_data, "Closest Middle School", "name")
        hs_freq = compute_points(data, hs_data, "Closest High School", "school_name")
        li_freq = compute_points(
            data, li_data, "Closest Individual Landmark", "LPC_NAME"
        )
        ls_freq = compute_points(data, ls_data, "Closest Scenic Landmark", "SCEN_LM_NA")
        b_freq = compute_points(data, b_data, "Closest Business", "Business Name")
        make_plot_reg(
            ms_freq,
            hs_freq,
            li_freq,
            ls_freq,
            b_freq,
            save_path=f"../../tasks/03/figs/top_points_of_interest_{args.data_format}.png",
        )
    else:
        data = data.compute()
        ms_data = ms_data.compute()
        hs_data = hs_data.compute()
        li_data = li_data.compute()
        ls_data = ls_data.compute()
        b_data = b_data.compute()
        tic = time.time()
        ms_freq = duckdb.query(
            'SELECT ms_data.name, Latitude, Longitude, Number_MS FROM ms_data JOIN (SELECT "Closest Middle School" as name, count(*) as Number_MS FROM data GROUP BY "Closest Middle School") AS tmp ON ms_data.name = tmp.name ORDER BY Number_MS DESC LIMIT 10'
        ).to_df()
        hs_freq = duckdb.query(
            'SELECT hs_data.school_name, Latitude, Longitude, Number_HS FROM hs_data JOIN (SELECT "Closest High School" as school_name, count(*) as Number_HS FROM data GROUP BY "Closest High School") AS tmp ON hs_data.school_name = tmp.school_name ORDER BY Number_HS DESC LIMIT 10'
        ).to_df()
        li_freq = duckdb.query(
            'SELECT li_data.LPC_NAME, Latitude, Longitude, Number_LI FROM li_data JOIN (SELECT "Closest Individual Landmark" as li_name, count(*) as Number_LI FROM data GROUP BY "Closest Individual Landmark") AS tmp ON li_data.LPC_NAME = tmp.li_name ORDER BY Number_LI DESC LIMIT 10'
        ).to_df()
        ls_freq = duckdb.query(
            'SELECT ls_data.SCEN_LM_NA, Latitude, Longitude, Number_LS FROM ls_data JOIN (SELECT "Closest Scenic Landmark" as name, count(*) as Number_LS FROM data GROUP BY "Closest Scenic Landmark") AS tmp ON ls_data.SCEN_LM_NA = tmp.name ORDER BY Number_LS DESC LIMIT 10'
        ).to_df()
        b_freq = duckdb.query(
            'SELECT b_data."Business Name", Latitude, Longitude, Number_B FROM b_data JOIN (SELECT "Closest Business" as name, count(*) as Number_B FROM data GROUP BY "Closest Business") AS tmp ON b_data."Business Name" = tmp.name ORDER BY Number_B DESC LIMIT 10'
        ).to_df()
        make_plot_duckdb(
            ms_freq,
            hs_freq,
            li_freq,
            ls_freq,
            b_freq,
            save_path=f"../../tasks/03/figs/top_points_of_interest_{args.data_format}.png",
        )

    print(f"Done in {time.time() - tic:.2f} seconds.")
