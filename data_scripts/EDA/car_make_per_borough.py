import argparse
import glob
import os
import sys
import time

import dask.dataframe as dd
import duckdb
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


def compute_counts(data):
    return (
        data.groupby(["Violation County", "Vehicle Make"])["Summons Number"]
        .count()
        .to_frame()
        .sort_values(by="Summons Number", ascending=False)
        .compute()
        .sort_index()
        .unstack()
    )


def make_plot_reg(data, save_path):
    fig = plt.figure(figsize=(30, 20))
    columns = 3
    rows = 2

    color_map = {
        "Bronx": "#7570b3",
        "Brooklyn": "#66a61e",
        "Manhattan": "#d95f02",
        "Queens": "#e7298a",
        "Staten Island": "#1b9e77",
    }

    for i in range(5):
        fig.add_subplot(rows, columns, i + 1)
        tmp = data.iloc[i].dropna().droplevel(0)
        tmp = tmp[~(tmp.index == "")].sort_values(ascending=False).head(10)
        plt.bar(tmp.index, tmp.values, color=color_map[data.index[i]])
        plt.title(data.index[i])

    plt.tight_layout()
    plt.savefig(save_path, dpi=300)


def make_plot_duckdb(data, save_path):
    data = data.compute()
    duckdb.query(
        'CREATE TEMP TABLE IF NOT EXISTS counts AS SELECT "Violation County", "Vehicle Make", count(*) as NumMake FROM data GROUP BY "Violation County", "Vehicle Make" ORDER BY count(*) DESC;'
    )
    res = (
        duckdb.query(
            'WITH PairCounts AS (SELECT "Violation County", "Vehicle Make", NumMake, ROW_NUMBER() OVER (PARTITION BY "Violation County" ORDER BY NumMake DESC) AS rn FROM counts) SELECT  "Violation County", "Vehicle Make", NumMake FROM PairCounts WHERE rn <= 10 GROUP BY "Violation County", "Vehicle Make", NumMake ORDER BY "Violation County", NumMake DESC;'
        )
        .to_df()
        .groupby("Violation County")
    )

    fig = plt.figure(figsize=(30, 20))
    columns = 3
    rows = 2

    color_map = {
        "Bronx": "#7570b3",
        "Brooklyn": "#66a61e",
        "Manhattan": "#d95f02",
        "Queens": "#e7298a",
        "Staten Island": "#1b9e77",
    }

    for i, (name, group) in enumerate(res):
        fig.add_subplot(rows, columns, i + 1)
        plt.bar(group["Vehicle Make"], group["NumMake"], color=color_map[name])
        plt.title(name)

    plt.tight_layout()
    plt.savefig(save_path, dpi=300)


if __name__ == "__main__":
    args = parse_args()

    tic = time.time()

    data = read_data(args.input_location, args.data_format)

    if not args.data_format == "duckdb":
        counts_per_borough = compute_counts(data)
        make_plot_reg(
            counts_per_borough,
            save_path=f"../../tasks/03/figs/car_make_per_borough_{args.data_format}.png",
        )
    else:
        make_plot_duckdb(
            data,
            save_path=f"../../tasks/03/figs/car_make_per_borough_{args.data_format}.png",
        )

    print(f"Done in {time.time() - tic:.2f} seconds.")
