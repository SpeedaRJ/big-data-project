import argparse
import glob
import os
import sys

import dask.dataframe as dd
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
        choices=["parquet", "h5"],
        help="The format of the input data file (parquet or hdf5).",
    )
    return parser.parse_args()


def read_data(location, format):
    files = glob.glob(f"{location}*.{format}")
    if format == "parquet":
        data = dd.read_parquet(location).sample(frac=0.02)
    elif format == "h5":
        data = dd.concat([read_hdf5(file) for file in files]).sample(frac=0.02)
    data = data.rename(columns={"Distance to CIS": "Distance to CSL"})
    return data


def make_plot(data, save_path):
    data.compute().boxplot(
        column=[
            "Distance to CMS",
            "Distance to CHS",
            "Distance to CSL",
            "Distance to CIL",
            "Distance to CB",
        ],
        showfliers=False,
        figsize=(14, 6),
    )
    plt.yscale("log")
    plt.ylabel("Distance [km]")
    # plt.title("Metrics of distances to nearest points of interest (per category)")
    plt.savefig(save_path, dpi=300)


if __name__ == "__main__":
    args = parse_args()
    data = read_data(args.input_location, args.data_format)

    make_plot(
        data,
        save_path=f"../../tasks/03/figs/distances_statistical_description_{args.data_format}.png",
    )
