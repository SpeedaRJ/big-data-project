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
        data = dd.concat([dd.from_pandas(read_hdf5(file)) for file in files]).sample(frac=0.02)
    return data


def make_plot_reg(data, weather_data, save_path):
    data.groupby("conditions")["Summons Number"].count().to_frame().merge(
        weather_data["conditions"].value_counts().to_frame(),
        how="left",
        left_index=True,
        right_index=True,
    ).assign(per_day=lambda x: x["Summons Number"] / x["count"]).sort_values(
        "per_day", ascending=True
    )[
        "per_day"
    ].compute().plot(
        kind="barh",
        figsize=(10, 10),
        title="Number of tickets per day of Weather Condition",
        color="#1b9e77",
    )
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)


def make_plot_duckdb(data, weather_data, save_path):
    data = data.compute()
    weather_data = weather_data.compute()
    tic = time.time()
    duckdb.query(
        "CREATE TEMP TABLE IF NOT EXISTS counts AS SELECT conditions, count(*) AS NumTickets FROM data GROUP BY conditions ORDER BY count(*) ASC"
    )
    duckdb.query(
        "CREATE TEMP TABLE IF NOT EXISTS weather_days AS SELECT conditions, count(*) AS NumDays FROM weather_data GROUP BY conditions ORDER BY count(*) ASC"
    )
    duckdb.query(
        "SELECT counts.conditions AS conditions, (NumTickets / NumDays) AS Ratio FROM counts JOIN weather_days ON counts.conditions = weather_days.conditions ORDER BY Ratio ASC"
    ).to_df().plot(
        kind="barh",
        figsize=(10, 10),
        title="Number of tickets per day of Weather Condition",
        color="skyblue",
        x="conditions",
    )
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    return tic


if __name__ == "__main__":
    args = parse_args()

    data = read_data(args.input_location, args.data_format)

    weather_data = dd.read_csv(
        "../../data/additional_data/weather/weather_NYC_2013_2024_processed.csv",
        sep=";",
    )

    if not args.data_format == "duckdb":
        tic = time.time()
        make_plot_reg(
            data,
            weather_data,
            save_path=f"../../tasks/03/figs/tickets_per_day_of_weather_condition_{args.data_format}.png",
        )
    else:
        tic = make_plot_duckdb(
            data,
            weather_data,
            save_path=f"../../tasks/03/figs/tickets_per_day_of_weather_condition_{args.data_format}.png",
        )

    print(f"Done in {time.time() - tic:.2f} seconds.")
