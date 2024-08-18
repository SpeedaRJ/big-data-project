import argparse
import glob
import os
import sys
import time
from datetime import datetime

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
        data = dd.concat([dd.from_pandas(read_hdf5(file)) for file in files[::2]]).sample(frac=0.02)
    return data


def subset_data(data):
    subset = data[["Violation County", "From Hours In Effect", "To Hours In Effect"]]
    subset = subset[
        (subset["From Hours In Effect"] != "") & (subset["To Hours In Effect"] != "")
    ]
    # We deal with problematic values of an inconsistent time format here
    subset["From Hours In Effect"] = (
        subset["From Hours In Effect"]
        .replace("ALL", "1200A", regex=True)
        .replace("A", " AM", regex=True)
        .replace("P", " PM", regex=True)
        .replace("19", "07", regex=True)
        .replace("16", "04", regex=True)
        .replace("18", "06", regex=True)
        .replace("13", "01", regex=True)
        .replace("23", "11", regex=True)
        .replace("0000", "1200", regex=True)
        .replace("0010", "1210", regex=True)
        .replace("1700", "0500", regex=True)
        .replace("1400", "0200", regex=True)
        .replace("2200", "1000", regex=True)
    )
    subset["To Hours In Effect"] = (
        subset["To Hours In Effect"]
        .replace("ALL", "1200P", regex=True)
        .replace("A", " AM", regex=True)
        .replace("P", " PM", regex=True)
        .replace("19", "07", regex=True)
        .replace("16", "04", regex=True)
        .replace("18", "06", regex=True)
        .replace("13", "01", regex=True)
        .replace("23", "11", regex=True)
        .replace("0000", "1200", regex=True)
        .replace("0010", "1210", regex=True)
        .replace("1700", "0500", regex=True)
        .replace("1400", "0200", regex=True)
        .replace("2200", "1000", regex=True)
    )
    subset = subset[~(subset["From Hours In Effect"] == "0  :")]
    subset = subset[~(subset["From Hours In Effect"] == "0  : AM")]
    subset = subset[~(subset["From Hours In Effect"] == "0  : PM")]
    subset = subset[~(subset["From Hours In Effect"].str.contains("^[0-9]{4}$"))]
    subset = subset[~(subset["From Hours In Effect"].str.contains("^[0-9]{2}\ "))]
    subset = subset[~(subset["From Hours In Effect"].str.contains("^[0-9]{2}$"))]
    subset = subset[~(subset["From Hours In Effect"].str.contains("^[2-9]{1}"))]
    subset = subset[
        ~(subset["From Hours In Effect"].str.contains("[0]{2}[0-9]{2} [AM,PM]"))
    ]
    subset = subset[
        ~(subset["From Hours In Effect"].str.contains("[0-9]{2}[6-9]{1}[0-9]"))
    ]
    subset = subset[~(subset["To Hours In Effect"] == "0  :")]
    subset = subset[~(subset["To Hours In Effect"] == "0  : AM")]
    subset = subset[~(subset["To Hours In Effect"] == "0  : PM")]
    subset = subset[~(subset["To Hours In Effect"].str.contains("^[0-9]{4}$"))]
    subset = subset[~(subset["To Hours In Effect"].str.contains("^[0-9]{2}\ "))]
    subset = subset[~(subset["To Hours In Effect"].str.contains("^[0-9]{2}$"))]
    subset = subset[~(subset["To Hours In Effect"].str.contains("^[2-9]{1}"))]
    subset = subset[
        ~(subset["To Hours In Effect"].str.contains("[0]{2}[0-9]{2} [AM,PM]"))
    ]
    subset = subset[
        ~(subset["To Hours In Effect"].str.contains("[0-9]{2}[6-9]{1}[0-9]"))
    ]
    return subset


def _aggregate_data(data):
    def wrapper(row):
        try:
            return abs(
                divmod(
                    (
                        datetime.strptime(row["To Hours In Effect"], "%I%M %p")
                        - datetime.strptime(row["From Hours In Effect"], "%I%M %p")
                    ).total_seconds(),
                    60,
                )[0]
            )
        except Exception as e:
            pass

    data = data.assign(
        time_diff=data.apply(
            wrapper,
            axis=1,
            meta=(None, "object"),
        )
    )

    data["time_diff"] = data["time_diff"].astype(float)
    return data


def make_plot_reg(data, save_path):
    data = _aggregate_data(data)

    custom_mean = dd.Aggregation(
        name="custom_mean",
        chunk=lambda s: (s.count(), s.sum()),
        agg=lambda count, sum: (count.sum(), sum.sum()),
        finalize=lambda count, sum: sum / count,
    )

    data.groupby("Violation County")["time_diff"].aggregate(
        custom_mean
    ).to_frame().sort_values(by="time_diff", ascending=False).compute().plot.barh(
        figsize=(12, 12),
        color="skyblue",
        edgecolor="black",
        legend=False,
    )

    plt.legend("")
    plt.ylabel(None)
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)


def make_plot_duckdb(data, save_path):
    data = data.compute()
    tic = time.time()
    duckdb.query(
        'CREATE TEMP TABLE IF NOT EXISTS parsed_times AS SELECT "Violation County", strptime("From Hours In Effect", \'%I%M %p\') AS start_time, strptime("To Hours In Effect", \'%I%M %p\') AS end_time FROM data'
    )
    duckdb.query(
        'SELECT "Violation County", mean(TimeDiff) AS AvgTimeDiff FROM (SELECT "Violation County", abs(date_diff(\'minute\', start_time, end_time)) AS TimeDiff FROM parsed_times) GROUP BY "Violation County" ORDER BY AvgTimeDiff DESC'
    ).to_df().plot(
        kind="barh",
        x="Violation County",
        legend=False,
        color="skyblue",
        edgecolor="black",
        figsize=(12, 12),
    )
    plt.legend("")
    plt.ylabel(None)
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    return tic


if __name__ == "__main__":
    args = parse_args()

    data = read_data(args.input_location, args.data_format)

    data = subset_data(data)

    if not args.data_format == "duckdb":
        tic = time.time()
        make_plot_reg(
            data,
            save_path=f"../../tasks/03/figs/time_per_borough_{args.data_format}.png",
        )
    else:
        tic = make_plot_duckdb(
            data,
            save_path=f"../../tasks/03/figs/time_per_borough_{args.data_format}.png",
        )

    print(f"Done in {time.time() - tic:.2f} seconds.")
