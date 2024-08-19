import argparse
import glob
import os
import sys
import time
import warnings

import dask.dataframe as dd
import ml_dask as dml
import ml_sklearn as skl
import ml_xgboost as xgb
from dask.distributed import Client, LocalCluster, performance_report
from dask_ml.metrics import mean_squared_error
from dask_ml.model_selection import train_test_split
from dask_ml.preprocessing import Categorizer, OrdinalEncoder

warnings.filterwarnings("ignore")

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
    parser.add_argument(
        "--ml_method",
        type=str,
        choices=["dask", "xgb", "sklearn"],
        help="The method to use for ML.",
    )
    return parser.parse_args()


def read_data(location, format):
    if format in ["duckdb", "parquet"]:
        data = dd.read_parquet(location)
    elif format == "h5":
        files = glob.glob(f"{location}*.{format}")
        data = dd.concat([dd.from_pandas(read_hdf5(file)) for file in files])
    return data


def chunk(s):
    # for the comments, assume only a single grouping column, the
    # implementation can handle multiple group columns.
    #
    # s is a grouped series. value_counts creates a multi-series like
    # (group, value): count
    return s.value_counts()


def agg(s):
    # s is a grouped multi-index series. In .apply the full sub-df will passed
    # multi-index and all. Group on the value level and sum the counts. The
    # result of the lambda function is a series. Therefore, the result of the
    # apply is a multi-index series like (group, value): count
    return s.apply(lambda s: s.groupby(level=-1, sort=False).sum())

    # # faster version using pandas internals
    # s = s._selected_obj
    # return s.groupby(level=list(range(s.index.nlevels))).sum()


def finalize(s):
    # s is a multi-index series of the form (group, value): count. First
    # manually group on the group part of the index. The lambda will receive a
    # sub-series with multi index. Next, drop the group part from the index.
    # Finally, determine the index with the maximum value, i.e., the mode.
    level = list(range(s.index.nlevels - 1))
    return (
        s.groupby(level=level, sort=False)
        # .apply(lambda s: s.reset_index(level=level, drop=True).argmax())
        .apply(lambda s: s.reset_index(level=level, drop=True).idxmax())
    )


if __name__ == "__main__":
    print("Getting Workers")
    cluster = LocalCluster(n_workers=8, processes=1, memory_limit="8GB")
    client = Client(cluster)

    print("Reading Data")
    args = parse_args()
    with performance_report(filename=f"../../tasks/05/dask_ml_report_{args.data_format}_{args.ml_method}.html"):
        data = read_data(args.input_location, args.data_format)

        mode = dd.Aggregation("mode", chunk, agg, finalize)

        tic = time.time()

        print("Beggining Aggregation")
        daily_ddf = data.groupby("Issue Date", sort=False).agg(
            {
                "tempmax": "first",
                "tempmin": "first",
                "temp": "first",
                "conditions": "first",
                "humidity": "first",
                "windspeed": "first",
                "visibility": "first",
                "Distance to CMS": "mean",
                "Distance to CHS": "mean",
                "Distance to CIL": "mean",
                "Distance to CIS": "mean",
                "Distance to CB": "mean",
            }
        )

        daily_ddf["count"] = data.groupby("Issue Date", sort=False).size()
        daily_ddf = daily_ddf.dropna()

        daily_mode_ddf = data.groupby("Issue Date", sort=False).agg(
            {
                "Registration State": mode,
                "Plate Type": mode,
                "Violation Code": mode,
                "Vehicle Body Type": mode,
                "Vehicle Make": mode,
                "Issuing Agency": mode,
                "Violation County": mode,
            }
        )

        data = daily_ddf.merge(daily_mode_ddf, on="Issue Date")

        del daily_ddf, daily_mode_ddf

        print("Start of Categorizer")
        ce = Categorizer(
            columns=[
                "conditions",
                "Registration State",
                "Plate Type",
                "Violation Code",
                "Vehicle Body Type",
                "Vehicle Make",
                "Issuing Agency",
                "Violation County",
            ]
        )
        data = ce.fit_transform(data)

        enc = OrdinalEncoder(
            columns=[
                "conditions",
                "Registration State",
                "Plate Type",
                "Violation Code",
                "Vehicle Body Type",
                "Vehicle Make",
                "Issuing Agency",
                "Violation County",
            ]
        )
        data = enc.fit_transform(data)

        data = data.persist()

        print("Splitting Data")
        X, y = data.drop(columns=["count"]), data["count"]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, shuffle=True, random_state=42
        )
        X_train_, y_train_ = X_train.values.persist(), y_train.values.persist()
        X_test_, y_test_ = X_test.values.persist(), y_test.values.persist()

        X_train_.compute_chunk_sizes(), y_train_.compute_chunk_sizes(), X_test_.compute_chunk_sizes(), y_test_.compute_chunk_sizes()

        print("Performing ML")
        if args.ml_method == "dask":
            preds = dml.make_fit_predict(X_train_, y_train_, X_test_)
        elif args.ml_method == "xgb":
            preds = xgb.make_fit_predict(X_train_, y_train_, X_test_)
        elif args.ml_method == "sklearn":
            preds = skl.make_fit_predict(X_train_, y_train_, X_test_)

    print(f"Dask ML MSE: {mean_squared_error(y_test_, preds, squared=False):.6f}")
    print(f"Dask ML Time: {time.time() - tic:.2f}s")
    client.close()
