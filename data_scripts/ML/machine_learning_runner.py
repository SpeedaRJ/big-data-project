import argparse
import glob
import os
import sys
import time

import dask.dataframe as dd
from dask.distributed import Client
from dask_jobqueue import SLURMCluster
from dask_ml.metrics import mean_squared_error
from dask_ml.model_selection import train_test_split

import dask_ml as dml
import xgboost_ml as xgb
import sklearn_ml as skl

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
    files = glob.glob(f"{location}*.{format}")
    if format in ["duckdb", "parquet"]:
        data = dd.read_parquet(location)
    elif format == "h5":
        data = dd.concat([read_hdf5(file) for file in files])
    return data


if __name__ == "__main__":
    cluster = SLURMCluster(cores=4, processes=1, memory="8GB")
    client = Client(cluster)

    cluster.scale(jobs=4)
    client.wait_for_workers(4)
    print(cluster.job_script())

    args = parse_args()
    data = read_data(args.input_location, args.data_format)

    tic = time.time()

    # TODO: Add data Preparation and Sub-sampling here

    X, y = data.drop(columns=["count"]), data["count"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, shuffle=True, random_state=42
    )
    X_train_, y_train_ = X_train.values.persist(), y_train.values.persist()
    X_test_, y_test_ = X_test.values.persist(), y_test.values.persist()

    if args.ml_method == "dask":
        preds = dml.make_fit_predict(X_train_, y_train_, X_test_)
    elif args.ml_method == "xgb":
        preds = xgb.make_fit_predict(X_train_, y_train_, X_test_)
    elif args.ml_method == "sklearn":
        preds = skl.make_fit_predict(X_train_, y_train_, X_test_)

    print(f"Dask ML Error: {mean_squared_error(y_test_, preds, squared=False):.6f}")
    print(f"Dask ML Time: {time.time() - tic:.2f}s")
