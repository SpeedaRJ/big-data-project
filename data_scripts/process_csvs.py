import argparse
import os
from pathlib import Path

import pandas as pd
from data_cleaning import (
    remove_mostly_null_files,
    require_fiscal_year,
    unify_column_names_and_dtypes,
)
from data_schema import DataSchema
from to_hdf5 import save_to_hdf5
from to_parquet import save_to_parquet
from tqdm import tqdm


def read_csvs(location):
    yearly_data_dict = {}
    for file in tqdm(os.listdir(location), desc="Reading CSV files by year"):
        path = os.path.join(location, file)
        year = int(Path(path).stem)
        schema = DataSchema(year)
        data = pd.read_csv(
            path, dtype=schema.get_schema(path), parse_dates=schema.get_dates()
        )
        data_processed = DataSchema.to_primitive_dtypes(DataSchema.fill_na(data), year)
        yearly_data_dict[year] = data_processed
    return yearly_data_dict


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("csv_location", type=str)
    parser.add_argument("parquet_location", type=str)
    parser.add_argument("hdf5_location", type=str)
    args = parser.parse_args()

    data = read_csvs(args.csv_location)

    data = require_fiscal_year(data)

    data = unify_column_names_and_dtypes(data)

    data_concat = pd.concat([data[year] for year in data.keys()])

    data_concat = remove_mostly_null_files(data_concat)

    print("Saving data to parquet")
    save_to_parquet(data_concat, args.parquet_location, "full_data")
    
    print("Saving data to hdf5")
    save_to_hdf5(data_concat, args.hdf5_location, "full_data")

    print("Saving data to csv")
    data_concat.to_csv(os.path.join(args.csv_location, "full_data.csv"), index=False)
