import dask.dataframe as dd
import pandas as pd
from data_schema import DataSchema

PARQUET_LOCATION = "/d/hpc/projects/FRI/bigdata/students/lsrj/parquet/"

renamed_columns = {
    "DATE": "Issue Date",
    "PRCP": "Daily Precipitation",
    "SNOW": "Daily Snowfall",
    "SNWD": "Daily Snow Depth",
    "TMIN": "Daily Temperature (Min)",
    "TMAX": "Daily Temperature (Max)",
}


def preprocess_weather(dataframe):
    dataframe.dropna(axis=0, inplace=True)
    dataframe.rename(columns=renamed_columns, inplace=True)

    dataframe["Issue Date"] = pd.to_datetime(dataframe["Issue Date"]).dt.strftime(
        "%m/%d/%Y"
    )

    return dd.from_pandas(dataframe, chunksize=6_400)


def add_weather_data(base_data):
    weather_data = pd.read_csv(
        "../additional_data/NYC_Central_Park_weather_1869-2022.csv"
    )
    weather_data = preprocess_weather(weather_data)
    
    return base_data.merge(weather_data, on="Issue Date", how="inner").compute()


if __name__ == "__main__":
    base_data = dd.read_parquet(
        PARQUET_LOCATION,
        columns=list(DataSchema.keys()),
        dtype=str,
        blocksize=64_000_000,
        sample=80_000,
        low_memory=False,
    ).sample(frac=0.001).persist()
    print(len(base_data))
    combined_data = add_weather_data(base_data)
    print(combined_data.columns)
    print(len(combined_data))
