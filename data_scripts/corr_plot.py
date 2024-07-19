import dask.dataframe as dd
import matplotlib.pyplot as plt
from add_weather_data import add_weather_data
from dask.distributed import Client
from dask_jobqueue import SLURMCluster
from data_schema import DataSchema

PARQUET_LOCATION = "/d/hpc/projects/FRI/bigdata/students/lsrj/parquet/"


def get_subsampled_corr_plot(dataframe):
    for column in dataframe.columns:
        dataframe[column] = dataframe[column].astype("category").factorize()[0]

    correlation_matrix = dataframe.corr(numeric_only=False)
    f = plt.figure(figsize=(12, 10))
    plt.matshow(correlation_matrix, fignum=f.number, cmap="Spectral")
    plt.xticks(
        range(len(dataframe.columns)), dataframe.columns, fontsize=8, rotation=90
    )
    plt.yticks(range(len(dataframe.columns)), dataframe.columns, fontsize=8)
    plt.colorbar(cmap="Spectral")
    plt.savefig("../results/figs/data_confusion_matrix.png")
    plt.close(f)


if __name__ == "__main__":
    base_data = (
        dd.read_parquet(
            PARQUET_LOCATION,
            columns=list(DataSchema.keys()),
            dtype=str,
            blocksize=64_000_000,
            sample=80_000,
            low_memory=False,
        )
        .sample(frac=0.001)
        .persist()
    )
    get_subsampled_corr_plot(add_weather_data(base_data))
