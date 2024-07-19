import dask.dataframe as dd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from data_schema import DataSchema

sns.set_theme()

PARQUET_LOCATION = "/d/hpc/projects/FRI/bigdata/students/lsrj/parquet/"


def weekday_frequency(column):
    issue_dates = pd.to_datetime(column)
    issue_weekdays = issue_dates.day_name()
    labels, counts = np.unique(issue_weekdays, return_counts=True)
    fig, ax = plt.subplots(figsize=(10, 6))
    plt.bar(x=labels, height=counts)
    fig.tight_layout()
    fig.savefig("../results/figs/violation_county.png")


def reg_frequency_plot(column, name):
    counted_values = column.value_counts().compute()
    labels = np.array(counted_values.index.values)
    counts = np.array(counted_values.values)
    mask = counts > np.round(np.quantile(counts, q=0.1))
    fig, ax = plt.subplots(figsize=(30, 20))
    plt.bar(x=labels[mask], height=counts[mask])
    plt.xticks(range(0, len(labels[mask])), labels[mask], rotation=90)
    fig.tight_layout()
    fig.savefig(f"../results/figs/{name}.png")


if __name__ == "__main__":
    base_data = dd.read_parquet(
        PARQUET_LOCATION,
        columns=list(DataSchema.keys()),
        dtype=str,
        blocksize=64_000_000,
        sample=80_000,
        low_memory=False,
    )
    weekday_frequency(base_data["Issue Date"].sample(frac=0.001).persist())

    reg_frequency_plot(
        base_data["Violation Location"].sample(frac=0.001).persist(),
        "viol_location_frequency",
    )

    reg_frequency_plot(
        base_data["Vehicle Make"].sample(frac=0.001).persist(), "vehicle_make_frequency"
    )
