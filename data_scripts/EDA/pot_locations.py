import os
import sys

import contextily as ctx
import dask.dataframe as dd
import geopandas as gpd
import geoplot as gplt
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


def make_plot(ms_data, hs_data, li_data, ls_data, b_data, save_path):
    nyc_boroughs = gpd.read_file(gplt.datasets.get_path("nyc_boroughs"))

    fig, axs = plt.subplots(1, 3, figsize=(30, 10))

    nyc_boroughs.plot(ax=axs[0], alpha=0.4, edgecolor="k")
    nyc_boroughs.plot(ax=axs[1], alpha=0.4, edgecolor="k")
    nyc_boroughs.plot(ax=axs[2], alpha=0.4, edgecolor="k")

    ms_data.compute().plot(
        kind="scatter",
        x="Longitude",
        y="Latitude",
        ax=axs[0],
        color="red",
        alpha=0.5,
        label="Middle Schools",
    )
    hs_data.compute().plot(
        kind="scatter",
        x="Longitude",
        y="Latitude",
        ax=axs[0],
        color="blue",
        alpha=0.5,
        label="High Schools",
    )
    li_data.compute().plot(
        kind="scatter",
        x="Longitude",
        y="Latitude",
        ax=axs[1],
        color="green",
        alpha=0.5,
        label="Individual Landmarks",
    )
    ls_data.compute().plot(
        kind="scatter",
        x="Longitude",
        y="Latitude",
        ax=axs[1],
        color="orange",
        alpha=0.5,
        label="Scenic Landmarks",
        ylabel="",
    )
    b_data.compute().plot(
        kind="scatter",
        x="Longitude",
        y="Latitude",
        ax=axs[2],
        color="purple",
        alpha=0.5,
        label="Businesses",
        ylabel="",
    )

    ctx.add_basemap(axs[0], crs=nyc_boroughs.crs.to_string(), source=ctx.providers.CartoDB.Positron)
    ctx.add_basemap(axs[1], crs=nyc_boroughs.crs.to_string(), source=ctx.providers.CartoDB.Positron)
    ctx.add_basemap(axs[2], crs=nyc_boroughs.crs.to_string(), source=ctx.providers.CartoDB.Positron)

    plt.tight_layout()
    plt.savefig(save_path, dpi=300)


if __name__ == "__main__":
    ms_data = dd.read_csv(
        "../../data/additional_data/schools/middle_schools_NYC_2021_processed.csv"
    )
    hs_data = dd.read_csv(
        "../../data/additional_data/schools/high_schools_NYC_2021_processed.csv"
    )
    li_data = dd.read_csv(
        "../../data/additional_data/landmarks/landmarks_NYC_individual_processed.csv"
    )
    ls_data = dd.read_csv(
        "../../data/additional_data/landmarks/landmarks_NYC_scenic_processed.csv"
    )
    try:
        b_data = dd.read_csv(
            "../../data/additional_data/businesses/businesses_NYC_2023_processed.csv"
        )
    except:
        b_data = dd.read_csv(
            "/d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/businesses/businesses_NYC_2023_processed.csv"
        )

    make_plot(
        ms_data,
        hs_data,
        li_data,
        ls_data,
        b_data,
        save_path=f"../../tasks/03/figs/locations_of_pot.png",
    )