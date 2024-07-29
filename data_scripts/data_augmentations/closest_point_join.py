import pandas as pd
import numpy as np
import dask
import dask.dataframe as dd
from haversine import haversine


# Function to find the closest point
def calculate_distances(row, ddf2: dd.DataFrame):
    latitude, longitude = row['latitude'].item(), row['longitude'].item()
    distances = ddf2.apply(lambda row2: haversine((latitude, longitude), (row2['latitude'], row2['longitude'])), axis=1, meta=('distance', 'float64')).compute()
    return distances


def closest_point_join(ddf1: dd.DataFrame, ddf2: dd.DataFrame, suffix='2'):
    ddf2 = ddf2.persist()
    min_indices, min_distances = [], []
    for index, series in ddf1.iterrows():
        # calculate the distances between the current row and all rows in the other dataframe
        distances = calculate_distances(series, ddf2)
        min_indices.append(distances.idxmin())
        min_distances.append(distances.min())
    # rename the columns of the second dataframe to avoid conflicts
    ddf2.columns = [f'{col}_{suffix}' for col in ddf2.columns]
    # compute everything and concatenate the results - TODO: problems with .iloc if we keep using dask
    # concatenated = pd.concat([ddf1.compute(), pd.Series(min_distances, name='min_distance'), ddf2.compute().iloc[min_indices]], axis=1)
    concatenated = dd.concat([ddf1, dd.from_pandas(pd.Series(min_distances, name='min_distance'), npartitions=2), ddf2.loc[min_indices]], axis=1)
    return concatenated

# Sample data
data1 = {
    'id': [1, 2, 3],
    'latitude': [34.05, 36.16, 40.71],
    'longitude': [-118.25, -115.15, -74.01]
}
data2 = {
    'name': ['LocationA', 'LocationB', 'LocationC'],
    'latitude': [34.05, 36.12, 41.0],
    'longitude': [-118.24, -115.17, -73.93]
}
# Convert to Dask DataFrames
ddf1 = dd.from_pandas(pd.DataFrame(data1), npartitions=2)
ddf2 = dd.from_pandas(pd.DataFrame(data2), npartitions=2)

res = closest_point_join(ddf1, ddf2)
res = res.compute()
print(res)
