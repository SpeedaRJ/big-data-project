import pandas as pd
import numpy as np
import dask
import dask.dataframe as dd
from haversine import haversine


# Function to find the closest point
def calculate_distances(row, ddf2: dd.DataFrame, row_lat_name='latitude', row_lon_name='longitude', ddf2_lat_name='latitude', ddf2_lon_name='longitude'):
    distances = ddf2.apply(lambda row2: haversine((row[row_lat_name], row[row_lon_name]), (row2[ddf2_lat_name], row2[ddf2_lon_name])), axis=1, meta=('distance', 'float64')).compute()
    assert distances.isna().sum() == 0, "There are NaN values in the distances"
    return distances


def closest_point_join(ddf1: dd.DataFrame, ddf2: dd.DataFrame, ddf1_lat_name='latitude', ddf1_lon_name='longitude', ddf2_lat_name='latitude', ddf2_lon_name='longitude', suffix='2'):
    min_indices, min_distances = [], []
    for index, series in ddf1.iterrows():
        # calculate the distances between the current row and all rows in the other dataframe
        distances = calculate_distances(series, ddf2, row_lat_name=ddf1_lat_name, row_lon_name=ddf1_lon_name, ddf2_lat_name=ddf2_lat_name, ddf2_lon_name=ddf2_lon_name)
        min_indices.append(distances.idxmin())
        min_distances.append(distances.min())
    # rename the columns of the second dataframe to avoid conflicts
    ddf2.columns = [f'{col}_{suffix}' for col in ddf2.columns]
    
    # Ensure ddf2 has proper divisions for indexing
    ddf2 = ddf2.set_index(ddf2.index, sorted=True)
    
    concatenated = dd.concat([ddf1, dd.from_pandas(pd.Series(min_distances, name='min_distance')), ddf2.loc[min_indices]], axis=1)
    return concatenated


def demo():
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


def schools_demo():
    high_schools = dd.read_csv("./data/additional_data/schools/high_schools_NYC_2021_processed.csv")
    middle_schools = dd.read_csv("./data/additional_data/schools/middle_schools_NYC_2021_processed.csv")
    
    res = closest_point_join(high_schools, middle_schools, ddf1_lat_name='Latitude', ddf1_lon_name='Longitude', ddf2_lat_name='Latitude', ddf2_lon_name='Longitude')
    res = res.compute()
    print(res)


def main():
    # print("Running demo")
    # demo()
    
    print("Running schools demo")
    schools_demo()


if __name__ == '__main__':
    main()
