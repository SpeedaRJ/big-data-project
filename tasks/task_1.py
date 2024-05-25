import os
import argparse
import numpy as np

def print_statistics(location, array):
    print(f"""
        {location} file format Statistics (in MB):
        - Number of files: {len(array)}
        - Total file size: {np.sum(array)}
        - Average file size: {np.mean(array)}
        - File size SD: {np.std(array)}
        - Min file size: {np.min(array)}
        - Max file size: {np.max(array)}
    """)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_location", type=str)
    parser.add_argument("raw_data_location", type=str)
    args = parser.parse_args()

    raw_data_list = []

    for file in os.listdir(args.raw_data_location):
        if file.find(".sh") == -1:
            raw_data_list.append(os.path.getsize(os.path.join(args.raw_data_location, file)) >> 20)

    print_statistics("CSV", raw_data_list)

    parquet_dir = os.path.join(args.data_location, "parquet")
    hdf_dir = os.path.join(args.data_location, "hdf")

    parquet_data_list = []

    for file in os.listdir(parquet_dir):
        parquet_data_list.append(os.path.getsize(os.path.join(parquet_dir, file)) >> 20)

    print_statistics("Parquet", parquet_data_list)

    hdf_data_list = []

    for file in os.listdir(hdf_dir):
        hdf_data_list.append(os.path.getsize(os.path.join(hdf_dir, file)) >> 20)

    print_statistics("HDF5", hdf_data_list)
    
