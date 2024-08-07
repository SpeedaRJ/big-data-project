#!/bin/bash

if [[ $# -eq 0 || $# -eq 2 ]]; then
    echo "Usage: $0 <base_path>"
    echo "OR"
    echo "Usage: $0 <csv_path> <parquet_path> <hdf5_path>"
    exit 1
fi

echo "Printing file sizes"

if [[ $# -eq 3 ]]; then

    echo "Raw CSV files:"
    ls -sh "$1"

    echo "Parquet files:"
    ls -sh "$2"

    echo "HDF5 files:"
    ls -sh "$3"

else
    echo "Raw CSV files:"
    ls -sh "$1/raw"

    echo "Parquet files:"
    ls -sh "$1/parquet"

    echo "HDF5 files:"
    ls -sh "$1/hdf5"
fi
