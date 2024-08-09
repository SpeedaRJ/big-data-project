#!/bin/bash

if [[ $# -eq 0 ]]; then
    echo "Call script as ... <parquet|hdf5> "
    exit
fi

[[ $1 == "parquet" ]] && ext="parquet" || ext="h5"

start=$(date +%s)

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_weather_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/parking_tickets/$1/filtered/2014-filtered.$ext --weather_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/weather/weather_NYC_2013_2024_processed.csv --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w/$1 --output_name tickets_weather_2014 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_weather_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/parking_tickets/$1/filtered/2015-filtered.$ext --weather_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/weather/weather_NYC_2013_2024_processed.csv --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w/$1 --output_name tickets_weather_2015 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_weather_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/parking_tickets/$1/filtered/2016-filtered.$ext --weather_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/weather/weather_NYC_2013_2024_processed.csv --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w/$1 --output_name tickets_weather_2016 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_weather_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/parking_tickets/$1/filtered/2017-filtered.$ext --weather_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/weather/weather_NYC_2013_2024_processed.csv --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w/$1 --output_name tickets_weather_2017 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_weather_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/parking_tickets/$1/filtered/2018-filtered.$ext --weather_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/weather/weather_NYC_2013_2024_processed.csv --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w/$1 --output_name tickets_weather_2018 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_weather_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/parking_tickets/$1/filtered/2019-filtered.$ext --weather_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/weather/weather_NYC_2013_2024_processed.csv --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w/$1 --output_name tickets_weather_2019 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_weather_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/parking_tickets/$1/filtered/2020-filtered.$ext --weather_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/weather/weather_NYC_2013_2024_processed.csv --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w/$1 --output_name tickets_weather_2020 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_weather_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/parking_tickets/$1/filtered/2021-filtered.$ext --weather_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/weather/weather_NYC_2013_2024_processed.csv --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w/$1 --output_name tickets_weather_2021 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_weather_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/parking_tickets/$1/filtered/2022-filtered.$ext --weather_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/weather/weather_NYC_2013_2024_processed.csv --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w/$1 --output_name tickets_weather_2022 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_weather_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/parking_tickets/$1/filtered/2023-filtered.$ext --weather_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/weather/weather_NYC_2013_2024_processed.csv --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w/$1 --output_name tickets_weather_2023 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_weather_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/parking_tickets/$1/filtered/2024-filtered.$ext --weather_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/weather/weather_NYC_2013_2024_processed.csv --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w/$1 --output_name tickets_weather_2024 --data_format $1

end=$(date +%s)
echo "Weather augmentations for $1-Dask took $(expr $end - $start) seconds."