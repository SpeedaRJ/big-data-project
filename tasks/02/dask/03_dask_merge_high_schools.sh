#!/bin/bash

if [[ $# -eq 0 ]]; then
    echo "Call script as ... <parquet|hdf5> "
    exit
fi

[[ $1 == "parquet" ]] && ext="parquet" || ext="h5"

start=$(date +%s)

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_location_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms/$1/tickets_w_ms_2014.$ext --df2_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/schools/high_schools_NYC_2021_processed.csv --df2_name_parameter "school_name" --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms+hs/$1 --output_name_column "Closest High School" --output_distance_column "Distance to CHS" --output_name tickets_w_ms_hs_2014 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_location_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms/$1/tickets_w_ms_2015.$ext --df2_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/schools/high_schools_NYC_2021_processed.csv --df2_name_parameter "school_name" --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms+hs/$1 --output_name_column "Closest High School" --output_distance_column "Distance to CHS" --output_name tickets_w_ms_hs_2015 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_location_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms/$1/tickets_w_ms_2016.$ext --df2_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/schools/high_schools_NYC_2021_processed.csv --df2_name_parameter "school_name" --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms+hs/$1 --output_name_column "Closest High School" --output_distance_column "Distance to CHS" --output_name tickets_w_ms_hs_2016 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_location_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms/$1/tickets_w_ms_2017.$ext --df2_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/schools/high_schools_NYC_2021_processed.csv --df2_name_parameter "school_name" --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms+hs/$1 --output_name_column "Closest High School" --output_distance_column "Distance to CHS" --output_name tickets_w_ms_hs_2017 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_location_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms/$1/tickets_w_ms_2018.$ext --df2_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/schools/high_schools_NYC_2021_processed.csv --df2_name_parameter "school_name" --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms+hs/$1 --output_name_column "Closest High School" --output_distance_column "Distance to CHS" --output_name tickets_w_ms_hs_2018 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_location_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms/$1/tickets_w_ms_2019.$ext --df2_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/schools/high_schools_NYC_2021_processed.csv --df2_name_parameter "school_name" --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms+hs/$1 --output_name_column "Closest High School" --output_distance_column "Distance to CHS" --output_name tickets_w_ms_hs_2019 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_location_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms/$1/tickets_w_ms_2020.$ext --df2_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/schools/high_schools_NYC_2021_processed.csv --df2_name_parameter "school_name" --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms+hs/$1 --output_name_column "Closest High School" --output_distance_column "Distance to CHS" --output_name tickets_w_ms_hs_2020 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_location_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms/$1/tickets_w_ms_2021.$ext --df2_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/schools/high_schools_NYC_2021_processed.csv --df2_name_parameter "school_name" --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms+hs/$1 --output_name_column "Closest High School" --output_distance_column "Distance to CHS" --output_name tickets_w_ms_hs_2021 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_location_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms/$1/tickets_w_ms_2022.$ext --df2_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/schools/high_schools_NYC_2021_processed.csv --df2_name_parameter "school_name" --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms+hs/$1 --output_name_column "Closest High School" --output_distance_column "Distance to CHS" --output_name tickets_w_ms_hs_2022 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_location_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms/$1/tickets_w_ms_2023.$ext --df2_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/schools/high_schools_NYC_2021_processed.csv --df2_name_parameter "school_name" --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms+hs/$1 --output_name_column "Closest High School" --output_distance_column "Distance to CHS" --output_name tickets_w_ms_hs_2023 --data_format $1

python /d/hpc/home/rj7149/BD/project/big-data-project/data_scripts/data_augmentations/merge_location_data.py --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms/$1/tickets_w_ms_2024.$ext --df2_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/schools/high_schools_NYC_2021_processed.csv --df2_name_parameter "school_name" --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/w+ms+hs/$1 --output_name_column "Closest High School" --output_distance_column "Distance to CHS" --output_name tickets_w_ms_hs_2024 --data_format $1

end=$(date +%s)
echo "High School augmentations for $1-Dask took $(expr $end - $start) seconds."
