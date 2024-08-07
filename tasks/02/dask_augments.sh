#!/bin/bash

if [[ $# -le 1 ]]; then
    echo "Call script as ... [base project dir] <parquet|hdf5>"
    exit
fi

[[ $2 == "parquet" ]] && extension="parquet" || extension="h5"

start=$(date +%s)

python $1/data_scripts/data_augmentations/merge_weather_data.py \
    --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/parking_tickets/$2/full_data_cleaned.$extension \
    --weather_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/weather/weather_NYC_2013_2024_processed.csv \
    --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/$2 \
    --output_name tickets_x_w \
    --data_format $2

weather=$(date +%s)
echo "Weather augmentations for $2-Dask took $(expr $weather - $start) seconds."

python $1/data_scripts/data_augmentations/merge_location_data.py \
    --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/$2/tickets_x_w.$extension \
    --df2_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/schools/middle_schools_NYC_2021_processed.csv \
    --df2_name_parameter "name" \
    --output_name_column "Closest Middle School" \
    --output_distance_column "Distance to CMS" \
    --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/$2 \
    --output_name tickets_x_ms \
    --data_format $2

middle_schools=$(date +%s)
echo "Middle School augmentations for $2-Dask took $(expr $middle_schools - $weather) seconds."

python $1/data_scripts/data_augmentations/merge_location_data.py \
    --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/$2/tickets_x_ms.$extension \
    --df2_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/schools/high_schools_NYC_2021_processed.csv \
    --df2_name_parameter "school_name" \
    --output_name_column "Closest High School" \
    --output_distance_column "Distance to CHS" \
    --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/$2 \
    --output_name tickets_x_hs \
    --data_format $2

high_schools=$(date +%s)
echo "High School augmentations for $2-Dask took $(expr $high_schools - $middle_schools) seconds."

python $1/data_scripts/data_augmentations/merge_location_data.py \
    --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/$2/tickets_x_hs.$extension \
    --df2_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/landmarks/landmarks_NYC_individual_processed.csv \
    --df2_name_parameter "LPC_NAME" \
    --output_name_column "Closest Landmark (LPC)" \
    --output_distance_column "Distance to CL" \
    --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/$2 \
    --output_name tickets_x_li \
    --data_format $2

individual_l=$(date +%s)
echo "Individual Landmark augmentations for $2-Dask took $(expr $individual_l - $high_schools) seconds."

python $1/data_scripts/data_augmentations/merge_location_data.py \
    --tickets_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/$2/tickets_x_li.$extension \
    --df2_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/landmarks/landmarks_NYC_scenic_processed.csv \
    --df2_name_parameter "SCEN_LM_NA" \
    --output_name_column "Closest Landmark (SCEN)" \
    --output_distance_column "Distance to CS" \
    --output_location /d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data/$2 \
    --output_name tickets_x_ls \
    --data_format $2

end=$(date +%s)
echo "Scenic Landmark augmentations for $2-Dask took $(expr $end - $individual_l) seconds."
echo "All data augmentations for $2-Dask took $(expr $end - $start) seconds."
