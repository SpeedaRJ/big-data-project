# big-data-project
Repository aimed at holding the code, report, and results of the Big Data course project.

# Environment Setup

```bash
conda env create -f env.yaml
```

# Repository Overview

Directory tree view of the repository

```sh
.
├── data
│   ├── additional_data
│   │   ├── businesses
│   │   ├── events
│   │   ├── landmarks
│   │   ├── schools
│   │   └── weather
│   ├── meta_data
│   └── parking_tickets
│       ├── hdf5
│       ├── parquet
│       └── raw
├── data_scripts
│   └── data_augmentations
├── notebooks
├── results
│   └── ...
├── scripts
└── tasks
    ├── 01
    └── ...
```

# Running Notes

The raw data files are located at:
```bash
/d/hpc/projects/FRI/bigdata/data/NYTickets
```

All of our data on the hpc is located in the `data` folder at:
```bash
/d/hpc/projects/FRI/bigdata/students/lsrj
```

## Parsing the Street Codes

We obtained a dataset of the street code - name combinations from the [NYC Dataset website](https://data.cityofnewyork.us/City-Government/Street-Name-Dictionary/w4v2-rv6b/about_data ). The data came in the form of a text file, which we made a parser for (located in `big-data-project\meta_data`). This parser uses the Nominatim web API to obtain the necessary information about the streets (latitude and longitude) which we can later use for joining the secondary datasets. In the process of parsing the street codes, we are also creating a look-up table, which we can use to enrich the dataset, instead of relying on the API while streaming the dataset or otherwise processing it.

Information on the meaning of the street codes was found on [Geosupport](https://nycplanning.github.io/Geosupport-UPG/chapters/chapterIV/chapterIV/).

# Task Solutions

## Task 1

> NOTE: The below file conversions and base data preprocessing steps were done locally, and therefore we used `pandas` to simplify the process. Consequent tasks were done using `dask` and `duckdb` as required.

Below are displayed the file sizes after format conversion. Before any data has been cleaned, omitted, processed or removed. The only preprocessing step at this point was filling `None` values with either `-1` or `''` based on the column data type. The `Issue Date` column, was also converted to the `np.int64` data type in the form of a UNIX timestamp, instead of the default `datetime` type.

| **File Name** | **CSV Size [MD]** | **Parquet Size [MB]** | **HDF5 Size [MB]** |
| ------------- | ----------------- | --------------------- | ------------------ |
| *2014.csv*    | 1710.72           | 330.06                | 184.14             |
| *2015.csv*    | 2393.49           | 511.80                | 318.64             |
| *2016.csv*    | 1971.17           | 352.62                | 192.56             |
| *2017.csv*    | 1990.24           | 519.51                | 351.12             |
| *2018.csv*    | 2073.71           | 401.29                | 232.82             |
| *2019.csv*    | 1910.91           | 364.27                | 211.75             |
| *2020.csv*    | 2214.43           | 382.20                | 216.46             |
| *2021.csv*    | 2618.82           | 445.62                | 252.72             |
| *2022.csv*    | 2645.07           | 418.49                | 218.41             |
| *2023.csv*    | 3839.23           | 617.47                | 310.59             |
| *2024.csv*    | 2645.07           | 418.49                | 216.27             |

File conversions were performed locally, and file sizes were obtained using the following command:
```pwsh
ls | Select-Object Name, @{Name="MegaBytes";Expression={$_.Length / 1MB}}
```

> Note: A solution script can also be found in `big-data-project\tasks\01`, to list all file sizes either in a structured directory (with `raw`, `parquet` and `hdf5` sub-folders) or 3 separate directories.

### File Preprocessing

In order to allow for better processing and data enrichment down the line, we then performed some preprocessing steps on the above that. To simplify things, we re-read the `*.csv` files. To avoid repeating the same actions on the `parquet` and `hdf5` data files. We did the following:
1. We limited the data from each year, to the fiscal year as denoted in the data description on the official website. The fiscal year, refers to the period `year - 1/7/1` to `year/6/30`.
2. Then we unified the column names, and made sure that they don't contain any whitespace characters.
3. In addition, we also unified the column data types, since these in some cases had conflicting descriptions in the yearly reports.
4. Lastly, after combining all the yearly data into a single data frame, we performed an analysis of nullable values for each column. We remove the columns, where we encounter more than $75\%$ of nullable values.
5. Then, since most of our data augmentations revolve around location, we limited the data to only those rows, which have at least one of 3 street codes present, and removed the rest. Then following that, since we require the borough of the location (to avoid repeated street codes which are unique only inside a borough) we removed all rows which do not have a specified violation county. We followed this up by unifying the borough names, and removing a few instances of unidentifiable boroughs (PXA, USA, 0000, etc.).
6. Lastly we localize each of the entries via their street codes for which we obtained the Latitude and Longitude previously. We perform the localization by checking which of the 3 street codes are given for each entry, and compute the average of the given street code coordinates. For some street codes we were unable to obtain the coordinates from the API, thus some entries had a coordinate pair of `(0,0)`, which we removed. This left us with about 80 million remaining data entries, and file sizes in different formats as displayed in the table below.

> Note: Nullable values above refer to either `''` for string data types, or `-1` for numeric data types. Since, we assume that 0 can be a value of the dataset in some cases.

| **File Name**  | **CSV Size [GB]** | **Parquet Size [GB]** | **HDF5 Size [GB]** |
| -------------- | ----------------- | --------------------- | ------------------ |
| *full_dataset* | 16.33             | 3.32                  | 2.44               |

To pre-process the CSV files, run the following command, where the paths refer to where the original raw `csv` files are located, and where you want to store the full dataset in parquet and hdf5 formats:
```sh
python ./process_csvs.py <csv_path> <parquet_path>  <hdf5_path>
```

## Task 2

> TODO

```bash
srun python data_scripts/lat_lon_join.py --df1_location "/d/hpc/projects/FRI/bigdata/students/lsrj/data/parking_tickets/parquet/full_data_cleaned.parquet" --df1_key "Summons Number" --df2_location "/d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/schools/high_schools_NYC_2021_processed.csv" --df2_key "school_name" --output_location "/d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data" --output_name "tickets_high_schools_agg"
```

Weather data
```bash
srun python data_scripts/data_augmentations/add_weather_data.py --tickets_location "/d/hpc/projects/FRI/bigdata/students/lsrj/data/parking_tickets/parquet/full_data_cleaned.parquet" --weather_location "/d/hpc/projects/FRI/bigdata/students/lsrj/data/additional_data/weather/weather_NYC_2013_2024_processed.csv" --output_location "/d/hpc/projects/FRI/bigdata/students/lsrj/data/aggregated_data" --output_name "tickets_weather_agg" --data_format "parquet"
```


## Task 4 (Streaming)

1. Set up kafka with docker (we use the same configuration as we used in the assignment)
    ```bash
    docker compose -f "./tasks/04/docker-compose.yml" up -d
    ```
    TODO: commands for deleting all of the currently stored data so we can start from scratch


2. Run the consumer(s) - consume the specified topics containing rolling descriptive statistics.
    ```bash
    python ./tasks/04/simple_topic_consumer.py --topics <topic1> <topic2> <topic3> ... --save <False|True>
    ```

3. Run the stream processing program(s) that will consume raw data and produce rolling descriptive statistics.
    ```bash
    python ./tasks/04/stream_processing.py worker -l info
    ```

4. Run the producer - stream each line from specified raw files (We assume the data is chronologically ordered).
    ```bash
    python ./tasks/04/raw_data_producer.py --data-dir "./data/raw" --years <year1> <year2> <year3> ... --n-lines <number of lines to stream | -1 for whole file>
    ```