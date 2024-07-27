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

We obtained a dataset of the street code - name combinations from the [NYC Dataset website](https://data.cityofnewyork.us/City-Government/Street-Name-Dictionary/w4v2-rv6b/about_data ). The data came in the form of a text file, which we made a parser for (located in `big-data-project\meta_data`). This parser uses the Nominatim web API to obtain the necessary information about the streets (latitude and longitude) which we can later use for joining the secondary datasets. In the process of parsing the street codes, we are also creating a look-up table, which we can use to enrich the dataset, instead of relying on the API while streaming the dataset or otherwise processing it.

Information on the meaning of the street codes was found on [Geosupport](https://nycplanning.github.io/Geosupport-UPG/chapters/chapterIV/chapterIV/).

# Task Solutions

## Task 1

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

## File Preprocessing

In order to allow for better processing and data enrichment down the line, we then performed some preprocessing steps on the above that. To simplify things, we re-read the `*.csv` files. To avoid repeating the same actions on the `parquet` and `hdf5` data files. We did the following:
1. We limited the data from each year, to the fiscal year as denoted in the data description on the official website. The fiscal year, refers to the period `year - 1/7/1` to `year/6/30`.
2. Then we unified the column names, and made sure that they don't contain any whitespace characters.
3. In addition, we also unified the column data types, since these in some cases had conflicting descriptions in the yearly reports.
4. Lastly, after combining all the yearly data into a single data frame, we performed an analysis of nullable values for each column. We remove the columns, where we encounter more than $75\%$ of nullable values.

> Note: Nullable values above refer to either `''` for string data types, or `-1` for numeric data types. Since, we assume that 0 can be a value of the dataset in some cases.

| **File Name**  | **CSV Size [GB]** | **Parquet Size [GB]** | **HDF5 Size [GB]** |
| -------------- | ----------------- | --------------------- | ------------------ |
| *full_dataset* | 20.96             | 4.46                  | 2.3                |

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