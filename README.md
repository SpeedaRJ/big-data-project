# big-data-project
Repository aimed at holding the code, report, and results of the Big Data course project.

# Good to Know Information

Environment setup:  
```bash
conda env create -f env.yaml
```

Data location on HPC:
```bash
/d/hpc/projects/FRI/bigdata/students/lsrj/data
```

# Repository Overview

Directory tree view of the repository

```sh
.
├── data
│   ├── aggregated_data
│   │   ├── parquet
│   │   └── hdf5
│   ├── additional_data
│   │   ├── businesses
│   │   ├── landmarks
│   │   ├── schools
│   │   └── weather
│   ├── meta_data
│   └── parking_tickets
│       ├── hdf5
│       │   ├── filtered
│       │   └── unprocessed
│       ├── parquet
│       │   ├── filtered
│       │   └── unprocessed
│       └── raw
├── data_scripts
│   ├── data_augmentations
│   ├── EDA
│   ├── ML
│   └── tmp
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

We obtained a dataset of the street code - name combinations from the [NYC Dataset website](https://data.cityofnewyork.us/City-Government/Street-Name-Dictionary/w4v2-rv6b/about_data ). The data came in the form of a text file, which we made a parser for (located in `big-data-project\meta_data`). This parser uses the Nominatim web API to obtain the necessary information about the streets (latitude and longitude) which we can later use for joining the secondary datasets. In the process of parsing the street codes, we are also creating a look-up table, which we can use to enrich the dataset, instead of relying on the API while streaming the dataset or otherwise processing it. Lastly, we combed through the acquired lookup table, in order to remove any obtain coordinates that fall outside of New York City.

Information on the meaning of the street codes was found on [Geosupport](https://nycplanning.github.io/Geosupport-UPG/chapters/chapterIV/chapterIV/).

# Task Solutions

## Task 1

> Note: The below file conversions and base data preprocessing steps were done locally, and therefore we used `pandas` to simplify the process. Consequent tasks were done using `dask` and `duckdb` as required.

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
| *full_dataset* | 16.33             | 3.28                  | 2.62               |

To pre-process the CSV files, run the following command, where the paths refer to where the original raw `csv` files are located, and where you want to store the full dataset in parquet and hdf5 formats:
```sh
python ./process_csvs.py <csv_path> <parquet_path> <hdf5_path>
```

> We detected an anomaly in the 2022 dataset, where we were left with only 150 rows once filtering for the fiscal year. See `notebooks\2022_date_verification.ipynb`.

## Task 2

We obtained our data for augmentations from various sources, most coming in different datasets from the [NYC OpenData](https://data.cityofnewyork.us/) website. We obtained the following additional datasets, for which we will describe the processing and merging steps below:
- We obtained the daily weather in New York (Central Park) from the [VisualCrossing](https://www.visualcrossing.com) API, that allowed to obtain more descriptive information which we assume we can use later. This approach also gave us the ability to get data for each day we required  in a unified format.
- We obtained a dataset of [Legally Operating Businesses](https://data.cityofnewyork.us/Business/Legally-Operating-Businesses/w7w3-xahh/about_data) from NYC OpenData.
- From NYC OpenData, we also obtained the [2021 DOE High School Directory](https://data.cityofnewyork.us/Education/2021-DOE-High-School-Directory/8b6c-7uty/about_data) and [2021 DOE Middle School Directory](https://data.cityofnewyork.us/Education/2021-DOE-Middle-School-Directory/f6s7-vytj/about_data) dataset on school locations. These were the most up to date datasets we were able to find on their website.
- Lastly, again from NYC OpenData, we also obtained the [Scenic Landmarks (Map)](https://data.cityofnewyork.us/Housing-Development/Scenic-Landmarks-Map-/gi7d-8gt5) and [Individual Landmark Sites (Map)](https://data.cityofnewyork.us/Housing-Development/Individual-Landmark-Sites-Map-/ts56-fkf5) datasets, which give us the spatial positioning of important New York landmarks.

> Note: We also planned to join Event data for each date, but we were unable to find an appropriate location attribute to use for merging.

### Preprocessing and Merging Procedure
All preprocessing steps for files can be found in `data_scripts\data_augmentations\augmentation_data_selector.py`. But to summarize, for each of the location based datasets (schools, landmarks and businesses) we kept the spatial information and some attributes of interest (typically only name). Businesses were slightly special in this, as we also had to consider when the business was operational, so we also kept that information. For weather, we kept the date column, as well as the columns we assumed most interesting in the context of EDA and ML for tasks 3 and 5. While the business and schools datasets gave us explicit latitude and longitude coordinates, the landmarks only contained the information of the polygon of any given landmark (list of coordinates that construct the outline of the landmark). Because we deemed this to complicated to merge later on, we implemented a procedure that extracted the central location of the landmark, based on it's polygon, and we stored that for later.

Merging procedures can be found in the `data_scripts\data_augmentations` directory. However again to summarize, we have 3 merging procedures:
- The weather data is simply merged by the data in timestamp format. We perform a left merge on the data columns of each of the datasets.
- Merging of the other datasets, with the exception of businesses is performed in the following way: We read the secondary data frame, and using it we construct an [RTree index](https://rtree.readthedocs.io/en/latest/), which we later use for quick distance computations. We then parse the entire dataset, to retrieve the closest secondary entity (landmark, school or business) to the given violation based on the previously computed latitude and longitude. Based on the coordinates of the pair of entities we then compute the [Haversine](https://pypi.org/project/haversine/) distance between them. After computing the `(<closest entity name>, <distance to ce>)` pairs for each row of the main dataset, we construct an additional data frame, which then use to perform a left join on the indexes of the two data frames.
- Merging the business information, largely follows the above procedure, with a few additional steps: Instead of retrieving the single closest entity to the location of the parking violation, we retrieve `n=1` entities, ordered by distance. We then filter these entities based on the `Issue Date` column to check if it falls between the businesses license creation and license expiration dates. We retrieve the entity in the first row of said result. If no result is given, we perform a recursive search with `n=n * 2`. We do this to limit the computational time, but still ensure we get a result for each entry. Here, instead of the above pair, we store `(<closest entity name>, <industry of ce>, <distance to ce>)`, since we tough this information might be interesting.

### Merging Times
| **Dataset**                          | **Parquet-Dask** | **HDF5-Dask** | **Parquet-DuckDB** |
| ------------------------------------ | ---------------- | ------------- | ------------------ |
| *Weather augmentations*              | 166.04 sec       | sum([]) sec   | sum([]) sec        |
| *Middle School augmentations*        | 6169.37 sec      | sum([]) sec   | sum([]) sec        |
| *High School augmentations*          | 7118.34 sec      | sum([]) sec   | sum([]) sec        |
| *Individual Landmarks augmentations* | 6164.79 sec      | sum([]) sec   | sum([]) sec        |
| *Scenic Landmarks*                   | 3475.09 sec      | sum([]) sec   | sum([]) sec        |
| *Businesses augmentations*           | 120755 sec       | sum([]) sec   | sum([]) sec        |

> TODO: Fill out above table

> Note: The directory `tasks\02` contains `sh` files, that run the above mentioned merging procedures for each year of the data separately. They also return the time required for the total merging time for all years with the selected augmentation dataset. These files were used to obtain the above results.

> Note: While the results may not completely show it, the processing time for the HDF5 file versions was 50% or faster than with the Parquet file versions when it came to processing with `dask` (e.g. `itertuples` taking 7 minutes to process compared to 15, and `merge` taking about 1 second compared to 20 - numbers given for average observation). The problem we did notice with the HDF5 file versions, was the speed of the I/O operations, particularly when it came to saving the data to files. However, it is unclear if this is a limitation of the file type, or our implementation of saving it / library we are using.

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

    ```bash
    python ./tasks/04/simple_topic_consumer.py --topics rolling_stats_all rolling_stats_boroughs rolling_stats_streets kmeans birch --save True
    ```

3. Run the stream processing program(s) that will consume raw data and produce rolling descriptive statistics.
    ```bash
    python ./tasks/04/stream_processing.py worker -l info
    ```

4. Run the producer - stream each line from specified raw files (We assume the data is chronologically ordered).
    ```bash
    python ./tasks/04/data_producer.py --tickets_file "./data/parking_tickets/parquet/full_data_cleaned.parquet" --weather_file "./data/additional_data/weather/weather_NYC_2013_2024_processed.csv" --fiscal_year 2014 --limit -1
    ```