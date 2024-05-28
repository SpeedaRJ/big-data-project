# big-data-project
Repository aimed at holding the code, report, and results of the Big Data course project.

# Running Notes

To get to the HPC data location for our data, run: 
```bash
cd /d/hpc/projects/FRI/bigdata/students/lsrj
```

While the raw data files are located at:
```bash
/d/hpc/projects/FRI/bigdata/data/NYTickets
```

## Data Format Conversion

> To Parquet file conversion took: $841.37$s or $14$min using blocksize of $64 000 000$. That means it on average took $2.02$s to generate each of the 415 files (see below).

> To HDF5 file conversion took: $4309.29$s or $71.82$min using blocksize of $640 000$. That means it on average took $0.1$s to generate each of the 42003 files (see below).

## Adding Additional Data

We acquired weather information from the Kaggle [New York City Weather: A 154-Year Retrospective](https://kaggle.com/datasets/danbraswell/new-york-city-weather-18692022/data).

> To simplify the testing and exploratory data analysis process we subsampled 0.1% of the data and performed an inner merge with the weather dataset. This process lost us 40k samples in one of the test we tracked this.

# Task Solutions

## Task 1

| **Metric**                        | **CSV Format (in MB)** | **Parquet Format (in MB)** |  **HDF5 Format (in MB)** |
|-------------------------------|--------------------|------------------------|------------------------|
| *Number of files*               | 11                | 415                    | 42003 |
| *Total file size*               | 25635             | 5309                   | 150431 |
| *Average file size*             | 2330.45           | 12.79                  | 3.58 |
| *File size standard deviation (SD)* | 561.83          | 1.92                   | 2.37 |
| *Min file size*                 | 1710              | 9                      | 2 |
| *Max file size*                 | 3839              | 18                     | 456 |
