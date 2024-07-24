# big-data-project
Repository aimed at holding the code, report, and results of the Big Data course project.

> TODO: Change text in Running notes, it is no longer relevant.
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

Below are displayed the file sizes after format conversion. Before any data has been cleaned, omitted, processed or removed. The only preprocessing step at this point was filling `None` values with either `0` or `''` based on the column data type. The `Issue Date` column, was also converted to the `np.int64` data type in the form of a UNIX timestamp, instead of the default `datetime` type.

| **File Name** | **CSV Size [MD]** | **Parquet Size [MB]** | **HDF5 Size [MD]** |
| ------------- | ----------------- | --------------------- | ------------------ |
| *2014.csv*    |          1710.72  |        330.06         |        184.14      | 
| *2015.csv*    |      2393.49      |         511.80        |        318.64      |
| *2016.csv*    |    1971.17        |          352.62       |        192.56      |
| *2017.csv*    |        1990.24    |        519.51         |       351.12       |
| *2018.csv*    |    2073.71        |        401.29         |      232.82        |
| *2019.csv*    |       1910.91     |        364.27         |       211.75       |
| *2020.csv*    |     2214.43       |       382.20          |        216.46      |
| *2021.csv*    |       2618.82     |         445.62        |      252.72        |
| *2022.csv*    |        2645.07    |         418.49        |       218.41       |
| *2023.csv*    |        3839.23    |         617.47        |        310.59      |
| *2024.csv*    |      2645.07      |          418.49       |      216.27        |

File conversions were performed locally, and file sizes were obtained using the following command:
```pwsh
ls | Select-Object Name, @{Name="MegaBytes";Expression={$_.Length / 1MB}}
```
