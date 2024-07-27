#!/bin/bash

if [ ! -d ../additional_data  ]; then
    mkdir ../additional_data
    echo "Created ../additional_data as base directory."
fi

if [ ! -f ../additional_data/events/events_NYC_2014_2024.csv ]; then
    mkdir ../additional_data/events
    echo "Created ../additional_data/events. Proceeding to download files."

    curl https://data.cityofnewyork.us/api/views/bkfu-528j/rows.csv -o ../additional_data/events/events_NYC_2014_2024.csv
fi

if [ ! -f ../additional_data/businesses/businesses_NYC_2023.csv ]; then
    mkdir ../additional_data/businesses
    echo "Created ../additional_data/businesses. Proceeding to download files."

    curl https://data.cityofnewyork.us/api/views/w7w3-xahh/rows.csv -o ../additional_data/businesses/businesses_NYC_2023.csv
fi

if [ ! -f ../additional_data/schoolshigh_schools_NYC_2021.csv ]; then
    mkdir ../additional_data/schools
    echo "Created ../additional_data/schools. Proceeding to download files."

    curl https://data.cityofnewyork.us/api/views/8b6c-7uty/rows.csv -o ../additional_data/schools/high_schools_NYC_2021.csv
    curl https://data.cityofnewyork.us/api/views/f6s7-vytj/rows.csv -o ../additional_data/schools/middle_schools_NYC_2021.csv
fi