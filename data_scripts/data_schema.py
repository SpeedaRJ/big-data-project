import csv

import numpy as np
import pandas as pd


class DataSchema:
    def __init__(self, year):
        self.dates = ["Issue Date"]

        self.summons_number = pd.Int64Dtype()
        self.plate_id = pd.StringDtype()
        self.registration_state = pd.StringDtype()
        self.plate_type = pd.StringDtype()
        if year in [2000]:
            self.issue_date = pd.Int64Dtype()
            self.dates = []
        self.violation_code = pd.Int64Dtype()
        self.vehicle_body_type = pd.StringDtype()
        self.vehicle_make = pd.StringDtype()
        self.issuing_agency = pd.StringDtype()
        self.street_code1 = pd.Int64Dtype()
        self.street_code2 = pd.Int64Dtype()
        self.street_code3 = pd.Int64Dtype()
        if year not in [2015]:
            self.vehicle_expiration_date = pd.Int64Dtype()
        else:
            self.vehicle_expiration_date = pd.StringDtype()
        if year in [2024, 2017, 2016, 2015, 2014]:
            self.violation_location = pd.StringDtype()
        elif year in [2000, 2023, 2022, 2021, 2020, 2019, 2018]:
            self.violation_location = pd.Int64Dtype()
        self.violation_precinct = pd.Int64Dtype()
        self.issuer_precinct = pd.Int64Dtype()
        self.issuer_code = pd.Int64Dtype()
        self.issuer_command = pd.StringDtype()
        self.issuer_squad = pd.StringDtype()
        self.violation_time = pd.StringDtype()
        self.time_first_observed = pd.StringDtype()
        self.violation_county = pd.StringDtype()
        self.violation_in_front_of_or_opposite = pd.StringDtype()
        if year == 2014:
            self.number = pd.StringDtype()
            self.street = pd.StringDtype()
        else:
            self.house_number = pd.StringDtype()
            self.street_name = pd.StringDtype()
        self.intersecting_street = pd.StringDtype()
        if year not in [2015]:
            self.date_first_observed = pd.Int64Dtype()
        else:
            self.date_first_observed = pd.StringDtype()
        self.law_section = pd.Int64Dtype()
        self.sub_division = pd.StringDtype()
        self.violation_legal_code = pd.StringDtype()
        self.days_parking_in_effect = pd.StringDtype()
        self.from_hours_in_effect = pd.StringDtype()
        self.to_hours_in_effect = pd.StringDtype()
        self.vehicle_color = pd.StringDtype()
        if year in [2000, 2024, 2017, 2016, 2015, 2014]:
            self.unregistered_vehicle = pd.StringDtype()
        else:
            self.unregistered_vehicle = pd.Int64Dtype()
        if year in [2000, 2024, 2022, 2021, 2020, 2019, 2018, 2017]:
            self.vehicle_year = pd.Int64Dtype()
        else:
            self.vehicle_year = pd.StringDtype()
        self.meter_number = pd.StringDtype()
        self.feet_from_curb = pd.Int64Dtype()
        self.violation_post_code = pd.StringDtype()
        self.violation_description = pd.StringDtype()
        self.no_standing_or_stopping_violation = pd.StringDtype()
        self.hydrant_violation = pd.StringDtype()
        self.double_parking_violation = pd.StringDtype()

    def get_schema_dict(self):
        """
        Generates a dictionary representation of the object's attributes, excluding the 'dates' attribute.

        Returns:
            dict: A dictionary containing the object's attributes and their values, excluding the 'dates' attribute.
        """
        return {key: value for key, value in self.__dict__.items() if key != "dates"}

    def get_dates(self):
        """
        Retrieves the 'dates' attribute of the object.

        Returns:
            list: The 'dates' attribute, which is expected to be a list of date values.
        """
        return self.dates

    def get_schema(self, filename):
        """
        Reads a CSV file and maps its field names to the corresponding schema attributes.

        Args:
            filename (str): The path to the CSV file to read.

        Returns:
            dict: A dictionary where the keys are the field names from the CSV file and the values are the corresponding schema attributes.

        This function performs the following steps:
        1. Reads the schema dictionary using `self.get_schema_dict()`.
        2. Reads the CSV file and extracts its field names.
        3. Maps each field name to the corresponding schema attribute, if it exists in the schema dictionary.
        """
        def parse_name(name):
            return name.lower().strip().replace(" ", "_").replace("?", "")

        schema = self.get_schema_dict()
        with open(filename, "r") as infile:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames
            return {
                field: schema[parse_name(field)]
                for field in fieldnames
                if parse_name(field) in schema
            }

    @staticmethod
    def fill_na(data):
        """
        Fills missing values in the DataFrame with specified default values based on column data types.

        Args:
            data (pd.DataFrame): The DataFrame to fill missing values in.

        Returns:
            pd.DataFrame: The DataFrame with missing values filled.

        This function performs the following steps:
        1. Iterates over each column in the DataFrame.
        2. Checks the data type of the column.
        3. Fills missing values with -1 for columns with integer data type.
        4. Fills missing values with an empty string for columns with other data types.
        """
        for col in data:
            dt = data[col].dtype
            if dt == pd.Int64Dtype():
                data.fillna({col: -1}, inplace=True)
            else:
                data.fillna({col: ""}, inplace=True)
        return data

    @staticmethod
    def to_primitive_dtypes(data, year):
        """
        Converts the data types of the DataFrame columns to primitive types and processes date columns.

        Args:
            data (pd.DataFrame): The DataFrame whose columns' data types need to be converted.
            year (int): The fiscal year used to retrieve date columns from the DataSchema.

        Returns:
            pd.DataFrame: The DataFrame with columns converted to primitive data types and date columns processed.

        This function performs the following steps:
        1. Converts columns with integer data type to `np.int64`.
        2. Converts columns with string data type to `str`.
        3. Converts date columns to Unix timestamp in milliseconds.

        Note:
            The date columns are retrieved using the `get_dates` method of the `DataSchema` class.
        """
        d = dict.fromkeys(data.select_dtypes(pd.Int64Dtype()).columns, np.int64)
        data = data.astype(d)
        d = dict.fromkeys(data.select_dtypes(pd.StringDtype()).columns, str)
        data = data.astype(d)
        for date_column in DataSchema(year).get_dates():
            data[date_column] = (
                pd.to_datetime(data[date_column]).astype(np.int64) // 10**6
            )
        return data
