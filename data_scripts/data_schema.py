import numpy as np
import pandas as pd


class DataSchema:
    schema = {
        "Summons Number": pd.Int64Dtype(),
        "Plate ID": pd.StringDtype(),
        "Registration State": pd.StringDtype(),
        "Plate Type": pd.StringDtype(),
        "Violation Code": pd.Int64Dtype(),
        "Vehicle Body Type": pd.StringDtype(),
        "Vehicle Make": pd.StringDtype(),
        "Issuing Agency": pd.StringDtype(),
        "Street Code1": pd.Int64Dtype(),
        "Street Code2": pd.Int64Dtype(),
        "Street Code3": pd.Int64Dtype(),
        "Vehicle Expiration Date": pd.Int64Dtype(),
        "Violation Location": pd.Int64Dtype(),
        "Violation Precinct": pd.Int64Dtype(),
        "Issuer Precinct": pd.Int64Dtype(),
        "Issuer Code": pd.Int64Dtype(),
        "Issuer Command": pd.StringDtype(),
        "Issuer Squad": pd.StringDtype(),
        "Violation Time": pd.StringDtype(),
        "Time First Observed": pd.StringDtype(),
        "Violation County": pd.StringDtype(),
        "Violation In Front Of Or Opposite": pd.StringDtype(),
        "Number": pd.StringDtype(),
        "Street": pd.StringDtype(),
        "Intersecting Street": pd.StringDtype(),
        "Date First Observed": pd.Int64Dtype(),
        "Law Section": pd.Int64Dtype(),
        "Sub Division": pd.StringDtype(),
        "Violation Legal Code": pd.StringDtype(),
        "Days Parking In Effect": pd.StringDtype(),
        "From Hours In Effect": pd.StringDtype(),
        "To Hours In Effect": pd.StringDtype(),
        "Vehicle Color": pd.StringDtype(),
        "Unregistered Vehicle?": pd.Int64Dtype(),
        "Vehicle Year": pd.Int64Dtype(),
        "Meter Number": pd.StringDtype(),
        "Feet From Curb": pd.Int64Dtype(),
        "Violation Post Code": pd.StringDtype(),
        "Violation Description": pd.StringDtype(),
        "No Standing or Stopping Violation": pd.Int64Dtype(),
        "Hydrant Violation": pd.Int64Dtype(),
        "Double Parking Violation": pd.Int64Dtype(),
    }
    dates = ["Issue Date"]

    @staticmethod
    def fill_na(data):
        for col in data:
            dt = data[col].dtype
            if dt == pd.Int64Dtype():
                data.fillna({col: -1}, inplace=True)
            else:
                data.fillna({col: ""}, inplace=True)
        return data

    @staticmethod
    def to_primitive_dtypes(data):
        d = dict.fromkeys(data.select_dtypes(pd.Int64Dtype()).columns, np.int64)
        data = data.astype(d)
        d = dict.fromkeys(data.select_dtypes(pd.StringDtype()).columns, str)
        data = data.astype(d)
        for date_column in DataSchema.dates:
            data[date_column] = pd.to_datetime(data[date_column]).astype(np.int64) // 10**6
        return data
