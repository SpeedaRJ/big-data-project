import pandas as pd
import faust

BROKER = 'kafka://localhost:29092'
TOPIC = "raw-data"

class RawData(faust.Record, serializer='json'):
    SUMMONS_NUMBER: str
    PLATE_ID: str
    # REGISTRATION_STATE: str
    # PLATE_TYPE: str
    ISSUE_DATE: str
    # VIOLATION_CODE: str
    # VEHICLE_BODY_TYPE: str
    VEHICLE_MAKE: str
    # ISSUING_AGENCY: str
    # STREET_CODE1: str
    # STREET_CODE2: str
    # STREET_CODE3: str
    # VEHICLE_EXPIRATION_DATE: str
    # VIOLATION_LOCATION: str
    # VIOLATION_PRECINCT: str
    # ISSUER_PRECINCT: str
    # ISSUER_CODE: str
    # ISSUER_COMMAND: str
    # ISSUER_SQUAD: str
    # VIOLATION_TIME: str
    VIOLATION_COUNTY: str
    # VIOLATION_IN_FRONT_OF_OR_OPPOSITE: str
    # HOUSE_NUMBER: str
    STREET_NAME: str
    # INTERSECTING_STREET: str
    # DATE_FIRST_OBSERVED: str
    # LAW_SECTION: str
    # SUB_DIVISION: str
    # VIOLATION_LEGAL_CODE: str
    # DAYS_PARKING_IN_EFFECT: str
    # FROM_HOURS_IN_EFFECT: str
    # TO_HOURS_IN_EFFECT: str
    # VEHICLE_COLOR: str
    VEHICLE_YEAR: str
    # FEET_FROM_CURB: str
    # VIOLATION_POST_CODE: str
    # VIOLATION_DESCRIPTION: str
    LATITUDE: str
    LONGITUDE: str


# define the application and the topic to consume from
app = faust.App('faust-stream', broker=BROKER)
topic = app.topic(TOPIC, value_type=RawData)


# # test if we can consume the data
# test_topic = app.topic('test', value_serializer='json', internal=True, partitions=1)
# @app.agent(topic)
# async def process_test(stream):
#     async for raw_data in stream:
#         print(raw_data)


# topics that will send the rolling statistics for all of the data, boroughs and for most interesting streets
rolling_stats_all_topic = app.topic('rolling_stats_all', value_serializer='json', internal=True, partitions=1)
rolling_stats_boroughs_topic = app.topic('rolling_stats_boroughs', value_serializer='json', internal=True, partitions=1)
rolling_stats_streets_topic = app.topic('rolling_stats_streets', value_serializer='json', internal=True, partitions=1)
# columns we are interested in
COLUMNS = ["VEHICLE_MAKE", "VEHICLE_YEAR"]
# we set a fixed window size to simulate data coming in real time and having an application that process it at fixed intervals
WINDOW_SIZE = 100

@app.agent(topic)
async def rolling_stats(stream):
    async for values in stream.take(WINDOW_SIZE, within=10): # within specifies the time window to wait for more data
        df = pd.DataFrame(columns=["VEHICLE_MAKE", "VEHICLE_YEAR", "BOROUGH", "STREET_NAME"])
        for value in values:
            df.loc[len(df)] = [value.VEHICLE_MAKE, value.VEHICLE_YEAR, value.VIOLATION_COUNTY, value.STREET_NAME]
        df["VEHICLE_YEAR"] = pd.to_numeric(df["VEHICLE_YEAR"], errors='coerce')
        current_year = value.ISSUE_DATE.split("-")[0] # sent values from producer value["Issue Date"].strftime("%Y-%m-%d")
        valid_years_mask = (df["VEHICLE_YEAR"] > 0) & (df["VEHICLE_YEAR"] < int(current_year))
        
        # rolling statistics for all data
        vehicle_make = df["VEHICLE_MAKE"].value_counts().to_dict()
        temp = df[valid_years_mask]
        vehicle_year_mean = temp["VEHICLE_YEAR"].mean()
        vehicle_year_std = temp["VEHICLE_YEAR"].std()
        vehicle_year_min = temp["VEHICLE_YEAR"].min()
        vehicle_year_max = temp["VEHICLE_YEAR"].max()
        vehicle_year_per_25 = temp["VEHICLE_YEAR"].quantile(0.25)
        vehicle_year_per_50 = temp["VEHICLE_YEAR"].quantile(0.50)
        vehicle_year_per_75 = temp["VEHICLE_YEAR"].quantile(0.75)
        print(vehicle_make, vehicle_year_mean, vehicle_year_std, vehicle_year_min, vehicle_year_max, vehicle_year_per_25, vehicle_year_per_50, vehicle_year_per_75)
        
        # rolling statistics for boroughs
        # boroughs = df.groupby("BOROUGH")
        # vehicle_make_boroughs = boroughs["VEHICLE_MAKE"].value_counts().to_dict()
        # vehicle_year_mean_boroughs = boroughs["VEHICLE_YEAR"].mean().to_dict()  
        # vehicle_year_std_boroughs = boroughs["VEHICLE_YEAR"].std().to_dict()
        # vehicle_year_min_boroughs = boroughs["VEHICLE_YEAR"].min().to_dict()
        # vehicle_year_max_boroughs = boroughs["VEHICLE_YEAR"].max().to_dict()
        # vehicle_year_per_25_boroughs = boroughs["VEHICLE_YEAR"].quantile(0.25).to_dict()
        # vehicle_year_per_50_boroughs = boroughs["VEHICLE_YEAR"].quantile(0.50).to_dict()
        # vehicle_year_per_75_boroughs = boroughs["VEHICLE_YEAR"].quantile(0.75).to_dict()
        # print(vehicle_make_boroughs, vehicle_year_mean_boroughs, vehicle_year_std_boroughs, vehicle_year_min_boroughs, vehicle_year_max_boroughs, vehicle_year_per_25_boroughs, vehicle_year_per_50_boroughs, vehicle_year_per_75_boroughs)
        
        # # rolling statistics for streets
        # streets = df.groupby("STREET_NAME")
        # vehicle_make_streets = streets["VEHICLE_MAKE"].value_counts().to_dict()
        # vehicle_year_mean_streets = streets["VEHICLE_YEAR"].mean().to_dict()
        # vehicle_year_std_streets = streets["VEHICLE_YEAR"].std().to_dict()
        # vehicle_year_min_streets = streets["VEHICLE_YEAR"].min().to_dict()
        # vehicle_year_max_streets = streets["VEHICLE_YEAR"].max().to_dict()
        # vehicle_year_per_25_streets = streets["VEHICLE_YEAR"].quantile(0.25).to_dict()
        # vehicle_year_per_50_streets = streets["VEHICLE_YEAR"].quantile(0.50).to_dict()
        # vehicle_year_per_75_streets = streets["VEHICLE_YEAR"].quantile(0.75).to_dict()
        # print(vehicle_make_streets, vehicle_year_mean_streets, vehicle_year_std_streets, vehicle_year_min_streets, vehicle_year_max_streets, vehicle_year_per_25_streets, vehicle_year_per_50_streets, vehicle_year_per_75_streets)


if __name__ == '__main__':
    app.main()

