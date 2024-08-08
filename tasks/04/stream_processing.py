import numpy as np
import pandas as pd
import faust

from sklearn.cluster import MiniBatchKMeans, Birch, DBSCAN

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
    STREET_CODE1: str
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
# we set a fixed window size to simulate data coming in real time and having an application that process it at fixed intervals
WINDOW_SIZE = 100
TOP_STREETS = [10010, 10110, 10210, 10410, 10510, 10810, 13610, 24890, 25390, 59990]

@app.agent(topic)
async def rolling_stats(stream):
    async for values in stream.take(WINDOW_SIZE, within=10): # within specifies the time window to wait for more data
        df = pd.DataFrame(columns=["VEHICLE_MAKE", "VEHICLE_YEAR", "BOROUGH", "STREET_NAME"])
        for value in values:
            df.loc[len(df)] = [value.VEHICLE_MAKE, value.VEHICLE_YEAR, value.VIOLATION_COUNTY, value.STREET_NAME]
        df["VEHICLE_YEAR"] = pd.to_numeric(df["VEHICLE_YEAR"], errors='coerce')
        current_year = value.ISSUE_DATE.split("-")[0] # sent values from producer value["Issue Date"].strftime("%Y-%m-%d")
        
        # rolling statistics for all data
        vehicle_make = df["VEHICLE_MAKE"].value_counts().to_dict()
        temp = df[(df["VEHICLE_YEAR"] > 0) & (df["VEHICLE_YEAR"] < int(current_year))]
        await rolling_stats_all_topic.send(value={
            "vehicle_make": vehicle_make, 
            "vehicle_year_mean": str(temp["VEHICLE_YEAR"].mean()),
            "vehicle_year_std": str(temp["VEHICLE_YEAR"].std()),
            "vehicle_year_min": str(temp["VEHICLE_YEAR"].min()),
            "vehicle_year_max": str(temp["VEHICLE_YEAR"].max()),
            "vehicle_year_per_25": str(temp["VEHICLE_YEAR"].quantile(0.25)),
            "vehicle_year_per_50": str(temp["VEHICLE_YEAR"].quantile(0.50)),
            "vehicle_year_per_75": str(temp["VEHICLE_YEAR"].quantile(0.75))
        })
        
        # rolling statistics for boroughs
        boroughs = df.groupby("BOROUGH")
        for group_name, group_df in boroughs:
            vehicle_make_boroughs = group_df["VEHICLE_MAKE"].value_counts().to_dict()
            temp = group_df[(group_df["VEHICLE_YEAR"] > 0) & (group_df["VEHICLE_YEAR"] < int(current_year))]
            await rolling_stats_boroughs_topic.send(value={
                "borough": group_name,
                "vehicle_make": vehicle_make_boroughs, 
                "vehicle_year_mean": str(temp["VEHICLE_YEAR"].mean()),
                "vehicle_year_std": str(temp["VEHICLE_YEAR"].std()),
                "vehicle_year_min": str(temp["VEHICLE_YEAR"].min()),
                "vehicle_year_max": str(temp["VEHICLE_YEAR"].max()),
                "vehicle_year_per_25": str(temp["VEHICLE_YEAR"].quantile(0.25)),
                "vehicle_year_per_50": str(temp["VEHICLE_YEAR"].quantile(0.50)),
                "vehicle_year_per_75": str(temp["VEHICLE_YEAR"].quantile(0.75))
            })
            
        
        # rolling statistics for streets
        streets = df[df["STREET_CODE1"].isin(TOP_STREETS)]
        streets = streets.groupby("STREET_CODE1")
        for group_name, group_df in streets:
            vehicle_make_streets = group_df["VEHICLE_MAKE"].value_counts().to_dict()
            temp = group_df[(group_df["VEHICLE_YEAR"] > 0) & (group_df["VEHICLE_YEAR"] < int(current_year))]
            await rolling_stats_streets_topic.send(value={
                "street_code1": group_name,
                "vehicle_make": vehicle_make_streets, 
                "vehicle_year_mean": str(temp["VEHICLE_YEAR"].mean()),
                "vehicle_year_std": str(temp["VEHICLE_YEAR"].std()),
                "vehicle_year_min": str(temp["VEHICLE_YEAR"].min()),
                "vehicle_year_max": str(temp["VEHICLE_YEAR"].max()),
                "vehicle_year_per_25": str(temp["VEHICLE_YEAR"].quantile(0.25)),
                "vehicle_year_per_50": str(temp["VEHICLE_YEAR"].quantile(0.50)),
                "vehicle_year_per_75": str(temp["VEHICLE_YEAR"].quantile(0.75))
            })



##### spatial stream clustering
kmeans_topic = app.topic('kmeans', value_serializer='json', internal=True, partitions=1)
birch_topic = app.topic('birch', value_serializer='json', internal=True, partitions=1)
WINDOW_SIZE = 1024
N_CLUSTERS = 8

@app.agent(topic)
async def rolling_stats(stream):
    kmeans = MiniBatchKMeans(n_clusters=N_CLUSTERS, batch_size=WINDOW_SIZE, random_state=42)
    # birch = Birch(n_clusters=N_CLUSTERS)
    async for values in stream.take(WINDOW_SIZE, within=10): # within specifies the time window to wait for more data
        X = np.array([[value.LATITUDE, value.LONGITUDE] for value in values])
        
        kmeans.partial_fit(X)
        
        centroids = kmeans.cluster_centers_
        labels = kmeans.labels_
        await kmeans_topic.send(value={
            "centroids": centroids.tolist(),
            "labels": labels.tolist()
        })


if __name__ == '__main__':
    app.main()

