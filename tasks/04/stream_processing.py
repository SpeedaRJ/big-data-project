import pandas as pd
import faust

BROKER = 'kafka://localhost:29092'
TOPIC = "raw-data"

class RawData(faust.Record, serializer='json'):
    SUMMONS_NUMBER: str
    PLATE_ID: str
    REGISTRATION_STATE: str
    PLATE_TYPE: str
    ISSUE_DATE: str
    VIOLATION_CODE: str
    VEHICLE_BODY_TYPE: str
    VEHICLE_MAKE: str
    ISSUING_AGENCY: str
    STREET_CODE1: str
    STREET_CODE2: str
    STREET_CODE3: str
    VEHICLE_EXPIRATION_DATE: str
    VIOLATION_LOCATION: str
    VIOLATION_PRECINCT: str
    ISSUER_PRECINCT: str
    ISSUER_CODE: str
    ISSUER_COMMAND: str
    ISSUER_SQUAD: str
    VIOLATION_TIME: str
    VIOLATION_COUNTY: str
    VIOLATION_IN_FRONT_OF_OR_OPPOSITE: str
    HOUSE_NUMBER: str
    STREET_NAME: str
    INTERSECTING_STREET: str
    DATE_FIRST_OBSERVED: str
    LAW_SECTION: str
    SUB_DIVISION: str
    VIOLATION_LEGAL_CODE: str
    DAYS_PARKING_IN_EFFECT: str
    FROM_HOURS_IN_EFFECT: str
    TO_HOURS_IN_EFFECT: str
    VEHICLE_COLOR: str
    VEHICLE_YEAR: str
    FEET_FROM_CURB: str
    VIOLATION_POST_CODE: str
    VIOLATION_DESCRIPTION: str
    LATITUDE: str
    LONGITUDE: str
    DATETIME: str
    TEMPMAX: str
    TEMPMIN: str
    TEMP: str
    CONDITIONS: str
    HUMIDITY: str
    WINDSPEED: str
    VISIBILITY: str


# define the application and the topic to consume from
app = faust.App('faust-stream', broker=BROKER)
topic = app.topic(TOPIC, value_type=RawData)


# test if we can consume the data
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

@app.agent(topic)
async def rolling_stats(stream):
    async for values in stream.take(WINDOW_SIZE, within=10): # within specifies the time window to wait for more data
        rolling_stats = pd.DataFrame(columns=["TEMP", "HUMIDITY", "WINDSPEED", "VISIBILITY", "BOROUGH", "STREET"])
        # go over the 100 values and calculate the stats
        for value in values:
            rolling_stats.loc[len(rolling_stats)] = [value.TEMP, value.HUMIDITY, value.WINDSPEED, value.VISIBILITY, value.VIOLATION_COUNTY, value.STREET_NAME]
        
        print(rolling_stats)
        # for col in ["TEMP", "HUMIDITY", "WINDSPEED", "VISIBILITY"]:
        # all data
        
        # boroughs
        
        # streets
        


if __name__ == '__main__':
    app.main()

