import faust

BROKER = 'kafka://localhost:29092'


class RawData(faust.Record, serializer='json'):
    summons_number: str
    plate_id: str
    registration_state: str
    plate_type: str
    issue_date: str
    violation_code: str
    vehicle_body_type: str
    vehicle_make: str
    issuing_agency: str
    street_code1: str
    street_code2: str
    street_code3: str
    vehicle_expiration_date: str
    violation_location: str
    violation_precinct: str
    issuer_precinct: str
    issuer_code: str
    issuer_command: str
    issuer_squad: str
    violation_time: str
    time_first_observed: str
    violation_county: str
    violation_in_front_of_or_opposite: str
    house_number: str
    street_name: str
    intersecting_street: str
    date_first_observed: str
    law_section: str
    sub_division: str
    violation_legal_code: str
    days_parking_in_effect: str
    from_hours_in_effect: str
    to_hours_in_effect: str
    vehicle_color: str
    unregistered_vehicle: str
    vehicle_year: str
    meter_number: str
    feet_from_curb: str
    violation_post_code: str
    violation_description: str
    no_standing_or_stopping_violation: str
    hydrant_violation: str
    double_parking_violation: str


# define the application and the topic to consume from
app = faust.App('faust-stream', broker=BROKER)
topic = app.topic('raw-data', value_type=RawData)


# test if we can consume the data
test_topic = app.topic('test', value_serializer='json', internal=True, partitions=1)
@app.agent(topic)
async def process_test(stream):
    async for raw_data in stream:
        # print(raw_data)
        pass


# non overlapping windows
freq_no_overlap_topic = app.topic('registration-state-freq-no-overlap', value_serializer='json', internal=True, partitions=1)
freq_window_size_no_overlap = 100

@app.agent(topic)
async def process_registration_state_freq(stream):
    async for values in stream.take(freq_window_size_no_overlap, within=10):
        registration_state_freq = {}
        for value in values:
            registration_state = value.registration_state
            if registration_state in registration_state_freq:
                registration_state_freq[registration_state] += 1
            else:
                registration_state_freq[registration_state] = 1
        # print(registration_state_freq)
        await freq_no_overlap_topic.send(value={f"registration_state_freq_no_overlap_{freq_window_size_no_overlap}": registration_state_freq})


# overlapping windows
freq_overlap_topic = app.topic('registration-state-freq-overlap', value_serializer='json', internal=True, partitions=1)
freq_window_size_overlap = 100

@app.agent(topic)
async def process_registration_state_freq_overlapping(stream):
    registration_states = []
    async for value in stream:
        # once the window is full calculate the frequency of each registration state and pop the first (oldest) element
        if len(registration_states) == freq_window_size_overlap:
            registration_state_freq = {}
            for registration_state in registration_states:
                if registration_state in registration_state_freq:
                    registration_state_freq[registration_state] += 1
                else:
                    registration_state_freq[registration_state] = 1
            
            # print(registration_state_freq)
            await freq_overlap_topic.send(value={f"registration_state_freq_overlap_{freq_window_size_overlap}": registration_state_freq})
            
            # remove the oldest element
            registration_states.pop(0)
            
        # add the new element to the window
        registration_states.append(value.registration_state)



if __name__ == '__main__':
    app.main()

