import polars as pl
import os
import uuid
import datetime
import random
import json
from collections import defaultdict

# Raw data to generate events from:
trips_path = "data/raw/yellow_tripdata_2024-01.parquet"


# Generate trip events from a row of completed trip table 
def generate_trip_events(row, driver_id):
    # Sample Data:
    # {'tpep_pickup_datetime': datetime.datetime(2024, 1, 1, 0, 57, 55), 'tpep_dropoff_datetime': datetime.datetime(2024, 1, 1, 1, 17, 43), 'passenger_count': 1, 'trip_distance': 1.72, 'PULocationID': 186, 'DOLocationID': 79, 'payment_type': 2, 'fare_amount': 17.7}

    start_time = row['tpep_pickup_datetime']
    end_time = row['tpep_dropoff_datetime']

    trip_id = str(uuid.uuid4())
    
    events = []

    # 1. Generate Trip Requested
    events.append({
        'event_id': str(uuid.uuid4()),
        'trip_id': trip_id,
        'event_type': "TRIP_REQUESTED",
        'timestamp': (start_time - datetime.timedelta(seconds=30)).isoformat(),
        'payload': {
            'pickup_location_id': row['PULocationID']
        }
    })

    # 2. Generate DRIVER_ASSIGNED
    events.append({
        'event_id': str(uuid.uuid4()),
        'trip_id': trip_id,
        'event_type': "DRIVER_ASSIGNED",
        'timestamp': (start_time - datetime.timedelta(seconds=15)).isoformat(),
        'payload': {
            'driver_id': driver_id,
            'pickup_location_id': row['PULocationID']
        }
    })

    # 3. Generate TRIP_STARTED
    events.append({
        'event_id': str(uuid.uuid4()),
        'trip_id': trip_id,
        'event_type': "TRIP_STARTED",
        'timestamp': start_time.isoformat(),
        'payload': {
            'pickup_location_id': row['PULocationID']
        }
    })

    # 4. Generate GPS Pings
    total_time = round((end_time - start_time).total_seconds())
    for i in range(0, total_time, 60):
        events.append({
            'event_id': str(uuid.uuid4()),
            'trip_id': trip_id,
            'event_type': "GPS_PING",
            'timestamp': (start_time + datetime.timedelta(seconds=i)).isoformat(),
            'payload': {
                'lat': 40.7589,  # Dummy data for now
                'lon': -73.9851  # Dummy data for now
            }
        })

    # 5. Generate TRIP_COMPLETED
    events.append({
        'event_id': str(uuid.uuid4()),
        'trip_id': trip_id,
        'event_type': "TRIP_COMPLETED",
        'timestamp': end_time.isoformat(),
        'payload': {
            'trip_distance': row['trip_distance'],
            'dropoff_location_id': row['DOLocationID']
        }
    })

    # 6. Generate PAYMENT_COMPLETED
    events.append({
        'event_id': str(uuid.uuid4()),
        'trip_id': trip_id,
        'event_type': "PAYMENT_COMPLETED",
        'timestamp': (end_time + datetime.timedelta(seconds=10)).isoformat(),
        'payload': {
            'total_amount': row['total_amount']
        }
    })
    # os.makedirs(folder_path, exist_ok=True)
    
    # with open(folder_path + "/events_001.jsonl", 'a') as f:
    #     for event in events:
    #         f.write(json.dumps(event) + '\n')
    return events


chunk_events = []

if __name__ == '__main__':
    if not os.path.exists(trips_path):
        print("❌ ERROR: Files not found.")
        exit()

    driver_pool = [str(uuid.uuid4()) for _ in range(5000)]
    chunk_size = 5000

    df_trips = pl.read_parquet(trips_path)
    
    # Process rows in batches/chunks
    total_rows = df_trips.height
    for start_row in range(0, 5000, chunk_size):
        batch = df_trips.slice(start_row, chunk_size)

        # Gather all events for a chunk
        for row in batch.iter_rows(named=True):
            events = generate_trip_events(row, random.choice(driver_pool))
            chunk_events.extend(events)


        grouped_events  = defaultdict(list)
        # In memory dict for all chunks
        for event in chunk_events:
            event_date = event['timestamp'][:10] # grabs "2024-01-01"
            folder_path = f"data/landing/ingest_year={event_date[:4]}/ingest_month={event_date[5:7]}/ingest_day={event_date[8:10]}"
            grouped_events[folder_path].append(event)

        for folder_path, event_list in grouped_events.items():
            os.makedirs(folder_path, exist_ok=True)

            file_path = folder_path + '/events_001.jsonl'

            with open(file_path, 'a') as f:
                for event in event_list:
                    f.write(json.dumps(event) + '\n')


        




