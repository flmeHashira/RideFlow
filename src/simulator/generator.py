import polars as pl
import os
import uuid
import datetime

trips_path = "data/raw/yellow_tripdata_2024-01.parquet"

def generate_trip_events(row):
    trip_id = str(uuid.uuid4())
    start_time = row['tpep_pickup_datetime']
    end_time = row['tpep_dropoff_datetime']
    
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
            'driver_id': str(uuid.uuid4()),
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

    return events

if __name__ == '__main__':
    if not os.path.exists(trips_path):
        print("❌ ERROR: Files not found.")
        exit()
        
    df_trips = pl.read_parquet(trips_path)
    
    # Process exactly one row to test
    row = df_trips.row(0, named=True)
    events = generate_trip_events(row)
    
    for e in events:
        print(e)