import duckdb
import os

def build_gold_layer():
    print("--- Building Gold Layer (Materializing to Disk) ---")
    
    # Create the gold directory if it doesn't exist
    os.makedirs("data/gold", exist_ok=True)
    
    con = duckdb.connect()

    # Daily Aggregates Table
    print("Writing agg_daily_metrics.parquet...")
    con.execute("""
        COPY (
            SELECT 
                event_date,
                COUNT(CASE WHEN event_type = 'TRIP_COMPLETED' THEN 1 END) AS total_trips,
                SUM(CASE WHEN event_type = 'PAYMENT_COMPLETED' THEN CAST(payload->>'total_amount' AS DOUBLE) ELSE 0 END) AS total_revenue
            FROM read_parquet('data/silver/**/*.parquet')
            GROUP BY event_date
            ORDER BY event_date
        ) TO 'data/gold/agg_daily_metrics.parquet' (FORMAT PARQUET);
    """)

    # Fact Trips Table (Kimball Star Schema)
    print("Writing fact_trips.parquet...")
    con.execute("""
        COPY (
            SELECT 
                trip_id,
                MAX(CASE WHEN event_type = 'TRIP_REQUESTED' THEN timestamp END) AS request_time,
                MAX(CASE WHEN event_type = 'TRIP_STARTED' THEN timestamp END) AS start_time,
                EXTRACT(HOUR FROM MAX(CASE WHEN event_type = 'TRIP_STARTED' THEN timestamp END)) AS start_hour,
                MAX(CASE WHEN event_type = 'TRIP_STARTED' THEN CAST(payload->>'pickup_location_id' AS INT) END) AS pickup_location_id,
                MAX(CASE WHEN event_type = 'TRIP_COMPLETED' THEN CAST(payload->>'dropoff_location_id' AS INT) END) AS dropoff_location_id,
                MAX(CASE WHEN event_type = 'TRIP_COMPLETED' THEN CAST(payload->>'trip_distance' AS DOUBLE) END) AS trip_distance,
                MAX(CASE WHEN event_type = 'PAYMENT_COMPLETED' THEN CAST(payload->>'total_amount' AS DOUBLE) END) AS total_amount
            FROM read_parquet('data/silver/**/*.parquet')
            WHERE event_type IN ('TRIP_REQUESTED', 'TRIP_STARTED', 'TRIP_COMPLETED', 'PAYMENT_COMPLETED')
            GROUP BY trip_id
        ) TO 'data/gold/fact_trips.parquet' (FORMAT PARQUET);
    """)
    
    print("Gold Layer materialized successfully.")

if __name__ == "__main__":
    build_gold_layer()