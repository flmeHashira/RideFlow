import duckdb
import pandas as pd
import time
import os
import glob

def get_directory_size(directory):
    total_size = 0
    # Recursively find all parquet files and sum their sizes in bytes
    for filepath in glob.glob(f'{directory}/**/*.parquet', recursive=True):
        total_size += os.path.getsize(filepath)
    # Convert bytes to Megabytes (MB)
    return total_size / (1024 * 1024)

def build_gold_metrics():
    print("--- Building Gold Layer Analytics ---\n")
    
    # 1. Calculate Data Size
    silver_size_mb = get_directory_size("data/silver")
    print(f"📊 Scanning Silver Layer: {silver_size_mb:.2f} MB of Parquet files")
    
    # 2. Start Timer
    start_time = time.perf_counter()
    
    # 3. Connect to DuckDB
    con = duckdb.connect()
    
    # 4. The SQL Query
    sql_query = """
        SELECT 
            event_date,
            COUNT(CASE WHEN event_type = 'TRIP_COMPLETED' THEN 1 END) AS total_trips,
            SUM(CASE WHEN event_type = 'PAYMENT_COMPLETED' THEN CAST(payload->>'total_amount' AS DOUBLE) ELSE 0 END) AS total_revenue
        FROM read_parquet('data/silver/**/*.parquet')
        GROUP BY event_date
        ORDER BY event_date
    """
    
    # 5. Execute Query
    try:
        result_df = con.execute(sql_query).fetch_df()
        
        # 6. Stop Timer
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        
        # 7. Print Results + Metrics
        print("\n✅ Daily Trips and Revenue (Gold Layer):")
        print(result_df.to_string(index=False))
        
        print(f"\n⏱️ Query Execution Time: {execution_time:.4f} seconds")
        print(f"⚡ Throughput: {silver_size_mb / execution_time:.2f} MB/sec processed")
        
    except Exception as e:
        print(f"❌ Error executing query: {e}")

if __name__ == "__main__":
    build_gold_metrics()