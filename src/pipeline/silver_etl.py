import glob
import polars as pl
import os

files = glob.glob('data/bronze/**/*.parquet', recursive=True)

for file in files:
    df  = pl.read_parquet(file)
    
    # Deduplicate and extract event_date
    df = df.unique(subset=['event_id'], keep='first')
    df = df.with_columns(
        pl.col("timestamp").dt.date().alias("event_date")
    )
    df = df.sort('timestamp')
    
    # Extract lineage information for the filename
    original_filename = os.path.basename(file)
    path_parts = file.split('/')
    ingest_date_part = [p for p in path_parts if p.startswith('ingest_date=')][0]
    bronze_ingest_date = ingest_date_part.split('=')[1] 
    silver_filename = f"ingest_{bronze_ingest_date}_{original_filename}"
    
    # Group by event_date to handle mixed dates in a single Bronze file
    grouped_events = df.partition_by('event_date')
    
    # Iterate through the split DataFrames
    for group_df in grouped_events:
        # Extract the date string from the first row of THIS SPECIFIC GROUP
        event_date_str = group_df['event_date'][0].strftime("%Y-%m-%d")
        
        # Build the path
        folder_path = f"data/silver/event_date={event_date_str}"
        os.makedirs(folder_path, exist_ok=True)
        
        silver_file_path = f"{folder_path}/{silver_filename}"
        
        # Write only the grouped rows to their correct partition
        group_df.write_parquet(silver_file_path)