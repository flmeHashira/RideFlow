import glob
import polars as pl
import datetime, os

files = glob.glob('data/bronze/**/*.parquet', recursive=True)

for file in files:
    df  = pl.read_parquet(file)
    df = df.unique(subset=['event_id'], keep='first')
    df = df.with_columns(
        pl.col("timestamp").str.slice(0, 10).str.strptime(pl.Date, format="%Y-%m-%d").alias("event_date")
    )
    df = df.sort('timestamp')
    # Extract the date as a string
    event_date_str = df['event_date'][0].strftime("%Y-%m-%d")
    
    # Build the path
    folder_path = f"data/silver/event_date={event_date_str}"
    os.makedirs(folder_path, exist_ok=True)
    
    # Get the original file name to keep it unique
    original_filename = os.path.basename(file) 
    silver_file_path = f"{folder_path}/{original_filename}"
    
    df.write_parquet(silver_file_path)

