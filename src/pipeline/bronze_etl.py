import glob
import polars as pl
import os

files  = glob.glob('data/queue/**/*.jsonl', recursive=True)

for file in files:
    events = pl.read_ndjson(file, schema_overrides={'payload': pl.Utf8})


    # Parse the timestamp and generate a pseudo-random roll using the event_id hash
    # No native .random() function in polars!
    events = events.with_columns([
        pl.col('timestamp').str.to_datetime().alias('timestamp'),
        # Hash the string, mod 100000, divide by 100000 -> float between 0.0 and 0.99999
        ((pl.col('event_id').hash(seed=42) % 100000) / 100000).alias('roll')                          
    ])

    events = events.with_columns(
        pl.when(pl.col('roll') < 0.95)
        .then(pl.col('roll') * 60)  #0-60 seconds
        .when(pl.col('roll') < 0.99)
        .then(3600 + (pl.col("roll") * 18000))  # 1-6 hours
        .otherwise(43200 + (pl.col("roll") * 129600))  # 12-48 hours
        .alias("delay_seconds")
    )

    events = events.with_columns(
        (pl.col('timestamp') + pl.duration(seconds=(pl.col('delay_seconds')))).alias('ingest_timestamp')
    )

    events  = events.with_columns(
        pl.col('ingest_timestamp').dt.strftime("%Y-%m-%d").alias("ingest_date")
    )

    # Inject Duplicates (1% of rows)
    duplicates = events.filter((pl.col('event_id').hash(seed=99) % 100) == 0)
    events = pl.concat([events, duplicates], how="vertical_relaxed")

    # Inject Out-of-Order delivery (Network Shuffle)
    events = events.sample(fraction=1, shuffle=True)

    grouped_events  = events.partition_by('ingest_date')
    for group_df in grouped_events:
        date_str = group_df['ingest_date'][0]

        folder_path = f"data/bronze/ingest_date={date_str}"
        os.makedirs(folder_path, exist_ok=True)

        filename = os.path.basename(file).replace('.jsonl', '.parquet')
        bronze_file_path = f"{folder_path}/{filename}"

        group_df.write_parquet(bronze_file_path)