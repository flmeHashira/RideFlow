import glob
import polars as pl
import datetime, os

files = glob.glob('data/landing/**/*.jsonl', recursive=True)

for file in files:
    events = pl.read_ndjson(file, schema_overrides={'payload': pl.Utf8})
    events = events.with_columns(
        pl.lit(datetime.datetime.now().isoformat()).alias("ingest_timestamp")
    )
    bronze_file_path = file.replace("landing", "bronze").replace(".jsonl", ".parquet")
    folder_path = os.path.dirname(bronze_file_path)
    os.makedirs(folder_path, exist_ok=True)
    events.write_parquet(bronze_file_path)
