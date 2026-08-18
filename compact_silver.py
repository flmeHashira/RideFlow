import duckdb
import os
import shutil

def compact_silver():
    con = duckdb.connect()
    print("--- Running Silver Layer Compaction ---")
    
    # Write compacted files to a temporary directory
    # OVERWRITE_OR_IGNORE ensures it handles existing folders safely
    compact_query = """
        COPY (
            SELECT * FROM read_parquet('data/silver/**/*.parquet', hive_partitioning=1)
        ) TO 'data/silver_tmp/' (FORMAT PARQUET, PARTITION_BY event_date, OVERWRITE_OR_IGNORE 1);
    """
    
    print("Compacting files into temporary directory...")
    con.execute(compact_query)
    
    # 2. The Atomic Swap
    print("Swapping directories...")
    if os.path.exists('data/silver'):
        shutil.rmtree('data/silver') # Delete old tiny files
    os.rename('data/silver_tmp', 'data/silver') # Rename tmp to silver
    
    print("✅ Compaction Complete. Old files deleted, new compacted files in place.")

if __name__ == "__main__":
    compact_silver()