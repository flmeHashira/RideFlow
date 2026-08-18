import duckdb
import re

def run_audit(data_path="data/silver/**/*.parquet", test_date="2024-01-16"):
    print("="*50)
    print("RIDEFLOW LAKEHOUSE STRUCTURAL AUDIT")
    print("="*50)

    with duckdb.connect() as con:
        
        # TEST 1: Folder Consistency (Hive Partitioning)
        print("\n[1/3] Checking Partition Consistency...")
        consistency_query = f"""
            SELECT COUNT(*) as mismatched_rows
            FROM read_parquet('{data_path}', hive_partitioning=1)
            WHERE CAST(event_date AS VARCHAR) != STRFTIME(timestamp, '%Y-%m-%d')
        """
        mismatches = con.execute(consistency_query).fetchone()[0]
        
        if mismatches == 0:
            print("✅ PASS: 0 mismatches. All files are in their correct event_date partitions.")
        else:
            print(f"❌ FAIL: Found {mismatches} rows where the file's internal date doesn't match the folder!")


        # TEST 2: Space Optimization (Small File Problem)
        print("\n[2/3] Checking Space Optimization (Small File Problem)...")
        size_query = f"""
            SELECT 
                COUNT(*) as total_files,
                ROUND(AVG(total_compressed_size) / 1024.0 / 1024.0, 3) as avg_size_mb,
                SUM(CASE WHEN total_compressed_size < 1048576 THEN 1 ELSE 0 END) as files_under_1mb
            FROM parquet_metadata('{data_path}')
        """
        total_files, avg_size_mb, files_under_1mb = con.execute(size_query).fetchone()
        
        # Handle case where directory might be empty
        if total_files == 0:
            print("⚠️ WARNING: No Parquet files found in the specified path.")
            return

        print(f"   Total Silver Files: {total_files}")
        print(f"   Average File Size:  {avg_size_mb} MB")
        print(f"   Files under 1MB:    {files_under_1mb}")
        
        if files_under_1mb > (total_files * 0.5):
            print("⚠️ WARNING: Majority of files are <1MB. You have a Small File Problem. Consider a compaction job.")
        else:
            print("✅ PASS: File sizes are healthy for an MVP.")


        # TEST 3: Query Optimization (Partition Pruning)
        print("\n[3/3] Checking Query Optimization (Partition Pruning)...")
        explain_query = f"""
            EXPLAIN 
            SELECT * FROM read_parquet('{data_path}', hive_partitioning=1) 
            WHERE event_date = '{test_date}'
        """
        
        # Fetch the plan and convert the nested tuple structure into a single string
        plan = con.execute(explain_query).fetchall()
        plan_text = "".join(str(item) for item in plan)
        
        # Use Regex to find the exact "Scanning Files: X/Y" metric in DuckDB's physical plan
        scan_match = re.search(r"Scanning Files:\s*(\d+)/(\d+)", plan_text)
        
        if scan_match:
            scanned = int(scan_match.group(1))
            total = int(scan_match.group(2))
            
            if scanned < total:
                print(f"✅ PASS: Partition pruning successful!")
                print(f"   Skipped {total - scanned} irrelevant files (Scanned {scanned} out of {total} files).")
            else:
                print(f"⚠️ WARNING: Query scanned all {total} files. Partition pruning failed or all files matched the date.")
        else:
            # Fallback if the regex doesn't match the specific DuckDB version output
            print("⚠️ WARNING: Could not parse file scanning metrics from EXPLAIN output. Run manually to verify.")
            
    print("\n" + "="*50)
    print("AUDIT COMPLETE")
    print("="*50)

if __name__ == "__main__":
    run_audit(data_path="data/silver/**/*.parquet", test_date="2024-01-16")