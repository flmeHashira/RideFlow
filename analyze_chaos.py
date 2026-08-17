import duckdb

def analyze_chaos_distribution():
    con = duckdb.connect()
    
    print("--- Analyzing Chaos Engine Distribution ---\n")
    
    # We use the DATEDIFF function to calculate the delay in hours between event and ingest
    query = """
        WITH delays AS (
            SELECT 
                event_type,
                DATE_DIFF('hour', timestamp, ingest_timestamp) AS delay_hours
            FROM read_parquet('data/silver/**/*.parquet')
        )
        SELECT 
            CASE 
                WHEN delay_hours = 0 THEN '0. Instant (< 1 hr)'
                WHEN delay_hours BETWEEN 1 AND 6 THEN '1. Late (1-6 hrs)'
                WHEN delay_hours BETWEEN 12 AND 48 THEN '2. Very Late (12-48 hrs)'
                ELSE '3. Unexpected Delay'
            END AS delay_bucket,
            COUNT(*) AS event_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
        FROM delays
        GROUP BY delay_bucket
        ORDER BY delay_bucket
    """
    
    result_df = con.execute(query).fetch_df()
    print(result_df.to_string(index=False))

if __name__ == "__main__":
    analyze_chaos_distribution()