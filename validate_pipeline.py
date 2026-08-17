import duckdb
import json
import os

def validate_pipeline():
    con = duckdb.connect()
    
    print("--- Running Source-to-Gold Data Validation ---\n")
    
    # 1. Read the Source Manifest (Ground Truth)
    if not os.path.exists("data/queue/source_manifest.json"):
        print("❌ ERROR: source_manifest.json not found. Run generator.py first.")
        return
        
    with open("data/queue/source_manifest.json", "r") as f:
        manifest = json.load(f)
        
    source_trips = manifest["total_trips"]
    source_revenue = manifest["total_revenue"]
    
    # 2. Query the SILVER Lakehouse Data (The Pipeline Output)
    silver_metrics = con.execute("""
        SELECT 
            COUNT(CASE WHEN event_type = 'TRIP_COMPLETED' THEN 1 END) AS silver_trips,
            SUM(CASE WHEN event_type = 'PAYMENT_COMPLETED' THEN CAST(payload->>'total_amount' AS DOUBLE) ELSE 0 END) AS silver_revenue
        FROM read_parquet('data/silver/**/*.parquet')
    """).fetchone()
    
    # 3. Print Comparison
    print(f"Source (Manifest) Trips:   {source_trips:,}")
    print(f"Silver (Lakehouse) Trips:  {silver_metrics[0]:,}")
    print("-" * 45)
    print(f"Source (Manifest) Revenue: ${source_revenue:,.2f}")
    print(f"Silver (Lakehouse) Revenue:${silver_metrics[1]:,.2f}")
    print("-" * 45)
    
    # 4. Drift Calculations
    trip_drift = source_trips - silver_metrics[0]
    revenue_drift = source_revenue - silver_metrics[1]
    
    if trip_drift == 0 and abs(revenue_drift) < 0.01:
        print("✅ PASS: Zero data drift between Source and Lakehouse.")
    else:
        print(f"❌ FAIL: Drift detected! Trip Drift: {trip_drift}, Revenue Drift: ${revenue_drift:.2f}")

if __name__ == "__main__":
    validate_pipeline()