import streamlit as st
import duckdb
import plotly.express as px

st.set_page_config(page_title="RideFlow Analytics", layout="wide", page_icon="🚖")

@st.cache_data
def get_daily_metrics():
    con = duckdb.connect()
    return con.execute("""
        SELECT event_date, total_trips, total_revenue 
        FROM read_parquet('data/gold/agg_daily_metrics.parquet')
        ORDER BY event_date
    """).fetch_df()

@st.cache_data
def get_top_zones(is_pickup=True):
    con = duckdb.connect()
    location_col = 'pickup_location_id' if is_pickup else 'dropoff_location_id'
    alias_col = 'pickup_zone' if is_pickup else 'dropoff_zone'
    
    query = f"""
        SELECT 
            z.Zone AS {alias_col},
            COUNT(*) AS total_trips
        FROM read_parquet('data/gold/fact_trips.parquet') AS f
        JOIN read_csv('data/raw/taxi_zone_lookup.csv', header=true) AS z
            ON f.{location_col} = z.LocationID
        GROUP BY z.Zone
        ORDER BY total_trips DESC
        LIMIT 10
    """
    return con.execute(query).fetch_df()

@st.cache_data
def get_peak_hours():
    con = duckdb.connect()
    return con.execute("""
        SELECT start_hour, COUNT(*) AS total_trips
        FROM read_parquet('data/gold/fact_trips.parquet')
        GROUP BY start_hour
        ORDER BY start_hour
    """).fetch_df()

@st.cache_data
def get_revenue_by_borough():
    con = duckdb.connect()
    return con.execute("""
        SELECT 
            z.Borough, 
            SUM(f.total_amount) AS revenue
        FROM read_parquet('data/gold/fact_trips.parquet') AS f
        JOIN read_csv('data/raw/taxi_zone_lookup.csv', header=true) AS z
            ON f.pickup_location_id = z.LocationID
        GROUP BY z.Borough
        ORDER BY revenue DESC
    """).fetch_df()

@st.cache_data
def get_operational_kpis():
    con = duckdb.connect()
    return con.execute("""
        SELECT 
            AVG(EPOCH(start_time - request_time) / 60) AS avg_wait_minutes,
            AVG(trip_distance) AS avg_distance
        FROM read_parquet('data/gold/fact_trips.parquet')
    """).fetchone()

# Load Data
daily_df = get_daily_metrics()
pickup_df = get_top_zones(is_pickup=True)
dropoff_df = get_top_zones(is_pickup=False)
peak_df = get_peak_hours()
borough_df = get_revenue_by_borough()
avg_wait, avg_distance = get_operational_kpis()

#  Build the Dashboard UI
st.title("RideFlow Lakehouse")
st.markdown("Real-time analytics powered by Bronze/Silver/Gold Parquet architecture & DuckDB.")

# Top KPI Row
st.divider()
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Revenue", f"${daily_df['total_revenue'].sum():,.2f}")
col2.metric("Total Trips", f"{daily_df['total_trips'].sum():,}")
col3.metric("Avg Wait Time", f"{avg_wait:.2f} mins")
col4.metric("Avg Trip Distance", f"{avg_distance:.2f} miles")

# Daily Revenue Chart
st.divider()
st.subheader("Daily Revenue Trend")
fig_rev = px.line(daily_df, x='event_date', y='total_revenue', title="Daily Revenue")
fig_rev.update_yaxes(tickprefix="$")
st.plotly_chart(fig_rev, use_container_width=True)

# Top Zones (Side by Side)
col_left, col_right = st.columns(2)
with col_left:
    st.markdown("#### Top 10 Pickup Zones")
    fig_pick = px.bar(pickup_df, x='pickup_zone', y='total_trips', labels={'pickup_zone': 'Pickup Zone'})
    st.plotly_chart(fig_pick, use_container_width=True)

with col_right:
    st.markdown("#### Top 10 Dropoff Zones")
    fig_drop = px.bar(dropoff_df, x='dropoff_zone', y='total_trips', labels={'dropoff_zone': 'Dropoff Zone'})
    st.plotly_chart(fig_drop, use_container_width=True)

# Peak Hours & Borough Revenue (Side by Side)
st.divider()
col_left2, col_right2 = st.columns(2)
with col_left2:
    st.markdown("#### Peak Trip Hours")
    fig_peak = px.bar(peak_df, x='start_hour', y='total_trips', labels={'start_hour': 'Hour of Day (0-23)'})
    st.plotly_chart(fig_peak, use_container_width=True)

with col_right2:
    st.markdown("#### Revenue by Borough")
    fig_borough = px.pie(borough_df, values='revenue', names='Borough', hole=0.4)
    st.plotly_chart(fig_borough, use_container_width=True)