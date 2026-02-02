"""
Logistics Data Platform - Streamlit Dashboard

A simple interactive dashboard to visualize logistics data.
Run with: streamlit run src/dashboard/app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

# Try to import visualization libraries
try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

# =============================================================================
# Configuration
# =============================================================================

st.set_page_config(
    page_title="Logistics Data Platform",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded"
)

DATA_DIR = Path(__file__).parent.parent.parent / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"


# =============================================================================
# Data Loading Functions
# =============================================================================

@st.cache_data(ttl=60)
def load_parquet_data(path: str) -> pd.DataFrame:
    """Load data from Parquet files."""
    try:
        if DUCKDB_AVAILABLE:
            return duckdb.query(f"SELECT * FROM read_parquet('{path}/**/*.parquet')").df()
        else:
            return pd.read_parquet(path)
    except Exception as e:
        st.warning(f"Could not load data from {path}: {e}")
        return pd.DataFrame()


def get_data_summary() -> dict:
    """Get summary of available data."""
    summary = {}

    tables = [
        ("vehicle_positions", BRONZE_DIR / "vehicle_positions"),
        ("shipment_events", BRONZE_DIR / "shipment_events"),
        ("delivery_events", BRONZE_DIR / "delivery_events"),
        ("trips", SILVER_DIR / "fleet" / "trips"),
        ("journeys", SILVER_DIR / "shipment" / "journeys"),
        ("agent_shifts", SILVER_DIR / "delivery" / "agent_shifts"),
    ]

    for name, path in tables:
        if path.exists():
            try:
                df = load_parquet_data(str(path))
                summary[name] = {"rows": len(df), "path": str(path)}
            except:
                summary[name] = {"rows": 0, "path": str(path)}
        else:
            summary[name] = {"rows": 0, "path": str(path)}

    return summary


# =============================================================================
# Dashboard Pages
# =============================================================================

def show_overview():
    """Show overview dashboard."""
    st.title("🚚 Unified Logistics Data Platform")
    st.markdown("---")

    # Data summary
    st.subheader("📊 Data Summary")
    summary = get_data_summary()

    cols = st.columns(3)

    bronze_tables = ["vehicle_positions", "shipment_events", "delivery_events"]
    silver_tables = ["trips", "journeys", "agent_shifts"]

    with cols[0]:
        st.markdown("### Bronze Layer")
        for table in bronze_tables:
            info = summary.get(table, {"rows": 0})
            st.metric(table, f"{info['rows']:,} rows")

    with cols[1]:
        st.markdown("### Silver Layer")
        for table in silver_tables:
            info = summary.get(table, {"rows": 0})
            st.metric(table, f"{info['rows']:,} rows")

    with cols[2]:
        st.markdown("### System Status")
        total_rows = sum(s["rows"] for s in summary.values())
        st.metric("Total Records", f"{total_rows:,}")
        st.metric("Tables", len([s for s in summary.values() if s["rows"] > 0]))
        st.metric("Last Updated", datetime.now().strftime("%H:%M:%S"))

    st.markdown("---")

    # Quick stats
    st.subheader("📈 Quick Stats")

    # Load some data for stats
    vehicle_positions = load_parquet_data(str(BRONZE_DIR / "vehicle_positions"))
    shipment_events = load_parquet_data(str(BRONZE_DIR / "shipment_events"))
    delivery_events = load_parquet_data(str(BRONZE_DIR / "delivery_events"))

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if len(vehicle_positions) > 0:
            unique_vehicles = vehicle_positions['vehicle_id'].nunique()
            st.metric("Active Vehicles", unique_vehicles)
        else:
            st.metric("Active Vehicles", "N/A")

    with col2:
        if len(shipment_events) > 0:
            unique_shipments = shipment_events['shipment_id'].nunique()
            st.metric("Shipments Tracked", unique_shipments)
        else:
            st.metric("Shipments Tracked", "N/A")

    with col3:
        if len(delivery_events) > 0:
            delivered = len(delivery_events[delivery_events['event_type'] == 'DELIVERED'])
            st.metric("Deliveries Completed", delivered)
        else:
            st.metric("Deliveries Completed", "N/A")

    with col4:
        if len(delivery_events) > 0 and 'event_type' in delivery_events.columns:
            total = len(delivery_events)
            delivered = len(delivery_events[delivery_events['event_type'] == 'DELIVERED'])
            rate = (delivered / total * 100) if total > 0 else 0
            st.metric("Delivery Success Rate", f"{rate:.1f}%")
        else:
            st.metric("Delivery Success Rate", "N/A")


def show_fleet_dashboard():
    """Show fleet telematics dashboard."""
    st.title("🚛 Fleet Telematics")
    st.markdown("---")

    # Load data
    positions = load_parquet_data(str(BRONZE_DIR / "vehicle_positions"))
    trips = load_parquet_data(str(SILVER_DIR / "fleet" / "trips"))

    if len(positions) == 0:
        st.warning("No vehicle position data available. Run the simulators first!")
        return

    # Vehicle selector
    vehicles = positions['vehicle_id'].unique()
    selected_vehicle = st.selectbox("Select Vehicle", ["All"] + list(vehicles))

    if selected_vehicle != "All":
        positions = positions[positions['vehicle_id'] == selected_vehicle]

    # Metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Positions", f"{len(positions):,}")
    with col2:
        avg_speed = positions['speed_kmh'].mean() if 'speed_kmh' in positions.columns else 0
        st.metric("Avg Speed", f"{avg_speed:.1f} km/h")
    with col3:
        if len(trips) > 0:
            st.metric("Trips Reconstructed", len(trips))
        else:
            st.metric("Trips Reconstructed", "N/A")
    with col4:
        unique_drivers = positions['driver_id'].nunique() if 'driver_id' in positions.columns else 0
        st.metric("Active Drivers", unique_drivers)

    st.markdown("---")

    # Speed distribution
    st.subheader("Speed Distribution")
    if PLOTLY_AVAILABLE and 'speed_kmh' in positions.columns:
        fig = px.histogram(
            positions.sample(min(10000, len(positions))),
            x='speed_kmh',
            nbins=50,
            title='Vehicle Speed Distribution'
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.bar_chart(positions['speed_kmh'].value_counts().head(20))

    # Map visualization
    st.subheader("Vehicle Positions")
    if PLOTLY_AVAILABLE:
        sample = positions.sample(min(5000, len(positions)))
        fig = px.scatter_mapbox(
            sample,
            lat='latitude',
            lon='longitude',
            color='speed_kmh' if 'speed_kmh' in sample.columns else None,
            zoom=4,
            center={"lat": 20.5937, "lon": 78.9629},
            mapbox_style="carto-positron",
            title="Vehicle Locations"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.map(positions[['latitude', 'longitude']].sample(min(1000, len(positions))))


def show_shipment_dashboard():
    """Show shipment tracking dashboard."""
    st.title("📦 Shipment Tracking")
    st.markdown("---")

    # Load data
    events = load_parquet_data(str(BRONZE_DIR / "shipment_events"))
    journeys = load_parquet_data(str(SILVER_DIR / "shipment" / "journeys"))

    if len(events) == 0:
        st.warning("No shipment event data available. Run the simulators first!")
        return

    # Metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Events", f"{len(events):,}")
    with col2:
        unique_shipments = events['shipment_id'].nunique()
        st.metric("Unique Shipments", unique_shipments)
    with col3:
        unique_hubs = events['hub_id'].nunique() if 'hub_id' in events.columns else 0
        st.metric("Hubs Active", unique_hubs)
    with col4:
        if len(journeys) > 0 and 'sla_status' in journeys.columns:
            sla_met = len(journeys[journeys['sla_status'] == 'MET'])
            total = len(journeys[journeys['sla_status'].isin(['MET', 'BREACHED'])])
            rate = (sla_met / total * 100) if total > 0 else 0
            st.metric("SLA Compliance", f"{rate:.1f}%")
        else:
            st.metric("SLA Compliance", "N/A")

    st.markdown("---")

    # Event type distribution
    st.subheader("Event Type Distribution")
    if 'event_type' in events.columns:
        event_counts = events['event_type'].value_counts()
        if PLOTLY_AVAILABLE:
            fig = px.bar(
                x=event_counts.index,
                y=event_counts.values,
                title="Shipment Events by Type"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.bar_chart(event_counts)

    # Hub activity
    st.subheader("Hub Activity")
    if 'hub_name' in events.columns:
        hub_activity = events.groupby('hub_name').size().sort_values(ascending=False)
        if PLOTLY_AVAILABLE:
            fig = px.bar(
                x=hub_activity.values,
                y=hub_activity.index,
                orientation='h',
                title="Events by Hub"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.bar_chart(hub_activity)

    # Journey outcomes
    if len(journeys) > 0 and 'journey_outcome' in journeys.columns:
        st.subheader("Journey Outcomes")
        outcome_counts = journeys['journey_outcome'].value_counts()
        if PLOTLY_AVAILABLE:
            fig = px.pie(
                values=outcome_counts.values,
                names=outcome_counts.index,
                title="Shipment Journey Outcomes"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.bar_chart(outcome_counts)


def show_delivery_dashboard():
    """Show last-mile delivery dashboard."""
    st.title("🛵 Last-Mile Delivery")
    st.markdown("---")

    # Load data
    events = load_parquet_data(str(BRONZE_DIR / "delivery_events"))
    shifts = load_parquet_data(str(SILVER_DIR / "delivery" / "agent_shifts"))

    if len(events) == 0:
        st.warning("No delivery event data available. Run the simulators first!")
        return

    # Metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Delivery Events", f"{len(events):,}")
    with col2:
        unique_agents = events['agent_id'].nunique() if 'agent_id' in events.columns else 0
        st.metric("Active Agents", unique_agents)
    with col3:
        delivered = len(events[events['event_type'] == 'DELIVERED']) if 'event_type' in events.columns else 0
        st.metric("Successful Deliveries", delivered)
    with col4:
        if 'customer_rating' in events.columns:
            avg_rating = events['customer_rating'].mean()
            st.metric("Avg Customer Rating", f"{avg_rating:.2f} ⭐")
        else:
            st.metric("Avg Customer Rating", "N/A")

    st.markdown("---")

    # Delivery outcomes
    st.subheader("Delivery Outcomes")
    if 'event_type' in events.columns:
        outcome_counts = events['event_type'].value_counts()
        if PLOTLY_AVAILABLE:
            colors = {
                'DELIVERED': 'green',
                'DELIVERY_ATTEMPTED': 'orange',
                'DELIVERY_FAILED': 'red'
            }
            fig = px.pie(
                values=outcome_counts.values,
                names=outcome_counts.index,
                title="Delivery Outcomes",
                color=outcome_counts.index,
                color_discrete_map=colors
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.bar_chart(outcome_counts)

    # Zone performance
    if 'zone_id' in events.columns:
        st.subheader("Zone Performance")
        zone_stats = events.groupby('zone_id').agg({
            'event_id': 'count',
            'customer_rating': 'mean'
        }).rename(columns={'event_id': 'deliveries', 'customer_rating': 'avg_rating'})

        st.dataframe(zone_stats.sort_values('deliveries', ascending=False))

    # Agent performance (if shifts available)
    if len(shifts) > 0:
        st.subheader("Top Performing Agents")
        if 'successful_deliveries' in shifts.columns:
            top_agents = shifts.nlargest(10, 'successful_deliveries')[
                ['agent_id', 'successful_deliveries', 'delivery_success_rate', 'zone_id']
            ]
            st.dataframe(top_agents)


def show_data_quality():
    """Show data quality dashboard."""
    st.title("✅ Data Quality")
    st.markdown("---")

    quality_reports_dir = DATA_DIR / "quality_reports"

    if not quality_reports_dir.exists():
        st.warning("No quality reports found. Run `make quality` first!")
        return

    # List available reports
    reports = list(quality_reports_dir.glob("*.json"))

    if not reports:
        st.warning("No quality reports found.")
        return

    # Load latest report
    import json
    latest_report = max(reports, key=lambda x: x.stat().st_mtime)

    with open(latest_report) as f:
        report = json.load(f)

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    summary = report.get('summary', {})

    with col1:
        st.metric("Total Checks", summary.get('total_checks', 0))
    with col2:
        st.metric("Passed", summary.get('passed', 0))
    with col3:
        st.metric("Failed", summary.get('failed', 0))
    with col4:
        pass_rate = summary.get('pass_rate', 0)
        st.metric("Pass Rate", f"{pass_rate}%")

    st.markdown("---")

    # Overall status
    if report.get('overall_success'):
        st.success("✅ All quality checks passed!")
    else:
        st.error("❌ Some quality checks failed!")

    # Detailed results
    st.subheader("Check Results")
    checks = report.get('checks', [])

    if checks:
        df = pd.DataFrame(checks)

        # Color code by success
        def highlight_status(row):
            if row.get('success'):
                return ['background-color: #d4edda'] * len(row)
            else:
                return ['background-color: #f8d7da'] * len(row)

        st.dataframe(df)


# =============================================================================
# Main App
# =============================================================================

def main():
    """Main application entry point."""

    # Sidebar navigation
    st.sidebar.title("🚚 Navigation")

    pages = {
        "📊 Overview": show_overview,
        "🚛 Fleet Telematics": show_fleet_dashboard,
        "📦 Shipment Tracking": show_shipment_dashboard,
        "🛵 Last-Mile Delivery": show_delivery_dashboard,
        "✅ Data Quality": show_data_quality,
    }

    selection = st.sidebar.radio("Go to", list(pages.keys()))

    # Run selected page
    pages[selection]()

    # Sidebar info
    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    st.sidebar.info(
        "Unified Logistics Data Platform\n\n"
        "• Fleet Telematics\n"
        "• Shipment Tracking\n"
        "• Last-Mile Delivery"
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "Made with ❤️ using Streamlit"
    )


if __name__ == "__main__":
    main()
