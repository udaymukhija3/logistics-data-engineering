"""
Logistics Data Platform - Streamlit Dashboard

Interactive dashboard to visualize logistics data.
Works with both live pipeline data and pre-generated sample data.
Run with: streamlit run src/dashboard/app.py
"""

import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# =============================================================================
# Configuration
# =============================================================================

st.set_page_config(
    page_title="Logistics Data Platform",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded",
)

ROOT_DIR = Path(__file__).parent.parent.parent
DATA_DIR = ROOT_DIR / "data"
SAMPLE_DIR = DATA_DIR / "sample"


# Use sample data if live data doesn't exist
def _resolve_dir(subpath: str) -> Path:
    live = DATA_DIR / subpath
    sample = SAMPLE_DIR / subpath
    if live.exists() and any(live.rglob("*.parquet")):
        return live
    return sample


BRONZE_DIR_VEHICLES = _resolve_dir("bronze/vehicle_positions")
BRONZE_DIR_SHIPMENTS = _resolve_dir("bronze/shipment_events")
BRONZE_DIR_DELIVERY = _resolve_dir("bronze/delivery_events")
SILVER_DIR_TRIPS = _resolve_dir("silver/fleet/trips")
SILVER_DIR_JOURNEYS = _resolve_dir("silver/shipment/journeys")
SILVER_DIR_SHIFTS = _resolve_dir("silver/delivery/agent_shifts")

USING_SAMPLE = str(SAMPLE_DIR) in str(BRONZE_DIR_VEHICLES)


def _resolve_quality_dir() -> Path:
    live = DATA_DIR / "quality_reports"
    sample = SAMPLE_DIR / "quality_reports"
    if live.exists() and any(live.glob("*.json")):
        return live
    return sample


QUALITY_DIR = _resolve_quality_dir()


# =============================================================================
# Data Loading
# =============================================================================


@st.cache_data(ttl=300)
def load_parquet(path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except Exception:
        try:
            import duckdb

            parquet_glob = f"{path.as_posix()}/**/*.parquet"
            conn = duckdb.connect()
            try:
                return conn.execute("SELECT * FROM read_parquet(?)", [parquet_glob]).fetchdf()
            finally:
                conn.close()
        except Exception as e:
            st.warning(f"Could not load data from {path}: {e}")
            return pd.DataFrame()


def _alias_columns(df: pd.DataFrame, alias_map: dict[str, list[str]]) -> pd.DataFrame:
    """Backfill canonical column names from known source aliases."""
    if df.empty:
        return df

    normalized = df.copy()
    for canonical, candidates in alias_map.items():
        if canonical in normalized.columns:
            continue
        for candidate in candidates:
            if candidate in normalized.columns:
                normalized[canonical] = normalized[candidate]
                break
    return normalized


def normalize_trip_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize trip columns across live and sample datasets."""
    return _alias_columns(
        df,
        {
            "distance_km": ["total_distance_km"],
            "duration_minutes": ["trip_duration_minutes"],
        },
    )


# =============================================================================
# Pages
# =============================================================================


def show_overview():
    st.title("Unified Logistics Data Platform")

    if USING_SAMPLE:
        st.info(
            "Viewing pre-generated sample data. "
            "Run `make simulate-demo && make batch` to see live pipeline data."
        )

    st.markdown(
        "Real-time and batch data platform for Fleet Telematics, Shipment Tracking, and Last-Mile Delivery analytics."
    )
    st.markdown("---")

    positions = load_parquet(BRONZE_DIR_VEHICLES)
    shipments = load_parquet(BRONZE_DIR_SHIPMENTS)
    deliveries = load_parquet(BRONZE_DIR_DELIVERY)
    trips = normalize_trip_schema(load_parquet(SILVER_DIR_TRIPS))
    journeys = load_parquet(SILVER_DIR_JOURNEYS)
    shifts = load_parquet(SILVER_DIR_SHIFTS)

    # KPI row
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric(
            "Vehicles Tracked", len(positions["vehicle_id"].unique()) if len(positions) else 0
        )
    with col2:
        st.metric("Shipments", len(shipments["shipment_id"].unique()) if len(shipments) else 0)
    with col3:
        delivered = (
            len(deliveries[deliveries["event_type"] == "DELIVERED"]) if len(deliveries) else 0
        )
        st.metric("Deliveries", delivered)
    with col4:
        if len(deliveries) and "event_type" in deliveries.columns:
            total = len(deliveries)
            rate = delivered / total * 100 if total else 0
            st.metric("Success Rate", f"{rate:.1f}%")
        else:
            st.metric("Success Rate", "N/A")
    with col5:
        st.metric("Trips Reconstructed", len(trips) if len(trips) else 0)
    with col6:
        if len(journeys) and "sla_status" in journeys.columns:
            met = len(journeys[journeys["sla_status"] == "MET"])
            total_sla = len(journeys[journeys["sla_status"].isin(["MET", "BREACHED"])])
            sla_rate = met / total_sla * 100 if total_sla else 0
            st.metric("SLA Compliance", f"{sla_rate:.1f}%")
        else:
            st.metric("SLA Compliance", "N/A")

    st.markdown("---")

    # Data pipeline summary
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Data Pipeline Layers")
        layer_data = pd.DataFrame(
            {
                "Layer": [
                    "Bronze - Vehicle Positions",
                    "Bronze - Shipment Events",
                    "Bronze - Delivery Events",
                    "Silver - Trips",
                    "Silver - Journeys",
                    "Silver - Agent Shifts",
                ],
                "Records": [
                    len(positions),
                    len(shipments),
                    len(deliveries),
                    len(trips),
                    len(journeys),
                    len(shifts),
                ],
                "Status": ["Active"] * 6,
            }
        )
        st.dataframe(layer_data, use_container_width=True, hide_index=True)

    with col_right:
        st.subheader("Event Distribution")
        if len(shipments) and "event_type" in shipments.columns:
            event_counts = shipments["event_type"].value_counts().head(10)
            fig = px.bar(
                x=event_counts.values,
                y=event_counts.index,
                orientation="h",
                labels={"x": "Count", "y": "Event Type"},
                color_discrete_sequence=["#FF6B35"],
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=300)
            st.plotly_chart(fig, use_container_width=True)

    # Map
    if len(positions) and "latitude" in positions.columns:
        st.subheader("Fleet Positions Across India")
        sample = positions.sample(min(2000, len(positions)))
        fig = px.scatter_mapbox(
            sample,
            lat="latitude",
            lon="longitude",
            color="speed_kmh" if "speed_kmh" in sample.columns else None,
            color_continuous_scale="RdYlGn_r",
            zoom=4,
            center={"lat": 20.5937, "lon": 78.9629},
            mapbox_style="carto-positron",
            height=500,
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)


def show_fleet_dashboard():
    st.title("Fleet Telematics")
    st.markdown("GPS tracking, trip reconstruction, and driver performance analytics.")
    st.markdown("---")

    positions = load_parquet(BRONZE_DIR_VEHICLES)
    trips = normalize_trip_schema(load_parquet(SILVER_DIR_TRIPS))

    if len(positions) == 0:
        st.warning("No vehicle position data available.")
        return

    vehicles = sorted(positions["vehicle_id"].unique())
    selected = st.selectbox("Select Vehicle", ["All"] + list(vehicles))
    if selected != "All":
        positions = positions[positions["vehicle_id"] == selected]

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("GPS Points", f"{len(positions):,}")
    with col2:
        st.metric("Avg Speed", f"{positions['speed_kmh'].mean():.1f} km/h")
    with col3:
        st.metric("Trips", len(trips) if len(trips) else "N/A")
    with col4:
        st.metric(
            "Drivers", positions["driver_id"].nunique() if "driver_id" in positions.columns else 0
        )

    st.markdown("---")
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Speed Distribution")
        fig = px.histogram(
            positions.sample(min(5000, len(positions))),
            x="speed_kmh",
            nbins=40,
            color_discrete_sequence=["#FF6B35"],
            labels={"speed_kmh": "Speed (km/h)"},
        )
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Vehicle Type Breakdown")
        if "vehicle_type" in positions.columns:
            type_counts = positions.groupby("vehicle_type")["vehicle_id"].nunique().reset_index()
            type_counts.columns = ["Vehicle Type", "Count"]
            fig = px.pie(
                type_counts,
                values="Count",
                names="Vehicle Type",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=350)
            st.plotly_chart(fig, use_container_width=True)

    # Trip analysis
    if len(trips) > 0:
        st.markdown("---")
        st.subheader("Trip Analysis")
        col1, col2 = st.columns(2)

        with col1:
            fig = px.histogram(
                trips,
                x="distance_km",
                nbins=30,
                labels={"distance_km": "Distance (km)"},
                color_discrete_sequence=["#2E86AB"],
            )
            fig.update_layout(
                title="Trip Distance Distribution", margin=dict(l=0, r=0, t=30, b=0), height=300
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.scatter(
                trips,
                x="distance_km",
                y="duration_minutes",
                color="vehicle_type" if "vehicle_type" in trips.columns else None,
                labels={"distance_km": "Distance (km)", "duration_minutes": "Duration (min)"},
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_layout(
                title="Distance vs Duration", margin=dict(l=0, r=0, t=30, b=0), height=300
            )
            st.plotly_chart(fig, use_container_width=True)

    # Map
    st.subheader("Vehicle Positions")
    sample = positions.sample(min(3000, len(positions)))
    fig = px.scatter_mapbox(
        sample,
        lat="latitude",
        lon="longitude",
        color="speed_kmh",
        color_continuous_scale="RdYlGn_r",
        zoom=4,
        center={"lat": 20.5937, "lon": 78.9629},
        mapbox_style="carto-positron",
        height=500,
    )
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)


def show_shipment_dashboard():
    st.title("Shipment Tracking")
    st.markdown("Package scan events, journey reconstruction, and SLA monitoring.")
    st.markdown("---")

    events = load_parquet(BRONZE_DIR_SHIPMENTS)
    journeys = load_parquet(SILVER_DIR_JOURNEYS)

    if len(events) == 0:
        st.warning("No shipment event data available.")
        return

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Events", f"{len(events):,}")
    with col2:
        st.metric("Shipments", events["shipment_id"].nunique())
    with col3:
        st.metric("Hubs Active", events["hub_id"].nunique() if "hub_id" in events.columns else 0)
    with col4:
        if len(journeys) and "sla_status" in journeys.columns:
            met = len(journeys[journeys["sla_status"] == "MET"])
            total = len(journeys[journeys["sla_status"].isin(["MET", "BREACHED"])])
            st.metric("SLA Compliance", f"{met / total * 100:.1f}%" if total else "N/A")
        else:
            st.metric("SLA Compliance", "N/A")

    st.markdown("---")
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Event Pipeline")
        if "event_type" in events.columns:
            event_counts = events["event_type"].value_counts()
            fig = px.bar(
                x=event_counts.index,
                y=event_counts.values,
                labels={"x": "Event Type", "y": "Count"},
                color_discrete_sequence=["#FF6B35"],
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=350, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Hub Activity")
        if "hub_name" in events.columns:
            hub_counts = events.groupby("hub_name").size().sort_values(ascending=True)
            fig = px.bar(
                x=hub_counts.values,
                y=hub_counts.index,
                orientation="h",
                labels={"x": "Events", "y": "Hub"},
                color_discrete_sequence=["#2E86AB"],
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=350)
            st.plotly_chart(fig, use_container_width=True)

    if len(journeys) > 0:
        st.markdown("---")
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Journey Outcomes")
            if "journey_outcome" in journeys.columns:
                outcomes = journeys["journey_outcome"].value_counts()
                colors = {"DELIVERED": "#28a745", "FAILED": "#dc3545", "IN_TRANSIT": "#ffc107"}
                fig = px.pie(
                    values=outcomes.values,
                    names=outcomes.index,
                    color=outcomes.index,
                    color_discrete_map=colors,
                )
                fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=300)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("SLA Status")
            if "sla_status" in journeys.columns:
                sla = journeys["sla_status"].value_counts()
                sla_colors = {
                    "MET": "#28a745",
                    "BREACHED": "#dc3545",
                    "ON_TRACK": "#17a2b8",
                    "AT_RISK": "#ffc107",
                }
                fig = px.pie(
                    values=sla.values,
                    names=sla.index,
                    color=sla.index,
                    color_discrete_map=sla_colors,
                )
                fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=300)
                st.plotly_chart(fig, use_container_width=True)


def show_delivery_dashboard():
    st.title("Last-Mile Delivery")
    st.markdown("Delivery agent performance, zone analysis, and customer satisfaction.")
    st.markdown("---")

    events = load_parquet(BRONZE_DIR_DELIVERY)
    shifts = load_parquet(SILVER_DIR_SHIFTS)

    if len(events) == 0:
        st.warning("No delivery event data available.")
        return

    col1, col2, col3, col4 = st.columns(4)
    delivered = (
        len(events[events["event_type"] == "DELIVERED"]) if "event_type" in events.columns else 0
    )
    with col1:
        st.metric("Delivery Events", f"{len(events):,}")
    with col2:
        st.metric(
            "Active Agents", events["agent_id"].nunique() if "agent_id" in events.columns else 0
        )
    with col3:
        st.metric("Successful Deliveries", delivered)
    with col4:
        if "customer_rating" in events.columns:
            avg = events["customer_rating"].dropna().mean()
            st.metric("Avg Rating", f"{avg:.2f} / 5.0")
        else:
            st.metric("Avg Rating", "N/A")

    st.markdown("---")
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Delivery Outcomes")
        if "event_type" in events.columns:
            outcomes = events["event_type"].value_counts()
            colors = {
                "DELIVERED": "#28a745",
                "DELIVERY_ATTEMPTED": "#ffc107",
                "DELIVERY_FAILED": "#dc3545",
            }
            fig = px.pie(
                values=outcomes.values,
                names=outcomes.index,
                color=outcomes.index,
                color_discrete_map=colors,
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Rating Distribution")
        if "customer_rating" in events.columns:
            ratings = events["customer_rating"].dropna()
            fig = px.histogram(
                ratings, nbins=5, labels={"value": "Rating"}, color_discrete_sequence=["#FF6B35"]
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=350)
            st.plotly_chart(fig, use_container_width=True)

    # Zone performance
    if "zone_id" in events.columns:
        st.markdown("---")
        st.subheader("Zone Performance")
        zone_stats = (
            events.groupby("zone_id")
            .agg(
                deliveries=("event_id", "count"),
                avg_rating=("customer_rating", "mean"),
            )
            .sort_values("deliveries", ascending=False)
            .reset_index()
        )
        zone_stats["avg_rating"] = zone_stats["avg_rating"].round(2)
        st.dataframe(zone_stats, use_container_width=True, hide_index=True)

    # Top agents
    if len(shifts) > 0 and "successful_deliveries" in shifts.columns:
        st.markdown("---")
        st.subheader("Top Performing Agents")
        top = shifts.nlargest(10, "successful_deliveries")[
            [
                "agent_id",
                "zone_id",
                "successful_deliveries",
                "delivery_success_rate",
                "avg_customer_rating",
            ]
        ].reset_index(drop=True)
        st.dataframe(top, use_container_width=True, hide_index=True)


def show_data_quality():
    st.title("Data Quality")
    st.markdown("Automated quality checks across bronze and silver layers.")
    st.markdown("---")

    if not QUALITY_DIR.exists():
        st.warning("No quality reports found. Run `make quality` first.")
        return

    reports = list(QUALITY_DIR.glob("*.json"))
    if not reports:
        st.warning("No quality reports found.")
        return

    latest = max(reports, key=lambda x: x.stat().st_mtime)
    with open(latest) as f:
        report = json.load(f)

    summary = report.get("summary", {})
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Checks", summary.get("total_checks", 0))
    with col2:
        st.metric("Passed", summary.get("passed", 0))
    with col3:
        st.metric("Failed", summary.get("failed", 0))
    with col4:
        st.metric("Pass Rate", f"{summary.get('pass_rate', 0)}%")

    st.markdown("---")

    if report.get("overall_success"):
        st.success("All quality checks passed!")
    else:
        st.error("Some quality checks failed - see details below.")

    checks = report.get("checks", [])
    if checks:
        df = pd.DataFrame(checks)
        st.dataframe(df, use_container_width=True, hide_index=True)


def show_architecture():
    st.title("Platform Architecture")
    st.markdown("End-to-end data engineering platform built with the modern data stack.")
    st.markdown("---")

    st.subheader("System Overview")
    st.code(
        """
    DATA SOURCES                    INGESTION              STORAGE & PROCESSING
    +--------------+               +----------+           +--------------------+
    | GPS Devices  |--+            |          |           |                    |
    | (Vehicles)   |  |            |  Apache  |   Bronze  |   Spark Streaming  |
    +--------------+  +----------->|  Kafka   |---------->|   (Schema valid,   |
    | Scanner Apps |  |            |  (6      |           |    partitioning)   |
    | (Hub Workers)|--+            |  topics) |           |                    |
    +--------------+  |            |          |           +--------+-----------+
    | Delivery App |--+            +----------+                    |
    | (Agents)     |                                               v
    +--------------+                                      +--------+-----------+
                                                          |   Delta Lake       |
                      ANALYTICS         GOLD              |   Bronze Layer     |
                    +----------+   +-----------+          +--------+-----------+
                    |          |   |           |                    |
                    |Streamlit |<--|  DuckDB   |<--- dbt ---+      v
                    |Dashboard |   |  (Star    |            | +----+-----------+
                    |          |   |   Schema) |            | | Spark Batch    |
                    +----------+   +-----------+            | | (Trip/Journey  |
                                                            | |  Reconstruct) |
                    +----------+   +-----------+            | +----+-----------+
                    | Jupyter  |<--|  Airflow  |            |      |
                    | Notebook |   | (2AM DAG) |------------+      v
                    +----------+   +-----------+              +----+-----------+
                                                              | Delta Lake     |
                                                              | Silver Layer   |
                                                              +----------------+
    """,
        language=None,
    )

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Technology Stack")
        tech = pd.DataFrame(
            {
                "Layer": [
                    "Messaging",
                    "Stream Processing",
                    "Batch Processing",
                    "Storage",
                    "Transformations",
                    "Orchestration",
                    "Data Quality",
                    "Warehouse",
                    "Dashboard",
                    "Infrastructure",
                ],
                "Technology": [
                    "Apache Kafka",
                    "Spark Structured Streaming",
                    "Apache Spark 3.5",
                    "Delta Lake (ACID)",
                    "dbt 1.7+",
                    "Apache Airflow",
                    "Custom Framework + dbt Tests",
                    "DuckDB",
                    "Streamlit + Plotly",
                    "Docker Compose",
                ],
            }
        )
        st.dataframe(tech, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Data Model (Star Schema)")
        model = pd.DataFrame(
            {
                "Table": [
                    "fct_trips",
                    "fct_driver_performance",
                    "fct_shipments",
                    "fct_hub_daily",
                    "fct_agent_daily",
                    "fct_zone_daily",
                    "dim_time",
                    "dim_hubs",
                ],
                "Module": [
                    "Fleet",
                    "Fleet",
                    "Shipment",
                    "Shipment",
                    "Delivery",
                    "Delivery",
                    "Common",
                    "Common",
                ],
                "Grain": [
                    "Per trip",
                    "Per driver/day",
                    "Per shipment",
                    "Per hub/day",
                    "Per agent/day",
                    "Per zone/day",
                    "Date",
                    "Hub",
                ],
            }
        )
        st.dataframe(model, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Medallion Architecture")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("### Bronze (Raw)")
        st.markdown(
            "- Raw Kafka events\n"
            "- Schema validation\n"
            "- Ingestion metadata\n"
            "- Partitioned by date\n"
            "- Delta Lake format"
        )

    with col2:
        st.markdown("### Silver (Processed)")
        st.markdown(
            "- Trip reconstruction\n"
            "- Journey reconstruction\n"
            "- Agent shift aggregation\n"
            "- Business logic applied\n"
            "- Cleaned & validated"
        )

    with col3:
        st.markdown("### Gold (Analytics)")
        st.markdown(
            "- dbt dimensional model\n"
            "- 6 fact tables\n"
            "- 2 dimension tables\n"
            "- Star schema in DuckDB\n"
            "- Optimized for queries"
        )

    st.markdown("---")
    st.subheader("Key Features")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(
            "**Fleet Telematics**\n"
            "- GPS tracking (10s intervals)\n"
            "- Trip reconstruction\n"
            "- Driving event detection\n"
            "- Fuel consumption modeling"
        )
    with col2:
        st.markdown(
            "**Shipment Tracking**\n"
            "- Hub scan event pipeline\n"
            "- Journey reconstruction\n"
            "- SLA monitoring\n"
            "- Bottleneck detection"
        )
    with col3:
        st.markdown(
            "**Last-Mile Delivery**\n"
            "- Agent GPS tracking\n"
            "- Shift aggregation\n"
            "- Zone performance\n"
            "- Customer satisfaction"
        )


# =============================================================================
# Main
# =============================================================================


def main():
    st.sidebar.title("Navigation")

    pages = {
        "Overview": show_overview,
        "Fleet Telematics": show_fleet_dashboard,
        "Shipment Tracking": show_shipment_dashboard,
        "Last-Mile Delivery": show_delivery_dashboard,
        "Data Quality": show_data_quality,
        "Architecture": show_architecture,
    }

    selection = st.sidebar.radio("Go to", list(pages.keys()))
    pages[selection]()

    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    st.sidebar.info(
        "**Unified Logistics Data Platform**\n\n"
        "End-to-end data engineering project:\n"
        "Kafka | Spark | Delta Lake | dbt | Airflow | DuckDB"
    )
    data_mode = "Sample Data" if USING_SAMPLE else "Live Pipeline"
    st.sidebar.caption(f"Data mode: {data_mode}")


if __name__ == "__main__":
    main()
