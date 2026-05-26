"""
Logistics Data Platform - Streamlit Dashboard

Interactive dashboard to visualize logistics data.
Works with both live pipeline data and pre-generated sample data.
Run with: streamlit run src/dashboard/app.py
"""

import json
import os
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
DATA_MODE = os.getenv("LOGISTICS_DATA_MODE", "sample").strip().lower()

CORE_DATASETS = (
    "bronze/vehicle_positions",
    "bronze/shipment_events",
    "bronze/delivery_events",
    "silver/fleet/trips",
    "silver/shipment/journeys",
    "silver/delivery/agent_shifts",
)

PARQUET_DATASETS = (
    ("Bronze", "Vehicle Positions", "bronze/vehicle_positions", "bronze.vehicle_positions"),
    ("Bronze", "Shipment Events", "bronze/shipment_events", "bronze.shipment_events"),
    ("Bronze", "Delivery Events", "bronze/delivery_events", "bronze.delivery_events"),
    ("Silver", "Trips", "silver/fleet/trips", "silver.fleet.trips"),
    ("Silver", "Journeys", "silver/shipment/journeys", "silver.shipment.journeys"),
    ("Silver", "Agent Shifts", "silver/delivery/agent_shifts", "silver.delivery.agent_shifts"),
    (
        "Silver",
        "Zone Performance",
        "silver/delivery/zone_performance",
        "silver.delivery.zone_performance",
    ),
)

WAREHOUSE_RELATIONS = (
    "main_marts.fct_trips",
    "main_marts.fct_driver_performance",
    "main_marts.fct_shipments",
    "main_marts.fct_hub_daily",
    "main_marts.fct_agent_daily",
    "main_marts.fct_zone_daily",
    "main_marts.dim_hubs",
    "main_marts.dim_time",
)

CORE_MART_RELATIONS = (
    "main_marts.fct_trips",
    "main_marts.fct_shipments",
    "main_marts.fct_agent_daily",
)

PALETTE = {
    "ink": "#102a43",
    "slate": "#486581",
    "sand": "#f6f1e9",
    "card": "#fffaf4",
    "accent": "#d96c3d",
    "accent_alt": "#1f7a8c",
    "success": "#2d936c",
    "warning": "#d08c2f",
    "danger": "#b8405e",
}


def _has_parquet(path: Path) -> bool:
    return path.exists() and any(path.rglob("*.parquet"))


def _resolve_data_root(mode: str) -> tuple[Path, str]:
    normalized = mode if mode in {"sample", "live", "auto"} else "sample"
    if normalized == "sample":
        return SAMPLE_DIR, "sample"
    if normalized == "live":
        return DATA_DIR, "live"

    live_ready = all(_has_parquet(DATA_DIR / subpath) for subpath in CORE_DATASETS)
    if live_ready:
        return DATA_DIR, "live"
    return SAMPLE_DIR, "sample"


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT_DIR))
    except ValueError:
        return str(path)


DATA_ROOT, EFFECTIVE_MODE = _resolve_data_root(DATA_MODE)
MANIFEST_PATH = DATA_ROOT / "manifest.json"


def _resolve_warehouse_path(data_root: Path) -> Path:
    override = os.getenv("LOGISTICS_DUCKDB_PATH")
    if override:
        return Path(override)
    bundled = data_root / "warehouse" / "logistics.duckdb"
    if bundled.exists():
        return bundled
    return DATA_DIR / "warehouse" / "logistics.duckdb"


WAREHOUSE_DB_PATH = _resolve_warehouse_path(DATA_ROOT)


BRONZE_DIR_VEHICLES = DATA_ROOT / "bronze" / "vehicle_positions"
BRONZE_DIR_SHIPMENTS = DATA_ROOT / "bronze" / "shipment_events"
BRONZE_DIR_DELIVERY = DATA_ROOT / "bronze" / "delivery_events"
SILVER_DIR_TRIPS = DATA_ROOT / "silver" / "fleet" / "trips"
SILVER_DIR_JOURNEYS = DATA_ROOT / "silver" / "shipment" / "journeys"
SILVER_DIR_SHIFTS = DATA_ROOT / "silver" / "delivery" / "agent_shifts"

USING_SAMPLE = EFFECTIVE_MODE == "sample"
QUALITY_DIR = DATA_ROOT / "quality_reports"


def inject_theme():
    st.markdown(
        f"""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500&display=swap');

            .stApp {{
                background:
                    radial-gradient(circle at top left, rgba(217, 108, 61, 0.12), transparent 30%),
                    radial-gradient(circle at top right, rgba(31, 122, 140, 0.12), transparent 28%),
                    linear-gradient(180deg, #fcf8f3 0%, #f2ede6 100%);
                color: {PALETTE["ink"]};
            }}

            html, body, [class*="css"] {{
                font-family: "Space Grotesk", "Avenir Next", "Segoe UI", sans-serif;
            }}

            [data-testid="stSidebar"] {{
                background: linear-gradient(180deg, rgba(16, 42, 67, 0.98) 0%, rgba(29, 53, 87, 0.98) 100%);
            }}

            [data-testid="stSidebar"] * {{
                color: #f8fafc;
            }}

            div[data-testid="metric-container"] {{
                background: linear-gradient(180deg, rgba(255, 250, 244, 0.95) 0%, rgba(255, 255, 255, 0.95) 100%);
                border: 1px solid rgba(16, 42, 67, 0.08);
                border-radius: 20px;
                padding: 1rem 1.15rem;
                box-shadow: 0 18px 45px rgba(16, 42, 67, 0.08);
            }}

            .hero-card {{
                background:
                    linear-gradient(135deg, rgba(16, 42, 67, 0.97) 0%, rgba(17, 74, 95, 0.96) 55%, rgba(217, 108, 61, 0.92) 100%);
                border-radius: 28px;
                padding: 1.7rem 1.8rem;
                color: #f8fafc;
                box-shadow: 0 28px 65px rgba(16, 42, 67, 0.2);
                margin-bottom: 1.2rem;
                overflow: hidden;
                position: relative;
            }}

            .hero-card::after {{
                content: "";
                position: absolute;
                inset: auto -40px -55px auto;
                width: 180px;
                height: 180px;
                background: rgba(255, 255, 255, 0.08);
                border-radius: 50%;
            }}

            .hero-kicker {{
                font-family: "IBM Plex Mono", monospace;
                font-size: 0.78rem;
                letter-spacing: 0.14em;
                text-transform: uppercase;
                opacity: 0.8;
                margin-bottom: 0.75rem;
            }}

            .hero-title {{
                font-size: 2.45rem;
                font-weight: 700;
                line-height: 1.05;
                margin-bottom: 0.55rem;
            }}

            .hero-copy {{
                font-size: 1rem;
                max-width: 48rem;
                line-height: 1.6;
                opacity: 0.92;
                margin-bottom: 1rem;
            }}

            .hero-chip-row {{
                display: flex;
                flex-wrap: wrap;
                gap: 0.55rem;
            }}

            .hero-chip {{
                display: inline-flex;
                align-items: center;
                gap: 0.35rem;
                padding: 0.42rem 0.72rem;
                border-radius: 999px;
                background: rgba(255, 255, 255, 0.15);
                border: 1px solid rgba(255, 255, 255, 0.12);
                font-size: 0.88rem;
                backdrop-filter: blur(6px);
            }}

            .section-label {{
                font-family: "IBM Plex Mono", monospace;
                text-transform: uppercase;
                letter-spacing: 0.12em;
                color: {PALETTE["slate"]};
                font-size: 0.76rem;
                margin-bottom: 0.35rem;
            }}

            .domain-card {{
                background: rgba(255, 250, 244, 0.92);
                border: 1px solid rgba(16, 42, 67, 0.08);
                border-radius: 22px;
                padding: 1rem 1.1rem;
                min-height: 150px;
            }}

            .domain-card h4 {{
                margin: 0;
                color: {PALETTE["ink"]};
            }}

            .domain-card .domain-metric {{
                font-size: 1.7rem;
                font-weight: 700;
                margin: 0.6rem 0 0.35rem;
                color: {PALETTE["accent"]};
            }}

            .domain-card p {{
                margin: 0;
                color: {PALETTE["slate"]};
                line-height: 1.5;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )


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


@st.cache_data(ttl=300)
def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    return {}


@st.cache_data(ttl=300)
def load_latest_quality_report() -> dict:
    if not QUALITY_DIR.exists():
        return {}

    reports = sorted(QUALITY_DIR.glob("*.json"))
    if not reports:
        return {}

    latest = max(reports, key=lambda path: path.stat().st_mtime)
    return json.loads(latest.read_text(encoding="utf-8"))


@st.cache_data(ttl=300)
def load_duckdb_query(query: str) -> pd.DataFrame:
    if not WAREHOUSE_DB_PATH.exists():
        return pd.DataFrame()

    try:
        import duckdb

        with duckdb.connect(str(WAREHOUSE_DB_PATH), read_only=True) as conn:
            return conn.execute(query).fetchdf()
    except Exception:
        return pd.DataFrame()


def load_scalar(query: str, default: float | int | None = 0):
    df = load_duckdb_query(query)
    if df.empty:
        return default

    value = df.iloc[0, 0]
    if pd.isna(value):
        return default
    return value


@st.cache_data(ttl=300)
def load_warehouse_relation(relation_name: str, limit: int | None = None) -> pd.DataFrame:
    if relation_name not in WAREHOUSE_RELATIONS:
        return pd.DataFrame()

    query = f"select * from {relation_name}"
    if limit is not None:
        query += f" limit {int(limit)}"

    return load_duckdb_query(query)


@st.cache_data(ttl=300)
def load_warehouse_inventory() -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for relation_name in WAREHOUSE_RELATIONS:
        row_count = int(load_scalar(f"select count(*) from {relation_name}", default=0) or 0)
        rows.append(
            {
                "Layer": "Gold",
                "Dataset": relation_name.split(".", 1)[1],
                "Rows": row_count,
                "Path": relation_name,
                "Status": "Ready" if row_count > 0 else "Missing",
            }
        )

    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_pipeline_inventory() -> pd.DataFrame:
    manifest = load_manifest().get("datasets", {})
    rows: list[dict[str, object]] = []

    for layer_name, dataset_name, relative_path, manifest_key in PARQUET_DATASETS:
        dataset_path = DATA_ROOT / relative_path
        manifest_rows = manifest.get(manifest_key, {}).get("rows")
        row_count = int(manifest_rows) if manifest_rows is not None else len(load_parquet(dataset_path))
        rows.append(
            {
                "Layer": layer_name,
                "Dataset": dataset_name,
                "Rows": row_count,
                "Path": _display_path(dataset_path),
                "Status": "Ready" if row_count > 0 else "Missing",
            }
        )

    warehouse_inventory = load_warehouse_inventory()
    if not warehouse_inventory.empty:
        rows.extend(warehouse_inventory.to_dict("records"))

    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_warehouse_kpis() -> dict[str, float | int]:
    trip_distance = load_scalar(
        "select coalesce(sum(total_distance_km), 0) from main_marts.fct_trips", default=0
    )
    sla_met_rate = load_scalar(
        "select coalesce(avg(sla_met_flag) * 100, 0) from main_marts.fct_shipments", default=0
    )
    productive_agent_rate = load_scalar(
        "select coalesce(avg(is_productive_flag) * 100, 0) from main_marts.fct_agent_daily",
        default=0,
    )

    return {
        "trip_rows": int(load_scalar("select count(*) from main_marts.fct_trips", default=0) or 0),
        "shipment_rows": int(
            load_scalar("select count(*) from main_marts.fct_shipments", default=0) or 0
        ),
        "agent_daily_rows": int(
            load_scalar("select count(*) from main_marts.fct_agent_daily", default=0) or 0
        ),
        "trip_distance_km": float(trip_distance or 0),
        "sla_met_rate": float(sla_met_rate or 0),
        "productive_agent_rate": float(productive_agent_rate or 0),
    }


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


def format_compact_number(value: float | int | None) -> str:
    if value is None:
        return "N/A"
    numeric = float(value)
    if numeric >= 1_000_000:
        return f"{numeric / 1_000_000:.1f}M"
    if numeric >= 1_000:
        return f"{numeric / 1_000:.1f}K"
    return f"{numeric:,.0f}" if numeric.is_integer() else f"{numeric:,.1f}"


def render_page_header(title: str, description: str, chips: list[str] | None = None):
    chip_markup = "".join(
        f"<span class='hero-chip'>{chip}</span>" for chip in (chips or []) if chip
    )
    st.markdown(
        f"""
        <section class="hero-card">
            <div class="hero-kicker">Unified Logistics Platform</div>
            <div class="hero-title">{title}</div>
            <div class="hero-copy">{description}</div>
            <div class="hero-chip-row">{chip_markup}</div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_section_label(label: str):
    st.markdown(f"<div class='section-label'>{label}</div>", unsafe_allow_html=True)


def style_figure(fig, height: int | None = None):
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.65)",
        font=dict(family="Space Grotesk, Avenir Next, sans-serif", color=PALETTE["ink"]),
        margin=dict(l=0, r=0, t=32, b=0),
    )
    if height is not None:
        fig.update_layout(height=height)
    return fig


def render_domain_snapshot(title: str, metric: str, description: str):
    st.markdown(
        f"""
        <div class="domain-card">
            <h4>{title}</h4>
            <div class="domain-metric">{metric}</div>
            <p>{description}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# =============================================================================
# Pages
# =============================================================================


def show_overview():
    manifest = load_manifest()
    quality_report = load_latest_quality_report()
    quality_summary = quality_report.get("summary", {})
    warehouse_kpis = load_warehouse_kpis()
    pipeline_inventory = load_pipeline_inventory()
    freshness = manifest.get("generated_at") or quality_report.get("run_time", "Unavailable")

    render_page_header(
        "Operations Control Tower",
        "One view of fleet movement, shipment SLAs, last-mile capacity, and the data quality behind them &mdash; computed from a bundled dataset of 500 shipments, 30 vehicles, and 60 agent shifts across 8 hubs.",
        [
            f"{warehouse_kpis['shipment_rows']} shipments",
            f"{warehouse_kpis['trip_rows']} trips",
            f"{warehouse_kpis['agent_daily_rows']} agent days",
            f"{quality_summary.get('passed', 0)}/{quality_summary.get('total_checks', 0)} quality checks passing",
        ],
    )

    positions = load_parquet(BRONZE_DIR_VEHICLES)
    marts_ready = warehouse_kpis["trip_rows"] > 0 or warehouse_kpis["shipment_rows"] > 0

    render_section_label("Operations at a glance")
    snapshot_cols = st.columns(3)
    with snapshot_cols[0]:
        render_domain_snapshot(
            "Fleet on the road",
            format_compact_number(positions["vehicle_id"].nunique() if len(positions) else 0),
            "Vehicles whose GPS pings rolled up into reconstructed trips for utilization tracking.",
        )
    with snapshot_cols[1]:
        render_domain_snapshot(
            "Trips reconstructed",
            format_compact_number(warehouse_kpis["trip_rows"]),
            "Discrete vehicle trips inferred from telemetry, with distance, duration, and route efficiency.",
        )
    with snapshot_cols[2]:
        render_domain_snapshot(
            "Shipments tracked",
            format_compact_number(warehouse_kpis["shipment_rows"]),
            "End-to-end shipment journeys with SLA outcome, hop count, and delivery attempts.",
        )

    render_section_label("Key operational KPIs")
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric("Trips", warehouse_kpis["trip_rows"])
    with col2:
        st.metric("Shipments", warehouse_kpis["shipment_rows"])
    with col3:
        st.metric("Agent days", warehouse_kpis["agent_daily_rows"])
    with col4:
        st.metric("SLA met", f"{warehouse_kpis['sla_met_rate']:.1f}%")
    with col5:
        st.metric("Distance", f"{warehouse_kpis['trip_distance_km']:.0f} km")
    with col6:
        st.metric("Productive agents", f"{warehouse_kpis['productive_agent_rate']:.1f}%")

    render_section_label("Run readiness")
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Pipeline Inventory")
        st.dataframe(pipeline_inventory, use_container_width=True, hide_index=True)

    with col_right:
        st.subheader("Operator Notes")
        notes = pd.DataFrame(
            {
                "Control": [
                    "Selected mode",
                    "Effective data root",
                    "Warehouse database",
                    "Warehouse ready",
                    "Quality report path",
                ],
                "Value": [
                    "Sample" if USING_SAMPLE else "Live",
                    _display_path(DATA_ROOT),
                    _display_path(WAREHOUSE_DB_PATH),
                    "Yes" if marts_ready else "No",
                    _display_path(QUALITY_DIR),
                ],
            }
        )
        st.dataframe(notes, use_container_width=True, hide_index=True)

        if not marts_ready:
            st.error(
                "Core marts are empty or missing. Re-run `make demo-build` before using this dashboard as a walkthrough."
            )
        elif not quality_report:
            st.warning("No quality artifact was found for the selected data root.")
        else:
            st.success("Warehouse marts and quality artifacts are both available for the selected mode.")

    render_section_label("Core Mart Preview")
    preview_tabs = st.tabs(["Trips", "Shipments", "Agent Daily"])
    preview_relations = [
        "main_marts.fct_trips",
        "main_marts.fct_shipments",
        "main_marts.fct_agent_daily",
    ]
    for tab, relation_name in zip(preview_tabs, preview_relations, strict=False):
        with tab:
            preview = load_warehouse_relation(relation_name, limit=10)
            if preview.empty:
                st.warning(f"{relation_name} is not available in the warehouse yet.")
            else:
                st.dataframe(preview, use_container_width=True, hide_index=True)

    if len(positions) and "latitude" in positions.columns:
        render_section_label("Fleet Footprint")
        st.subheader("Vehicle Positions")
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
        style_figure(fig, height=500)
        st.plotly_chart(fig, use_container_width=True)


def show_pipeline_status():
    quality_report = load_latest_quality_report()
    quality_summary = quality_report.get("summary", {})
    inventory = load_pipeline_inventory()
    warehouse_inventory = load_warehouse_inventory()

    render_page_header(
        "Pipeline Status",
        "A read-only operator panel for the selected data root. It shows which parquet datasets and marts are actually populated, where the artifacts live, and whether quality ran.",
        [
            "Parquet inventory",
            "DuckDB marts",
            f"Mode: {'sample' if USING_SAMPLE else 'live'}",
        ],
    )

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Ready Relations", int((inventory["Status"] == "Ready").sum()) if not inventory.empty else 0)
    with col2:
        st.metric("Missing Relations", int((inventory["Status"] == "Missing").sum()) if not inventory.empty else 0)
    with col3:
        st.metric("Quality Passed", quality_summary.get("passed", 0))
    with col4:
        st.metric("Quality Failed", quality_summary.get("failed", 0))

    st.subheader("Selected Data Root")
    st.code(_display_path(DATA_ROOT), language=None)

    left, right = st.columns(2)
    with left:
        st.subheader("Parquet + Warehouse Inventory")
        st.dataframe(inventory, use_container_width=True, hide_index=True)
    with right:
        st.subheader("Core Mart Status")
        core_inventory = warehouse_inventory[
            warehouse_inventory["Path"].isin(CORE_MART_RELATIONS)
        ].reset_index(drop=True)
        st.dataframe(core_inventory, use_container_width=True, hide_index=True)

        failed_checks = [
            check for check in quality_report.get("checks", []) if not check.get("success", False)
        ]
        if failed_checks:
            st.error("The latest quality run contains failing checks.")
            st.dataframe(pd.DataFrame(failed_checks), use_container_width=True, hide_index=True)
        elif quality_report:
            st.success("The latest quality run is clean for the selected data root.")
        else:
            st.warning("No quality report found for the selected data root.")


def show_warehouse_explorer():
    render_page_header(
        "Warehouse Explorer",
        "Direct previews of the DuckDB models that make the demo credible. Use this to inspect row counts, columns, and example records from the built marts.",
        ["DuckDB", "dbt marts", "Read-only preview"],
    )

    warehouse_inventory = load_warehouse_inventory()
    if warehouse_inventory.empty:
        st.warning(
            "No warehouse relations are available. Run `make demo-build` or `make dbt-build DBT_DATA_MODE=sample` first."
        )
        return

    st.dataframe(warehouse_inventory, use_container_width=True, hide_index=True)

    relation_name = st.selectbox(
        "Inspect relation",
        list(warehouse_inventory["Path"]),
        index=0,
    )
    preview = load_warehouse_relation(relation_name, limit=100)

    metric_left, metric_right, metric_third = st.columns(3)
    with metric_left:
        selected_row_count = int(
            load_scalar(f"select count(*) from {relation_name}", default=0) or 0
        )
        st.metric("Rows", selected_row_count)
    with metric_right:
        st.metric("Columns", len(preview.columns))
    with metric_third:
        st.metric("Warehouse DB", _display_path(WAREHOUSE_DB_PATH))

    if preview.empty:
        st.warning(f"{relation_name} is empty.")
    else:
        st.dataframe(preview, use_container_width=True, hide_index=True)


def show_fleet_dashboard():
    render_page_header(
        "Fleet Telematics",
        "Vehicle telemetry, route reconstruction, and driver behavior analytics built to showcase operational observability at fleet scale.",
        ["GPS traces", "Trip reconstruction", "Driver performance"],
    )

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

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Speed Distribution")
        fig = px.histogram(
            positions.sample(min(5000, len(positions))),
            x="speed_kmh",
            nbins=40,
            color_discrete_sequence=[PALETTE["accent"]],
            labels={"speed_kmh": "Speed (km/h)"},
        )
        style_figure(fig, height=350)
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
            style_figure(fig, height=350)
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
            fig.update_layout(title="Trip Distance Distribution")
            style_figure(fig, height=300)
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
            fig.update_layout(title="Distance vs Duration")
            style_figure(fig, height=300)
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
    style_figure(fig, height=500)
    st.plotly_chart(fig, use_container_width=True)


def show_shipment_dashboard():
    render_page_header(
        "Shipment Tracking",
        "Journey-level visibility across first mile, hub operations, and last-mile SLA performance.",
        ["Lifecycle scans", "Hub throughput", "SLA monitoring"],
    )

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

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Event Pipeline")
        if "event_type" in events.columns:
            event_counts = events["event_type"].value_counts()
            fig = px.bar(
                x=event_counts.index,
                y=event_counts.values,
                labels={"x": "Event Type", "y": "Count"},
                color_discrete_sequence=[PALETTE["accent"]],
            )
            fig.update_layout(xaxis_tickangle=-45)
            style_figure(fig, height=350)
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
                color_discrete_sequence=[PALETTE["accent_alt"]],
            )
            style_figure(fig, height=350)
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
                style_figure(fig, height=300)
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
                style_figure(fig, height=300)
                st.plotly_chart(fig, use_container_width=True)


def show_delivery_dashboard():
    render_page_header(
        "Last-Mile Delivery",
        "Agent efficiency, zone-level performance, and customer experience metrics tied back to operational execution.",
        ["Agent shifts", "Zone analytics", "Customer satisfaction"],
    )

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
            style_figure(fig, height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Rating Distribution")
        if "customer_rating" in events.columns:
            ratings = events["customer_rating"].dropna()
            fig = px.histogram(
                ratings, nbins=5, labels={"value": "Rating"}, color_discrete_sequence=[PALETTE["accent"]]
            )
            style_figure(fig, height=350)
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
    render_page_header(
        "Data Quality",
        "Automated contract checks spanning Bronze ingestion and Silver business entities, with durable JSON artifacts that the demo can point to directly.",
        [
            "Bronze + Silver coverage",
            "JSON reports",
            f"Root: {_display_path(DATA_ROOT)}",
        ],
    )

    if not QUALITY_DIR.exists():
        st.warning("No quality reports found. Run `make quality` first.")
        return

    report = load_latest_quality_report()
    if not report:
        st.warning("No quality reports found.")
        return

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

    if report.get("overall_success"):
        st.success("All quality checks passed!")
    else:
        st.error("Some quality checks failed - see details below.")

    st.caption(f"Latest report loaded from `{_display_path(QUALITY_DIR)}`")

    checks = report.get("checks", [])
    if checks:
        df = pd.DataFrame(checks)
        st.dataframe(df, use_container_width=True, hide_index=True)


def show_architecture():
    render_page_header(
        "Platform Architecture",
        "The intended full-stack architecture plus the smaller verified demo path that keeps this repo runnable and demonstrable on a laptop.",
        ["Kafka", "Spark", "dbt", "Airflow", "DuckDB", "Streamlit"],
    )

    st.info(
        "Verified demo path: sample parquet -> DuckDB source views -> dbt marts -> Streamlit. "
        "Kafka, Spark, and Airflow remain available for live-stack walkthroughs but are not required for the credible demo path."
    )

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
    inject_theme()
    st.sidebar.title("Navigation")

    pages = {
        "Overview": show_overview,
        "Pipeline Status": show_pipeline_status,
        "Warehouse Explorer": show_warehouse_explorer,
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
        "Verified demo path:\n"
        "Parquet | DuckDB | dbt | Quality Reports | Streamlit"
    )
    data_mode = "Sample bundle" if USING_SAMPLE else "Live parquet"
    st.sidebar.caption(f"Data mode: {data_mode}")
    st.sidebar.caption(f"Data root: {_display_path(DATA_ROOT)}")
    st.sidebar.caption(f"Warehouse: {_display_path(WAREHOUSE_DB_PATH)}")


if __name__ == "__main__":
    main()
