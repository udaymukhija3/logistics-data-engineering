# Demo Walkthrough

This is a 5-minute script for showing the platform end to end. It assumes the
sample-mode demo path: no Kafka, no Spark cluster, no Airflow. The bundled
`data/sample/` directory carries enough data to render every page.

## Three ways to run the demo

| Path                         | Command                                                                                  | Why                              |
|------------------------------|------------------------------------------------------------------------------------------|----------------------------------|
| Hosted (no install)          | Visit the Render URL in [README.md](../README.md)                                        | Fastest, works for non-engineers |
| Docker (no Python toolchain) | `docker build --target dashboard -t logistics-dashboard . && docker run --rm -p 8501:8501 logistics-dashboard` | Mirrors the Render deploy bit-for-bit |
| Local Python                 | `pip install -r requirements-streamlit.txt && streamlit run src/dashboard/app.py`        | Best for tweaking the UI live    |

For the deeper data-engineering walkthrough (rebuild sample bundle, run dbt,
refresh quality), use `make demo-build && make dashboard`.

## Suggested 5-minute walkthrough

The dashboard sidebar has six pages. Hit them in order — each one reinforces
a different layer of the stack.

### 1. Overview (45s)

- Top hero card states the platform's purpose and shows whether you're on
  sample or live data.
- "Platform Snapshot" cards summarize fleet, shipment, and last-mile counts.
- "Service Level KPIs" row shows the headline metrics: vehicles, shipments,
  delivery success rate, SLA compliance.
- "Pipeline Coverage" table lists every Bronze and Silver dataset and its row
  count, which is the easiest way to prove the data layers actually wired up.
- The map is a real Plotly mapbox view of vehicle positions.

> Talk track: "This is the operator surface. It's reading the same DuckDB
> warehouse and parquet datasets that the live pipeline writes — sample mode
> just substitutes a deterministic snapshot."

### 2. Fleet Telematics (45s)

- Filter by vehicle to show single-trip drilldowns.
- Speed distribution and vehicle-type pie are reading directly from the
  bronze GPS feed.
- Distance-vs-duration scatter is reading from `silver.fleet.trips`, which is
  reconstructed from raw GPS pings.

> Talk track: "Trip reconstruction is the first non-trivial transform — we
> turn 2,880 raw GPS pings into 90 logical trips."

### 3. Shipment Tracking (45s)

- Event Pipeline bar shows the lifecycle distribution from origin pickup to
  delivery.
- Hub Activity ranks hubs by event volume — the bottleneck story.
- SLA Status pie is the headline outcome: did each shipment beat its
  promised delivery window.

> Talk track: "11k bronze events become 500 silver journeys. SLA status is
> a derived business outcome, not a raw field."

### 4. Last-Mile Delivery (45s)

- Delivery outcomes pie + customer rating histogram give the human-experience
  picture.
- Zone Performance table is the operational levered question: which zones are
  underperforming.
- Top Performing Agents joins the silver `agent_shifts` aggregate.

### 5. Data Quality (30s)

- Headline counters show pass/fail across 31 contract checks.
- Each check is a row with the rule name, layer, and status.

> Talk track: "Quality is part of the build, not a separate audit. The
> latest report is JSON-on-disk so the dashboard can render it without
> hitting any service."

### 6. Architecture (30s)

- Tech stack table and star-schema model give the system overview.
- Use this page to anchor the deeper architecture questions.

## What to point at if someone wants the live pipeline

Everything from the sample-mode walkthrough is also runnable against the live
streaming stack. See **Full Live-Stack Walkthrough** in
[README.md](../README.md). The dashboard auto-detects whether `data/live/`
contains parquet files and switches modes; no code changes required.

## Troubleshooting on stage

- **Port 8501 in use** — pass `--server.port=8502` or set `PORT=8502` in the
  Docker run.
- **Blank charts** — open the "Data Quality" page first; if it shows
  31/31 the data is healthy. Otherwise re-run `make demo-build`.
- **Render deploy stuck on the first cold start** — Render free-tier pulls
  the image fresh. Check `/_stcore/health` directly; it returns `ok` once
  Streamlit is up.
