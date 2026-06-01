---
title: Unified Logistics Data Platform
emoji: 🚚
colorFrom: indigo
colorTo: yellow
sdk: streamlit
sdk_version: 1.53.1
app_file: streamlit_app.py
pinned: true
license: mit
short_description: Logistics lakehouse with a live operations dashboard.
---

# Logistics Data Platform

One link to show recruiters:

<https://logistics-data-engineering.vercel.app/>

The page shows the running dashboard first, then concise notes on features,
data flow, and engineering choices. Hugging Face is only the Streamlit runtime
embedded inside that page.

## What Runs

- Generated logistics events for fleet, shipment, and last-mile delivery.
- Batch jobs that reconstruct trips, shipment journeys, and agent shifts.
- dbt marts in DuckDB for dashboard-ready facts.
- JSON quality reports for schema, freshness, and business-rule checks.
- Streamlit dashboard reading the built data artifacts.

## Features And Engineering

| Feature | Engineering implementation |
| --- | --- |
| Fleet tracking | GPS events -> trip reconstruction -> `fct_trips` |
| Shipment SLAs | Hub scans -> journey reconstruction -> `fct_shipments` |
| Last-mile ops | Delivery attempts -> agent and zone marts |
| Data quality | Contract, freshness, and business-rule checks |
| Explorer | DuckDB relation previews and row counts |

## Demo Data

- 500 shipments
- 30 vehicles
- 60 agent shifts
- 8 hubs

## Run Locally

```bash
make demo-build
make dashboard
```

Open <http://localhost:8501>.

## Repo Map

| Path | Purpose |
| --- | --- |
| `src/simulators/` | event generation |
| `src/batch/` | trip, journey, and shift reconstruction |
| `dbt_logistics/` | warehouse models and tests |
| `src/quality/` | quality checks and reports |
| `src/dashboard/app.py` | Streamlit dashboard |
| `site/` | Vercel recruiter page |

## Verified Path

```text
sample parquet -> DuckDB source views -> dbt marts -> quality reports -> Streamlit
```
