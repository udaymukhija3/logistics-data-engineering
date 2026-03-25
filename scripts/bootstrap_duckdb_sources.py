"""
Register Bronze and Silver parquet datasets as DuckDB views for dbt.

This gives the dbt project a reliable local bridge from filesystem datasets
to `source()` tables without requiring any external warehouse bootstrap.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

DEFAULT_DB_PATH = Path("data/warehouse/logistics.duckdb")
SOURCE_LAYOUT = {
    "bronze": (
        "vehicle_positions",
        "vehicle_telemetry",
        "shipment_events",
        "agent_positions",
        "delivery_events",
        "alerts",
    ),
    "silver": (
        "trips",
        "journeys",
        "agent_shifts",
        "zone_performance",
    ),
}
SILVER_PATHS = {
    "trips": "fleet/trips",
    "journeys": "shipment/journeys",
    "agent_shifts": "delivery/agent_shifts",
    "zone_performance": "delivery/zone_performance",
}


def _dataset_path(root: Path, layer: str, table: str) -> Path:
    if layer == "bronze":
        return root / layer / table
    return root / "silver" / SILVER_PATHS[table]


def _resolve_data_root(data_path: Path) -> Path:
    candidates = [data_path, data_path / "sample"]
    best_root = data_path
    best_score = -1

    for candidate in candidates:
        score = 0
        for layer, tables in SOURCE_LAYOUT.items():
            for table in tables:
                dataset_path = _dataset_path(candidate, layer, table)
                if dataset_path.exists() and any(dataset_path.rglob("*.parquet")):
                    score += 1
        if score > best_score:
            best_root = candidate
            best_score = score

    return best_root


def bootstrap_sources(data_path: Path, db_path: Path) -> list[str]:
    """Create or replace DuckDB views for available parquet-backed datasets."""
    source_root = _resolve_data_root(data_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    created_views: list[str] = []

    with duckdb.connect(str(db_path)) as conn:
        conn.execute("create schema if not exists main")

        for layer, tables in SOURCE_LAYOUT.items():
            for table in tables:
                dataset_path = _dataset_path(source_root, layer, table)
                if not dataset_path.exists():
                    continue

                parquet_files = list(dataset_path.rglob("*.parquet"))
                if not parquet_files:
                    continue

                parquet_glob = dataset_path.as_posix() + "/**/*.parquet"
                escaped_glob = parquet_glob.replace("'", "''")
                conn.execute(
                    f"""
                    create or replace view main.{table} as
                    select *
                    from read_parquet('{escaped_glob}', union_by_name=true)
                    """
                )
                created_views.append(f"main.{table}")

    return created_views


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap DuckDB source views for dbt")
    parser.add_argument(
        "--data-path",
        default="data",
        help="Root data path containing bronze/ and silver/ directories",
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Path to the DuckDB database file",
    )
    args = parser.parse_args()

    data_path = Path(args.data_path).resolve()
    db_path = Path(args.db_path).resolve()
    created_views = bootstrap_sources(data_path, db_path)

    print(f"DuckDB database: {db_path}")
    print(f"Source root: {_resolve_data_root(data_path)}")
    if created_views:
        print("Registered views:")
        for view_name in created_views:
            print(f"  - {view_name}")
    else:
        print(f"No parquet-backed datasets found under {data_path}")


if __name__ == "__main__":
    main()
