"""Tests for local DuckDB source bootstrapping."""

import duckdb

from scripts.bootstrap_duckdb_sources import bootstrap_sources
from scripts.generate_sample_data import build_sample_dataset


class TestDuckDbBootstrap:
    def test_bootstrap_registers_sample_bundle_views(self, tmp_path):
        sample_root = tmp_path / "sample"
        db_path = tmp_path / "warehouse" / "logistics.duckdb"

        build_sample_dataset(sample_root)
        created_views = bootstrap_sources(tmp_path, db_path)

        assert "main.vehicle_positions" in created_views
        assert "main.agent_positions" in created_views

        with duckdb.connect(str(db_path)) as conn:
            vehicle_count = conn.execute("select count(*) from main.vehicle_positions").fetchone()[
                0
            ]
            agent_count = conn.execute("select count(*) from main.agent_positions").fetchone()[0]

        assert vehicle_count > 0
        assert agent_count > 0
