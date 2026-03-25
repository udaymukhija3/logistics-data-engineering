"""Regression tests for the sample data bundle generator."""

import json

import pandas as pd

from scripts.generate_sample_data import build_sample_dataset
from src.quality.quality_checks import run_quality_checks


class TestSampleDataGenerator:
    def test_generated_bundle_matches_quality_contracts(self, tmp_path):
        result = build_sample_dataset(tmp_path)

        success, report = run_quality_checks(
            layer="all",
            data_path=str(tmp_path),
            output_path=str(tmp_path / "quality_recheck"),
            use_spark=False,
        )

        assert success is True
        assert report["overall_success"] is True
        assert result["quality_report"]["overall_success"] is True

    def test_generated_bundle_contains_canonical_columns(self, tmp_path):
        build_sample_dataset(tmp_path)

        vehicle_positions = pd.read_parquet(
            tmp_path / "bronze" / "vehicle_positions" / "sample_data.parquet"
        )
        trips = pd.read_parquet(tmp_path / "silver" / "fleet" / "trips" / "sample_data.parquet")
        manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))

        assert {"fuel_level_pct", "trip_id", "state"} <= set(vehicle_positions.columns)
        assert {"total_distance_km", "trip_duration_minutes"} <= set(trips.columns)
        assert manifest["quality"]["failed"] == 0
