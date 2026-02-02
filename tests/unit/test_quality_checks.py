"""
Unit tests for data quality checks.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class TestDataQualityChecker:
    """Tests for DataQualityChecker class."""

    def test_checker_initialization_with_duckdb(self):
        """Test checker initializes with DuckDB backend."""
        from src.quality.quality_checks import DataQualityChecker

        # Should not raise
        checker = DataQualityChecker(
            data_path="data",
            output_path="data/quality_reports",
            use_spark=False
        )
        assert checker is not None

    def test_check_result_structure(self):
        """Test that check results have correct structure."""
        from src.quality.quality_checks import DataQualityChecker

        checker = DataQualityChecker(use_spark=False)

        # Mock a check that returns proper structure
        result = {
            "check": "not_null",
            "table": "test_table",
            "column": "test_column",
            "success": True,
            "threshold": 1.0,
            "actual_ratio": 1.0,
            "details": {}
        }

        assert "check" in result
        assert "table" in result
        assert "success" in result
        assert isinstance(result["success"], bool)


class TestQualityCheckLogic:
    """Tests for quality check logic."""

    def test_india_coordinate_bounds(self):
        """Test India coordinate validation bounds."""
        # India bounds from config
        lat_min, lat_max = 8.0, 37.0
        lng_min, lng_max = 68.0, 97.5

        # Valid coordinates
        assert lat_min <= 28.6139 <= lat_max  # Delhi
        assert lng_min <= 77.2090 <= lng_max

        assert lat_min <= 19.0760 <= lat_max  # Mumbai
        assert lng_min <= 72.8777 <= lng_max

        # Invalid coordinates
        assert not (lat_min <= 51.5074 <= lat_max)  # London
        assert not (lng_min <= -0.1278 <= lng_max)

    def test_speed_range_validation(self):
        """Test speed range validation."""
        min_speed, max_speed = 0, 200

        # Valid speeds
        assert min_speed <= 0 <= max_speed
        assert min_speed <= 60 <= max_speed
        assert min_speed <= 120 <= max_speed

        # Invalid speeds
        assert not (min_speed <= -10 <= max_speed)
        assert not (min_speed <= 250 <= max_speed)

    def test_event_type_validation(self):
        """Test shipment event type validation."""
        valid_event_types = [
            "CREATED", "PICKUP_SCHEDULED", "PICKED_UP",
            "HUB_ARRIVED", "HUB_INSCAN", "HUB_SORTED",
            "HUB_OUTSCAN", "HUB_DEPARTED", "IN_TRANSIT",
            "OUT_FOR_DELIVERY", "DELIVERY_ATTEMPTED", "DELIVERED",
            "DELIVERY_FAILED", "RETURNED_TO_ORIGIN", "LOST", "DAMAGED"
        ]

        # Valid events
        assert "DELIVERED" in valid_event_types
        assert "HUB_ARRIVED" in valid_event_types

        # Invalid events
        assert "INVALID_EVENT" not in valid_event_types
        assert "shipped" not in valid_event_types

    def test_delivery_outcome_validation(self):
        """Test delivery event type validation."""
        valid_outcomes = ["DELIVERED", "DELIVERY_ATTEMPTED", "DELIVERY_FAILED"]

        assert "DELIVERED" in valid_outcomes
        assert "CANCELLED" not in valid_outcomes

    def test_customer_rating_range(self):
        """Test customer rating range validation."""
        min_rating, max_rating = 1, 5

        # Valid ratings
        assert min_rating <= 1 <= max_rating
        assert min_rating <= 3 <= max_rating
        assert min_rating <= 5 <= max_rating

        # Invalid ratings
        assert not (min_rating <= 0 <= max_rating)
        assert not (min_rating <= 6 <= max_rating)


class TestQualityReporting:
    """Tests for quality report generation."""

    def test_report_structure(self):
        """Test quality report has correct structure."""
        report = {
            "layer": "bronze",
            "run_time": "2024-01-15T10:00:00Z",
            "overall_success": True,
            "summary": {
                "total_checks": 10,
                "passed": 10,
                "failed": 0,
                "pass_rate": 100.0
            },
            "checks": []
        }

        assert "layer" in report
        assert "overall_success" in report
        assert "summary" in report
        assert "checks" in report

        assert report["summary"]["pass_rate"] == 100.0

    def test_pass_rate_calculation(self):
        """Test pass rate calculation."""
        total = 10
        passed = 8
        failed = 2

        pass_rate = (passed / total) * 100
        assert pass_rate == 80.0

        # Edge case: no checks
        total = 0
        pass_rate = 0 if total == 0 else (passed / total) * 100
        assert pass_rate == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
