"""Integration tests for quality checks against sample data."""

from src.quality.quality_checks import run_quality_checks


class TestQualityChecksIntegration:
    def test_run_quality_checks_generates_report_for_sample_data(self, tmp_path):
        success, report = run_quality_checks(
            layer="all",
            data_path="data/sample",
            output_path=str(tmp_path),
            use_spark=False,
        )

        assert isinstance(success, bool)
        assert report["layer"] == "all"
        assert "summary" in report
        assert report["summary"]["total_checks"] > 0
        assert isinstance(report.get("checks"), list)

        report_files = list(tmp_path.glob("quality_all_*.json"))
        assert len(report_files) == 1
