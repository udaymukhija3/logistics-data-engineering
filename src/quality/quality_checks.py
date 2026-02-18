"""
Data Quality Checks for Unified Logistics Platform.

Validates data quality across Bronze, Silver, and Gold layers using
both Great Expectations-style checks and custom validation logic.
"""

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from src.domain.constants import DELIVERY_EVENT_TYPES, INDIA_BOUNDS, SHIPMENT_EVENT_TYPES
from src.utils.validation import require_non_negative_int, require_ratio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import PySpark - fall back to DuckDB if not available
try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

try:
    import duckdb

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


class DataQualityChecker:
    """
    Runs data quality checks on logistics data.

    Supports both Spark and DuckDB backends for flexibility.
    """

    def __init__(
        self,
        data_path: str = "data",
        output_path: str = "data/quality_reports",
        use_spark: bool = False,
    ):
        self.data_path = Path(data_path)
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.use_spark = use_spark and SPARK_AVAILABLE
        self._duckdb_cache: Dict[str, Any] = {}
        self._spark_cache: Dict[str, "DataFrame"] = {}

        if self.use_spark:
            self.spark = self._create_spark_session()
        elif DUCKDB_AVAILABLE:
            self.conn = duckdb.connect()
        else:
            raise RuntimeError("Neither PySpark nor DuckDB is available")

    def _create_spark_session(self) -> "SparkSession":
        """Create Spark session for data quality checks."""
        return (
            SparkSession.builder.appName("DataQualityChecks")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .getOrCreate()
        )

    def _read_parquet_duckdb(self, path: str) -> Any:
        """Read Parquet files with DuckDB."""
        data_dir = Path(path)
        cache_key = str(data_dir.resolve())
        if cache_key in self._duckdb_cache:
            return self._duckdb_cache[cache_key]

        if not data_dir.exists():
            raise FileNotFoundError(f"Path not found: {path}")
        if not list(data_dir.rglob("*.parquet")):
            raise FileNotFoundError(f"No parquet files found under: {path}")

        parquet_glob = f"{data_dir.as_posix()}/**/*.parquet"
        try:
            df = self.conn.execute("SELECT * FROM read_parquet(?)", [parquet_glob]).fetchdf()
        except Exception:
            # Keep a safe fallback if parameter binding is unavailable in older DuckDB builds.
            escaped_glob = parquet_glob.replace("'", "''")
            df = self.conn.execute(f"SELECT * FROM read_parquet('{escaped_glob}')").fetchdf()

        self._duckdb_cache[cache_key] = df
        return df

    def _read_delta_spark(self, path: str) -> "DataFrame":
        """Read Delta table with Spark."""
        if path in self._spark_cache:
            return self._spark_cache[path]

        df = self.spark.read.format("delta").load(path).cache()
        self._spark_cache[path] = df
        return df

    def _error_result(
        self,
        check: str,
        table_name: str,
        error: Exception,
        column: str = None,
    ) -> Dict[str, Any]:
        """Build a consistent error payload for failed checks."""
        payload: Dict[str, Any] = {
            "check": check,
            "table": table_name,
            "success": False,
            "error": str(error),
        }
        if column:
            payload["column"] = column
        return payload

    def close(self):
        """Close backend resources if initialized."""
        for cached_df in self._spark_cache.values():
            try:
                cached_df.unpersist()
            except Exception:
                logger.exception("Failed to unpersist Spark cached DataFrame")
        self._spark_cache.clear()
        self._duckdb_cache.clear()

        if hasattr(self, "conn"):
            try:
                self.conn.close()
            except Exception:
                logger.exception("Failed to close DuckDB connection")

        if hasattr(self, "spark"):
            try:
                self.spark.stop()
            except Exception:
                logger.exception("Failed to stop Spark session")

    def check_not_null(
        self, table_name: str, column: str, data_path: str, threshold: float = 1.0
    ) -> Dict[str, Any]:
        """Check that a column has no null values (or within threshold)."""
        try:
            require_ratio(threshold, "threshold")

            if self.use_spark:
                df = self._read_delta_spark(data_path)
                total = df.count()
                non_null = df.filter(F.col(column).isNotNull()).count()
            else:
                df = self._read_parquet_duckdb(data_path)
                total = len(df)
                non_null = df[column].notna().sum()

            if total == 0:
                return {
                    "check": "not_null",
                    "table": table_name,
                    "column": column,
                    "success": True,
                    "message": "No data to check",
                    "details": {"total_rows": 0},
                }

            ratio = non_null / total
            success = ratio >= threshold

            return {
                "check": "not_null",
                "table": table_name,
                "column": column,
                "success": success,
                "threshold": threshold,
                "actual_ratio": round(ratio, 4),
                "details": {
                    "total_rows": total,
                    "non_null_rows": int(non_null),
                    "null_rows": int(total - non_null),
                },
            }
        except Exception as e:
            logger.exception("check_not_null failed for %s.%s", table_name, column)
            return self._error_result("not_null", table_name, e, column=column)

    def check_unique(self, table_name: str, column: str, data_path: str) -> Dict[str, Any]:
        """Check that a column has unique values."""
        try:
            if self.use_spark:
                df = self._read_delta_spark(data_path)
                total = df.count()
                distinct = df.select(column).distinct().count()
            else:
                df = self._read_parquet_duckdb(data_path)
                total = len(df)
                distinct = df[column].nunique()

            success = total == distinct

            return {
                "check": "unique",
                "table": table_name,
                "column": column,
                "success": success,
                "details": {
                    "total_rows": total,
                    "distinct_values": int(distinct),
                    "duplicate_count": int(total - distinct),
                },
            }
        except Exception as e:
            logger.exception("check_unique failed for %s.%s", table_name, column)
            return self._error_result("unique", table_name, e, column=column)

    def check_value_range(
        self,
        table_name: str,
        column: str,
        data_path: str,
        min_value: float = None,
        max_value: float = None,
        threshold: float = 0.98,
    ) -> Dict[str, Any]:
        """Check that values are within expected range."""
        try:
            require_ratio(threshold, "threshold")
            if min_value is not None and max_value is not None and min_value > max_value:
                raise ValueError("min_value cannot be greater than max_value")

            if self.use_spark:
                df = self._read_delta_spark(data_path)
                total = df.filter(F.col(column).isNotNull()).count()

                conditions = []
                if min_value is not None:
                    conditions.append(F.col(column) >= min_value)
                if max_value is not None:
                    conditions.append(F.col(column) <= max_value)

                if conditions:
                    from functools import reduce

                    combined = reduce(lambda a, b: a & b, conditions)
                    in_range = df.filter(combined).count()
                else:
                    in_range = total

                stats = df.select(
                    F.min(column).alias("min"),
                    F.max(column).alias("max"),
                    F.avg(column).alias("avg"),
                ).first()
                actual_min = stats["min"]
                actual_max = stats["max"]
                actual_avg = stats["avg"]
            else:
                df = self._read_parquet_duckdb(data_path)
                col_data = df[column].dropna()
                total = len(col_data)

                mask = True
                if min_value is not None:
                    mask = mask & (col_data >= min_value)
                if max_value is not None:
                    mask = mask & (col_data <= max_value)

                in_range = mask.sum() if isinstance(mask, bool) is False else total

                actual_min = col_data.min()
                actual_max = col_data.max()
                actual_avg = col_data.mean()

            if total == 0:
                return {
                    "check": "value_range",
                    "table": table_name,
                    "column": column,
                    "success": True,
                    "message": "No data to check",
                }

            ratio = in_range / total
            success = ratio >= threshold

            return {
                "check": "value_range",
                "table": table_name,
                "column": column,
                "success": success,
                "threshold": threshold,
                "actual_ratio": round(ratio, 4),
                "expected_range": {"min": min_value, "max": max_value},
                "details": {
                    "total_rows": total,
                    "in_range_rows": int(in_range),
                    "actual_min": float(actual_min) if actual_min is not None else None,
                    "actual_max": float(actual_max) if actual_max is not None else None,
                    "actual_avg": float(actual_avg) if actual_avg is not None else None,
                },
            }
        except Exception as e:
            logger.exception("check_value_range failed for %s.%s", table_name, column)
            return self._error_result("value_range", table_name, e, column=column)

    def check_accepted_values(
        self,
        table_name: str,
        column: str,
        data_path: str,
        accepted_values: List[str],
        threshold: float = 1.0,
    ) -> Dict[str, Any]:
        """Check that values are in accepted set."""
        try:
            require_ratio(threshold, "threshold")
            if not accepted_values:
                raise ValueError("accepted_values cannot be empty")

            if self.use_spark:
                df = self._read_delta_spark(data_path)
                total = df.filter(F.col(column).isNotNull()).count()
                valid = df.filter(F.col(column).isin(accepted_values)).count()

                invalid_values = (
                    df.filter(~F.col(column).isin(accepted_values))
                    .select(column)
                    .distinct()
                    .limit(10)
                    .collect()
                )
                invalid_list = [row[0] for row in invalid_values]
            else:
                df = self._read_parquet_duckdb(data_path)
                col_data = df[column].dropna()
                total = len(col_data)
                valid = col_data.isin(accepted_values).sum()
                invalid_list = col_data[~col_data.isin(accepted_values)].unique()[:10].tolist()

            if total == 0:
                return {
                    "check": "accepted_values",
                    "table": table_name,
                    "column": column,
                    "success": True,
                    "message": "No data to check",
                }

            ratio = valid / total
            success = ratio >= threshold

            return {
                "check": "accepted_values",
                "table": table_name,
                "column": column,
                "success": success,
                "threshold": threshold,
                "actual_ratio": round(ratio, 4),
                "accepted_values": accepted_values,
                "details": {
                    "total_rows": total,
                    "valid_rows": int(valid),
                    "invalid_values_sample": invalid_list if not success else [],
                },
            }
        except Exception as e:
            logger.exception("check_accepted_values failed for %s.%s", table_name, column)
            return self._error_result("accepted_values", table_name, e, column=column)

    def check_row_count(
        self, table_name: str, data_path: str, min_count: int = 1
    ) -> Dict[str, Any]:
        """Check that table has minimum number of rows."""
        try:
            require_non_negative_int(min_count, "min_count")

            if self.use_spark:
                df = self._read_delta_spark(data_path)
                count = df.count()
            else:
                df = self._read_parquet_duckdb(data_path)
                count = len(df)

            success = count >= min_count

            return {
                "check": "row_count",
                "table": table_name,
                "success": success,
                "expected_min": min_count,
                "actual_count": count,
            }
        except Exception as e:
            logger.exception("check_row_count failed for %s", table_name)
            return self._error_result("row_count", table_name, e)


def run_bronze_checks(checker: DataQualityChecker) -> List[Dict[str, Any]]:
    """Run quality checks on Bronze layer data."""
    results = []
    bronze_path = checker.data_path / "bronze"

    # Vehicle Positions checks
    vp_path = str(bronze_path / "vehicle_positions")
    if Path(vp_path).exists():
        results.append(checker.check_row_count("vehicle_positions", vp_path, min_count=1))
        results.append(checker.check_not_null("vehicle_positions", "event_id", vp_path))
        results.append(checker.check_not_null("vehicle_positions", "vehicle_id", vp_path))
        results.append(checker.check_not_null("vehicle_positions", "latitude", vp_path))
        results.append(checker.check_not_null("vehicle_positions", "longitude", vp_path))
        results.append(
            checker.check_value_range(
                "vehicle_positions",
                "latitude",
                vp_path,
                min_value=INDIA_BOUNDS["lat_min"],
                max_value=INDIA_BOUNDS["lat_max"],
                threshold=0.98,
            )
        )
        results.append(
            checker.check_value_range(
                "vehicle_positions",
                "longitude",
                vp_path,
                min_value=INDIA_BOUNDS["lng_min"],
                max_value=INDIA_BOUNDS["lng_max"],
                threshold=0.98,
            )
        )
        results.append(
            checker.check_value_range(
                "vehicle_positions",
                "speed_kmh",
                vp_path,
                min_value=0,
                max_value=200,
                threshold=0.99,
            )
        )
    else:
        results.append(
            {
                "check": "table_exists",
                "table": "vehicle_positions",
                "success": False,
                "error": f"Path not found: {vp_path}",
            }
        )

    # Shipment Events checks
    se_path = str(bronze_path / "shipment_events")
    if Path(se_path).exists():
        results.append(checker.check_row_count("shipment_events", se_path, min_count=1))
        results.append(checker.check_not_null("shipment_events", "event_id", se_path))
        results.append(checker.check_not_null("shipment_events", "shipment_id", se_path))
        results.append(checker.check_not_null("shipment_events", "event_type", se_path))
        results.append(
            checker.check_accepted_values(
                "shipment_events",
                "event_type",
                se_path,
                accepted_values=list(SHIPMENT_EVENT_TYPES),
            )
        )
    else:
        results.append(
            {
                "check": "table_exists",
                "table": "shipment_events",
                "success": False,
                "error": f"Path not found: {se_path}",
            }
        )

    # Agent Positions checks
    ap_path = str(bronze_path / "agent_positions")
    if Path(ap_path).exists():
        results.append(checker.check_row_count("agent_positions", ap_path, min_count=1))
        results.append(checker.check_not_null("agent_positions", "agent_id", ap_path))
        results.append(
            checker.check_value_range(
                "agent_positions",
                "latitude",
                ap_path,
                min_value=INDIA_BOUNDS["lat_min"],
                max_value=INDIA_BOUNDS["lat_max"],
                threshold=0.98,
            )
        )
        results.append(
            checker.check_value_range(
                "agent_positions",
                "longitude",
                ap_path,
                min_value=INDIA_BOUNDS["lng_min"],
                max_value=INDIA_BOUNDS["lng_max"],
                threshold=0.98,
            )
        )
    else:
        results.append(
            {
                "check": "table_exists",
                "table": "agent_positions",
                "success": False,
                "error": f"Path not found: {ap_path}",
            }
        )

    # Delivery Events checks
    de_path = str(bronze_path / "delivery_events")
    if Path(de_path).exists():
        results.append(checker.check_row_count("delivery_events", de_path, min_count=1))
        results.append(checker.check_not_null("delivery_events", "event_id", de_path))
        results.append(checker.check_not_null("delivery_events", "agent_id", de_path))
        results.append(
            checker.check_accepted_values(
                "delivery_events",
                "event_type",
                de_path,
                accepted_values=list(DELIVERY_EVENT_TYPES),
            )
        )
        results.append(
            checker.check_value_range(
                "delivery_events",
                "customer_rating",
                de_path,
                min_value=1,
                max_value=5,
                threshold=1.0,
            )
        )
    else:
        results.append(
            {
                "check": "table_exists",
                "table": "delivery_events",
                "success": False,
                "error": f"Path not found: {de_path}",
            }
        )

    return results


def run_silver_checks(checker: DataQualityChecker) -> List[Dict[str, Any]]:
    """Run quality checks on Silver layer data."""
    results = []
    silver_path = checker.data_path / "silver"

    # Trips checks
    trips_path = str(silver_path / "fleet" / "trips")
    if Path(trips_path).exists():
        results.append(checker.check_row_count("trips", trips_path, min_count=1))
        results.append(checker.check_not_null("trips", "trip_id", trips_path))
        results.append(checker.check_unique("trips", "trip_id", trips_path))
        results.append(
            checker.check_value_range(
                "trips",
                "total_distance_km",
                trips_path,
                min_value=0,
                max_value=2000,
                threshold=0.99,
            )
        )
    else:
        results.append(
            {
                "check": "table_exists",
                "table": "trips",
                "success": False,
                "error": f"Path not found: {trips_path}",
            }
        )

    # Journeys checks
    journeys_path = str(silver_path / "shipment" / "journeys")
    if Path(journeys_path).exists():
        results.append(checker.check_row_count("journeys", journeys_path, min_count=1))
        results.append(checker.check_not_null("journeys", "shipment_id", journeys_path))
        results.append(checker.check_unique("journeys", "shipment_id", journeys_path))
    else:
        results.append(
            {
                "check": "table_exists",
                "table": "journeys",
                "success": False,
                "error": f"Path not found: {journeys_path}",
            }
        )

    # Agent Shifts checks
    shifts_path = str(silver_path / "delivery" / "agent_shifts")
    if Path(shifts_path).exists():
        results.append(checker.check_row_count("agent_shifts", shifts_path, min_count=1))
        results.append(checker.check_not_null("agent_shifts", "agent_id", shifts_path))
    else:
        results.append(
            {
                "check": "table_exists",
                "table": "agent_shifts",
                "success": False,
                "error": f"Path not found: {shifts_path}",
            }
        )

    return results


def run_quality_checks(
    layer: str = "bronze",
    data_path: str = "data",
    output_path: str = "data/quality_reports",
    use_spark: bool = False,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Run data quality checks for specified layer.

    Returns:
        Tuple of (overall_success, results_dict)
    """
    logger.info(f"=" * 60)
    logger.info(f"Running Data Quality Checks - Layer: {layer.upper()}")
    logger.info(f"=" * 60)

    checker = DataQualityChecker(data_path=data_path, output_path=output_path, use_spark=use_spark)
    try:
        if layer == "bronze":
            results = run_bronze_checks(checker)
        elif layer == "silver":
            results = run_silver_checks(checker)
        elif layer == "all":
            results = run_bronze_checks(checker) + run_silver_checks(checker)
        else:
            raise ValueError("layer must be one of: bronze, silver, all")
    finally:
        checker.close()

    # Calculate summary
    total_checks = len(results)
    passed_checks = sum(1 for r in results if r.get("success", False))
    failed_checks = total_checks - passed_checks
    overall_success = failed_checks == 0

    # Build report
    report = {
        "layer": layer,
        "run_time": datetime.utcnow().isoformat(),
        "overall_success": overall_success,
        "summary": {
            "total_checks": total_checks,
            "passed": passed_checks,
            "failed": failed_checks,
            "pass_rate": round(passed_checks / total_checks * 100, 2) if total_checks > 0 else 0,
        },
        "checks": results,
    }

    # Log summary
    logger.info(f"\n{'=' * 60}")
    logger.info(f"QUALITY CHECK SUMMARY")
    logger.info(f"{'=' * 60}")
    logger.info(f"Layer: {layer}")
    logger.info(f"Total Checks: {total_checks}")
    logger.info(f"Passed: {passed_checks}")
    logger.info(f"Failed: {failed_checks}")
    logger.info(f"Pass Rate: {report['summary']['pass_rate']}%")
    logger.info(f"Overall Status: {'PASSED' if overall_success else 'FAILED'}")
    logger.info(f"{'=' * 60}")

    # Log failed checks
    if not overall_success:
        logger.warning("\nFailed Checks:")
        for check in results:
            if not check.get("success", False):
                logger.warning(
                    f"  - {check.get('table', 'unknown')}.{check.get('column', 'N/A')}: "
                    f"{check.get('check', 'unknown')} - {check.get('error', 'Check failed')}"
                )

    # Save report
    report_path = (
        Path(output_path) / f"quality_{layer}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)

    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info(f"\nReport saved to: {report_path}")

    return overall_success, report


def main():
    parser = argparse.ArgumentParser(description="Run data quality checks")
    parser.add_argument(
        "--layer",
        type=str,
        default="bronze",
        choices=["bronze", "silver", "all"],
        help="Data layer to check",
    )
    parser.add_argument("--data-path", type=str, default="data", help="Path to data directory")
    parser.add_argument(
        "--output-path", type=str, default="data/quality_reports", help="Path for quality reports"
    )
    parser.add_argument("--use-spark", action="store_true", help="Use Spark instead of DuckDB")

    args = parser.parse_args()

    success, _ = run_quality_checks(
        layer=args.layer,
        data_path=args.data_path,
        output_path=args.output_path,
        use_spark=args.use_spark,
    )

    if not success:
        exit(1)


if __name__ == "__main__":
    main()
