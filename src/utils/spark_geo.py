"""Spark SQL geography helpers."""

from pyspark.sql import Column
from pyspark.sql import functions as F

EARTH_RADIUS_KM = 6371.0


def haversine_distance_km_expr(
    lat1: Column,
    lng1: Column,
    lat2: Column,
    lng2: Column,
) -> Column:
    """Return a Spark column expression for haversine distance in km."""
    has_null = lat1.isNull() | lng1.isNull() | lat2.isNull() | lng2.isNull()

    lat1_rad = F.radians(lat1)
    lat2_rad = F.radians(lat2)
    delta_lat = F.radians(lat2 - lat1)
    delta_lng = F.radians(lng2 - lng1)

    a = F.pow(F.sin(delta_lat / 2), 2) + F.cos(lat1_rad) * F.cos(lat2_rad) * F.pow(
        F.sin(delta_lng / 2), 2
    )
    # Floating-point error can make (1 - a) slightly negative for near-identical points.
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(F.greatest(F.lit(0.0), F.lit(1.0) - a)))

    return F.when(has_null, F.lit(None)).otherwise(F.lit(EARTH_RADIUS_KM) * c)
