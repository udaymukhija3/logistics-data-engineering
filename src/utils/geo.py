"""Geospatial utility functions shared across simulators."""

import math
from typing import Tuple

EARTH_RADIUS_KM = 6371.0


def haversine_distance_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calculate great-circle distance between two points in kilometers."""
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lng = math.radians(lng2 - lng1)

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lng / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_RADIUS_KM * c


def bearing_degrees(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calculate bearing from point 1 to point 2 in degrees from north."""
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lng = math.radians(lng2 - lng1)

    x = math.sin(delta_lng) * math.cos(lat2_rad)
    y = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(
        lat2_rad
    ) * math.cos(delta_lng)

    return (math.degrees(math.atan2(x, y)) + 360) % 360


def move_point(lat: float, lng: float, bearing: float, distance_km: float) -> Tuple[float, float]:
    """Move a point by distance_km in the bearing direction."""
    lat_rad = math.radians(lat)
    lng_rad = math.radians(lng)
    bearing_rad = math.radians(bearing)

    new_lat_rad = math.asin(
        math.sin(lat_rad) * math.cos(distance_km / EARTH_RADIUS_KM)
        + math.cos(lat_rad) * math.sin(distance_km / EARTH_RADIUS_KM) * math.cos(bearing_rad)
    )

    new_lng_rad = lng_rad + math.atan2(
        math.sin(bearing_rad) * math.sin(distance_km / EARTH_RADIUS_KM) * math.cos(lat_rad),
        math.cos(distance_km / EARTH_RADIUS_KM) - math.sin(lat_rad) * math.sin(new_lat_rad),
    )

    return math.degrees(new_lat_rad), math.degrees(new_lng_rad)
