"""Unit tests for geospatial helper functions."""

from src.utils.geo import bearing_degrees, haversine_distance_km, move_point


class TestGeoUtils:
    def test_haversine_distance_zero_for_same_point(self):
        assert haversine_distance_km(28.6139, 77.2090, 28.6139, 77.2090) == 0.0

    def test_haversine_distance_matches_expected_city_distance(self):
        # Delhi -> Mumbai is roughly 1,100 km to 1,450 km depending on assumptions.
        distance = haversine_distance_km(28.6139, 77.2090, 19.0760, 72.8777)
        assert 1100 < distance < 1450

    def test_bearing_cardinal_directions(self):
        north = bearing_degrees(0.0, 0.0, 1.0, 0.0)
        east = bearing_degrees(0.0, 0.0, 0.0, 1.0)
        assert north < 1.0 or north > 359.0
        assert 89.0 < east < 91.0

    def test_move_point_preserves_requested_distance(self):
        origin_lat, origin_lng = 12.9716, 77.5946
        moved_lat, moved_lng = move_point(origin_lat, origin_lng, bearing=45, distance_km=10)

        moved_distance = haversine_distance_km(origin_lat, origin_lng, moved_lat, moved_lng)
        assert 9.8 < moved_distance < 10.2
