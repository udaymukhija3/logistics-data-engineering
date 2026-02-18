"""Unit tests for shared validation helpers."""

import pytest

from src.utils.validation import (
    require_non_negative_int,
    require_non_negative_number,
    require_positive_int,
    require_positive_number,
    require_ratio,
)


class TestValidationHelpers:
    def test_require_positive_int_accepts_positive_int(self):
        assert require_positive_int(3, "count") == 3

    @pytest.mark.parametrize("value", [0, -1, 1.5, True, "3", None])
    def test_require_positive_int_rejects_invalid_values(self, value):
        with pytest.raises(ValueError):
            require_positive_int(value, "count")

    def test_require_non_negative_int_accepts_zero(self):
        assert require_non_negative_int(0, "offset") == 0

    @pytest.mark.parametrize("value", [-1, 2.2, False, "0", None])
    def test_require_non_negative_int_rejects_invalid_values(self, value):
        with pytest.raises(ValueError):
            require_non_negative_int(value, "offset")

    def test_require_positive_number_accepts_numeric_values(self):
        assert require_positive_number(1, "rate") == 1.0
        assert require_positive_number(1.5, "rate") == 1.5

    @pytest.mark.parametrize("value", [0, -0.1, False, "1.5", None])
    def test_require_positive_number_rejects_invalid_values(self, value):
        with pytest.raises(ValueError):
            require_positive_number(value, "rate")

    def test_require_non_negative_number_accepts_zero(self):
        assert require_non_negative_number(0, "delay") == 0.0

    @pytest.mark.parametrize("value", [-0.1, True, "0.1", None])
    def test_require_non_negative_number_rejects_invalid_values(self, value):
        with pytest.raises(ValueError):
            require_non_negative_number(value, "delay")

    def test_require_ratio_accepts_bounds(self):
        assert require_ratio(0, "threshold") == 0.0
        assert require_ratio(1, "threshold") == 1.0
        assert require_ratio(0.42, "threshold") == 0.42

    @pytest.mark.parametrize("value", [-0.01, 1.01, False, "0.5", None])
    def test_require_ratio_rejects_invalid_values(self, value):
        with pytest.raises(ValueError):
            require_ratio(value, "threshold")
