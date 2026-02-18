"""Small validation helpers used across runtime modules."""

from numbers import Real


def require_positive_int(value: int, name: str) -> int:
    """Ensure an integer input is strictly positive."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def require_non_negative_int(value: int, name: str) -> int:
    """Ensure an integer input is non-negative."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def require_positive_number(value: Real, name: str) -> float:
    """Ensure a numeric input is strictly positive."""
    if isinstance(value, bool) or not isinstance(value, Real) or value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return float(value)


def require_non_negative_number(value: Real, name: str) -> float:
    """Ensure a numeric input is non-negative."""
    if isinstance(value, bool) or not isinstance(value, Real) or value < 0:
        raise ValueError(f"{name} must be a non-negative number")
    return float(value)


def require_ratio(value: Real, name: str) -> float:
    """Ensure a ratio input lies between 0 and 1 inclusive."""
    if isinstance(value, bool) or not isinstance(value, Real) or not 0 <= float(value) <= 1:
        raise ValueError(f"{name} must be between 0 and 1")
    return float(value)
