"""Shared domain constants for logistics pipelines and simulators."""

from typing import Final

INDIA_BOUNDS: Final = {
    "lat_min": 8.0,
    "lat_max": 37.0,
    "lng_min": 68.0,
    "lng_max": 97.5,
}

SHIPMENT_EVENT_TYPES: Final = (
    "CREATED",
    "PICKUP_SCHEDULED",
    "PICKED_UP",
    "HUB_ARRIVED",
    "HUB_INSCAN",
    "HUB_SORTED",
    "HUB_OUTSCAN",
    "HUB_DEPARTED",
    "IN_TRANSIT",
    "OUT_FOR_DELIVERY",
    "DELIVERY_ATTEMPTED",
    "DELIVERED",
    "DELIVERY_FAILED",
    "RETURNED_TO_ORIGIN",
    "LOST",
    "DAMAGED",
)

DELIVERY_EVENT_TYPES: Final = (
    "DELIVERED",
    "DELIVERY_ATTEMPTED",
    "DELIVERY_FAILED",
)

DELIVERY_FAILURE_REASONS: Final = (
    "CUSTOMER_NOT_AVAILABLE",
    "WRONG_ADDRESS",
    "ACCESS_RESTRICTED",
    "CUSTOMER_REFUSED",
    "PAYMENT_ISSUE",
    "DAMAGED_PACKAGE",
    "OTHER",
)

TOPIC_VEHICLE_POSITIONS: Final = "vehicle_positions"
TOPIC_VEHICLE_TELEMETRY: Final = "vehicle_telemetry"
TOPIC_SHIPMENT_EVENTS: Final = "shipment_events"
TOPIC_AGENT_POSITIONS: Final = "agent_positions"
TOPIC_DELIVERY_EVENTS: Final = "delivery_events"
TOPIC_ALERTS: Final = "alerts"
