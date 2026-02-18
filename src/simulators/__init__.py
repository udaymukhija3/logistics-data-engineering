"""Public exports for simulator modules."""

from .base import BaseSimulator

__all__ = ["VehicleSimulator", "ShipmentSimulator", "DeliverySimulator", "BaseSimulator"]


def __getattr__(name: str):
    if name == "VehicleSimulator":
        from .vehicle_simulator import VehicleSimulator

        return VehicleSimulator
    if name == "ShipmentSimulator":
        from .shipment_simulator import ShipmentSimulator

        return ShipmentSimulator
    if name == "DeliverySimulator":
        from .delivery_simulator import DeliverySimulator

        return DeliverySimulator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
