"""
Unit tests for data simulators.
"""

import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.simulators.vehicle_simulator import VehicleSimulator, Vehicle
from src.simulators.shipment_simulator import ShipmentSimulator, Shipment, ShipmentState
from src.simulators.delivery_simulator import DeliverySimulator, DeliveryAgent


class TestVehicleSimulator:
    """Tests for VehicleSimulator."""

    def test_initialization(self):
        """Test simulator initializes with correct number of vehicles."""
        simulator = VehicleSimulator(num_vehicles=10)
        assert len(simulator.vehicles) == 10

    def test_vehicle_has_required_attributes(self):
        """Test that vehicles have all required attributes."""
        simulator = VehicleSimulator(num_vehicles=1)
        vehicle = simulator.vehicles[0]

        assert hasattr(vehicle, 'vehicle_id')
        assert hasattr(vehicle, 'driver_id')
        assert hasattr(vehicle, 'current_lat')
        assert hasattr(vehicle, 'current_lng')
        assert hasattr(vehicle, 'current_speed')
        assert hasattr(vehicle, 'vehicle_type')

    def test_vehicle_position_in_india(self):
        """Test that vehicles start within India bounds."""
        simulator = VehicleSimulator(num_vehicles=50)

        for vehicle in simulator.vehicles:
            assert 8.0 <= vehicle.current_lat <= 37.0, f"Latitude {vehicle.current_lat} out of bounds"
            assert 68.0 <= vehicle.current_lng <= 97.5, f"Longitude {vehicle.current_lng} out of bounds"

    def test_generate_event_returns_dict(self):
        """Test that generate_event returns a dictionary."""
        simulator = VehicleSimulator(num_vehicles=1)
        event = simulator.generate_event(simulator.vehicles[0])

        assert isinstance(event, dict)
        assert 'event_id' in event
        assert 'vehicle_id' in event
        assert 'latitude' in event
        assert 'longitude' in event
        assert 'speed_kmh' in event

    def test_haversine_distance(self):
        """Test haversine distance calculation."""
        simulator = VehicleSimulator(num_vehicles=1)

        # Delhi to Mumbai approx 1400km
        distance = simulator._haversine_distance(28.6139, 77.2090, 19.0760, 72.8777)
        assert 1100 < distance < 1500, f"Expected ~1400km, got {distance}km"

        # Same point should be 0
        distance = simulator._haversine_distance(28.6139, 77.2090, 28.6139, 77.2090)
        assert distance < 0.001


class TestShipmentSimulator:
    """Tests for ShipmentSimulator."""

    def test_initialization(self):
        """Test simulator initializes correctly."""
        simulator = ShipmentSimulator(shipments_per_minute=5)
        assert simulator.shipments_per_minute == 5
        assert len(simulator.active_shipments) == 0

    def test_create_shipment(self):
        """Test shipment creation."""
        simulator = ShipmentSimulator()
        shipment = simulator._create_shipment()

        assert isinstance(shipment, Shipment)
        assert shipment.shipment_id.startswith('SHP-')
        assert shipment.origin_hub in simulator.hub_network
        assert shipment.destination_hub in simulator.hub_network
        assert len(shipment.route) >= 1

    def test_find_route_returns_valid_path(self):
        """Test route finding between hubs."""
        simulator = ShipmentSimulator()

        route = simulator._find_route('HUB_DEL_01', 'HUB_BLR_01')
        assert route[0] == 'HUB_DEL_01'
        assert route[-1] == 'HUB_BLR_01'
        assert len(route) >= 2

    def test_shipment_state_transitions(self):
        """Test valid state transitions."""
        simulator = ShipmentSimulator()
        shipment = simulator._create_shipment()

        # Initial state should be CREATED
        assert shipment.state == ShipmentState.CREATED

        # Get next state
        next_state = simulator._get_next_state(shipment)
        assert next_state == ShipmentState.PICKUP_SCHEDULED

    def test_generate_event_returns_dict(self):
        """Test event generation."""
        simulator = ShipmentSimulator()
        shipment = simulator._create_shipment()
        event = simulator.generate_event(shipment)

        assert isinstance(event, dict)
        assert 'event_id' in event
        assert 'shipment_id' in event
        assert 'event_type' in event
        assert 'timestamp' in event


class TestDeliverySimulator:
    """Tests for DeliverySimulator."""

    def test_initialization(self):
        """Test simulator initializes with correct number of agents."""
        simulator = DeliverySimulator(num_agents=20)
        assert len(simulator.agents) == 20

    def test_agent_has_required_attributes(self):
        """Test that agents have all required attributes."""
        simulator = DeliverySimulator(num_agents=1)
        agent = simulator.agents[0]

        assert hasattr(agent, 'agent_id')
        assert hasattr(agent, 'zone_id')
        assert hasattr(agent, 'current_lat')
        assert hasattr(agent, 'current_lng')
        assert hasattr(agent, 'vehicle_type')

    def test_agent_position_in_india(self):
        """Test that agents start within India bounds."""
        simulator = DeliverySimulator(num_agents=50)

        for agent in simulator.agents:
            assert 8.0 <= agent.current_lat <= 37.0, f"Latitude {agent.current_lat} out of bounds"
            assert 68.0 <= agent.current_lng <= 97.5, f"Longitude {agent.current_lng} out of bounds"

    def test_agents_have_orders(self):
        """Test that agents start with pending orders."""
        simulator = DeliverySimulator(num_agents=10)

        # At least some agents should have orders
        agents_with_orders = sum(1 for a in simulator.agents if len(a.pending_orders) > 0)
        assert agents_with_orders > 0

    def test_generate_event_returns_dict(self):
        """Test event generation."""
        simulator = DeliverySimulator(num_agents=1)
        event = simulator.generate_event(simulator.agents[0])

        assert isinstance(event, dict)
        assert 'event_id' in event
        assert 'agent_id' in event
        assert 'latitude' in event
        assert 'longitude' in event


class TestDataQuality:
    """Tests for data quality validations."""

    def test_event_ids_are_unique(self):
        """Test that generated event IDs are unique."""
        simulator = VehicleSimulator(num_vehicles=10)

        event_ids = set()
        for vehicle in simulator.vehicles:
            for _ in range(10):
                event = simulator.generate_event(vehicle)
                assert event['event_id'] not in event_ids, "Duplicate event ID"
                event_ids.add(event['event_id'])

    def test_timestamps_are_valid(self):
        """Test that timestamps are in valid format."""
        simulator = VehicleSimulator(num_vehicles=1)
        event = simulator.generate_event(simulator.vehicles[0])

        timestamp = event['timestamp']
        assert timestamp.endswith('Z'), "Timestamp should be UTC"
        assert 'T' in timestamp, "Timestamp should be ISO format"

    def test_speed_is_reasonable(self):
        """Test that generated speeds are reasonable."""
        simulator = VehicleSimulator(num_vehicles=10)

        for vehicle in simulator.vehicles:
            event = simulator.generate_event(vehicle)
            speed = event['speed_kmh']
            assert 0 <= speed <= 200, f"Speed {speed} is out of reasonable range"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
