"""Unit tests for simulator orchestration logic."""

import pytest

from src.simulators.run_all import SimulatorOrchestrator


class _DummySimulator:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class TestSimulatorOrchestrator:
    def test_constructor_validates_inputs(self):
        with pytest.raises(ValueError):
            SimulatorOrchestrator(num_vehicles=0)
        with pytest.raises(ValueError):
            SimulatorOrchestrator(num_agents=0)
        with pytest.raises(ValueError):
            SimulatorOrchestrator(shipments_per_minute=0)

    def test_start_validates_optional_limits(self):
        orchestrator = SimulatorOrchestrator()
        with pytest.raises(ValueError):
            orchestrator.start(duration=0)
        with pytest.raises(ValueError):
            orchestrator.start(max_events_per_sim=0)

    def test_run_with_guard_records_failures_and_stops(self):
        orchestrator = SimulatorOrchestrator()
        stop_called = {"value": False}

        def bad_runner(duration=None, max_events=None):
            raise RuntimeError("boom")

        def mark_stop():
            stop_called["value"] = True

        orchestrator.stop = mark_stop
        orchestrator._run_with_guard(bad_runner, name="bad-runner")

        assert "bad-runner" in orchestrator.errors
        assert stop_called["value"] is True

    def test_start_and_wait_runs_all_simulators(self):
        orchestrator = SimulatorOrchestrator()
        calls = []

        def make_runner(name):
            def _runner(duration=None, max_events=None):
                calls.append((name, duration, max_events))

            return _runner

        orchestrator._run_vehicle_simulator = make_runner("vehicle")
        orchestrator._run_shipment_simulator = make_runner("shipment")
        orchestrator._run_delivery_simulator = make_runner("delivery")

        orchestrator.start(duration=2, max_events_per_sim=5)
        orchestrator.wait()

        names = sorted(name for name, _, _ in calls)
        assert names == ["delivery", "shipment", "vehicle"]
        assert all(duration == 2 for _, duration, _ in calls)
        assert all(max_events == 5 for _, _, max_events in calls)

    def test_wait_raises_when_any_simulator_fails(self):
        orchestrator = SimulatorOrchestrator()

        def fail_vehicle(duration=None, max_events=None):
            raise RuntimeError("vehicle failed")

        orchestrator._run_vehicle_simulator = fail_vehicle
        orchestrator._run_shipment_simulator = lambda duration=None, max_events=None: None
        orchestrator._run_delivery_simulator = lambda duration=None, max_events=None: None

        orchestrator.start(duration=1, max_events_per_sim=1)
        with pytest.raises(RuntimeError, match="VehicleSimulator"):
            orchestrator.wait()

    def test_stop_closes_registered_simulators(self):
        orchestrator = SimulatorOrchestrator()
        simulators = [_DummySimulator(), _DummySimulator()]
        orchestrator.simulators = simulators
        orchestrator.running = True

        orchestrator.stop()

        assert orchestrator.running is False
        assert all(sim.closed for sim in simulators)
