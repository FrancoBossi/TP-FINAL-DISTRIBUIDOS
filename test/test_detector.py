import threading
import unittest

from common.control_wire import ProtocolError
from common.health import STATUS_DEGRADED, STATUS_OK, STATUS_STARTING
from failure_manager.config import DetectionSettings
from failure_manager.detector import (
    UNREACHABLE,
    Detector,
    Probe,
    Suspect,
    probe_target,
)
from failure_manager.topology import Target

OK = Probe(True, STATUS_OK, "inc-1")


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class Scenario:
    """A detector over fake targets whose answers the test controls."""

    def __init__(self, names=("a", "b", "c"), **settings):
        self.clock = FakeClock()
        self.answers = {name: OK for name in names}
        self.settings = DetectionSettings(
            **{
                "failure_threshold": 2,
                "restart_cooldown": 10.0,
                "startup_grace": 30.0,
                "initial_grace": 20.0,
                **settings,
            }
        )
        self.targets = [Target.named(name) for name in names]
        self.detector = Detector(
            self.targets,
            prober=lambda target: self._answer(target.name),
            settings=self.settings,
            clock=self.clock,
        )

    def _answer(self, name):
        answer = self.answers[name]
        if isinstance(answer, Exception):
            raise answer
        return answer

    def round(self, advance=1.0):
        self.clock.advance(advance)
        return {suspect.target.name: suspect for suspect in self.detector.check_round()}

    def settle(self):
        """Let every target be seen healthy once, past the initial grace."""
        self.round()
        self.clock.advance(self.settings.initial_grace)

    def close(self):
        self.detector.close()


class TestDetector(unittest.TestCase):
    def scenario(self, **kwargs):
        scenario = Scenario(**kwargs)
        self.addCleanup(scenario.close)
        return scenario

    def test_healthy_targets_are_never_suspected(self):
        scenario = self.scenario()

        for _ in range(5):
            self.assertEqual(scenario.round(), {})

    def test_a_target_is_suspected_after_the_threshold_of_consecutive_rounds(self):
        scenario = self.scenario(failure_threshold=3)
        scenario.settle()
        scenario.answers["b"] = UNREACHABLE

        self.assertEqual(scenario.round(), {})
        self.assertEqual(scenario.round(), {})
        first_failure = scenario.clock.now - 1.0
        suspect = scenario.round()["b"]

        self.assertEqual(suspect.failures, 3)
        self.assertEqual(suspect.incarnation_id, "inc-1")
        self.assertEqual(suspect.first_failure_at, first_failure)
        self.assertEqual(suspect.reason, "no answer to PING")

    def test_an_answer_in_between_resets_the_count(self):
        scenario = self.scenario()
        scenario.settle()

        scenario.answers["a"] = UNREACHABLE
        self.assertEqual(scenario.round(), {})
        scenario.answers["a"] = OK
        self.assertEqual(scenario.round(), {})
        scenario.answers["a"] = UNREACHABLE
        self.assertEqual(scenario.round(), {})

    def test_simultaneous_failures_are_all_reported_in_one_round(self):
        scenario = self.scenario(names=("a", "b", "c", "d"))
        scenario.settle()
        for name in ("a", "c", "d"):
            scenario.answers[name] = UNREACHABLE

        scenario.round()
        suspects = scenario.round()

        self.assertEqual(set(suspects), {"a", "c", "d"})

    def test_a_probe_that_raises_counts_as_no_answer(self):
        scenario = self.scenario()
        scenario.settle()
        scenario.answers["a"] = RuntimeError("probe bug")

        scenario.round()

        with self.assertLogs("failure_manager.detector", level="ERROR"):
            self.assertIn("a", scenario.round())

    def test_a_degraded_process_that_is_alive_is_suspected(self):
        scenario = self.scenario()
        scenario.settle()
        scenario.answers["a"] = Probe(True, STATUS_DEGRADED, "inc-1")

        scenario.round()
        suspect = scenario.round()["a"]

        self.assertEqual(suspect.reason, "reports itself degraded")

    def test_starting_is_tolerated_until_the_startup_grace_expires(self):
        scenario = self.scenario(startup_grace=30.0)
        scenario.settle()
        scenario.answers["a"] = Probe(True, STATUS_STARTING, "inc-2")

        for _ in range(30):
            self.assertEqual(scenario.round(), {})

        # Exactly at the grace it is still tolerated; the next round is the first
        # failure and the threshold of two consecutive failures makes the second
        # one a suspicion.
        self.assertEqual(scenario.round(), {})
        self.assertEqual(scenario.round(), {})
        suspect = scenario.round()["a"]
        self.assertEqual(suspect.reason, "stuck while starting")

    def test_unreachable_targets_never_seen_get_the_initial_grace(self):
        scenario = self.scenario(initial_grace=20.0)
        scenario.answers["a"] = UNREACHABLE

        for _ in range(19):
            self.assertEqual(scenario.round(), {})

        scenario.round()
        self.assertIn("a", scenario.round())

    def test_seen_targets_get_no_initial_grace(self):
        scenario = self.scenario(initial_grace=1000.0)
        scenario.round()
        scenario.answers["a"] = UNREACHABLE

        scenario.round()

        self.assertIn("a", scenario.round())

    def test_cooldown_after_a_restart_then_recovery_is_logged(self):
        scenario = self.scenario(restart_cooldown=10.0)
        scenario.settle()
        scenario.answers["a"] = UNREACHABLE
        scenario.round()
        suspect = scenario.round()["a"]

        scenario.detector.mark_restart_attempted(suspect.target)

        for _ in range(9):
            self.assertEqual(scenario.round(), {})

        scenario.answers["a"] = Probe(True, STATUS_OK, "inc-2")
        with self.assertLogs("failure_manager.detector", level="INFO") as logs:
            self.assertEqual(scenario.round(), {})

        self.assertTrue(any("recovered" in line for line in logs.output))

    def test_still_failing_after_the_cooldown_is_suspected_again(self):
        scenario = self.scenario(restart_cooldown=5.0)
        scenario.settle()
        scenario.answers["a"] = UNREACHABLE
        scenario.round()
        suspect = scenario.round()["a"]
        scenario.detector.mark_restart_attempted(suspect.target)

        for _ in range(5):
            scenario.round()
        scenario.round()

        self.assertIn("a", scenario.round())

    def test_a_new_incarnation_clears_earlier_failures(self):
        scenario = self.scenario()
        scenario.settle()
        scenario.answers["a"] = UNREACHABLE
        scenario.round()

        scenario.answers["a"] = Probe(True, STATUS_OK, "inc-2")

        self.assertEqual(scenario.round(), {})
        scenario.answers["a"] = UNREACHABLE
        self.assertEqual(scenario.round(), {})

    def test_reset_forgets_suspicions_of_a_previous_coordinator(self):
        scenario = self.scenario()
        scenario.settle()
        scenario.answers["a"] = UNREACHABLE
        scenario.round()

        scenario.detector.reset()

        self.assertEqual(scenario.round(), {})
        self.assertIn("a", scenario.round())


class TestConfirmation(unittest.TestCase):
    def setUp(self):
        self.scenario = Scenario(names=("a",))
        self.addCleanup(self.scenario.close)
        self.scenario.settle()
        self.scenario.answers["a"] = UNREACHABLE
        self.scenario.round()
        self.suspect = self.scenario.round()["a"]

    def confirm(self, probe):
        self.scenario.answers["a"] = probe
        return self.scenario.detector.confirm(self.suspect)

    def test_still_unreachable_confirms(self):
        self.assertTrue(self.confirm(UNREACHABLE))

    def test_a_healthy_answer_is_a_false_suspicion(self):
        with self.assertLogs("failure_manager.detector", level="INFO"):
            self.assertFalse(self.confirm(OK))

    def test_an_answer_from_a_newer_instance_cancels_the_restart(self):
        with self.assertLogs("failure_manager.detector", level="INFO"):
            self.assertFalse(self.confirm(Probe(True, STATUS_DEGRADED, "inc-new")))

    def test_the_same_instance_still_degraded_confirms(self):
        self.assertTrue(self.confirm(Probe(True, STATUS_DEGRADED, "inc-1")))

    def test_an_instance_that_is_starting_does_not_confirm_within_the_grace(self):
        self.assertFalse(self.confirm(Probe(True, STATUS_STARTING, "inc-1")))

    def test_a_suspect_never_seen_before_is_cancelled_by_any_healthy_answer(self):
        suspect = Suspect(Target.named("a"), None, 2, 0.0, "no answer to PING")
        self.scenario.answers["a"] = OK

        self.assertFalse(self.scenario.detector.confirm(suspect))


class TestProbeTarget(unittest.TestCase):
    TARGET = Target("worker-1", "127.0.0.1", 7000, "worker-1")

    def probe(self, outcomes, attempts=3):
        calls = []

        def fake_ping(host, port, timeout, nonce):
            calls.append((host, port, timeout))
            outcome = outcomes[min(len(calls) - 1, len(outcomes) - 1)]
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        probe = probe_target(self.TARGET, 0.5, attempts, ping_function=fake_ping)
        return probe, calls

    def test_the_first_valid_pong_ends_the_probe(self):
        pong = {"name": "worker-1", "status": STATUS_OK, "incarnation_id": "x"}

        probe, calls = self.probe([pong])

        self.assertEqual(probe, Probe(True, STATUS_OK, "x"))
        self.assertEqual(calls, [("127.0.0.1", 7000, 0.5)])

    def test_failed_attempts_are_retried(self):
        pong = {"name": "worker-1", "status": STATUS_OK, "incarnation_id": "x"}

        probe, calls = self.probe([TimeoutError(), EOFError(), pong])

        self.assertTrue(probe.reachable)
        self.assertEqual(len(calls), 3)

    def test_gives_up_after_the_configured_attempts(self):
        probe, calls = self.probe([ProtocolError("bad")], attempts=2)

        self.assertEqual(probe, UNREACHABLE)
        self.assertEqual(len(calls), 2)

    def test_an_answer_from_another_component_is_ignored(self):
        impostor = {"name": "worker-2", "status": STATUS_OK, "incarnation_id": "y"}

        probe, _calls = self.probe([impostor], attempts=2)

        self.assertEqual(probe, UNREACHABLE)

    def test_an_unknown_status_is_treated_as_degraded(self):
        pong = {"name": "worker-1", "status": "WEIRD", "incarnation_id": "x"}

        probe, _calls = self.probe([pong])

        self.assertEqual(probe.status, STATUS_DEGRADED)


class TestConcurrentRounds(unittest.TestCase):
    def test_all_targets_are_probed_in_parallel(self):
        names = [f"t{i}" for i in range(8)]
        arrived = threading.Barrier(len(names), timeout=5)

        def prober(_target):
            arrived.wait()
            return OK

        detector = Detector(
            [Target.named(name) for name in names],
            prober=prober,
            settings=DetectionSettings(),
        )
        self.addCleanup(detector.close)

        self.assertEqual(detector.check_round(), [])


class TestSettings(unittest.TestCase):
    def test_rejects_invalid_values(self):
        for kwargs in (
            {"ping_interval": 0},
            {"ping_timeout": -1},
            {"ping_attempts": 0},
            {"failure_threshold": 0},
            {"restart_cooldown": 0},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                DetectionSettings(**kwargs)


if __name__ == "__main__":
    unittest.main()
