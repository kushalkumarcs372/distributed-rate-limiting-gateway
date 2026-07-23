import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from loadbalancer.lb import Backend, FAILURE_THRESHOLD, OPEN_COOLDOWN_SECONDS


def test_starts_closed_and_allows_traffic():
    b = Backend("http://fake:8000")
    assert b.circuit_state == "CLOSED"
    assert b.circuit_allows_traffic() is True


def test_trips_open_after_threshold_consecutive_failures():
    b = Backend("http://fake:8000")
    for _ in range(FAILURE_THRESHOLD - 1):
        b.record_failure()
        assert b.circuit_state == "CLOSED", "should not trip before threshold"

    b.record_failure()  # this one hits the threshold
    assert b.circuit_state == "OPEN"
    assert b.circuit_allows_traffic() is False


def test_a_success_in_between_resets_the_failure_streak():
    b = Backend("http://fake:8000")
    b.record_failure()
    b.record_failure()
    b.record_success()  # resets consecutive_failures to 0
    b.record_failure()
    assert b.circuit_state == "CLOSED", "streak was reset, should not have tripped yet"


def test_moves_to_half_open_after_cooldown_and_recovers_on_success():
    b = Backend("http://fake:8000")
    for _ in range(FAILURE_THRESHOLD):
        b.record_failure()
    assert b.circuit_state == "OPEN"

    # simulate cooldown having elapsed
    b.opened_at = time.time() - OPEN_COOLDOWN_SECONDS - 1

    assert b.circuit_allows_traffic() is True  # transitions to HALF_OPEN
    assert b.circuit_state == "HALF_OPEN"

    b.record_success()  # the trial request succeeded
    assert b.circuit_state == "CLOSED"
    assert b.consecutive_failures == 0


def test_half_open_failure_reopens_with_fresh_cooldown():
    b = Backend("http://fake:8000")
    for _ in range(FAILURE_THRESHOLD):
        b.record_failure()
    b.opened_at = time.time() - OPEN_COOLDOWN_SECONDS - 1
    b.circuit_allows_traffic()  # -> HALF_OPEN
    assert b.circuit_state == "HALF_OPEN"

    b.record_failure()  # trial request failed
    assert b.circuit_state == "OPEN"
    assert b.circuit_allows_traffic() is False  # fresh cooldown, not expired yet
