from types import SimpleNamespace

import pytest

from pbpstats.resources.enhanced_pbp.enhanced_pbp_item import EnhancedPbpItem


class DummyEnhancedEvent(EnhancedPbpItem):
    def __init__(self, *, game_id, period, seconds_remaining, previous_event):
        self.game_id = game_id
        self.period = period
        self._seconds_remaining = seconds_remaining
        self.previous_event = previous_event
        self.next_event = None

    @property
    def is_possession_ending_event(self):
        return False

    @property
    def event_stats(self):
        return []

    def get_offense_team_id(self):
        return None

    @property
    def seconds_remaining(self):
        return self._seconds_remaining


def _wire_events(events):
    for index, event in enumerate(events):
        event.previous_event = events[index - 1] if index else None
        event.next_event = events[index + 1] if index + 1 < len(events) else None


@pytest.mark.parametrize(
    "game_id,period,start_seconds",
    [
        ("0022500001", 2, 720.0),
        ("0022500001", 5, 300.0),
        ("1022500001", 2, 600.0),
        ("1029700001", 2, 1200.0),
        ("1029700001", 3, 300.0),
    ],
)
def test_seconds_since_previous_event_zeroes_league_aware_period_boundaries(
    game_id,
    period,
    start_seconds,
):
    previous = SimpleNamespace(period=period - 1, seconds_remaining=0.0)
    event = DummyEnhancedEvent(
        game_id=game_id,
        period=period,
        seconds_remaining=start_seconds,
        previous_event=previous,
    )

    assert event.seconds_since_previous_event == 0


def test_seconds_since_previous_event_keeps_same_period_elapsed_time():
    previous = SimpleNamespace(period=2, seconds_remaining=100.0)
    event = DummyEnhancedEvent(
        game_id="1022500001",
        period=2,
        seconds_remaining=90.0,
        previous_event=previous,
    )

    assert event.seconds_since_previous_event == 10.0


def test_seconds_since_previous_event_clamps_repaired_same_period_clock_reversal():
    previous = SimpleNamespace(period=1, seconds_remaining=314.0)
    event = DummyEnhancedEvent(
        game_id="1022500001",
        period=1,
        seconds_remaining=317.0,
        previous_event=previous,
    )

    assert event.seconds_since_previous_event == 0


def test_seconds_since_previous_event_does_not_double_credit_after_clock_backtrack():
    first = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=322.0,
        previous_event=None,
    )
    advanced = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=271.0,
        previous_event=first,
    )
    backtracked = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=322.0,
        previous_event=advanced,
    )
    after_backtrack = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=268.0,
        previous_event=backtracked,
    )

    assert advanced.seconds_since_previous_event == 51.0
    assert backtracked.seconds_since_previous_event == 0
    assert after_backtrack.seconds_since_previous_event == 3.0


def test_seconds_since_previous_event_defers_to_later_duplicate_clock_after_backtrack():
    high_clock_event = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=187.0,
        previous_event=None,
    )
    first_lower_clock_event = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=184.0,
        previous_event=high_clock_event,
    )
    backtracked_admin_event = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=187.0,
        previous_event=first_lower_clock_event,
    )
    later_duplicate_lower_clock_event = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=184.0,
        previous_event=backtracked_admin_event,
    )
    _wire_events(
        [
            high_clock_event,
            first_lower_clock_event,
            backtracked_admin_event,
            later_duplicate_lower_clock_event,
        ]
    )

    assert first_lower_clock_event.seconds_since_previous_event == 0
    assert backtracked_admin_event.seconds_since_previous_event == 0
    assert later_duplicate_lower_clock_event.seconds_since_previous_event == 3.0
