from types import SimpleNamespace

import pytest

from pbpstats.resources.enhanced_pbp.enhanced_pbp_item import EnhancedPbpItem


class DummyEnhancedEvent(EnhancedPbpItem):
    def __init__(self, *, game_id, period, seconds_remaining, previous_event):
        self.game_id = game_id
        self.period = period
        self._seconds_remaining = seconds_remaining
        self.previous_event = previous_event

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
