from collections import defaultdict
from types import SimpleNamespace

import pytest

from pbpstats.resources.enhanced_pbp.enhanced_pbp_item import EnhancedPbpItem


class DummyEnhancedEvent(EnhancedPbpItem):
    def __init__(
        self,
        *,
        game_id,
        period,
        seconds_remaining,
        previous_event,
        current_players=None,
        offense_team_id=1,
        data_provider="live",
    ):
        self.game_id = game_id
        self.period = period
        self._seconds_remaining = seconds_remaining
        self.previous_event = previous_event
        self.next_event = None
        self._current_players = current_players
        self.offense_team_id = offense_team_id
        self.data_provider = data_provider
        self.player_game_fouls = defaultdict(int)

    @property
    def is_possession_ending_event(self):
        return False

    @property
    def event_stats(self):
        return []

    def get_offense_team_id(self):
        return self.offense_team_id

    @property
    def seconds_remaining(self):
        return self._seconds_remaining

    @property
    def current_players(self):
        if self._current_players is not None:
            return self._current_players
        return super().current_players


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


def test_seconds_since_previous_event_clamps_live_same_period_clock_reversal():
    previous = SimpleNamespace(period=1, seconds_remaining=314.0)
    event = DummyEnhancedEvent(
        game_id="1022500001",
        period=1,
        seconds_remaining=317.0,
        previous_event=previous,
    )

    assert event.seconds_since_previous_event == 0


def test_seconds_since_previous_event_preserves_non_live_same_period_negative_elapsed():
    previous = SimpleNamespace(period=2, seconds_remaining=251.0)
    event = DummyEnhancedEvent(
        game_id="0021900700",
        period=2,
        seconds_remaining=254.0,
        previous_event=previous,
        data_provider="stats_nba",
    )

    assert event.seconds_since_previous_event == -3.0


def test_seconds_played_base_stats_emit_non_live_negative_elapsed_compensation():
    players = {1: [11, 12, 13, 14, 15], 2: [21, 22, 23, 24, 25]}
    previous = DummyEnhancedEvent(
        game_id="0021900700",
        period=2,
        seconds_remaining=251.0,
        previous_event=None,
        current_players=players,
        offense_team_id=1,
        data_provider="stats_nba",
    )
    event = DummyEnhancedEvent(
        game_id="0021900700",
        period=2,
        seconds_remaining=254.0,
        previous_event=previous,
        current_players=players,
        offense_team_id=1,
        data_provider="stats_nba",
    )
    _wire_events([previous, event])

    seconds_rows = [
        row
        for row in event.base_stats
        if row["stat_key"] in {"SecondsPlayedOff", "SecondsPlayedDef"}
    ]

    assert len(seconds_rows) == 10
    assert {row["player_id"] for row in seconds_rows} == {
        11,
        12,
        13,
        14,
        15,
        21,
        22,
        23,
        24,
        25,
    }
    assert {row["stat_value"] for row in seconds_rows} == {-3.0}


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


def test_seconds_since_previous_event_defers_original_elapsed_on_partial_backtrack():
    event_100 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=100.0,
        previous_event=None,
    )
    early_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=event_100,
    )
    partial_backtrack_95 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=95.0,
        previous_event=early_90,
    )
    later_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=partial_backtrack_95,
    )
    _wire_events([event_100, early_90, partial_backtrack_95, later_90])

    assert early_90.seconds_since_previous_event == 0
    assert partial_backtrack_95.seconds_since_previous_event == 0
    assert later_90.seconds_since_previous_event == 10.0


def test_seconds_since_previous_event_defers_original_elapsed_on_overshoot_backtrack():
    event_100 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=100.0,
        previous_event=None,
    )
    early_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=event_100,
    )
    overshoot_105 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=105.0,
        previous_event=early_90,
    )
    later_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=overshoot_105,
    )
    _wire_events([event_100, early_90, overshoot_105, later_90])

    assert early_90.seconds_since_previous_event == 0
    assert overshoot_105.seconds_since_previous_event == 0
    assert later_90.seconds_since_previous_event == 10.0


def test_seconds_since_previous_event_does_not_replay_deferred_elapsed_on_post_duplicate_same_clock_event():
    event_100 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=100.0,
        previous_event=None,
    )
    early_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=event_100,
    )
    admin_95 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=95.0,
        previous_event=early_90,
    )
    later_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=admin_95,
    )
    same_clock_admin_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=later_90,
    )
    _wire_events([event_100, early_90, admin_95, later_90, same_clock_admin_90])

    assert early_90.seconds_since_previous_event == 0
    assert admin_95.seconds_since_previous_event == 0
    assert later_90.seconds_since_previous_event == 10.0
    assert same_clock_admin_90.seconds_since_previous_event == 0


def test_seconds_since_previous_event_finds_positive_early_anchor_when_early_duplicate_has_same_clock_cluster():
    event_100 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=100.0,
        previous_event=None,
    )
    early_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=event_100,
    )
    early_same_clock_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=early_90,
    )
    admin_95 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=95.0,
        previous_event=early_same_clock_90,
    )
    later_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=admin_95,
    )
    _wire_events([event_100, early_90, early_same_clock_90, admin_95, later_90])

    assert early_90.seconds_since_previous_event == 0
    assert early_same_clock_90.seconds_since_previous_event == 0
    assert admin_95.seconds_since_previous_event == 0
    assert later_90.seconds_since_previous_event == 10.0


def test_seconds_since_previous_event_does_not_replay_deferred_elapsed_on_repeated_backtrack_cycle():
    event_100 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=100.0,
        previous_event=None,
    )
    early_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=event_100,
    )
    admin_95 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=95.0,
        previous_event=early_90,
    )
    later_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=admin_95,
    )
    admin2_95 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=95.0,
        previous_event=later_90,
    )
    later2_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=admin2_95,
    )
    _wire_events([event_100, early_90, admin_95, later_90, admin2_95, later2_90])

    assert early_90.seconds_since_previous_event == 0
    assert admin_95.seconds_since_previous_event == 0
    assert later_90.seconds_since_previous_event == 10.0
    assert admin2_95.seconds_since_previous_event == 0
    assert later2_90.seconds_since_previous_event == 0


def test_seconds_since_previous_event_respects_prior_low_watermark_for_duplicate_anchor():
    event_100 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=100.0,
        previous_event=None,
    )
    event_80 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=80.0,
        previous_event=event_100,
    )
    backtrack_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=event_80,
    )
    early_85 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=85.0,
        previous_event=backtrack_90,
    )
    second_backtrack_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=early_85,
    )
    later_85 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=85.0,
        previous_event=second_backtrack_90,
    )
    _wire_events([event_100, event_80, backtrack_90, early_85, second_backtrack_90, later_85])

    assert event_80.seconds_since_previous_event == 20.0
    assert backtrack_90.seconds_since_previous_event == 0
    assert early_85.seconds_since_previous_event == 0
    assert second_backtrack_90.seconds_since_previous_event == 0
    assert later_85.seconds_since_previous_event == 0


def test_seconds_played_base_stats_credit_deferred_duplicate_clock_to_later_lineup():
    pre_lineup = {1: [11, 12, 13, 14, 15], 2: [21, 22, 23, 24, 25]}
    post_lineup = {1: [11, 12, 13, 14, 16], 2: [21, 22, 23, 24, 26]}
    high_clock_event = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=100.0,
        previous_event=None,
        current_players=pre_lineup,
        offense_team_id=1,
    )
    early_lower_clock_event = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=high_clock_event,
        current_players=pre_lineup,
        offense_team_id=1,
    )
    backtracked_admin_event = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=95.0,
        previous_event=early_lower_clock_event,
        current_players=post_lineup,
        offense_team_id=1,
    )
    later_duplicate_lower_clock_event = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=backtracked_admin_event,
        current_players=post_lineup,
        offense_team_id=1,
    )
    _wire_events(
        [
            high_clock_event,
            early_lower_clock_event,
            backtracked_admin_event,
            later_duplicate_lower_clock_event,
        ]
    )

    early_seconds = [
        row
        for row in early_lower_clock_event.base_stats
        if row["stat_key"] in {"SecondsPlayedOff", "SecondsPlayedDef"}
    ]
    later_seconds = [
        row
        for row in later_duplicate_lower_clock_event.base_stats
        if row["stat_key"] in {"SecondsPlayedOff", "SecondsPlayedDef"}
    ]

    assert early_seconds == []
    assert {row["player_id"] for row in later_seconds} == {11, 12, 13, 14, 16, 21, 22, 23, 24, 26}
    assert {row["stat_value"] for row in later_seconds} == {10.0}


def test_seconds_since_previous_event_normal_same_clock_substitution_cluster():
    event_100 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=100.0,
        previous_event=None,
    )
    same_clock_sub = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=100.0,
        previous_event=event_100,
    )
    event_90 = DummyEnhancedEvent(
        game_id="1022500001",
        period=3,
        seconds_remaining=90.0,
        previous_event=same_clock_sub,
    )
    _wire_events([event_100, same_clock_sub, event_90])

    assert same_clock_sub.seconds_since_previous_event == 0
    assert event_90.seconds_since_previous_event == 10.0
