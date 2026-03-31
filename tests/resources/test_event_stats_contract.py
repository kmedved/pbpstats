from collections import defaultdict

import pytest

from pbpstats.resources.enhanced_pbp import Substitution
from pbpstats.resources.enhanced_pbp.enhanced_pbp_item import (
    IncompleteEventStatsContextError,
)
from pbpstats.resources.enhanced_pbp.stats_nba.field_goal import StatsFieldGoal
from pbpstats.resources.enhanced_pbp.stats_nba.foul import StatsFoul
from pbpstats.resources.enhanced_pbp.stats_nba.free_throw import StatsFreeThrow
from pbpstats.resources.enhanced_pbp.stats_nba.rebound import StatsRebound
from pbpstats.resources.enhanced_pbp.stats_nba.turnover import StatsTurnover
from pbpstats.resources.enhanced_pbp.stats_nba.violation import StatsViolation
from pbpstats.resources.possessions.possession import Possession
from pbpstats.resources.possessions.possessions import Possessions


class SeedEvent:
    def __init__(
        self,
        current_players,
        *,
        game_id="0021900001",
        period=1,
        clock="0:45",
    ):
        self._players = {
            int(team_id): list(player_ids)
            for team_id, player_ids in current_players.items()
        }
        self.game_id = game_id
        self.period = period
        self.clock = clock
        self.order = 0
        try:
            minutes, seconds = clock.split(":")
            self.seconds_remaining = int(minutes) * 60 + float(seconds)
        except Exception:
            self.seconds_remaining = 0.0
        self.previous_event = None
        self.next_event = None
        self.score = defaultdict(int)
        self.player_game_fouls = defaultdict(int)
        self.is_possession_ending_event = False

    @property
    def current_players(self):
        return {
            int(team_id): list(player_ids)
            for team_id, player_ids in self._players.items()
        }

    @property
    def _raw_current_players(self):
        return self.current_players


class BrokenEvent:
    def __init__(self):
        self.game_id = "0021900001"
        self.period = 1
        self.clock = "0:45"

    @property
    def event_stats(self):
        raise IncompleteEventStatsContextError("synthetic broken event")


class DummySubstitution(Substitution):
    def __init__(self, *, team_id, incoming_player_id, outgoing_player_id):
        self.team_id = team_id
        self._incoming_player_id = incoming_player_id
        self._outgoing_player_id = outgoing_player_id

    @property
    def incoming_player_id(self):
        return self._incoming_player_id

    @property
    def outgoing_player_id(self):
        return self._outgoing_player_id


def test_stats_free_throw_event_stats_without_game_id_do_not_crash(monkeypatch):
    ft = {
        "EVENTMSGTYPE": 3,
        "EVENTMSGACTIONTYPE": 10,
        "HOMEDESCRIPTION": "Free Throw 1 of 1",
        "PCTIMESTRING": "0:45",
        "PLAYER1_TEAM_ID": 1,
        "PLAYER1_ID": 1,
    }
    seed = SeedEvent(
        {
            1: [1, 2, 3, 4, 5],
            2: [6, 7, 8, 9, 10],
        }
    )
    event = StatsFreeThrow(ft, 1)
    event.previous_event = seed
    event.next_event = None
    monkeypatch.setattr(StatsFreeThrow, "base_stats", property(lambda self: []))

    stats = event.event_stats

    assert event.free_throw_type == "1 Shot Away From Play"
    assert any(
        stat["stat_key"] == "1 Shot Away From Play Free Throw Trips" for stat in stats
    )


def test_field_goal_sparse_current_players_raise_incomplete_context(monkeypatch):
    event = StatsFieldGoal(
        {
            "GAME_ID": "0021900001",
            "EVENTNUM": 21,
            "PCTIMESTRING": "09:31",
            "VISITORDESCRIPTION": "Bogdanovic 2' Driving Layup (2 PTS)",
            "EVENTMSGACTIONTYPE": 42,
            "EVENTMSGTYPE": 1,
            "PLAYER1_ID": 202711,
            "PLAYER1_TEAM_ID": 1610612751,
        },
        1,
    )
    event.previous_event = SeedEvent({1610612751: [1, 2, 3, 4, 5]}, clock="09:31")
    monkeypatch.setattr(StatsFieldGoal, "base_stats", property(lambda self: []))

    with pytest.raises(IncompleteEventStatsContextError):
        _ = event.event_stats


def test_free_throw_sparse_current_players_raise_incomplete_context(monkeypatch):
    event = StatsFreeThrow(
        {
            "GAME_ID": "0021900001",
            "EVENTMSGTYPE": 3,
            "EVENTMSGACTIONTYPE": 10,
            "HOMEDESCRIPTION": "Free Throw 1 of 1",
            "PCTIMESTRING": "0:45",
            "PLAYER1_TEAM_ID": 1,
            "PLAYER1_ID": 1,
        },
        1,
    )
    event.previous_event = SeedEvent({1: [1, 2, 3, 4, 5]}, clock="0:45")
    event.next_event = None
    monkeypatch.setattr(StatsFreeThrow, "base_stats", property(lambda self: []))

    with pytest.raises(IncompleteEventStatsContextError):
        _ = event.event_stats


def test_foul_sparse_current_players_raise_incomplete_context(monkeypatch):
    event = StatsFoul(
        {
            "GAME_ID": "0021900001",
            "EVENTNUM": 1,
            "PCTIMESTRING": "11:06",
            "PERIOD": 4,
            "EVENTMSGACTIONTYPE": 1,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 891,
            "PLAYER1_TEAM_ID": 1610612752,
            "PLAYER2_ID": 901,
            "HOMEDESCRIPTION": "Test foul",
        },
        0,
    )
    event.previous_event = SeedEvent({1610612752: [1, 2, 3, 4, 5]}, clock="11:06")
    monkeypatch.setattr(StatsFoul, "base_stats", property(lambda self: []))

    with pytest.raises(IncompleteEventStatsContextError):
        _ = event.event_stats


def test_rebound_sparse_current_players_raise_incomplete_context(monkeypatch):
    missed_shot = StatsFieldGoal(
        {
            "GAME_ID": "0021900001",
            "EVENTNUM": 21,
            "PCTIMESTRING": "09:31",
            "VISITORDESCRIPTION": "MISS Bogdanovic 2' Driving Layup",
            "EVENTMSGACTIONTYPE": 42,
            "EVENTMSGTYPE": 2,
            "PLAYER1_ID": 202711,
            "PLAYER1_TEAM_ID": 1610612751,
        },
        1,
    )
    missed_shot.previous_event = SeedEvent({1610612751: [1, 2, 3, 4, 5]}, clock="09:31")
    event = StatsRebound(
        {
            "GAME_ID": "0021900001",
            "EVENTNUM": 22,
            "PCTIMESTRING": "09:31",
            "HOMEDESCRIPTION": "Team Rebound",
            "EVENTMSGACTIONTYPE": 0,
            "EVENTMSGTYPE": 4,
            "PLAYER1_ID": 0,
            "PLAYER1_TEAM_ID": 1610612748,
        },
        2,
    )
    missed_shot.next_event = event
    event.previous_event = missed_shot
    event.next_event = None
    monkeypatch.setattr(StatsRebound, "base_stats", property(lambda self: []))

    with pytest.raises(IncompleteEventStatsContextError):
        _ = event.event_stats


def test_anonymous_rebound_returns_base_stats_and_logs_warning(monkeypatch, caplog):
    missed_shot = StatsFieldGoal(
        {
            "GAME_ID": "0029600332",
            "EVENTNUM": 465,
            "PCTIMESTRING": "0:21",
            "VISITORDESCRIPTION": "MISS Graham 3PT Jump Shot",
            "EVENTMSGACTIONTYPE": 1,
            "EVENTMSGTYPE": 2,
            "PLAYER1_ID": 37,
            "PLAYER1_TEAM_ID": 1610612760,
        },
        1,
    )
    missed_shot.previous_event = SeedEvent(
        {
            1610612744: [769, 770, 771, 772, 773],
            1610612760: [37, 727, 766, 957, 958],
        },
        game_id="0029600332",
        clock="0:21",
    )
    event = StatsRebound(
        {
            "GAME_ID": "0029600332",
            "EVENTNUM": 466,
            "PCTIMESTRING": "0:19",
            "NEUTRALDESCRIPTION": "Unknown",
            "EVENTMSGACTIONTYPE": 0,
            "EVENTMSGTYPE": 4,
            "PLAYER1_ID": 0,
            "PLAYER1_TEAM_ID": None,
        },
        2,
    )
    missed_shot.next_event = event
    event.previous_event = missed_shot
    event.next_event = None
    monkeypatch.setattr(
        StatsRebound,
        "base_stats",
        property(lambda self: [{"stat_key": "sentinel", "stat_value": 1}]),
    )

    with caplog.at_level("WARNING"):
        stats = event.event_stats

    assert stats == [{"stat_key": "sentinel", "stat_value": 1}]
    assert any(
        "source_limited_anonymous_rebound" in message for message in caplog.messages
    )


def test_turnover_sparse_current_players_raise_incomplete_context(monkeypatch):
    event = StatsTurnover(
        {
            "GAME_ID": "0021900001",
            "EVENTNUM": 7,
            "PCTIMESTRING": "11:00",
            "EVENTMSGACTIONTYPE": 4,
            "EVENTMSGTYPE": 5,
            "PLAYER1_ID": 1,
            "PLAYER1_TEAM_ID": 1,
        },
        1,
    )
    event.previous_event = SeedEvent({1: [1, 2, 3, 4, 5]}, clock="11:00")
    monkeypatch.setattr(StatsTurnover, "base_stats", property(lambda self: []))

    with pytest.raises(IncompleteEventStatsContextError):
        _ = event.event_stats


def test_anonymous_no_shot_returns_base_stats_and_logs_warning(monkeypatch, caplog):
    event = StatsFieldGoal(
        {
            "GAME_ID": "0029600370",
            "EVENTNUM": 449,
            "PCTIMESTRING": "0:01",
            "NEUTRALDESCRIPTION": "No Shot",
            "EVENTMSGACTIONTYPE": 0,
            "EVENTMSGTYPE": 1,
            "PLAYER1_ID": 0,
            "PLAYER1_TEAM_ID": None,
        },
        1,
    )
    event.previous_event = SeedEvent(
        {
            1610612742: [157, 376, 423, 434, 754],
            1610612760: [88, 727, 728, 729, 730],
        },
        game_id="0029600370",
        clock="0:01",
    )
    event.next_event = None
    monkeypatch.setattr(
        StatsFieldGoal,
        "base_stats",
        property(lambda self: [{"stat_key": "sentinel", "stat_value": 1}]),
    )

    with caplog.at_level("WARNING"):
        stats = event.event_stats

    assert stats == [{"stat_key": "sentinel", "stat_value": 1}]
    assert any(
        "source_limited_anonymous_no_shot" in message
        for message in caplog.messages
    )


def test_violation_sparse_current_players_raise_incomplete_context(monkeypatch):
    event = StatsViolation(
        {
            "GAME_ID": "0021900001",
            "EVENTNUM": 8,
            "PCTIMESTRING": "10:45",
            "EVENTMSGACTIONTYPE": 2,
            "EVENTMSGTYPE": 7,
            "PLAYER1_ID": 1,
            "PLAYER1_TEAM_ID": 1,
        },
        1,
    )
    event.previous_event = SeedEvent({1: [1, 2, 3, 4, 5]}, clock="10:45")
    monkeypatch.setattr(StatsViolation, "base_stats", property(lambda self: []))

    with pytest.raises(IncompleteEventStatsContextError):
        _ = event.event_stats


def test_turnover_same_clock_lineup_fix_keeps_rows_fully_keyed(monkeypatch):
    event = StatsTurnover(
        {
            "GAME_ID": "0021900001",
            "EVENTNUM": 9,
            "PCTIMESTRING": "10:30",
            "EVENTMSGACTIONTYPE": 4,
            "EVENTMSGTYPE": 5,
            "PLAYER1_ID": 1,
            "PLAYER1_TEAM_ID": 1,
        },
        1,
    )
    event.previous_event = SeedEvent(
        {
            1: [1, 2, 3, 4, 5],
            2: [6, 7, 8, 9, 10],
        },
        clock="10:30",
    )
    event.lineup_override_by_team = {1: [2, 3, 4, 5, 11]}
    event.get_all_events_at_current_time = lambda: [
        event,
        DummySubstitution(team_id=1, incoming_player_id=11, outgoing_player_id=1),
    ]
    monkeypatch.setattr(StatsTurnover, "base_stats", property(lambda self: []))

    stats = event.event_stats

    assert stats
    assert all("lineup_id" in stat for stat in stats)
    assert all("opponent_team_id" in stat for stat in stats)
    assert all("opponent_lineup_id" in stat for stat in stats)
    assert all(stat["lineup_id"] == "1-2-3-4-5" for stat in stats if stat["team_id"] == 1)


@pytest.mark.parametrize(
    "accessor",
    [
        "possession_stats",
        "team_stats",
        "opponent_stats",
        "player_stats",
        "lineup_stats",
        "lineup_opponent_stats",
    ],
)
def test_broken_event_stats_raise_consistently_across_possession_surfaces(accessor):
    possession = Possession([BrokenEvent()])

    if accessor == "possession_stats":
        with pytest.raises(IncompleteEventStatsContextError):
            _ = possession.possession_stats
        return

    possessions = Possessions([possession])
    with pytest.raises(IncompleteEventStatsContextError):
        _ = getattr(possessions, accessor)
