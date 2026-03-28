from collections import defaultdict

import pytest

from pbpstats.resources.enhanced_pbp.enhanced_pbp_item import (
    IncompleteEventStatsContextError,
)
from pbpstats.resources.enhanced_pbp.stats_nba.field_goal import StatsFieldGoal
from pbpstats.resources.enhanced_pbp.stats_nba.foul import StatsFoul
from pbpstats.resources.enhanced_pbp.stats_nba.free_throw import StatsFreeThrow
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
