import pytest

from pbpstats.resources.enhanced_pbp import StartOfPeriod, Substitution
from pbpstats.resources.possessions.possession import Possession


class DummyStartOfPeriod(StartOfPeriod):
    def __init__(self, offense_team_id, game_id="0020000001", period=2, clock="12:00"):
        self.game_id = game_id
        self.period = period
        self.clock = clock
        self.offense_team_id = offense_team_id
        self.team_id = offense_team_id

    def get_period_starters(self, file_directory=None):
        return {}

    def get_offense_team_id(self):
        return self.offense_team_id


class DummySubstitution(Substitution):
    def __init__(
        self,
        team_id,
        offense_team_id,
        qualifiers=None,
        game_id="0020000001",
        period=2,
        clock="11:59",
    ):
        self.game_id = game_id
        self.period = period
        self.clock = clock
        self.team_id = team_id
        self.offense_team_id = offense_team_id
        self.qualifiers = qualifiers or []
        self._outgoing_player_id = 1
        self._incoming_player_id = 2

    @property
    def outgoing_player_id(self):
        return self._outgoing_player_id

    @property
    def incoming_player_id(self):
        return self._incoming_player_id

    def get_offense_team_id(self):
        return self.offense_team_id


class DummyPlay:
    def __init__(
        self,
        team_id,
        offense_team_id,
        game_id="0020000001",
        period=2,
        clock="11:58",
    ):
        self.game_id = game_id
        self.period = period
        self.clock = clock
        self.team_id = team_id
        self.offense_team_id = offense_team_id

    def get_offense_team_id(self):
        return self.offense_team_id


def test_possession_prefers_start_of_period_offense():
    start_event = DummyStartOfPeriod(offense_team_id=111)
    startperiod_sub = DummySubstitution(
        team_id=222, offense_team_id=222, qualifiers=["startperiod"]
    )
    first_play = DummyPlay(team_id=333, offense_team_id=333)

    possession = Possession([start_event, startperiod_sub, first_play])

    assert possession.offense_team_id == 111


def test_possession_without_start_of_period_uses_head_event():
    opening_sub = DummySubstitution(team_id=444, offense_team_id=444)
    next_play = DummyPlay(team_id=555, offense_team_id=555)

    possession = Possession([opening_sub, next_play])

    assert possession.offense_team_id == 444


def test_possession_skips_startperiod_sub_when_no_start_of_period():
    startperiod_sub = DummySubstitution(
        team_id=222, offense_team_id=222, qualifiers=["startperiod"]
    )
    first_play = DummyPlay(team_id=333, offense_team_id=333)

    possession = Possession([startperiod_sub, first_play])

    # Should skip the startperiod substitution and use the first real play
    assert possession.offense_team_id == 333
