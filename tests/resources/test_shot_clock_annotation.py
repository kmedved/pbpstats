import pbpstats
from pbpstats.resources.enhanced_pbp import Rebound, StartOfPeriod
from pbpstats.resources.enhanced_pbp.field_goal import FieldGoal
from pbpstats.resources.enhanced_pbp.shot_clock import annotate_shot_clock


class DummyStartOfPeriod(StartOfPeriod):
    @classmethod
    def get_period_starters(cls, file_directory):
        return {}

    def __init__(self, period, seconds_remaining):
        self.game_id = "0021600001"
        self.period = period
        self.seconds_remaining = seconds_remaining
        self.period_starters = {}
        self.next_event = None


class DummyFieldGoal(FieldGoal):
    def __init__(self, period, seconds_remaining, offense_team_id=None, description=""):
        self.period = period
        self.seconds_remaining = seconds_remaining
        self.offense_team_id = offense_team_id
        self.description = description
        self.next_event = None

    @property
    def is_made(self):
        return False

    @property
    def shot_value(self):
        return 2


class DummyRebound(Rebound):
    def __init__(self, period, seconds_remaining, oreb, offense_team_id=None):
        self.period = period
        self.seconds_remaining = seconds_remaining
        self._oreb = oreb
        self.offense_team_id = offense_team_id
        self.next_event = None
        self._is_real_rebound = True

    @property
    def is_real_rebound(self):
        return self._is_real_rebound

    @property
    def is_placeholder(self):
        return not self._is_real_rebound

    @property
    def oreb(self):
        return self._oreb


class DummyEvent:
    def __init__(self, period, seconds_remaining):
        self.period = period
        self.seconds_remaining = seconds_remaining
        self.next_event = None


def test_shot_clock_handles_start_of_period_and_substitutions():
    sop = DummyStartOfPeriod(period=1, seconds_remaining=720.0)
    substitution = DummyEvent(period=1, seconds_remaining=720.0)
    first_play = DummyFieldGoal(period=1, seconds_remaining=715.0)

    sop.next_event = substitution
    substitution.next_event = first_play

    annotate_shot_clock([sop, substitution, first_play], season_year=2023, league=pbpstats.NBA_STRING)

    assert sop.shot_clock == 24.0
    assert substitution.shot_clock == 24.0
    assert first_play.shot_clock == 19.0


def test_shot_clock_behavior_without_start_of_period():
    first_play = DummyFieldGoal(period=1, seconds_remaining=700.0, offense_team_id=1)
    rebound = DummyRebound(period=1, seconds_remaining=690.0, oreb=False, offense_team_id=2)

    first_play.next_event = rebound

    annotate_shot_clock([first_play, rebound], season_year=2023, league=pbpstats.NBA_STRING)

    assert first_play.shot_clock == 24.0
    assert rebound.shot_clock == 14.0
