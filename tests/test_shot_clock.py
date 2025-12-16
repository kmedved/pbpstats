import os
import sys

sys.path.insert(0, os.path.abspath("."))

from pbpstats.resources.enhanced_pbp.shot_clock import annotate_shot_clock
from pbpstats.resources.enhanced_pbp import (
    FieldGoal,
    FreeThrow,
    Rebound,
    Foul,
    Violation,
    Turnover,
    StartOfPeriod,
    JumpBall,
)


class DummyEvent:
    def __init__(self, *, period=1, seconds_remaining=0.0, description="", team_id=None, offense_team_id=None):
        self.period = period
        self.seconds_remaining = seconds_remaining
        self.description = description
        self.team_id = team_id
        self.offense_team_id = offense_team_id
        self.previous_event = None
        self.next_event = None
        self.order = 0
        self._events_at_time = None

    def get_offense_team_id(self):
        return self.offense_team_id

    def get_all_events_at_current_time(self):
        return self._events_at_time or [self]


class DummyStart(StartOfPeriod, DummyEvent):
    def get_period_starters(self):
        return {}

    def get_offense_team_id(self):
        return self.offense_team_id


class DummyFG(FieldGoal, DummyEvent):
    def __init__(self, *, is_made=False, shot_value=2, is_blocked=False, **kwargs):
        DummyEvent.__init__(self, **kwargs)
        self._is_made = is_made
        self._shot_value = shot_value
        if is_blocked:
            self.player3_id = 1

    @property
    def is_made(self):
        return self._is_made

    @property
    def shot_value(self):
        return self._shot_value


class DummyReb(Rebound, DummyEvent):
    def __init__(self, *, is_real_rebound=True, oreb=False, missed_shot=None, **kwargs):
        DummyEvent.__init__(self, **kwargs)
        self._is_real_rebound = is_real_rebound
        self._oreb = oreb
        self._missed_shot = missed_shot

    @property
    def is_real_rebound(self):
        return self._is_real_rebound

    @property
    def oreb(self):
        return self._oreb

    @property
    def missed_shot(self):
        return self._missed_shot


class DummyTO(Turnover, DummyEvent):
    def __init__(self, *, is_no_turnover=False, is_kicked_ball=False, is_shot_clock_violation=False, **kwargs):
        DummyEvent.__init__(self, **kwargs)
        self.is_no_turnover = is_no_turnover
        self.is_kicked_ball = is_kicked_ball
        self.is_shot_clock_violation = is_shot_clock_violation


class DummyFoul(Foul, DummyEvent):
    def __init__(
        self,
        *,
        is_technical=False,
        is_double_technical=False,
        is_double_foul=False,
        is_shooting_foul=False,
        is_shooting_block_foul=False,
        is_loose_ball_foul=False,
        **kwargs,
    ):
        DummyEvent.__init__(self, **kwargs)
        self._is_technical = is_technical
        self._is_double_technical = is_double_technical
        self._is_double_foul = is_double_foul
        self._is_shooting_foul = is_shooting_foul
        self._is_shooting_block_foul = is_shooting_block_foul
        self._is_loose_ball_foul = is_loose_ball_foul

    @property
    def is_technical(self):
        return self._is_technical

    @property
    def is_double_technical(self):
        return self._is_double_technical

    @property
    def is_double_foul(self):
        return self._is_double_foul

    @property
    def is_shooting_foul(self):
        return self._is_shooting_foul

    @property
    def is_shooting_block_foul(self):
        return self._is_shooting_block_foul

    @property
    def is_loose_ball_foul(self):
        return self._is_loose_ball_foul


class DummyViol(Violation, DummyEvent):
    def __init__(self, *, is_goaltend_violation=False, **kwargs):
        DummyEvent.__init__(self, **kwargs)
        self.is_goaltend_violation = is_goaltend_violation


class DummyJB(JumpBall, DummyEvent):
    pass


class DummyFT(FreeThrow, DummyEvent):
    def __init__(self, *, is_end_ft=False, **kwargs):
        DummyEvent.__init__(self, **kwargs)
        self.is_end_ft = is_end_ft


def _link_events(events):
    for idx, event in enumerate(events):
        event.order = idx
        if idx > 0:
            event.previous_event = events[idx - 1]
            events[idx - 1].next_event = event
    by_time = {}
    for ev in events:
        by_time.setdefault(ev.seconds_remaining, []).append(ev)
    for group in by_time.values():
        for ev in group:
            ev._events_at_time = list(group)


def test_blocked_miss_offensive_rebound_no_reset():
    sop = DummyStart(period=1, seconds_remaining=120)
    miss = DummyFG(
        is_made=False,
        description="blocked shot",
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
        is_blocked=True,
    )
    oreb = DummyReb(oreb=True, missed_shot=miss, team_id=1, offense_team_id=1, seconds_remaining=100)
    tip_in = DummyFG(is_made=True, team_id=1, offense_team_id=1, seconds_remaining=100)
    events = [sop, miss, oreb, tip_in]
    _link_events(events)

    annotate_shot_clock(events, season_year=2018, league="nba")

    assert tip_in.shot_clock == miss.shot_clock


def test_blocked_oreb_without_missed_shot_link_still_no_reset():
    sop = DummyStart(period=1, seconds_remaining=120)
    miss = DummyFG(
        is_made=False,
        description="blocked shot",
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
        is_blocked=True,
    )
    oreb = DummyReb(oreb=True, missed_shot=None, team_id=1, offense_team_id=1, seconds_remaining=100)
    tip_in = DummyFG(is_made=True, team_id=1, offense_team_id=1, seconds_remaining=100)
    events = [sop, miss, oreb, tip_in]
    _link_events(events)

    annotate_shot_clock(events, season_year=2018, league="nba")

    assert tip_in.shot_clock == miss.shot_clock


def test_blocked_oreb_without_link_ignores_later_miss_same_time():
    sop = DummyStart(period=1, seconds_remaining=120)
    miss = DummyFG(
        is_made=False,
        description="blocked shot",
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
        is_blocked=True,
    )
    oreb = DummyReb(oreb=True, missed_shot=None, team_id=1, offense_team_id=1, seconds_remaining=100)
    # Later miss at same timestamp (should NOT drive rim inference for the rebound)
    putback_miss = DummyFG(is_made=False, description="miss", team_id=1, offense_team_id=1, seconds_remaining=100)
    events = [sop, miss, oreb, putback_miss]
    _link_events(events)

    annotate_shot_clock(events, season_year=2018, league="nba")

    assert putback_miss.shot_clock == miss.shot_clock


def test_normal_miss_offensive_rebound_short_reset():
    sop = DummyStart(period=1, seconds_remaining=120)
    miss = DummyFG(is_made=False, description="miss", team_id=1, offense_team_id=1, seconds_remaining=100)
    oreb = DummyReb(oreb=True, missed_shot=miss, team_id=1, offense_team_id=1, seconds_remaining=100)
    tip_in = DummyFG(is_made=True, team_id=1, offense_team_id=1, seconds_remaining=100)
    events = [sop, miss, oreb, tip_in]
    _link_events(events)

    annotate_shot_clock(events, season_year=2018, league="nba")

    assert tip_in.shot_clock == 14.0


def test_shot_clock_violation_display_zero():
    sop = DummyStart(period=1, seconds_remaining=50)
    tov = DummyTO(team_id=2, offense_team_id=1, seconds_remaining=40, is_shot_clock_violation=True)
    events = [sop, tov]
    _link_events(events)

    annotate_shot_clock(events, season_year=2018, league="nba")

    assert tov.shot_clock == 0.0


def test_defensive_kicked_ball_retained_bumps_to_fourteen():
    sop = DummyStart(period=1, seconds_remaining=50)
    kicked = DummyTO(
        team_id=2,
        offense_team_id=1,
        seconds_remaining=35,
        is_kicked_ball=True,
    )
    next_ev = DummyFG(is_made=False, team_id=1, offense_team_id=1, seconds_remaining=30)
    events = [sop, kicked, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2018, league="nba")

    assert next_ev.shot_clock == 9.0


def test_defensive_goaltend_resets_full():
    sop = DummyStart(period=1, seconds_remaining=50)
    goaltend = DummyViol(team_id=2, offense_team_id=1, seconds_remaining=45, is_goaltend_violation=True)
    next_ev = DummyFG(is_made=False, team_id=2, offense_team_id=2, seconds_remaining=44)
    events = [sop, goaltend, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2018, league="nba")

    assert next_ev.shot_clock == 23.0


def test_jump_ball_retained_does_not_reset():
    sop = DummyStart(period=1, seconds_remaining=50)
    jump = DummyJB(team_id=2, offense_team_id=1, seconds_remaining=35)
    next_ev = DummyFG(is_made=False, team_id=1, offense_team_id=1, seconds_remaining=32)
    events = [sop, jump, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2018, league="nba")

    assert next_ev.shot_clock == 6.0


def test_loose_ball_foul_with_rim_context_resets_to_short():
    sop = DummyStart(period=1, seconds_remaining=120)
    miss = DummyFG(is_made=False, description="missed jumper", team_id=1, offense_team_id=1, seconds_remaining=118)
    foul = DummyFoul(
        team_id=2,
        offense_team_id=1,
        seconds_remaining=118,
        is_loose_ball_foul=True,
    )
    next_ev = DummyFG(is_made=False, team_id=1, offense_team_id=1, seconds_remaining=117)
    events = [sop, miss, foul, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2018, league="nba")

    assert next_ev.shot_clock == 13.0


def test_loose_ball_foul_does_not_use_later_miss_same_timestamp_as_rim_context():
    sop = DummyStart(period=1, seconds_remaining=50)
    foul = DummyFoul(
        team_id=2,
        offense_team_id=1,
        seconds_remaining=48,
        is_loose_ball_foul=True,
    )
    # Later miss at same timestamp (should NOT create rim context for the foul)
    late_miss = DummyFG(is_made=False, description="miss", team_id=1, offense_team_id=1, seconds_remaining=48)
    next_ev = DummyFG(is_made=False, description="miss", team_id=1, offense_team_id=1, seconds_remaining=47)
    events = [sop, foul, late_miss, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2018, league="nba")

    # If foul incorrectly hard-resets to 14, this would be 13.0 instead of 21.0
    assert next_ev.shot_clock == 21.0
