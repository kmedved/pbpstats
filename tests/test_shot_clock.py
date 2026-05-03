import os
import sys

sys.path.insert(0, os.path.abspath("."))

import pbpstats
from pbpstats.data_loader.live.enhanced_pbp.loader import LiveEnhancedPbpLoader
from pbpstats.data_loader.nba_enhanced_pbp_loader import NbaEnhancedPbpLoader
from pbpstats.resources.enhanced_pbp.live.rebound import LiveRebound
from pbpstats.resources.enhanced_pbp.live.turnover import LiveTurnover
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
    def __init__(
        self,
        *,
        period=1,
        seconds_remaining=0.0,
        description="",
        team_id=None,
        offense_team_id=None,
        game_id=None,
    ):
        self.game_id = game_id
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


class DummyBrokenDreb(Rebound, DummyEvent):
    @property
    def is_real_rebound(self):
        raise RuntimeError("missing linked shot")

    @property
    def oreb(self):
        return False

    @property
    def missed_shot(self):
        raise RuntimeError("missing linked shot")


class DummyMalformedDreb(DummyBrokenDreb):
    @property
    def oreb(self):
        raise AttributeError("missing subtype")


class DummyNoOffenseEvent(DummyEvent):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        del self.offense_team_id


class DummyTO(Turnover, DummyEvent):
    def __init__(self, *, is_no_turnover=False, is_kicked_ball=False, is_shot_clock_violation=False, **kwargs):
        DummyEvent.__init__(self, **kwargs)
        self.is_no_turnover = is_no_turnover
        self.is_kicked_ball = is_kicked_ball
        self.is_shot_clock_violation = is_shot_clock_violation


class DummyStatsKickedBallTO(DummyTO):
    def get_offense_team_id(self):
        return self.team_id


class DummyFoul(Foul, DummyEvent):
    def __init__(
        self,
        *,
        is_technical=False,
        is_double_technical=False,
        is_double_foul=False,
        is_clear_path_foul=False,
        is_flagrant1=False,
        is_flagrant2=False,
        is_shooting_foul=False,
        is_shooting_block_foul=False,
        is_loose_ball_foul=False,
        is_defensive_3_seconds=False,
        is_delay_of_game=False,
        **kwargs,
    ):
        DummyEvent.__init__(self, **kwargs)
        self._is_technical = is_technical
        self._is_double_technical = is_double_technical
        self._is_double_foul = is_double_foul
        self._is_clear_path_foul = is_clear_path_foul
        self._is_flagrant1 = is_flagrant1
        self._is_flagrant2 = is_flagrant2
        self._is_shooting_foul = is_shooting_foul
        self._is_shooting_block_foul = is_shooting_block_foul
        self._is_loose_ball_foul = is_loose_ball_foul
        self._is_defensive_3_seconds = is_defensive_3_seconds
        self._is_delay_of_game = is_delay_of_game

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
    def is_clear_path_foul(self):
        return self._is_clear_path_foul

    @property
    def is_flagrant1(self):
        return self._is_flagrant1

    @property
    def is_flagrant2(self):
        return self._is_flagrant2

    @property
    def is_shooting_foul(self):
        return self._is_shooting_foul

    @property
    def is_shooting_block_foul(self):
        return self._is_shooting_block_foul

    @property
    def is_loose_ball_foul(self):
        return self._is_loose_ball_foul

    @property
    def is_defensive_3_seconds(self):
        return self._is_defensive_3_seconds

    @property
    def is_delay_of_game(self):
        return self._is_delay_of_game


class DummyViol(Violation, DummyEvent):
    def __init__(self, *, is_goaltend_violation=False, **kwargs):
        DummyEvent.__init__(self, **kwargs)
        self.is_goaltend_violation = is_goaltend_violation


class DummyJB(JumpBall, DummyEvent):
    pass


class DummyFT(FreeThrow, DummyEvent):
    def __init__(self, *, is_end_ft=False, is_made=False, **kwargs):
        DummyEvent.__init__(self, **kwargs)
        self._is_end_ft = is_end_ft
        self._is_made = is_made

    @property
    def is_made(self):
        return self._is_made

    @property
    def is_ft_1_of_1(self):
        return self._is_end_ft

    @property
    def is_ft_1_of_2(self):
        return False

    @property
    def is_ft_2_of_2(self):
        return False

    @property
    def is_ft_1_of_3(self):
        return False

    @property
    def is_ft_2_of_3(self):
        return False

    @property
    def is_ft_3_of_3(self):
        return False

    @property
    def is_technical_ft(self):
        return False

    @property
    def is_ft_1pt(self):
        return False

    @property
    def is_ft_2pt(self):
        return False

    @property
    def is_ft_3pt(self):
        return False


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


def test_nba_short_reset_starts_in_2018():
    sop = DummyStart(period=1, seconds_remaining=120)
    miss = DummyFG(
        is_made=False,
        description="miss",
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
    )
    oreb = DummyReb(
        oreb=True,
        missed_shot=miss,
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
    )
    next_ev = DummyFG(
        is_made=False, team_id=1, offense_team_id=1, seconds_remaining=99
    )
    events = [sop, miss, oreb, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2017, league=pbpstats.NBA_STRING)
    assert next_ev.shot_clock == 23.0

    annotate_shot_clock(events, season_year=2018, league=pbpstats.NBA_STRING)
    assert next_ev.shot_clock == 13.0


def test_nba_loader_infers_season_from_game_id_for_short_reset():
    sop = DummyStart(period=1, seconds_remaining=120)
    miss = DummyFG(
        is_made=False,
        description="miss",
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
    )
    oreb = DummyReb(
        oreb=True,
        missed_shot=miss,
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
    )
    tip_in = DummyFG(
        is_made=True, team_id=1, offense_team_id=1, seconds_remaining=100
    )
    events = [sop, miss, oreb, tip_in]
    _link_events(events)

    loader = object.__new__(NbaEnhancedPbpLoader)
    loader.game_id = "0022300001"
    loader.league = pbpstats.NBA_STRING
    loader.items = events

    loader._annotate_shot_clock()

    assert tip_in.shot_clock == 14.0


def test_nba_loader_infers_league_and_season_from_numeric_game_id():
    sop = DummyStart(period=1, seconds_remaining=120)
    miss = DummyFG(
        is_made=False,
        description="miss",
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
    )
    oreb = DummyReb(
        oreb=True,
        missed_shot=miss,
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
    )
    tip_in = DummyFG(
        is_made=True, team_id=1, offense_team_id=1, seconds_remaining=100
    )
    events = [sop, miss, oreb, tip_in]
    _link_events(events)

    loader = object.__new__(NbaEnhancedPbpLoader)
    loader.game_id = 22300001
    loader.items = events

    loader._annotate_shot_clock()

    assert tip_in.shot_clock == 14.0


def test_annotate_shot_clock_infers_league_and_season_from_event_game_id():
    sop = DummyStart(
        period=1,
        seconds_remaining=120,
        game_id="1021500001",
    )
    miss = DummyFG(
        is_made=False,
        description="miss",
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
        game_id="1021500001",
    )
    oreb = DummyReb(
        oreb=True,
        missed_shot=miss,
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
        game_id="1021500001",
    )
    next_ev = DummyFG(
        is_made=False,
        team_id=1,
        offense_team_id=1,
        seconds_remaining=99,
        game_id="1021500001",
    )
    events = [sop, miss, oreb, next_ev]
    _link_events(events)

    annotate_shot_clock(events)

    assert next_ev.shot_clock == 23.0


def test_annotate_shot_clock_infers_nba_from_numeric_event_game_id():
    sop = DummyStart(
        period=1,
        seconds_remaining=120,
        game_id=22300001,
    )
    miss = DummyFG(
        is_made=False,
        description="miss",
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
        game_id=22300001,
    )
    oreb = DummyReb(
        oreb=True,
        missed_shot=miss,
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
        game_id=22300001,
    )
    next_ev = DummyFG(
        is_made=False,
        team_id=1,
        offense_team_id=1,
        seconds_remaining=99,
        game_id=22300001,
    )
    events = [sop, miss, oreb, next_ev]
    _link_events(events)

    annotate_shot_clock(events)

    assert next_ev.shot_clock == 13.0


def test_wnba_short_reset_starts_in_2016():
    sop = DummyStart(period=1, seconds_remaining=120)
    miss = DummyFG(
        is_made=False,
        description="miss",
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
    )
    oreb = DummyReb(
        oreb=True,
        missed_shot=miss,
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
    )
    next_ev = DummyFG(
        is_made=False, team_id=1, offense_team_id=1, seconds_remaining=99
    )
    events = [sop, miss, oreb, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2015, league=pbpstats.WNBA_STRING)
    assert next_ev.shot_clock == 23.0

    annotate_shot_clock(events, season_year=2016, league=pbpstats.WNBA_STRING)
    assert next_ev.shot_clock == 13.0


def test_g_league_short_reset_starts_in_2016():
    sop = DummyStart(period=1, seconds_remaining=120)
    miss = DummyFG(
        is_made=False,
        description="miss",
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
    )
    oreb = DummyReb(
        oreb=True,
        missed_shot=miss,
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
    )
    next_ev = DummyFG(
        is_made=False, team_id=1, offense_team_id=1, seconds_remaining=99
    )
    events = [sop, miss, oreb, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2015, league=pbpstats.G_LEAGUE_STRING)
    assert next_ev.shot_clock == 23.0

    annotate_shot_clock(events, season_year=2016, league=pbpstats.G_LEAGUE_STRING)
    assert next_ev.shot_clock == 13.0


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


def test_stats_style_kicked_ball_retained_bumps_to_fourteen():
    sop = DummyStart(period=1, seconds_remaining=50)
    kicked = DummyStatsKickedBallTO(
        team_id=2,
        offense_team_id=1,
        seconds_remaining=35,
        is_kicked_ball=True,
    )
    kicked.non_possession_changing_override = True
    next_ev = DummyFG(is_made=False, team_id=1, offense_team_id=1, seconds_remaining=30)
    events = [sop, kicked, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2018, league=pbpstats.NBA_STRING)

    assert next_ev.shot_clock == 9.0


def test_stats_style_kicked_ball_retained_without_override_bumps_to_fourteen():
    sop = DummyStart(period=1, seconds_remaining=50, offense_team_id=1)
    kicked = DummyStatsKickedBallTO(
        team_id=2,
        offense_team_id=1,
        seconds_remaining=35,
        is_kicked_ball=True,
    )
    next_ev = DummyFG(is_made=False, team_id=1, offense_team_id=1, seconds_remaining=30)
    events = [sop, kicked, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2018, league=pbpstats.NBA_STRING)

    assert next_ev.shot_clock == 9.0


def test_live_rebound_oreb_normalizes_sub_type_case_and_spacing():
    rebound = LiveRebound(
        {
            "period": 1,
            "actionNumber": 7,
            "clock": "PT1M00.00S",
            "actionType": "rebound",
            "subType": "Offen-sive",
            "teamId": 1,
            "personId": 1,
            "possession": 1,
        },
        "0022400001",
    )

    assert rebound.oreb


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


def test_defensive_technical_retained_bumps_to_fourteen():
    sop = DummyStart(period=1, seconds_remaining=50)
    technical = DummyFoul(
        team_id=2,
        offense_team_id=1,
        seconds_remaining=34,
        is_technical=True,
    )
    next_ev = DummyFG(is_made=False, team_id=1, offense_team_id=1, seconds_remaining=33)
    events = [sop, technical, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2023, league=pbpstats.NBA_STRING)

    assert next_ev.shot_clock == 13.0


def test_double_technical_retained_bumps_to_fourteen():
    sop = DummyStart(period=1, seconds_remaining=50)
    technical = DummyFoul(
        team_id=2,
        offense_team_id=1,
        seconds_remaining=34,
        is_double_technical=True,
    )
    next_ev = DummyFG(is_made=False, team_id=1, offense_team_id=1, seconds_remaining=33)
    events = [sop, technical, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2023, league=pbpstats.NBA_STRING)

    assert next_ev.shot_clock == 13.0


def test_double_foul_retained_bumps_to_fourteen():
    sop = DummyStart(period=1, seconds_remaining=50)
    foul = DummyFoul(
        team_id=2,
        offense_team_id=1,
        seconds_remaining=34,
        is_double_foul=True,
    )
    next_ev = DummyFG(is_made=False, team_id=1, offense_team_id=1, seconds_remaining=33)
    events = [sop, foul, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2023, league=pbpstats.NBA_STRING)

    assert next_ev.shot_clock == 13.0


def test_defensive_delay_of_game_retained_bumps_to_fourteen():
    sop = DummyStart(period=1, seconds_remaining=50)
    delay = DummyFoul(
        team_id=2,
        offense_team_id=1,
        seconds_remaining=34,
        is_delay_of_game=True,
    )
    next_ev = DummyFG(is_made=False, team_id=1, offense_team_id=1, seconds_remaining=33)
    events = [sop, delay, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2023, league=pbpstats.NBA_STRING)

    assert next_ev.shot_clock == 13.0


def test_offensive_technical_retained_does_not_bump():
    sop = DummyStart(period=1, seconds_remaining=50)
    technical = DummyFoul(
        team_id=1,
        offense_team_id=1,
        seconds_remaining=34,
        is_technical=True,
    )
    next_ev = DummyFG(is_made=False, team_id=1, offense_team_id=1, seconds_remaining=33)
    events = [sop, technical, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2023, league=pbpstats.NBA_STRING)

    assert next_ev.shot_clock == 7.0


def test_defensive_flagrant_foul_resets_full():
    sop = DummyStart(period=1, seconds_remaining=50)
    foul = DummyFoul(
        team_id=2,
        offense_team_id=1,
        seconds_remaining=34,
        is_flagrant1=True,
    )
    next_ev = DummyFG(is_made=False, team_id=1, offense_team_id=1, seconds_remaining=33)
    events = [sop, foul, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2023, league=pbpstats.NBA_STRING)

    assert next_ev.shot_clock == 23.0


def test_defensive_violation_after_rim_context_resets_to_short():
    sop = DummyStart(period=1, seconds_remaining=120)
    miss = DummyFG(
        is_made=False,
        description="missed jumper",
        team_id=1,
        offense_team_id=1,
        seconds_remaining=118,
    )
    violation = DummyViol(team_id=2, offense_team_id=1, seconds_remaining=118)
    next_ev = DummyFG(is_made=False, team_id=1, offense_team_id=1, seconds_remaining=117)
    events = [sop, miss, violation, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2023, league=pbpstats.NBA_STRING)

    assert next_ev.shot_clock == 13.0


def test_retained_deadball_rebound_after_rim_resets_to_short():
    sop = DummyStart(period=1, seconds_remaining=120)
    miss = DummyFG(
        is_made=False,
        description="missed jumper",
        team_id=1,
        offense_team_id=1,
        seconds_remaining=118,
    )
    team_rebound = DummyReb(
        is_real_rebound=False,
        oreb=False,
        missed_shot=miss,
        team_id=1,
        offense_team_id=1,
        seconds_remaining=118,
    )
    next_ev = DummyFG(is_made=False, team_id=1, offense_team_id=1, seconds_remaining=117)
    events = [sop, miss, team_rebound, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2023, league=pbpstats.NBA_STRING)

    assert next_ev.shot_clock == 13.0


def test_retained_deadball_rebound_after_airball_does_not_reset():
    sop = DummyStart(period=1, seconds_remaining=120)
    miss = DummyFG(
        is_made=False,
        description="missed airball",
        team_id=1,
        offense_team_id=1,
        seconds_remaining=118,
    )
    team_rebound = DummyReb(
        is_real_rebound=False,
        oreb=False,
        missed_shot=miss,
        team_id=1,
        offense_team_id=1,
        seconds_remaining=118,
    )
    next_ev = DummyFG(is_made=False, team_id=1, offense_team_id=1, seconds_remaining=117)
    events = [sop, miss, team_rebound, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2023, league=pbpstats.NBA_STRING)

    assert next_ev.shot_clock == 21.0


def test_non_final_free_throw_deadball_rebound_does_not_reset():
    sop = DummyStart(period=1, seconds_remaining=120)
    miss = DummyFT(
        is_end_ft=False,
        description="missed free throw 1 of 2",
        team_id=1,
        offense_team_id=1,
        seconds_remaining=118,
    )
    team_rebound = DummyReb(
        is_real_rebound=False,
        oreb=False,
        missed_shot=miss,
        team_id=1,
        offense_team_id=1,
        seconds_remaining=118,
    )
    next_ev = DummyFG(is_made=False, team_id=1, offense_team_id=1, seconds_remaining=117)
    events = [sop, miss, team_rebound, next_ev]
    _link_events(events)

    annotate_shot_clock(events, season_year=2023, league=pbpstats.NBA_STRING)

    assert next_ev.shot_clock == 21.0


def test_live_dreb_normalization_uses_linked_missed_shot():
    miss = DummyFG(
        is_made=False,
        description="miss",
        team_id=1,
        offense_team_id=1,
        seconds_remaining=100,
    )
    admin = DummyEvent(team_id=2, offense_team_id=2, seconds_remaining=100)
    dreb = DummyReb(
        oreb=False,
        missed_shot=miss,
        team_id=2,
        offense_team_id=2,
        seconds_remaining=99,
    )
    events = [miss, admin, dreb]
    _link_events(events)
    loader = object.__new__(LiveEnhancedPbpLoader)
    loader.items = events

    loader._change_team_id_on_drebs()

    assert dreb.offense_team_id == 1


def test_live_dreb_normalization_falls_back_when_linked_shot_unavailable():
    previous_event = DummyEvent(team_id=2, offense_team_id=1, seconds_remaining=100)
    dreb = DummyBrokenDreb(team_id=2, offense_team_id=2, seconds_remaining=99)
    events = [previous_event, dreb]
    _link_events(events)
    loader = object.__new__(LiveEnhancedPbpLoader)
    loader.items = events

    loader._change_team_id_on_drebs()

    assert dreb.offense_team_id == 1


def test_live_dreb_normalization_falls_back_when_oreb_flag_unavailable():
    previous_event = DummyEvent(team_id=2, offense_team_id=1, seconds_remaining=100)
    dreb = DummyMalformedDreb(team_id=2, offense_team_id=2, seconds_remaining=99)
    events = [previous_event, dreb]
    _link_events(events)
    loader = object.__new__(LiveEnhancedPbpLoader)
    loader.items = events

    loader._change_team_id_on_drebs()

    assert dreb.offense_team_id == 1


def test_live_dreb_normalization_does_not_crash_without_offense_fields():
    previous_event = DummyNoOffenseEvent(team_id=2, seconds_remaining=100)
    dreb = DummyMalformedDreb(team_id=2, offense_team_id=2, seconds_remaining=99)
    del dreb.offense_team_id
    events = [previous_event, dreb]
    _link_events(events)
    loader = object.__new__(LiveEnhancedPbpLoader)
    loader.items = events

    loader._change_team_id_on_drebs()

    assert dreb.offense_team_id is None


def test_live_lane_violation_turnover_normalizes_sub_type_case():
    event = {
        "period": 1,
        "actionNumber": 1,
        "clock": "PT10M00.00S",
        "actionType": "turnover",
        "subType": "lane violation",
        "teamId": 1,
        "personId": 1,
        "possession": 1,
        "orderNumber": 1,
    }

    turnover = LiveTurnover(event, "0022300001")

    assert turnover.is_lane_violation is True


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
