from pbpstats.resources.enhanced_pbp import (
    FieldGoal,
    JumpBall,
    StartOfPeriod,
    Substitution,
)
from pbpstats.resources.enhanced_pbp.live.field_goal import LiveFieldGoal
from pbpstats.resources.enhanced_pbp.live.free_throw import LiveFreeThrow
from pbpstats.resources.enhanced_pbp.live.jump_ball import LiveJumpBall
from pbpstats.resources.enhanced_pbp.live.start_of_period import LiveStartOfPeriod
from pbpstats.resources.enhanced_pbp.live.substitution import LiveSubstitution
from pbpstats.resources.possessions.possession import Possession

LIVE_GAME_ID = "1022500234"
HOME_ID = 1610612764
AWAY_ID = 1610612739


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


class DummyStatsStartOfPeriod(DummyStartOfPeriod):
    event_type = 12


class DummyStatsAdminEvent:
    def __init__(
        self,
        event_type,
        team_id,
        offense_team_id,
        game_id="0020000001",
        period=2,
        clock="12:00",
    ):
        self.event_type = event_type
        self.game_id = game_id
        self.period = period
        self.clock = clock
        self.team_id = team_id
        self.offense_team_id = offense_team_id

    def get_offense_team_id(self):
        return self.offense_team_id


class DummyPlay(FieldGoal):
    action_type = "2pt"

    def __init__(
        self,
        team_id,
        offense_team_id,
        is_made=False,
        game_id="0020000001",
        period=2,
        clock="11:58",
    ):
        self.game_id = game_id
        self.period = period
        self.clock = clock
        self.team_id = team_id
        self.offense_team_id = offense_team_id
        self._is_made = is_made

    def get_offense_team_id(self):
        return self.offense_team_id

    @property
    def is_made(self):
        return self._is_made

    @property
    def shot_value(self):
        return 2


class DummyJumpBall(JumpBall):
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


def _wire_events(events):
    for index, event in enumerate(events):
        event.previous_event = events[index - 1] if index else None
        event.next_event = events[index + 1] if index + 1 < len(events) else None
    return events


def _live_event_payload(
    *,
    action_number,
    action_type,
    sub_type,
    team_id=0,
    person_id=0,
    possession=0,
    period=2,
    clock="PT10M00.00S",
    shot_result=None,
    descriptor=None,
    qualifiers=None,
):
    payload = {
        "period": period,
        "actionNumber": action_number,
        "clock": clock,
        "actionType": action_type,
        "subType": sub_type,
        "teamId": team_id,
        "personId": person_id,
        "possession": possession,
        "description": "",
    }
    if shot_result is not None:
        payload["shotResult"] = shot_result
    if descriptor is not None:
        payload["descriptor"] = descriptor
    if qualifiers is not None:
        payload["qualifiers"] = qualifiers
    return payload


def test_possession_prefers_start_of_period_offense():
    start_event = DummyStartOfPeriod(offense_team_id=111)
    startperiod_sub = DummySubstitution(
        team_id=222, offense_team_id=222, qualifiers=["startperiod"]
    )
    first_play = DummyPlay(team_id=111, offense_team_id=111)

    possession = Possession([start_event, startperiod_sub, first_play])

    assert possession.offense_team_id == 111


def test_live_period_start_reconciles_stale_explicit_possession_with_field_goal():
    startperiod_sub = LiveSubstitution(
        _live_event_payload(
            action_number=1,
            action_type="substitution",
            sub_type="in",
            team_id=AWAY_ID,
            person_id=1,
            possession=AWAY_ID,
            qualifiers=["startperiod"],
        ),
        LIVE_GAME_ID,
    )
    start_event = LiveStartOfPeriod(
        _live_event_payload(
            action_number=2,
            action_type="period",
            sub_type="start",
            possession=AWAY_ID,
        ),
        LIVE_GAME_ID,
    )
    field_goal = LiveFieldGoal(
        _live_event_payload(
            action_number=3,
            action_type="2pt",
            sub_type="jump shot",
            team_id=HOME_ID,
            person_id=10,
            possession=HOME_ID,
            shot_result="Made",
            clock="PT09M50.00S",
        ),
        LIVE_GAME_ID,
    )
    events = _wire_events([startperiod_sub, start_event, field_goal])

    possession = Possession(events)

    assert possession._get_head_event_for_offense() is start_event
    assert start_event.get_offense_team_id() == HOME_ID
    assert possession.offense_team_id == HOME_ID


def test_live_period_start_keeps_explicit_possession_when_inference_unavailable():
    start_event = LiveStartOfPeriod(
        _live_event_payload(
            action_number=1,
            action_type="period",
            sub_type="start",
            possession=AWAY_ID,
        ),
        LIVE_GAME_ID,
    )
    start_event.previous_event = None
    start_event.next_event = None

    assert start_event.get_offense_team_id() == AWAY_ID


def test_live_period_start_technical_free_throw_does_not_drive_reconciliation():
    start_event = LiveStartOfPeriod(
        _live_event_payload(
            action_number=1,
            action_type="period",
            sub_type="start",
            possession=AWAY_ID,
        ),
        LIVE_GAME_ID,
    )
    technical_ft = LiveFreeThrow(
        _live_event_payload(
            action_number=2,
            action_type="freethrow",
            sub_type="1 of 1",
            team_id=HOME_ID,
            person_id=10,
            possession=HOME_ID,
            shot_result="Made",
            descriptor="technical",
            clock="PT10M00.00S",
        ),
        LIVE_GAME_ID,
    )
    field_goal = LiveFieldGoal(
        _live_event_payload(
            action_number=3,
            action_type="2pt",
            sub_type="jump shot",
            team_id=AWAY_ID,
            person_id=20,
            possession=AWAY_ID,
            shot_result="Made",
            clock="PT09M50.00S",
        ),
        LIVE_GAME_ID,
    )
    _wire_events([start_event, technical_ft, field_goal])

    assert start_event.get_offense_team_id() == AWAY_ID


def test_live_period_start_reconciles_stale_explicit_possession_with_normal_ft():
    start_event = LiveStartOfPeriod(
        _live_event_payload(
            action_number=1,
            action_type="period",
            sub_type="start",
            possession=HOME_ID,
        ),
        LIVE_GAME_ID,
    )
    free_throw = LiveFreeThrow(
        _live_event_payload(
            action_number=2,
            action_type="freethrow",
            sub_type="1 of 2",
            team_id=AWAY_ID,
            person_id=20,
            possession=AWAY_ID,
            shot_result="Made",
            clock="PT10M00.00S",
        ),
        LIVE_GAME_ID,
    )
    _wire_events([start_event, free_throw])

    assert start_event.get_offense_team_id() == AWAY_ID


def test_live_period_start_reconciles_stale_explicit_possession_with_jump_ball():
    start_event = LiveStartOfPeriod(
        _live_event_payload(
            action_number=1,
            action_type="period",
            sub_type="start",
            possession=HOME_ID,
            period=1,
            clock="PT10M00.00S",
        ),
        LIVE_GAME_ID,
    )
    jump_ball = LiveJumpBall(
        _live_event_payload(
            action_number=2,
            action_type="jumpball",
            sub_type="",
            team_id=AWAY_ID,
            person_id=20,
            possession=AWAY_ID,
            period=1,
            clock="PT10M00.00S",
        ),
        LIVE_GAME_ID,
    )
    _wire_events([start_event, jump_ball])

    assert start_event.get_offense_team_id() == AWAY_ID


def test_possession_without_start_of_period_uses_head_event():
    opening_sub = DummySubstitution(team_id=444, offense_team_id=444)
    next_play = DummyPlay(team_id=555, offense_team_id=555)

    possession = Possession([opening_sub, next_play])

    assert possession.offense_team_id == 444


def test_possession_skips_live_startperiod_sub_when_no_start_of_period():
    startperiod_sub = LiveSubstitution(
        _live_event_payload(
            action_number=1,
            action_type="substitution",
            sub_type="in",
            team_id=AWAY_ID,
            person_id=20,
            possession=AWAY_ID,
            qualifiers=["startperiod"],
        ),
        LIVE_GAME_ID,
    )
    first_play = LiveFieldGoal(
        _live_event_payload(
            action_number=2,
            action_type="2pt",
            sub_type="jump shot",
            team_id=HOME_ID,
            person_id=10,
            possession=HOME_ID,
            shot_result="Made",
            clock="PT09M50.00S",
        ),
        LIVE_GAME_ID,
    )

    possession = Possession([startperiod_sub, first_play])

    assert possession.offense_team_id == HOME_ID


def test_non_live_startperiod_sub_preserves_first_event_behavior():
    startperiod_sub = DummySubstitution(
        team_id=222, offense_team_id=222, qualifiers=["startperiod"]
    )
    first_play = DummyPlay(team_id=333, offense_team_id=333)

    possession = Possession([startperiod_sub, first_play])

    assert possession.offense_team_id == 222


def test_stats_style_period_start_keeps_start_period_inference():
    start_event = DummyStatsStartOfPeriod(offense_team_id=111)
    timeout = DummyStatsAdminEvent(event_type=9, team_id=222, offense_team_id=222)
    replay = DummyStatsAdminEvent(event_type=18, team_id=0, offense_team_id=222)
    first_play = DummyPlay(team_id=333, offense_team_id=333)

    possession = Possession([start_event, timeout, replay, first_play])

    assert possession._get_head_event_for_offense() is start_event
    assert possession.offense_team_id == 111


def test_single_jump_ball_possession_behavior_is_preserved():
    previous_event = DummyPlay(team_id=111, offense_team_id=111, is_made=True)
    jump_ball = DummyJumpBall(team_id=222, offense_team_id=222)
    previous_possession = Possession([previous_event])
    possession = Possession([jump_ball])
    previous_possession.previous_possession = None
    previous_possession.next_possession = possession
    possession.previous_possession = previous_possession
    possession.next_possession = None

    assert possession.offense_team_id == 222
