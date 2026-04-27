from historic_backfill.audits.core.event_player_on_court import _check_event_players


TEAM_ID = 1610612744
PLAYER_OUT = 101
PLAYER_IN = 202
OTHER_PLAYERS = [1, 2, 3, 4]


class _Event:
    def __init__(
        self,
        *,
        event_num,
        period=1,
        clock="4:03",
        player1_id=0,
        player2_id=0,
        player3_id=0,
        team_id=TEAM_ID,
        current_players=None,
        previous_event=None,
        description="",
    ):
        self.event_num = event_num
        self.period = period
        self.clock = clock
        self.player1_id = player1_id
        self.player2_id = player2_id
        self.player3_id = player3_id
        self.team_id = team_id
        self.current_players = current_players or {TEAM_ID: OTHER_PLAYERS + [PLAYER_IN]}
        self.previous_event = previous_event
        self.description = description


class StatsFoul(_Event):
    pass


class StatsFieldGoal(_Event):
    pass


class StatsFreeThrow(_Event):
    pass


class StatsRebound(_Event):
    pass


class StatsSubstitution(_Event):
    def __init__(self, *, outgoing_player_id=PLAYER_OUT, incoming_player_id=PLAYER_IN, **kwargs):
        super().__init__(
            player1_id=outgoing_player_id,
            player2_id=incoming_player_id,
            current_players={TEAM_ID: OTHER_PLAYERS + [incoming_player_id]},
            **kwargs,
        )
        self.outgoing_player_id = outgoing_player_id
        self.incoming_player_id = incoming_player_id


def _player_team_map():
    return {
        PLAYER_OUT: TEAM_ID,
        PLAYER_IN: TEAM_ID,
        **{player_id: TEAM_ID for player_id in OTHER_PLAYERS},
    }


def test_same_clock_sub_out_makes_fouler_control_eligible():
    prior = _Event(event_num=99, current_players={TEAM_ID: OTHER_PLAYERS + [PLAYER_OUT]})
    foul = StatsFoul(event_num=100, player1_id=PLAYER_OUT)
    sub = StatsSubstitution(event_num=101, previous_event=prior)

    issues = _check_event_players("0021700236", [foul, sub], _player_team_map())

    assert issues.empty


def test_same_clock_live_credit_makes_sub_out_control_eligible():
    prior = _Event(event_num=99, current_players={TEAM_ID: OTHER_PLAYERS + [PLAYER_IN]})
    foul = StatsFoul(event_num=100, player1_id=PLAYER_OUT)
    sub = StatsSubstitution(event_num=101, previous_event=prior)

    issues = _check_event_players("0021300593", [foul, sub], _player_team_map())

    assert issues.empty


def test_missing_sub_out_without_same_clock_live_credit_still_flags():
    prior = _Event(event_num=99, current_players={TEAM_ID: OTHER_PLAYERS + [PLAYER_IN]})
    sub = StatsSubstitution(event_num=101, previous_event=prior)

    issues = _check_event_players("0021300593", [sub], _player_team_map())

    assert len(issues[issues["event_num"] == 101]) == 1


def test_same_clock_sub_out_covers_legacy_foul_duplicate_player_field():
    prior = _Event(event_num=99, current_players={TEAM_ID: OTHER_PLAYERS + [PLAYER_OUT]})
    foul = StatsFoul(event_num=100, player1_id=PLAYER_OUT, player3_id=PLAYER_OUT)
    sub = StatsSubstitution(event_num=101, previous_event=prior)

    issues = _check_event_players("0049600063", [foul, sub], _player_team_map())

    assert issues.empty


def test_same_clock_control_rule_does_not_hide_ordinary_live_shot():
    shot = StatsFieldGoal(event_num=100, player1_id=PLAYER_OUT)
    sub = StatsSubstitution(event_num=101)

    issues = _check_event_players("0021700236", [shot, sub], _player_team_map())

    assert len(issues[issues["event_num"] == 100]) == 1


def test_same_clock_sub_in_makes_replacement_free_throw_control_eligible():
    prior = _Event(event_num=98, current_players={TEAM_ID: OTHER_PLAYERS + [PLAYER_OUT]})
    foul = StatsFoul(event_num=98, player1_id=999, player2_id=PLAYER_OUT, team_id=1610612760)
    free_throw = StatsFreeThrow(
        event_num=100,
        player1_id=PLAYER_IN,
        current_players={TEAM_ID: OTHER_PLAYERS + [PLAYER_OUT]},
        description="Lauvergne Free Throw 1 of 2 (9 PTS)",
    )
    sub = StatsSubstitution(
        event_num=99,
        incoming_player_id=PLAYER_IN,
        outgoing_player_id=PLAYER_OUT,
        previous_event=prior,
    )

    issues = _check_event_players("0021700337", [foul, sub, free_throw], _player_team_map())

    assert issues.empty


def test_same_clock_sub_in_does_not_hide_ordinary_non_replacement_free_throw():
    prior = _Event(event_num=98, current_players={TEAM_ID: OTHER_PLAYERS + [PLAYER_OUT]})
    free_throw = StatsFreeThrow(
        event_num=100,
        player1_id=PLAYER_IN,
        current_players={TEAM_ID: OTHER_PLAYERS + [PLAYER_OUT]},
        description="Player Free Throw 1 of 2",
    )
    sub = StatsSubstitution(
        event_num=99,
        incoming_player_id=PLAYER_IN,
        outgoing_player_id=PLAYER_OUT,
        previous_event=prior,
    )

    issues = _check_event_players("0021700337", [sub, free_throw], _player_team_map())

    assert len(issues[issues["event_num"] == 100]) == 1


def test_same_clock_sub_out_makes_technical_free_throw_control_eligible():
    prior = _Event(event_num=98, current_players={TEAM_ID: OTHER_PLAYERS + [PLAYER_OUT]})
    free_throw = StatsFreeThrow(
        event_num=100,
        player1_id=PLAYER_OUT,
        description="Curry Free Throw Technical (10 PTS)",
    )
    sub = StatsSubstitution(
        event_num=99,
        outgoing_player_id=PLAYER_OUT,
        incoming_player_id=PLAYER_IN,
        previous_event=prior,
    )

    issues = _check_event_players("0021700917", [sub, free_throw], _player_team_map())

    assert issues.empty


def test_same_clock_sub_in_makes_rebound_control_eligible():
    prior = _Event(event_num=98, current_players={TEAM_ID: OTHER_PLAYERS + [PLAYER_OUT]})
    rebound = StatsRebound(
        event_num=100,
        player1_id=PLAYER_IN,
        current_players={TEAM_ID: OTHER_PLAYERS + [PLAYER_OUT]},
    )
    sub = StatsSubstitution(
        event_num=99,
        incoming_player_id=PLAYER_IN,
        outgoing_player_id=PLAYER_OUT,
        previous_event=prior,
    )

    issues = _check_event_players("0021700236", [sub, rebound], _player_team_map())

    assert issues.empty
