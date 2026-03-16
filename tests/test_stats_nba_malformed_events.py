from types import SimpleNamespace

from pbpstats import NBA_STRING
from pbpstats.data_loader.nba_enhanced_pbp_loader import NbaEnhancedPbpLoader
from pbpstats.data_loader.nba_possession_loader import NbaPossessionLoader
from pbpstats.resources.enhanced_pbp.stats_nba.foul import StatsFoul
from pbpstats.resources.enhanced_pbp.stats_nba.jump_ball import StatsJumpBall
from pbpstats.resources.enhanced_pbp.stats_nba.start_of_period import StatsStartOfPeriod
from pbpstats.resources.enhanced_pbp.stats_nba.substitution import StatsSubstitution
from pbpstats.resources.enhanced_pbp.stats_nba.turnover import StatsTurnover


def _event(
    *,
    game_id: str,
    event_num: int,
    event_type: int,
    clock: str,
    period: int,
    player1_id: int | None = None,
    player1_team_id: int | None = None,
    player2_id: int | None = None,
    player2_team_id: int | None = None,
    player3_id: int | None = None,
    player3_team_id: int | None = None,
    home_description: str | None = "",
    visitor_description: str | None = "",
    neutral_description: str | None = None,
) -> dict:
    return {
        "GAME_ID": game_id,
        "EVENTNUM": event_num,
        "EVENTMSGTYPE": event_type,
        "EVENTMSGACTIONTYPE": 0,
        "PCTIMESTRING": clock,
        "PERIOD": period,
        "PLAYER1_ID": player1_id,
        "PLAYER1_TEAM_ID": player1_team_id,
        "PLAYER2_ID": player2_id,
        "PLAYER2_TEAM_ID": player2_team_id,
        "PLAYER3_ID": player3_id,
        "PLAYER3_TEAM_ID": player3_team_id,
        "HOMEDESCRIPTION": home_description,
        "VISITORDESCRIPTION": visitor_description,
        "NEUTRALDESCRIPTION": neutral_description,
        "VIDEO_AVAILABLE_FLAG": 0,
    }


def test_stats_substitution_missing_incoming_player_becomes_noop_lineup_change():
    sub = StatsSubstitution(
        _event(
            game_id="0029700621",
            event_num=579,
            event_type=8,
            clock="0:00",
            period=5,
            player1_id=688,
            player1_team_id=1610612749,
            home_description="",
            visitor_description="SUB:  FOR Curry",
        ),
        0,
    )
    previous_players = {
        1610612749: [688, 210, 238, 299, 951],
        1610612752: [168, 201, 275, 317, 369],
    }
    sub.previous_event = SimpleNamespace(current_players=previous_players)

    assert sub.incoming_player_id == 688
    assert sub.current_players == previous_players


def test_stats_substitution_with_no_valid_team_or_players_is_noop():
    sub = StatsSubstitution(
        _event(
            game_id="0020000883",
            event_num=458,
            event_type=8,
            clock="0:46",
            period=4,
            player1_id=0,
            player1_team_id=0,
            player2_id=0,
            player2_team_id=0,
            home_description="",
            visitor_description="",
        ),
        0,
    )
    previous_players = {
        1610612756: [219, 361, 386, 1609, 2074],
        1610612758: [57, 185, 1517, 2039, 2091],
    }
    sub.previous_event = SimpleNamespace(current_players=previous_players)

    assert sub.current_players == previous_players


def test_stats_jump_ball_with_incomplete_lineups_returns_empty_event_stats():
    start = StatsStartOfPeriod(
        _event(
            game_id="0049700045",
            event_num=0,
            event_type=12,
            clock="12:00",
            period=1,
            neutral_description="Start of 1st Period",
        ),
        0,
    )
    start.period_starters = {1610612741: [23, 166, 389, 893, 937]}

    jump_ball = StatsJumpBall(
        _event(
            game_id="0049700045",
            event_num=1,
            event_type=10,
            clock="12:00",
            period=1,
            player1_id=389,
            player1_team_id=1610612741,
            player2_id=124,
            player2_team_id=1610612766,
            player3_id=133,
            player3_team_id=1610612766,
            home_description="Jump Ball Kukoc vs. Divac: Tip to Wesley",
            visitor_description="",
        ),
        1,
    )
    jump_ball.previous_event = start
    jump_ball.next_event = SimpleNamespace(clock="11:43")

    assert jump_ball.event_stats == []


def test_stats_turnover_self_steal_stays_on_players_actual_team():
    turnover = StatsTurnover(
        _event(
            game_id="0029600879",
            event_num=133,
            event_type=5,
            clock="2:34",
            period=2,
            player1_id=173,
            player1_team_id=1610612748,
            player2_id=173,
            player2_team_id=1610612748,
            home_description="Askins STEAL (1 STL)",
            visitor_description="Askins Out Of Bounds Turnover (P1.T9)",
        ),
        0,
    )
    turnover.event_action_type = 3
    turnover.previous_event = SimpleNamespace(
        current_players={
            1610612748: [173, 136, 180, 469, 896],
            1610612766: [124, 193, 761, 779, 1047],
        },
        seconds_remaining=154.0,
        is_possession_ending_event=True,
        previous_event=None,
    )
    turnover.next_event = None

    event_stats = turnover.event_stats

    assert any(
        stat["player_id"] == 173
        and stat["team_id"] == 1610612748
        and stat["stat_key"] == "BadPassSteals"
        and stat["stat_value"] == 1
        for stat in event_stats
    )


def test_stats_turnover_no_turnover_with_steal_counts_as_live_turnover():
    turnover = StatsTurnover(
        _event(
            game_id="0029600067",
            event_num=360,
            event_type=5,
            clock="10:55",
            period=4,
            player1_id=1134,
            player1_team_id=1610612748,
            player2_id=434,
            player2_team_id=1610612742,
            home_description="Dumas STEAL (1 STL)",
            visitor_description="Austin No Turnover (P5.T12)",
        ),
        0,
    )
    turnover.event_action_type = 0
    turnover.previous_event = SimpleNamespace(
        current_players={
            1610612748: [105, 202, 896, 932, 1134],
            1610612742: [157, 376, 423, 434, 754],
        },
        seconds_remaining=655.0,
        is_possession_ending_event=False,
        previous_event=None,
        get_offense_team_id=lambda: 1610612748,
    )
    turnover.next_event = None

    event_stats = turnover.event_stats

    assert any(
        stat["player_id"] == 1134
        and stat["team_id"] == 1610612748
        and stat["stat_key"] == "LostBallTurnovers"
        and stat["stat_value"] == 1
        for stat in event_stats
    )
    assert any(
        stat["player_id"] == 434
        and stat["team_id"] == 1610612742
        and stat["stat_key"] == "LostBallSteals"
        and stat["stat_value"] == 1
        for stat in event_stats
    )


def test_stats_turnover_turnover_text_counts_as_dead_ball_turnover():
    turnover = StatsTurnover(
        _event(
            game_id="0041600214",
            event_num=171,
            event_type=5,
            clock="6:51",
            period=2,
            player1_id=2544,
            player1_team_id=1610612739,
            home_description="",
            visitor_description="James Turnover Turnover (P3.T4)",
        ),
        0,
    )
    turnover.event_action_type = 0
    turnover.previous_event = SimpleNamespace(
        current_players={
            1610612739: [2200, 2544, 2738, 2747, 201567],
            1610612761: [200768, 201942, 202335, 202687, 203998],
        },
        order=-1,
        seconds_remaining=411.0,
        is_possession_ending_event=False,
        previous_event=None,
        get_offense_team_id=lambda: 1610612739,
    )
    turnover.next_event = None

    event_stats = turnover.event_stats

    assert any(
        stat["player_id"] == 2544
        and stat["team_id"] == 1610612739
        and stat["stat_key"] == "DeadBallTurnovers"
        and stat["stat_value"] == 1
        for stat in event_stats
    )


def test_stats_turnover_no_turnover_without_steal_counts_as_dead_ball_turnover():
    turnover = StatsTurnover(
        _event(
            game_id="0021700398",
            event_num=12,
            event_type=5,
            clock="11:27",
            period=1,
            player1_id=203083,
            player1_team_id=1610612765,
            home_description="Drummond No Turnover (P1.T1)",
            visitor_description="",
        ),
        0,
    )
    turnover.event_action_type = 0
    turnover.previous_event = SimpleNamespace(
        current_players={
            1610612743: [201163, 203914, 203115, 1627750, 203999],
            1610612765: [202397, 203493, 203083, 202704, 203952],
        },
        order=-1,
        seconds_remaining=687.0,
        is_possession_ending_event=False,
        previous_event=None,
        get_offense_team_id=lambda: 1610612765,
    )
    turnover.next_event = None

    event_stats = turnover.event_stats

    assert any(
        stat["player_id"] == 203083
        and stat["team_id"] == 1610612765
        and stat["stat_key"] == "DeadBallTurnovers"
        and stat["stat_value"] == 1
        for stat in event_stats
    )


def test_stats_turnover_legacy_no_turnover_without_steal_stays_non_turnover():
    turnover = StatsTurnover(
        _event(
            game_id="0021601125",
            event_num=512,
            event_type=5,
            clock="5:42",
            period=4,
            player1_id=203468,
            player1_team_id=1610612757,
            home_description="",
            visitor_description="McCollum No Turnover (P3.T9)",
        ),
        0,
    )
    turnover.event_action_type = 0
    turnover.previous_event = SimpleNamespace(
        current_players={
            1610612745: [201935, 201569, 202331, 203991, 203471],
            1610612757: [202323, 203081, 203468, 203994, 1626242],
        },
        order=-1,
        seconds_remaining=342.0,
        is_possession_ending_event=False,
        previous_event=None,
        get_offense_team_id=lambda: 1610612757,
    )
    turnover.next_event = None

    event_stats = turnover.event_stats

    assert not any(
        stat["player_id"] == 203468
        and stat["team_id"] == 1610612757
        and stat["stat_key"] == "DeadBallTurnovers"
        for stat in event_stats
    )


def test_stats_turnover_same_clock_no_turnover_after_period_start_foul_does_not_recurse():
    start = StatsStartOfPeriod(
        _event(
            game_id="0021700478",
            event_num=328,
            event_type=12,
            clock="12:00",
            period=3,
            neutral_description="Start of 3rd Period",
        ),
        0,
    )
    start.period_starters = {
        1610612743: [202702, 203999, 1627750, 1627736, 203914],
        1610612757: [202334, 203994, 203468, 203081, 202683],
    }

    foul = StatsFoul(
        _event(
            game_id="0021700478",
            event_num=330,
            event_type=6,
            clock="11:55",
            period=3,
            player1_id=203999,
            player1_team_id=1610612743,
            visitor_description="Jokic L.B.FOUL (P3.T1) (K.Cutler)",
        ),
        1,
    )
    foul.event_action_type = 3
    foul.previous_event = start

    turnover = StatsTurnover(
        _event(
            game_id="0021700478",
            event_num=614,
            event_type=5,
            clock="11:55",
            period=3,
            player1_id=203999,
            player1_team_id=1610612743,
            visitor_description="Jokic No Turnover (P2.T8)",
        ),
        2,
    )
    turnover.event_action_type = 0
    turnover.previous_event = foul
    turnover.next_event = SimpleNamespace(clock="11:41")
    foul.next_event = turnover
    start.next_event = foul

    assert foul.get_offense_team_id() == 1610612743
    assert turnover.get_offense_team_id() == 1610612743


def test_stats_turnover_with_no_valid_team_or_player_returns_only_base_stats():
    turnover = StatsTurnover(
        _event(
            game_id="0049900030",
            event_num=188,
            event_type=5,
            clock="5:07",
            period=2,
            player1_id=0,
            player1_team_id=0,
            home_description="",
            visitor_description="",
        ),
        0,
    )
    turnover.event_action_type = 2
    turnover.previous_event = SimpleNamespace(
        current_players={
            1610612756: [406, 764, 893, 933, 1778],
            1610612759: [184, 245, 275, 312, 932],
        },
        seconds_remaining=307.0,
        is_possession_ending_event=False,
        previous_event=None,
        get_offense_team_id=lambda: 1610612756,
    )
    turnover.next_event = None

    assert turnover.event_stats == turnover.base_stats


def test_terminal_start_of_period_marker_has_no_next_event():
    class DummyTerminalStartOfPeriod(StatsStartOfPeriod):
        def get_team_starting_with_ball(self):
            raise AssertionError("terminal start marker should not infer possession")

        def get_period_starters(self, file_directory=None):
            raise AssertionError("terminal start marker should not infer starters")

    class DummyEnhancedLoader(NbaEnhancedPbpLoader):
        def __init__(self, items):
            self.game_id = "0021500916"
            self.league = NBA_STRING
            self.file_directory = None
            self.data_provider = "live"
            self.items = items

    end_of_regulation = SimpleNamespace(
        period=4,
        seconds_remaining=0.0,
        event_num=643,
        team_id=0,
        current_players={1610612757: [1, 2, 3, 4, 5], 1610612761: [6, 7, 8, 9, 10]},
    )
    bogus_ot_start = DummyTerminalStartOfPeriod(
        _event(
            game_id="0021500916",
            event_num=646,
            event_type=12,
            clock="5:00",
            period=5,
            neutral_description="Start of 1st OT",
        ),
        1,
    )

    loader = DummyEnhancedLoader([end_of_regulation, bogus_ot_start])
    loader._add_extra_attrs_to_all_events()

    assert bogus_ot_start.previous_event is None
    assert bogus_ot_start.next_event is None
    assert loader.start_period_indices[-1] == 1
    assert bogus_ot_start.team_starting_with_ball is None
    assert bogus_ot_start.period_starters == {}


def test_terminal_start_of_period_possession_has_no_next_possession():
    class DummyPossessionLoader(NbaPossessionLoader):
        def __init__(self, possessions):
            self.items = possessions

    bogus_ot_start = StatsStartOfPeriod(
        _event(
            game_id="0021500916",
            event_num=646,
            event_type=12,
            clock="5:00",
            period=5,
            neutral_description="Start of 1st OT",
        ),
        1,
    )
    prior_possession = SimpleNamespace(events=[SimpleNamespace()], period=4)
    terminal_possession = SimpleNamespace(events=[bogus_ot_start], period=5)

    loader = DummyPossessionLoader([prior_possession, terminal_possession])
    loader._add_extra_attrs_to_all_possessions()

    assert terminal_possession.previous_possession is None
    assert terminal_possession.next_possession is None
    assert terminal_possession.number == 1
