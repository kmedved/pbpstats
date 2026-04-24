import pandas as pd
import pytest

from pbpstats.offline.ordering import preserve_order_after_v3_repairs, reorder_with_v3
from pbpstats.offline.processor import PbpProcessor
from pbpstats.resources.enhanced_pbp.data_nba.field_goal import DataFieldGoal
from pbpstats.resources.enhanced_pbp.data_nba.rebound import DataRebound
from pbpstats.resources.enhanced_pbp.rebound import EventOrderError
from pbpstats.resources.enhanced_pbp.stats_nba.field_goal import StatsFieldGoal
from pbpstats.resources.enhanced_pbp.stats_nba.rebound import StatsRebound


def _stats_fg(event_num, msg_type, description, team_id, player_id=1, action_type=1, clock="06:47"):
    return {
        "GAME_ID": "0049600063",
        "EVENTNUM": event_num,
        "PERIOD": 1,
        "PCTIMESTRING": clock,
        "EVENTMSGACTIONTYPE": action_type,
        "EVENTMSGTYPE": msg_type,
        "PLAYER1_ID": player_id,
        "PLAYER1_TEAM_ID": team_id,
        "PLAYER2_ID": None,
        "PLAYER2_TEAM_ID": None,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
        "HOMEDESCRIPTION": "",
        "VISITORDESCRIPTION": description,
    }


def _stats_rebound(event_num, description, team_id, player_id=1, clock="06:47"):
    return {
        "GAME_ID": "0049600063",
        "EVENTNUM": event_num,
        "PERIOD": 1,
        "PCTIMESTRING": clock,
        "EVENTMSGACTIONTYPE": 0,
        "EVENTMSGTYPE": 4,
        "PLAYER1_ID": player_id,
        "PLAYER1_TEAM_ID": team_id,
        "PLAYER2_ID": None,
        "PLAYER2_TEAM_ID": None,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
        "HOMEDESCRIPTION": "",
        "VISITORDESCRIPTION": description,
    }


def _stats_sub(event_num, description, team_id, out_player_id, in_player_id, clock="06:47"):
    return {
        "GAME_ID": "0049600063",
        "EVENTNUM": event_num,
        "PERIOD": 1,
        "PCTIMESTRING": clock,
        "EVENTMSGACTIONTYPE": 0,
        "EVENTMSGTYPE": 8,
        "PLAYER1_ID": out_player_id,
        "PLAYER1_TEAM_ID": team_id,
        "PLAYER2_ID": in_player_id,
        "PLAYER2_TEAM_ID": team_id,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
        "HOMEDESCRIPTION": description,
        "VISITORDESCRIPTION": "",
    }


def _data_fg(event_num, event_type, description, team_id, player_id=1, action_type=1):
    return {
        "evt": event_num,
        "de": description,
        "cl": "06:47",
        "etype": event_type,
        "mtype": action_type,
        "pid": player_id,
        "tid": team_id,
        "epid": "",
        "oftid": team_id,
        "hs": 0,
        "vs": 0,
    }


def _data_rebound(event_num, description, team_id, player_id=1):
    return {
        "evt": event_num,
        "de": description,
        "cl": "06:47",
        "etype": 4,
        "mtype": 0,
        "pid": player_id,
        "tid": team_id,
        "epid": "",
        "oftid": team_id,
        "hs": 0,
        "vs": 0,
    }


def test_preserve_order_after_v3_repairs_keeps_putback_cluster_row_order():
    game_df = pd.DataFrame(
        [
            {"GAME_ID": "0029601035", "EVENTNUM": 463, "EVENTMSGTYPE": 2, "PERIOD": 4, "PCTIMESTRING": "2:01"},
            {"GAME_ID": "0029601035", "EVENTNUM": 465, "EVENTMSGTYPE": 4, "PERIOD": 4, "PCTIMESTRING": "2:00"},
            {"GAME_ID": "0029601035", "EVENTNUM": 464, "EVENTMSGTYPE": 1, "PERIOD": 4, "PCTIMESTRING": "2:00"},
        ]
    )
    v3_df = pd.DataFrame(
        [
            {"actionNumber": 463, "actionId": 491},
            {"actionNumber": 464, "actionId": 494},
            {"actionNumber": 465, "actionId": 493},
        ]
    )

    ordered = preserve_order_after_v3_repairs(game_df)

    assert ordered["EVENTNUM"].tolist() == [463, 465, 464]


def test_preserve_order_after_v3_repairs_keeps_missed_free_throw_cluster_row_order():
    game_df = pd.DataFrame(
        [
            {"GAME_ID": "0029601101", "EVENTNUM": 450, "EVENTMSGTYPE": 3, "PERIOD": 4, "PCTIMESTRING": "6:50"},
            {"GAME_ID": "0029601101", "EVENTNUM": 454, "EVENTMSGTYPE": 4, "PERIOD": 4, "PCTIMESTRING": "6:50"},
            {"GAME_ID": "0029601101", "EVENTNUM": 451, "EVENTMSGTYPE": 8, "PERIOD": 4, "PCTIMESTRING": "6:50"},
            {"GAME_ID": "0029601101", "EVENTNUM": 452, "EVENTMSGTYPE": 8, "PERIOD": 4, "PCTIMESTRING": "6:50"},
            {"GAME_ID": "0029601101", "EVENTNUM": 453, "EVENTMSGTYPE": 3, "PERIOD": 4, "PCTIMESTRING": "6:50"},
        ]
    )
    v3_df = pd.DataFrame(
        [
            {"actionNumber": 450, "actionId": 460},
            {"actionNumber": 451, "actionId": 461},
            {"actionNumber": 452, "actionId": 462},
            {"actionNumber": 453, "actionId": 463},
            {"actionNumber": 454, "actionId": 464},
        ]
    )

    ordered = preserve_order_after_v3_repairs(game_df)

    assert ordered["EVENTNUM"].tolist() == [450, 454, 451, 452, 453]


def test_preserve_order_after_v3_repairs_coerces_eventnum_to_int_without_resorting():
    game_df = pd.DataFrame(
        [
            {"GAME_ID": "0029601086", "EVENTNUM": "425", "EVENTMSGTYPE": 2, "PERIOD": 4},
            {"GAME_ID": "0029601086", "EVENTNUM": "427", "EVENTMSGTYPE": 4, "PERIOD": 4},
            {"GAME_ID": "0029601086", "EVENTNUM": "426", "EVENTMSGTYPE": 1, "PERIOD": 4},
        ]
    )

    ordered = preserve_order_after_v3_repairs(game_df)

    assert ordered["EVENTNUM"].tolist() == [425, 427, 426]
    assert ordered["EVENTNUM"].dtype.kind in {"i", "u"}


def test_reorder_with_v3_is_a_compatibility_alias():
    game_df = pd.DataFrame(
        [
            {"GAME_ID": "0029601086", "EVENTNUM": "425", "EVENTMSGTYPE": 2, "PERIOD": 4},
            {"GAME_ID": "0029601086", "EVENTNUM": "427", "EVENTMSGTYPE": 4, "PERIOD": 4},
            {"GAME_ID": "0029601086", "EVENTNUM": "426", "EVENTMSGTYPE": 1, "PERIOD": 4},
        ]
    )

    def fetcher(_game_id):
        raise AssertionError("reorder_with_v3 should not consult the v3 fetcher")

    with pytest.warns(
        DeprecationWarning,
        match="does not perform a v3-driven chronology rewrite",
    ):
        ordered = reorder_with_v3(game_df, "0029601086", fetcher)

    assert ordered["EVENTNUM"].tolist() == [425, 427, 426]
    assert ordered["EVENTNUM"].dtype.kind in {"i", "u"}


def test_stats_rebound_event_order_error_exposes_event_numbers():


    miss = StatsFieldGoal(
        _stats_fg(40, 2, "MISS Olajuwon  Layup", team_id=1610612745, player_id=165, action_type=5),
        0,
    )
    made_tip = StatsFieldGoal(
        _stats_fg(41, 1, "Barkley  Tip Shot (18 PTS)", team_id=1610612745, player_id=787, action_type=43),
        1,
    )
    rebound = StatsRebound(
        _stats_rebound(42, "Barkley REBOUND (Off:4 Def:7)", team_id=1610612745, player_id=787),
        2,
    )
    miss.next_event = made_tip
    made_tip.previous_event = miss
    made_tip.next_event = rebound
    rebound.previous_event = made_tip

    with pytest.raises(EventOrderError) as exc_info:
        rebound.missed_shot

    assert exc_info.value.rebound_event_num == 42
    assert exc_info.value.previous_event_num == 41


def test_data_rebound_event_order_error_exposes_event_numbers():
    miss = DataFieldGoal(
        _data_fg(40, 2, "[HOU] MISS Olajuwon Layup", team_id=1610612745, player_id=165, action_type=5),
        1,
        "0049600063",
    )
    made_tip = DataFieldGoal(
        _data_fg(41, 1, "[HOU] Barkley Tip Shot: Made", team_id=1610612745, player_id=787, action_type=43),
        1,
        "0049600063",
    )
    rebound = DataRebound(
        _data_rebound(42, "[HOU] Barkley REBOUND (Off:4 Def:7)", team_id=1610612745, player_id=787),
        1,
        "0049600063",
    )
    miss.next_event = made_tip
    made_tip.previous_event = miss
    made_tip.next_event = rebound
    rebound.previous_event = made_tip

    with pytest.raises(EventOrderError) as exc_info:
        rebound.missed_shot

    assert exc_info.value.rebound_event_num == 42
    assert exc_info.value.previous_event_num == 41


def test_fix_event_order_reorders_putback_rebound_before_made_tip():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0049600063"
    processor.data = [
        _stats_fg(40, 2, "MISS Olajuwon  Layup", team_id=1610612745, player_id=165, action_type=5),
        _stats_fg(41, 1, "Barkley  Tip Shot (18 PTS)", team_id=1610612745, player_id=787, action_type=43),
        _stats_rebound(42, "Barkley REBOUND (Off:4 Def:7)", team_id=1610612745, player_id=787),
    ]
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 42> previous event: <StatsFieldGoal EventNum: 41> is not a missed free throw or field goal",
        rebound_event_num=42,
        previous_event_num=41,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [40, 42, 41]


def test_fix_event_order_reorders_putback_rebound_before_made_layup_without_tip_keyword():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600101"
    processor.data = [
        _stats_fg(144, 2, "MISS Outlaw  3PT Jump Shot", team_id=1610612746, player_id=919, action_type=1),
        _stats_fg(145, 1, "Murray  Layup (4 PTS)", team_id=1610612746, player_id=78, action_type=5),
        _stats_rebound(146, "Murray REBOUND (Off:1 Def:0)", team_id=1610612746, player_id=78),
    ]
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 146> previous event: <StatsFieldGoal EventNum: 145> is not a missed free throw or field goal",
        rebound_event_num=146,
        previous_event_num=145,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [144, 146, 145]


def test_fix_event_order_still_supports_legacy_message_parsing():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0049600063"
    processor.data = [
        _stats_fg(40, 2, "MISS Olajuwon  Layup", team_id=1610612745, player_id=165, action_type=5),
        _stats_fg(41, 1, "Barkley  Tip Shot (18 PTS)", team_id=1610612745, player_id=787, action_type=43),
        _stats_rebound(42, "Barkley REBOUND (Off:4 Def:7)", team_id=1610612745, player_id=787),
    ]
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "previous event: <StatsFieldGoal GameId: 0049600063, Description: Barkley Tip Shot, Time: 6:47, EventNum: 41> is not a missed free throw or field goal"
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [40, 42, 41]


def test_fix_event_order_moves_delayed_same_clock_miss_and_rebound_ahead_of_sub_timeout_block():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0049700045"
    processor.data = [
        _stats_fg(73, 2, "MISS Pippen 25' 3PT Jump Shot", team_id=1610612741, player_id=937, clock="3:53"),
        _stats_rebound(74, "Mason REBOUND (Off:4 Def:4)", team_id=1610612766, player_id=193, clock="3:50"),
        {"GAME_ID": "0049700045", "EVENTNUM": 82, "PERIOD": 1, "PCTIMESTRING": "3:40", "EVENTMSGACTIONTYPE": 0, "EVENTMSGTYPE": 8, "PLAYER1_ID": 23, "PLAYER1_TEAM_ID": 1610612741, "PLAYER2_ID": None, "PLAYER2_TEAM_ID": None, "PLAYER3_ID": None, "PLAYER3_TEAM_ID": None, "HOMEDESCRIPTION": "SUB: Burrell FOR Rodman", "VISITORDESCRIPTION": "", "NEUTRALDESCRIPTION": ""},
        {"GAME_ID": "0049700045", "EVENTNUM": 81, "PERIOD": 1, "PCTIMESTRING": "3:40", "EVENTMSGACTIONTYPE": 0, "EVENTMSGTYPE": 8, "PLAYER1_ID": 166, "PLAYER1_TEAM_ID": 1610612741, "PLAYER2_ID": None, "PLAYER2_TEAM_ID": None, "PLAYER3_ID": None, "PLAYER3_TEAM_ID": None, "HOMEDESCRIPTION": "SUB: Kerr FOR Harper", "VISITORDESCRIPTION": "", "NEUTRALDESCRIPTION": ""},
        {"GAME_ID": "0049700045", "EVENTNUM": 80, "PERIOD": 1, "PCTIMESTRING": "3:40", "EVENTMSGACTIONTYPE": 1, "EVENTMSGTYPE": 9, "PLAYER1_ID": 1610612766, "PLAYER1_TEAM_ID": 0, "PLAYER2_ID": None, "PLAYER2_TEAM_ID": None, "PLAYER3_ID": None, "PLAYER3_TEAM_ID": None, "HOMEDESCRIPTION": "", "VISITORDESCRIPTION": "Hornets Timeout: Regular (Reg.5 Short 1)", "NEUTRALDESCRIPTION": ""},
        {"GAME_ID": "0049700045", "EVENTNUM": 121, "PERIOD": 1, "PCTIMESTRING": "3:40", "EVENTMSGACTIONTYPE": 0, "EVENTMSGTYPE": 8, "PLAYER1_ID": 193, "PLAYER1_TEAM_ID": 1610612766, "PLAYER2_ID": None, "PLAYER2_TEAM_ID": None, "PLAYER3_ID": None, "PLAYER3_TEAM_ID": None, "HOMEDESCRIPTION": "", "VISITORDESCRIPTION": "SUB: Divac FOR Mason", "NEUTRALDESCRIPTION": ""},
        {"GAME_ID": "0049700045", "EVENTNUM": 79, "PERIOD": 1, "PCTIMESTRING": "3:40", "EVENTMSGACTIONTYPE": 0, "EVENTMSGTYPE": 8, "PLAYER1_ID": 133, "PLAYER1_TEAM_ID": 1610612766, "PLAYER2_ID": None, "PLAYER2_TEAM_ID": None, "PLAYER3_ID": None, "PLAYER3_TEAM_ID": None, "HOMEDESCRIPTION": "", "VISITORDESCRIPTION": "SUB: Armstrong FOR Wesley", "NEUTRALDESCRIPTION": ""},
        {"GAME_ID": "0049700045", "EVENTNUM": 78, "PERIOD": 1, "PCTIMESTRING": "3:40", "EVENTMSGACTIONTYPE": 0, "EVENTMSGTYPE": 8, "PLAYER1_ID": 389, "PLAYER1_TEAM_ID": 1610612741, "PLAYER2_ID": None, "PLAYER2_TEAM_ID": None, "PLAYER3_ID": None, "PLAYER3_TEAM_ID": None, "HOMEDESCRIPTION": "SUB: Longley FOR Kukoc", "VISITORDESCRIPTION": "", "NEUTRALDESCRIPTION": ""},
        _stats_rebound(76, "Hornets Rebound", team_id=1610612766, player_id=1610612766, clock="3:40"),
        _stats_fg(75, 2, "MISS Mason  Layup", team_id=1610612766, player_id=193, action_type=5, clock="3:40"),
    ]
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 76> previous event: <StatsSubstitution EventNum: 78> is not a missed free throw or field goal",
        rebound_event_num=76,
        previous_event_num=78,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [73, 74, 75, 76, 82, 81, 80, 121, 79, 78]


def test_fix_event_order_moves_orphan_rebound_back_to_nearest_prior_miss():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0049600063"
    processor.data = [
        _stats_fg(228, 2, "MISS Maloney 24' 3PT Jump Shot", team_id=1610612745, player_id=1074, action_type=1),
        {
            "GAME_ID": "0049600063",
            "EVENTNUM": 230,
            "PCTIMESTRING": "00:34",
            "EVENTMSGACTIONTYPE": 1,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 1125,
            "PLAYER1_TEAM_ID": 1610612760,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "",
            "VISITORDESCRIPTION": "Stewart L.B.FOUL (P1.T1)",
        },
        _stats_rebound(229, "Rockets Rebound", team_id=0, player_id=1610612745),
    ]
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 229> previous event: <StatsFoul EventNum: 230> is not a missed free throw or field goal",
        rebound_event_num=229,
        previous_event_num=230,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [228, 229, 230]


def test_fix_event_order_reorders_rebound_before_missed_tip_followed_by_second_rebound():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0049600063"
    processor.data = [
        _stats_fg(99, 2, "MISS Kemp  Layup", team_id=1610612760, player_id=431, action_type=1),
        _stats_fg(100, 2, "MISS Cummings  Tip Shot", team_id=1610612760, player_id=187, action_type=43),
        _stats_rebound(101, "Cummings REBOUND (Off:3 Def:1)", team_id=1610612760, player_id=187),
        _stats_rebound(102, "Rockets Rebound", team_id=0, player_id=1610612745),
    ]
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 102> previous event: <StatsRebound EventNum: 101> is not a missed free throw or field goal",
        rebound_event_num=102,
        previous_event_num=101,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [99, 101, 100, 102]



def test_fix_event_order_reorders_rebound_before_missed_layup_followed_by_second_rebound():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600106"
    processor.data = [
        _stats_fg(54, 2, "MISS Iverson  Layup", team_id=1610612755, player_id=947, action_type=5),
        _stats_fg(55, 2, "MISS Williams  Layup", team_id=1610612755, player_id=281, action_type=5),
        _stats_rebound(56, "Williams REBOUND (Off:1 Def:1)", team_id=1610612755, player_id=281),
        _stats_rebound(57, "Coleman REBOUND (Off:2 Def:1)", team_id=1610612755, player_id=934),
    ]
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 57> previous event: <StatsRebound EventNum: 56> is not a missed free throw or field goal",
        rebound_event_num=57,
        previous_event_num=56,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [54, 56, 55, 57]


def test_fix_event_order_reorders_previous_rebound_before_second_miss_when_clocks_differ():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600119"
    processor.data = [
        _stats_fg(138, 2, "MISS Brandon 14' Jump Shot", team_id=1610612739, player_id=210, clock="6:33"),
        _stats_fg(139, 2, "MISS Lang 6' Jump Shot", team_id=1610612739, player_id=226, clock="6:29"),
        _stats_rebound(140, "Lang REBOUND (Off:1 Def:0)", team_id=1610612739, player_id=226, clock="6:31"),
        _stats_rebound(141, "Augmon REBOUND (Off:0 Def:1)", team_id=1610612765, player_id=278, clock="6:27"),
    ]
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 141> previous event: <StatsRebound EventNum: 140> is not a missed free throw or field goal",
        rebound_event_num=141,
        previous_event_num=140,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [138, 140, 139, 141]


def test_fix_event_order_moves_earlier_rebound_back_ahead_of_shooting_foul_free_throw_block():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029700652"
    processor.data = [
        _stats_fg(293, 2, "MISS McDyess 17' Jump Shot", team_id=1610612756, player_id=686, clock="1:32"),
        {
            "GAME_ID": "0029700652",
            "EVENTNUM": 295,
            "PERIOD": 3,
            "PCTIMESTRING": "1:31",
            "EVENTMSGACTIONTYPE": 2,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 754,
            "PLAYER1_TEAM_ID": 1610612755,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "Jackson S.FOUL (P3.T2)",
            "VISITORDESCRIPTION": "",
            "NEUTRALDESCRIPTION": "",
        },
        _stats_fg(296, 3, "Robinson Free Throw 1 of 2 (9 PTS)", team_id=1610612756, player_id=361, action_type=11, clock="1:31"),
        _stats_fg(297, 3, "MISS Robinson Free Throw 2 of 2", team_id=1610612756, player_id=361, action_type=12, clock="1:31"),
        _stats_rebound(294, "Robinson REBOUND (Off:3 Def:2)", team_id=1610612756, player_id=361, clock="1:30"),
        _stats_rebound(298, "Weatherspoon REBOUND (Off:2 Def:2)", team_id=1610612755, player_id=221, clock="1:29"),
    ]
    for row in processor.data:
        row["PERIOD"] = 3
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 298> previous event: <StatsRebound EventNum: 294> is not a missed free throw or field goal",
        rebound_event_num=298,
        previous_event_num=294,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [293, 294, 295, 296, 297, 298]


def test_fix_event_order_moves_delayed_miss_ahead_of_team_rebound_and_turnover():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600010"
    processor.data = [
        _stats_fg(490, 3, "Stith Free Throw 2 of 2 (13 PTS)", team_id=1610612743, player_id=179, action_type=12, clock="2:52"),
        _stats_rebound(492, "Mavericks Rebound", team_id=0, player_id=1610612742, clock="2:27"),
        {
            "GAME_ID": "0029600010",
            "EVENTNUM": 493,
            "PERIOD": 4,
            "PCTIMESTRING": "2:27",
            "EVENTMSGACTIONTYPE": 11,
            "EVENTMSGTYPE": 5,
            "PLAYER1_ID": 1610612742,
            "PLAYER1_TEAM_ID": 1610612742,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "",
            "VISITORDESCRIPTION": "Mavericks Turnover: Shot Clock (T#21)",
            "NEUTRALDESCRIPTION": "",
        },
        _stats_fg(491, 2, "MISS Jackson  Layup", team_id=1610612742, player_id=754, action_type=5, clock="2:26"),
    ]
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 492> previous event: <StatsFreeThrow EventNum: 490> is not a missed free throw or field goal",
        rebound_event_num=492,
        previous_event_num=490,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [490, 491, 492, 493]


def test_fix_event_order_moves_player_rebound_behind_future_missed_free_throw_predecessor():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600840"
    processor.data = [
        _stats_fg(364, 1, "Maxwell  3PT Jump Shot (14 PTS)", team_id=1610612759, player_id=137, action_type=1, clock="0:52"),
        _stats_rebound(368, "Alexander REBOUND (Off:0 Def:2)", team_id=1610612759, player_id=724, clock="0:46"),
        {
            "GAME_ID": "0029600840",
            "EVENTNUM": 365,
            "PERIOD": 4,
            "PCTIMESTRING": "0:45",
            "EVENTMSGACTIONTYPE": 1,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 724,
            "PLAYER1_TEAM_ID": 1610612759,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "Alexander P.FOUL (P3.PN)",
            "VISITORDESCRIPTION": "",
            "NEUTRALDESCRIPTION": "",
        },
        _stats_fg(366, 3, "Delk Free Throw 1 of 2 (12 PTS)", team_id=1610612766, player_id=960, action_type=11, clock="0:45"),
        _stats_fg(367, 3, "MISS Delk Free Throw 2 of 2", team_id=1610612766, player_id=960, action_type=12, clock="0:45"),
    ]
    for row in processor.data:
        row["PERIOD"] = 4
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 368> previous event: <StatsFieldGoal EventNum: 364> is not a missed free throw or field goal",
        rebound_event_num=368,
        previous_event_num=364,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [364, 365, 366, 367, 368]


def test_fix_event_order_moves_putback_rebound_back_ahead_of_missed_and_one_free_throw():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600085"
    processor.data = [
        _stats_fg(470, 2, "MISS Rose 13' Jump Shot", team_id=1610612754, player_id=147, action_type=1, clock="0:35"),
        _stats_fg(472, 1, "Rose 1' Layup (12 PTS)", team_id=1610612754, player_id=147, action_type=5, clock="0:33"),
        {
            "GAME_ID": "0029600085",
            "EVENTNUM": 473,
            "PERIOD": 4,
            "PCTIMESTRING": "0:33",
            "EVENTMSGACTIONTYPE": 2,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 376,
            "PLAYER1_TEAM_ID": 1610612742,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "Montross S.FOUL (P5.PN)",
            "VISITORDESCRIPTION": "",
            "NEUTRALDESCRIPTION": "",
        },
        _stats_fg(474, 3, "MISS Rose Free Throw 1 of 1", team_id=1610612754, player_id=147, action_type=10, clock="0:33"),
        _stats_rebound(471, "Rose REBOUND (Off:1 Def:3)", team_id=1610612754, player_id=147, clock="0:32"),
        _stats_rebound(475, "Walker REBOUND (Off:0 Def:1)", team_id=1610612742, player_id=955, clock="0:32"),
    ]
    for row in processor.data:
        row["PERIOD"] = 4
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 475> previous event: <StatsRebound EventNum: 471> is not a missed free throw or field goal",
        rebound_event_num=475,
        previous_event_num=471,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [470, 471, 472, 473, 474, 475]


def test_fix_event_order_moves_player_rebound_down_to_future_opponent_missed_free_throw():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600442"
    processor.data = [
        _stats_rebound(377, "Bullets Rebound", team_id=0, player_id=1610612764, clock="3:32"),
        _stats_rebound(385, "Mason REBOUND (Off:0 Def:6)", team_id=1610612766, player_id=193, clock="3:32"),
        {
            "GAME_ID": "0029600442",
            "EVENTNUM": 378,
            "PERIOD": 4,
            "PCTIMESTRING": "3:33",
            "EVENTMSGACTIONTYPE": 3,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 124,
            "PLAYER1_TEAM_ID": 1610612766,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "Divac L.B.FOUL (P4.PN)",
            "VISITORDESCRIPTION": "",
            "NEUTRALDESCRIPTION": "",
        },
        {
            "GAME_ID": "0029600442",
            "EVENTNUM": 379,
            "PERIOD": 4,
            "PCTIMESTRING": "3:33",
            "EVENTMSGACTIONTYPE": 0,
            "EVENTMSGTYPE": 8,
            "PLAYER1_ID": 924,
            "PLAYER1_TEAM_ID": 1610612766,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "SUB: Delk FOR Goldwire",
            "VISITORDESCRIPTION": "",
            "NEUTRALDESCRIPTION": "",
        },
        _stats_fg(381, 3, "MISS Muresan Free Throw 1 of 2", team_id=1610612764, player_id=49, action_type=11, clock="3:33"),
        _stats_rebound(382, "Bullets Rebound", team_id=0, player_id=1610612764, clock="3:33"),
        _stats_fg(384, 3, "MISS Muresan Free Throw 2 of 2", team_id=1610612764, player_id=49, action_type=12, clock="3:33"),
    ]
    for row in processor.data:
        row["PERIOD"] = 4
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 385> previous event: <StatsRebound EventNum: 377> is not a missed free throw or field goal",
        rebound_event_num=385,
        previous_event_num=377,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [377, 378, 379, 381, 382, 384, 385]


def test_fix_event_order_moves_player_rebound_back_to_earlier_missed_free_throw_before_dead_ball_block():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600545"
    processor.data = [
        {
            "GAME_ID": "0029600545",
            "EVENTNUM": 542,
            "PERIOD": 6,
            "PCTIMESTRING": "0:05",
            "EVENTMSGACTIONTYPE": 1,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 3,
            "PLAYER1_TEAM_ID": 1610612765,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "",
            "VISITORDESCRIPTION": "Long P.FOUL (P2.PN)",
            "NEUTRALDESCRIPTION": "",
        },
        _stats_fg(543, 3, "Campbell Free Throw 1 of 2 (19 PTS)", team_id=1610612747, player_id=922, action_type=11, clock="0:05"),
        _stats_fg(544, 3, "MISS Campbell Free Throw 2 of 2", team_id=1610612747, player_id=922, action_type=12, clock="0:05"),
        {
            "GAME_ID": "0029600545",
            "EVENTNUM": 546,
            "PERIOD": 6,
            "PCTIMESTRING": "0:04",
            "EVENTMSGACTIONTYPE": 1,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 89,
            "PLAYER1_TEAM_ID": 1610612747,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "Van Exel P.FOUL (P3.PN)",
            "VISITORDESCRIPTION": "",
            "NEUTRALDESCRIPTION": "",
        },
        {
            "GAME_ID": "0029600545",
            "EVENTNUM": 547,
            "PERIOD": 6,
            "PCTIMESTRING": "0:04",
            "EVENTMSGACTIONTYPE": 1,
            "EVENTMSGTYPE": 9,
            "PLAYER1_ID": 1610612747,
            "PLAYER1_TEAM_ID": 0,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "LAKERS Timeout: Regular (Full 8 Short 1)",
            "VISITORDESCRIPTION": "",
            "NEUTRALDESCRIPTION": "",
        },
        _stats_fg(550, 3, "Mills Free Throw 1 of 2 (8 PTS)", team_id=1610612765, player_id=371, action_type=11, clock="0:04"),
        {
            "GAME_ID": "0029600545",
            "EVENTNUM": 552,
            "PERIOD": 6,
            "PCTIMESTRING": "0:04",
            "EVENTMSGACTIONTYPE": 1,
            "EVENTMSGTYPE": 9,
            "PLAYER1_ID": 1610612747,
            "PLAYER1_TEAM_ID": 0,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "LAKERS Timeout: Regular (Full 9 Short 1)",
            "VISITORDESCRIPTION": "",
            "NEUTRALDESCRIPTION": "",
        },
        {
            "GAME_ID": "0029600545",
            "EVENTNUM": 553,
            "PERIOD": 6,
            "PCTIMESTRING": "0:04",
            "EVENTMSGACTIONTYPE": 0,
            "EVENTMSGTYPE": 8,
            "PLAYER1_ID": 371,
            "PLAYER1_TEAM_ID": 1610612765,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "",
            "VISITORDESCRIPTION": "SUB: Curry FOR Mills",
            "NEUTRALDESCRIPTION": "",
        },
        {
            "GAME_ID": "0029600545",
            "EVENTNUM": 554,
            "PERIOD": 6,
            "PCTIMESTRING": "0:04",
            "EVENTMSGACTIONTYPE": 0,
            "EVENTMSGTYPE": 8,
            "PLAYER1_ID": 922,
            "PLAYER1_TEAM_ID": 1610612747,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "SUB: Scott FOR Campbell",
            "VISITORDESCRIPTION": "",
            "NEUTRALDESCRIPTION": "",
        },
        {
            "GAME_ID": "0029600545",
            "EVENTNUM": 555,
            "PERIOD": 6,
            "PCTIMESTRING": "0:04",
            "EVENTMSGACTIONTYPE": 0,
            "EVENTMSGTYPE": 8,
            "PLAYER1_ID": 406,
            "PLAYER1_TEAM_ID": 1610612747,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "SUB: Kersey FOR O'Neal",
            "VISITORDESCRIPTION": "",
            "NEUTRALDESCRIPTION": "",
        },
        _stats_fg(551, 3, "Mills Free Throw 2 of 2 (9 PTS)", team_id=1610612765, player_id=371, action_type=12, clock="0:04"),
        _stats_rebound(545, "Mills REBOUND (Off:0 Def:8)", team_id=1610612765, player_id=371, clock="0:04"),
    ]
    for row in processor.data:
        row["PERIOD"] = 6
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 545> previous event: <StatsFreeThrow EventNum: 551> is not a missed free throw or field goal",
        rebound_event_num=545,
        previous_event_num=551,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [542, 543, 544, 545, 546, 547, 550, 552, 553, 554, 555, 551]


def test_fix_event_order_moves_subbed_in_rebounder_block_before_missed_ft():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0041900155"
    processor.data = [
        _stats_fg(347, 3, "Doncic Free Throw 1 of 2 (12 PTS)", team_id=1610612742, player_id=1629029, action_type=11, clock="0:03"),
        _stats_fg(352, 3, "MISS Doncic Free Throw 2 of 2", team_id=1610612742, player_id=1629029, action_type=12, clock="0:03"),
        _stats_sub(348, "SUB: Harrell FOR Zubac", team_id=1610612746, out_player_id=1627826, in_player_id=1626149, clock="0:03"),
        _stats_sub(349, "SUB: Kidd-Gilchrist FOR Finney-Smith", team_id=1610612742, out_player_id=1627827, in_player_id=203077, clock="0:03"),
        _stats_rebound(353, "Harrell REBOUND (Off:1 Def:3)", team_id=1610612746, player_id=1626149, clock="0:01"),
    ]
    for row in processor.data:
        row["PERIOD"] = 2
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 353> previous event: <StatsSubstitution EventNum: 349> is not a missed free throw or field goal",
        rebound_event_num=353,
        previous_event_num=349,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [347, 348, 349, 352, 353]

def test_fix_event_order_moves_stranded_same_team_rebound_behind_future_same_clock_miss():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600561"
    processor.data = [
        _stats_fg(79, 1, "Strickland 9' Jump Shot (2 PTS)", team_id=1610612764, player_id=393, action_type=1, clock="3:28"),
        _stats_rebound(90, "Grant REBOUND (Off:0 Def:1)", team_id=1610612764, player_id=265, clock="3:06"),
        _stats_fg(93, 2, "MISS Seikaly 5' Running Jump Shot", team_id=1610612753, player_id=938, action_type=2, clock="3:06"),
        _stats_fg(81, 1, "Strickland  Driving Layup (4 PTS)", team_id=1610612764, player_id=393, action_type=6, clock="3:02"),
    ]
    for row in processor.data:
        row["PERIOD"] = 1
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 90> previous event: <StatsFieldGoal EventNum: 79> is not a missed free throw or field goal",
        rebound_event_num=90,
        previous_event_num=79,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [79, 93, 90, 81]


def test_fix_event_order_moves_delayed_defensive_rebound_pair_ahead_of_sub_timeout_block():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0049700045"
    processor.data = [
        _stats_fg(138, 2, "MISS Burrell 18' Jump Shot", team_id=1610612741, player_id=197, clock="9:11"),
        _stats_rebound(139, "Rodman REBOUND (Off:5 Def:5)", team_id=1610612741, player_id=23, clock="9:08"),
        {"GAME_ID": "0049700045", "EVENTNUM": 147, "PERIOD": 2, "PCTIMESTRING": "9:06", "EVENTMSGACTIONTYPE": 0, "EVENTMSGTYPE": 8, "PLAYER1_ID": 70, "PLAYER1_TEAM_ID": 1610612741, "PLAYER2_ID": None, "PLAYER2_TEAM_ID": None, "PLAYER3_ID": None, "PLAYER3_TEAM_ID": None, "HOMEDESCRIPTION": "SUB: Jordan FOR Kerr", "VISITORDESCRIPTION": "", "NEUTRALDESCRIPTION": ""},
        {"GAME_ID": "0049700045", "EVENTNUM": 148, "PERIOD": 2, "PCTIMESTRING": "9:06", "EVENTMSGACTIONTYPE": 0, "EVENTMSGTYPE": 8, "PLAYER1_ID": 753, "PLAYER1_TEAM_ID": 1610612741, "PLAYER2_ID": None, "PLAYER2_TEAM_ID": None, "PLAYER3_ID": None, "PLAYER3_TEAM_ID": None, "HOMEDESCRIPTION": "SUB: Pippen FOR Brown", "VISITORDESCRIPTION": "", "NEUTRALDESCRIPTION": ""},
        {"GAME_ID": "0049700045", "EVENTNUM": 145, "PERIOD": 2, "PCTIMESTRING": "9:06", "EVENTMSGACTIONTYPE": 1, "EVENTMSGTYPE": 9, "PLAYER1_ID": 0, "PLAYER1_TEAM_ID": 0, "PLAYER2_ID": None, "PLAYER2_TEAM_ID": None, "PLAYER3_ID": None, "PLAYER3_TEAM_ID": None, "HOMEDESCRIPTION": "Timeout: Regular", "VISITORDESCRIPTION": "", "NEUTRALDESCRIPTION": ""},
        {"GAME_ID": "0049700045", "EVENTNUM": 142, "PERIOD": 2, "PCTIMESTRING": "9:06", "EVENTMSGACTIONTYPE": 2, "EVENTMSGTYPE": 9, "PLAYER1_ID": 1610612766, "PLAYER1_TEAM_ID": 0, "PLAYER2_ID": None, "PLAYER2_TEAM_ID": None, "PLAYER3_ID": None, "PLAYER3_TEAM_ID": None, "HOMEDESCRIPTION": "", "VISITORDESCRIPTION": "Hornets Timeout: Short (Reg.4 Short 1)", "NEUTRALDESCRIPTION": ""},
        _stats_rebound(141, "Royal REBOUND (Off:0 Def:2)", team_id=1610612766, player_id=140, clock="9:06"),
        _stats_fg(140, 2, "MISS Rodman  Layup", team_id=1610612741, player_id=23, action_type=5, clock="9:06"),
    ]
    for row in processor.data:
        row["PERIOD"] = 2
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 141> previous event: <StatsTimeout EventNum: 142> is not a missed free throw or field goal",
        rebound_event_num=141,
        previous_event_num=142,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [138, 139, 140, 141, 147, 148, 145, 142]


def test_fix_event_order_unwinds_stacked_same_clock_rebounds_before_opponent_rebound():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0049700045"
    processor.data = [
        _stats_fg(28, 2, "MISS Rodman  Layup", team_id=1610612741, player_id=23, action_type=5, clock="8:50"),
        _stats_fg(43, 2, "MISS Rodman  Tip Shot", team_id=1610612741, player_id=23, action_type=4, clock="8:41"),
        _stats_rebound(39, "Rodman REBOUND (Off:6 Def:9)", team_id=1610612741, player_id=23, clock="8:41"),
        _stats_fg(29, 2, "MISS Rodman  Tip Shot", team_id=1610612741, player_id=23, action_type=4, clock="8:41"),
        _stats_rebound(30, "Rodman REBOUND (Off:7 Def:9)", team_id=1610612741, player_id=23, clock="8:41"),
        _stats_rebound(31, "Mason REBOUND (Off:4 Def:6)", team_id=1610612766, player_id=193, clock="8:37"),
    ]
    for row in processor.data:
        row["PERIOD"] = 1
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 31> previous event: <StatsRebound EventNum: 30> is not a missed free throw or field goal",
        rebound_event_num=31,
        previous_event_num=30,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [28, 39, 43, 30, 29, 31]


def test_fix_event_order_moves_shadowing_team_rebound_behind_future_miss_and_rebound():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0049600063"
    processor.data = [
        _stats_fg(142, 2, "MISS Willis 11' Jump Shot", team_id=1610612745, player_id=788, action_type=1, clock="8:21"),
        _stats_fg(143, 2, "MISS Johnson  Tip Shot", team_id=1610612745, player_id=698, action_type=4, clock="8:19"),
        _stats_rebound(147, "Rockets Rebound", team_id=0, player_id=1610612745, clock="8:17"),
        _stats_rebound(144, "Johnson REBOUND (Off:1 Def:1)", team_id=1610612745, player_id=698, clock="8:19"),
        _stats_fg(145, 2, "MISS Willis  Tip Shot", team_id=1610612745, player_id=788, action_type=4, clock="8:17"),
        _stats_rebound(146, "Willis REBOUND (Off:2 Def:0)", team_id=1610612745, player_id=788, clock="8:17"),
    ]
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 144> previous event: <StatsRebound EventNum: 147> is not a missed free throw or field goal",
        rebound_event_num=144,
        previous_event_num=147,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [142, 143, 144, 145, 146, 147]


def test_fix_event_order_moves_shadowing_team_rebound_behind_future_missed_free_throw():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0049600063"
    processor.data = [
        _stats_fg(351, 2, "MISS Willis 12' Jump Shot", team_id=1610612745, player_id=788, action_type=1, clock="11:50"),
        _stats_rebound(358, "SUPERSONICS Rebound", team_id=0, player_id=1610612760, clock="11:20"),
        _stats_rebound(352, "Perkins REBOUND (Off:0 Def:1)", team_id=1610612760, player_id=64, clock="11:47"),
        _stats_fg(353, 1, "Cummings 15' Jump Shot (2 PTS)", team_id=1610612760, player_id=187, action_type=1, clock="11:38"),
        {
            "GAME_ID": "0049600063",
            "EVENTNUM": 359,
            "PERIOD": 4,
            "PCTIMESTRING": "11:20",
            "EVENTMSGACTIONTYPE": 3,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 788,
            "PLAYER1_TEAM_ID": 1610612745,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "",
            "VISITORDESCRIPTION": "Willis L.B.FOUL (P1.PN)",
            "NEUTRALDESCRIPTION": "",
        },
        {
            "GAME_ID": "0049600063",
            "EVENTNUM": 357,
            "PERIOD": 4,
            "PCTIMESTRING": "11:20",
            "EVENTMSGACTIONTYPE": 10,
            "EVENTMSGTYPE": 3,
            "PLAYER1_ID": 165,
            "PLAYER1_TEAM_ID": 1610612745,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "",
            "VISITORDESCRIPTION": "MISS Olajuwon Free Throw 1 of 1",
            "NEUTRALDESCRIPTION": "",
        },
    ]
    for row in processor.data:
        row["PERIOD"] = 4
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 352> previous event: <StatsRebound EventNum: 358> is not a missed free throw or field goal",
        rebound_event_num=352,
        previous_event_num=358,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [351, 352, 353, 359, 357, 358]


def test_fix_event_order_moves_second_rebound_behind_delayed_same_clock_miss():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029700846"
    processor.data = [
        _stats_fg(123, 2, "MISS Maxwell  Layup", team_id=1610612766, player_id=137, action_type=5, clock="8:57"),
        _stats_rebound(126, "Reid REBOUND (Off:2 Def:1)", team_id=1610612766, player_id=462, clock="8:55"),
        _stats_rebound(124, "Garnett REBOUND (Off:2 Def:4)", team_id=1610612750, player_id=708, clock="8:55"),
        _stats_fg(127, 2, "MISS Reid  Tip Shot", team_id=1610612766, player_id=462, action_type=4, clock="8:55"),
    ]
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 124> previous event: <StatsRebound EventNum: 126> is not a missed free throw or field goal",
        rebound_event_num=124,
        previous_event_num=126,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [123, 126, 127, 124]


def test_fix_event_order_moves_team_rebound_behind_delayed_same_clock_putback_miss():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029700846"
    processor.data = [
        _stats_fg(215, 2, "MISS Hammonds 5' Hook Shot", team_id=1610612750, player_id=67, action_type=3, clock="1:19"),
        _stats_rebound(217, "Parks REBOUND (Off:1 Def:1)", team_id=1610612750, player_id=685, clock="1:17"),
        _stats_rebound(216, "Timberwolves Rebound", team_id=0, player_id=1610612750, clock="1:17"),
        _stats_fg(218, 2, "MISS Parks  Layup", team_id=1610612750, player_id=685, action_type=5, clock="1:17"),
    ]
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 216> previous event: <StatsRebound EventNum: 217> is not a missed free throw or field goal",
        rebound_event_num=216,
        previous_event_num=217,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [215, 217, 218, 216]


def test_fix_event_order_reorders_same_team_rebound_chain_when_clocks_differ():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600028"
    processor.data = [
        _stats_fg(245, 2, "MISS Abdur-Rahim 19' Jump Shot", team_id=1610612763, player_id=949, clock="10:57"),
        _stats_fg(246, 2, "MISS Anthony  Layup", team_id=1610612763, player_id=21, action_type=5, clock="10:54"),
        _stats_rebound(249, "Reeves REBOUND (Off:4 Def:2)", team_id=1610612763, player_id=735, clock="10:53"),
        _stats_rebound(247, "Anthony REBOUND (Off:1 Def:1)", team_id=1610612763, player_id=21, clock="10:54"),
        _stats_fg(248, 1, "Reeves  Tip Shot (11 PTS)", team_id=1610612763, player_id=735, action_type=43, clock="10:53"),
    ]
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 247> previous event: <StatsRebound EventNum: 249> is not a missed free throw or field goal",
        rebound_event_num=247,
        previous_event_num=249,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [245, 249, 246, 247, 248]


def test_fix_event_order_reorders_start_of_period_rebound_cluster_with_forward_miss():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600324"
    processor.data = [
        {"GAME_ID": "0029600324", "EVENTNUM": 202, "PERIOD": 2, "PCTIMESTRING": "0:00", "EVENTMSGACTIONTYPE": 0, "EVENTMSGTYPE": 13, "PLAYER1_ID": 0, "PLAYER1_TEAM_ID": 0, "PLAYER2_ID": 0, "PLAYER2_TEAM_ID": 0, "PLAYER3_ID": 0, "PLAYER3_TEAM_ID": 0, "HOMEDESCRIPTION": "", "VISITORDESCRIPTION": "End of 2nd Period", "NEUTRALDESCRIPTION": ""},
        {"GAME_ID": "0029600324", "EVENTNUM": 207, "PERIOD": 3, "PCTIMESTRING": "12:00", "EVENTMSGACTIONTYPE": 0, "EVENTMSGTYPE": 12, "PLAYER1_ID": 0, "PLAYER1_TEAM_ID": 0, "PLAYER2_ID": 0, "PLAYER2_TEAM_ID": 0, "PLAYER3_ID": 0, "PLAYER3_TEAM_ID": 0, "HOMEDESCRIPTION": "", "VISITORDESCRIPTION": "Start of 3rd Period", "NEUTRALDESCRIPTION": ""},
        _stats_rebound(209, "Rogers REBOUND (Off:1 Def:2)", team_id=1610612756, player_id=55),
        _stats_fg(210, 1, "Sealy  Slam Dunk (11 PTS)", team_id=1610612756, player_id=199, action_type=7),
        _stats_fg(211, 2, "MISS Person 5' Jump Shot", team_id=1610612756, player_id=189, action_type=1),
    ]
    processor.data[2]["GAME_ID"] = "0029600324"
    processor.data[2]["PERIOD"] = 3
    processor.data[2]["PCTIMESTRING"] = "11:48"
    processor.data[3]["GAME_ID"] = "0029600324"
    processor.data[3]["PERIOD"] = 3
    processor.data[3]["PCTIMESTRING"] = "11:43"
    processor.data[4]["GAME_ID"] = "0029600324"
    processor.data[4]["PERIOD"] = 3
    processor.data[4]["PCTIMESTRING"] = "11:50"
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 209> previous event: <StatsStartOfPeriod EventNum: 207> is not a missed free throw or field goal",
        rebound_event_num=209,
        previous_event_num=207,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [202, 207, 211, 209, 210]


def test_fix_event_order_moves_start_of_period_rebound_to_later_eventnum_predecessor_miss():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600585"
    processor.data = [
        {"GAME_ID": "0029600585", "EVENTNUM": 192, "PERIOD": 3, "PCTIMESTRING": "12:00", "EVENTMSGACTIONTYPE": 0, "EVENTMSGTYPE": 12, "PLAYER1_ID": 0, "PLAYER1_TEAM_ID": 0, "PLAYER2_ID": 0, "PLAYER2_TEAM_ID": 0, "PLAYER3_ID": 0, "PLAYER3_TEAM_ID": 0, "HOMEDESCRIPTION": "", "VISITORDESCRIPTION": "Start of 3rd Period", "NEUTRALDESCRIPTION": ""},
        _stats_rebound(202, "Mason REBOUND (Off:1 Def:7)", team_id=1610612766, player_id=193, clock="12:00"),
        _stats_rebound(204, "Ewing REBOUND (Off:3 Def:4)", team_id=1610612752, player_id=121, clock="12:00"),
        {"GAME_ID": "0029600585", "EVENTNUM": 193, "PERIOD": 3, "PCTIMESTRING": "11:48", "EVENTMSGACTIONTYPE": 1, "EVENTMSGTYPE": 6, "PLAYER1_ID": 913, "PLAYER1_TEAM_ID": 1610612752, "PLAYER2_ID": None, "PLAYER2_TEAM_ID": None, "PLAYER3_ID": None, "PLAYER3_TEAM_ID": None, "HOMEDESCRIPTION": "", "VISITORDESCRIPTION": "Johnson P.FOUL (P4.T1)", "NEUTRALDESCRIPTION": ""},
        _stats_fg(201, 2, "MISS Johnson 3PT Jump Shot", team_id=1610612752, player_id=913, action_type=1, clock="10:54"),
        _stats_fg(203, 2, "MISS Divac 21' Jump Shot", team_id=1610612766, player_id=124, action_type=1, clock="10:39"),
    ]
    for row in processor.data:
        row["PERIOD"] = 3
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 202> previous event: <StatsStartOfPeriod EventNum: 192> is not a missed free throw or field goal",
        rebound_event_num=202,
        previous_event_num=192,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [192, 204, 193, 201, 202, 203]


def test_fix_event_order_deletes_team_rebound_at_period_start_without_cross_period_move():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600085"
    processor.data = [
        _stats_fg(230, 2, "MISS Allen 47' 3PT Jump Shot", team_id=1610612742, player_id=947, action_type=1),
        {"GAME_ID": "0029600085", "EVENTNUM": 232, "PERIOD": 2, "PCTIMESTRING": "0:00", "EVENTMSGACTIONTYPE": 0, "EVENTMSGTYPE": 13, "PLAYER1_ID": 0, "PLAYER1_TEAM_ID": 0, "PLAYER2_ID": 0, "PLAYER2_TEAM_ID": 0, "PLAYER3_ID": 0, "PLAYER3_TEAM_ID": 0, "HOMEDESCRIPTION": "", "VISITORDESCRIPTION": "End of 2nd Period", "NEUTRALDESCRIPTION": ""},
        {"GAME_ID": "0029600085", "EVENTNUM": 233, "PERIOD": 3, "PCTIMESTRING": "12:00", "EVENTMSGACTIONTYPE": 0, "EVENTMSGTYPE": 12, "PLAYER1_ID": 0, "PLAYER1_TEAM_ID": 0, "PLAYER2_ID": 0, "PLAYER2_TEAM_ID": 0, "PLAYER3_ID": 0, "PLAYER3_TEAM_ID": 0, "HOMEDESCRIPTION": "", "VISITORDESCRIPTION": "Start of 3rd Period", "NEUTRALDESCRIPTION": ""},
        {"GAME_ID": "0029600085", "EVENTNUM": 364, "PERIOD": 3, "PCTIMESTRING": "12:00", "EVENTMSGACTIONTYPE": 0, "EVENTMSGTYPE": 4, "PLAYER1_ID": 1610612754, "PLAYER1_TEAM_ID": 1610612754, "PLAYER2_ID": None, "PLAYER2_TEAM_ID": None, "PLAYER3_ID": None, "PLAYER3_TEAM_ID": None, "HOMEDESCRIPTION": "", "VISITORDESCRIPTION": "Pacers Rebound", "NEUTRALDESCRIPTION": ""},
    ]
    processor.data[0]["GAME_ID"] = "0029600085"
    processor.data[0]["PERIOD"] = 2
    processor.data[0]["PCTIMESTRING"] = "0:00"
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 364> previous event: <StatsStartOfPeriod EventNum: 233> is not a missed free throw or field goal",
        rebound_event_num=364,
        previous_event_num=233,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [230, 232, 233]


def test_fix_event_order_moves_rebound_back_to_nearby_eventnum_predecessor_miss_across_free_throw_block():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600585"
    processor.data = [
        _stats_fg(208, 2, "MISS Divac Layup", team_id=1610612766, player_id=124, action_type=5, clock="9:25"),
        {"GAME_ID": "0029600585", "EVENTNUM": 210, "PERIOD": 3, "PCTIMESTRING": "9:25", "EVENTMSGACTIONTYPE": 2, "EVENTMSGTYPE": 6, "PLAYER1_ID": 121, "PLAYER1_TEAM_ID": 1610612752, "PLAYER2_ID": None, "PLAYER2_TEAM_ID": None, "PLAYER3_ID": None, "PLAYER3_TEAM_ID": None, "HOMEDESCRIPTION": "", "VISITORDESCRIPTION": "Ewing S.FOUL (P2.T3)", "NEUTRALDESCRIPTION": ""},
        _stats_fg(211, 3, "Divac Free Throw 1 of 2 (14 PTS)", team_id=1610612766, player_id=124, action_type=11, clock="9:25"),
        _stats_fg(212, 3, "MISS Divac Free Throw 2 of 2", team_id=1610612766, player_id=124, action_type=12, clock="9:25"),
        _stats_rebound(213, "Ewing REBOUND (Off:3 Def:5)", team_id=1610612752, player_id=121, clock="9:25"),
        _stats_rebound(209, "Divac REBOUND (Off:2 Def:2)", team_id=1610612766, player_id=124, clock="9:23"),
    ]
    for row in processor.data:
        row["PERIOD"] = 3
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 209> previous event: <StatsRebound EventNum: 213> is not a missed free throw or field goal",
        rebound_event_num=209,
        previous_event_num=213,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [208, 209, 210, 211, 212, 213]


def test_fix_event_order_moves_real_player_rebound_ahead_of_foul_and_free_throw_block():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600245"
    processor.data = [
        _stats_fg(486, 2, "MISS Davis 24' 3PT Jump Shot", team_id=1610612755, player_id=707, action_type=1, clock="0:07"),
        {
            "GAME_ID": "0029600245",
            "EVENTNUM": 488,
            "PERIOD": 4,
            "PCTIMESTRING": "0:04",
            "EVENTMSGACTIONTYPE": 1,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 446,
            "PLAYER1_TEAM_ID": 1610612755,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "",
            "VISITORDESCRIPTION": "Harris P.FOUL (P2.PN)",
            "NEUTRALDESCRIPTION": "",
        },
        _stats_fg(489, 3, "MISS Harper Free Throw 1 of 2", team_id=1610612742, player_id=157, action_type=11, clock="0:04"),
        _stats_rebound(490, "Mavericks Rebound", team_id=0, player_id=1610612742, clock="0:04"),
        _stats_fg(491, 3, "Harper Free Throw 2 of 2 (20 PTS)", team_id=1610612742, player_id=157, action_type=12, clock="0:04"),
        _stats_rebound(487, "Harper REBOUND (Off:0 Def:3)", team_id=1610612742, player_id=157, clock="0:04"),
    ]
    for row in processor.data:
        row["PERIOD"] = 4
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 487> previous event: <StatsFreeThrow EventNum: 491> is not a missed free throw or field goal",
        rebound_event_num=487,
        previous_event_num=491,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [486, 487, 488, 489, 490, 491]


def test_fix_event_order_moves_stranded_rebound_behind_future_same_clock_miss():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600066"
    processor.data = [
        _stats_fg(255, 1, "Pippen  Layup (5 PTS)", team_id=1610612741, player_id=937, action_type=5, clock="11:06"),
        _stats_rebound(256, "Radja REBOUND (Off:2 Def:4)", team_id=1610612738, player_id=129, clock="10:46"),
        _stats_fg(263, 2, "MISS Fox 15' Jump Shot", team_id=1610612738, player_id=296, action_type=1, clock="10:46"),
        _stats_fg(266, 1, "Williams 16' Jump Shot (8 PTS)", team_id=1610612738, player_id=677, action_type=1, clock="10:46"),
        _stats_fg(270, 2, "MISS Jordan 19' Jump Shot", team_id=1610612741, player_id=893, action_type=1, clock="10:46"),
        _stats_rebound(276, "Celtics Rebound", team_id=1610612738, player_id=1610612738, clock="10:46"),
    ]
    for row in processor.data:
        row["PERIOD"] = 3
    processor._rebound_deletions_list = []

    error = EventOrderError(
        "rebound event: <StatsRebound EventNum: 256> previous event: <StatsFieldGoal EventNum: 255> is not a missed free throw or field goal",
        rebound_event_num=256,
        previous_event_num=255,
    )

    processor._fix_event_order(error)

    assert [row["EVENTNUM"] for row in processor.data] == [255, 263, 256, 266, 270, 276]


def test_repair_silent_ft_rebound_windows_reorders_reversed_and1_cluster():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0049600063"
    processor.data = [
        _stats_rebound(277, "Payton REBOUND (Off:1 Def:3)", team_id=1610612760, player_id=56, clock="7:30"),
        _stats_fg(276, 3, "MISS Barkley Free Throw 1 of 1", team_id=1610612745, player_id=787, action_type=10, clock="7:30"),
        {
            "GAME_ID": "0049600063",
            "EVENTNUM": 275,
            "PERIOD": 3,
            "PCTIMESTRING": "7:30",
            "EVENTMSGACTIONTYPE": 2,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 431,
            "PLAYER1_TEAM_ID": 1610612760,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "",
            "VISITORDESCRIPTION": "Kemp S.FOUL (P2.T3)",
            "NEUTRALDESCRIPTION": "",
        },
        _stats_fg(274, 1, "Barkley  Layup (11 PTS)", team_id=1610612745, player_id=787, action_type=5, clock="7:30"),
    ]
    for row in processor.data:
        row["PERIOD"] = 3

    processor._repair_silent_ft_rebound_windows()

    assert [row["EVENTNUM"] for row in processor.data] == [274, 275, 276, 277]


def test_repair_silent_ft_rebound_windows_reorders_reversed_two_shot_ft_block():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0049600063"
    processor.data = [
        _stats_fg(396, 3, "MISS Kemp Free Throw 2 of 2", team_id=1610612760, player_id=431, action_type=12, clock="7:39"),
        {
            "GAME_ID": "0049600063",
            "EVENTNUM": 395,
            "PERIOD": 4,
            "PCTIMESTRING": "7:39",
            "EVENTMSGACTIONTYPE": 0,
            "EVENTMSGTYPE": 4,
            "PLAYER1_ID": 1610612760,
            "PLAYER1_TEAM_ID": None,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "",
            "VISITORDESCRIPTION": "SUPERSONICS Rebound",
            "NEUTRALDESCRIPTION": "",
        },
        _stats_fg(394, 3, "MISS Kemp Free Throw 1 of 2", team_id=1610612760, player_id=431, action_type=11, clock="7:39"),
        {
            "GAME_ID": "0049600063",
            "EVENTNUM": 393,
            "PERIOD": 4,
            "PCTIMESTRING": "7:39",
            "EVENTMSGACTIONTYPE": 2,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 165,
            "PLAYER1_TEAM_ID": 1610612745,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "",
            "VISITORDESCRIPTION": "Olajuwon S.FOUL (P3.PN)",
            "NEUTRALDESCRIPTION": "",
        },
        _stats_rebound(397, "Barkley REBOUND (Off:0 Def:2)", team_id=1610612745, player_id=787, clock="7:38"),
    ]
    for row in processor.data:
        row["PERIOD"] = 4

    processor._repair_silent_ft_rebound_windows()

    assert [row["EVENTNUM"] for row in processor.data] == [393, 394, 395, 396, 397]


def test_repair_silent_ft_rebound_windows_moves_subs_before_terminal_ft_for_subbed_in_rebounder():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0041900155"
    processor.data = [
        _stats_fg(347, 3, "Doncic Free Throw 1 of 2 (12 PTS)", team_id=1610612742, player_id=1629029, action_type=11, clock="0:03"),
        _stats_fg(352, 3, "MISS Doncic Free Throw 2 of 2", team_id=1610612742, player_id=1629029, action_type=12, clock="0:03"),
        _stats_rebound(353, "Harrell REBOUND (Off:1 Def:3)", team_id=1610612746, player_id=1626149, clock="0:01"),
        _stats_sub(348, "SUB: Harrell FOR Zubac", team_id=1610612746, out_player_id=1627826, in_player_id=1626149, clock="0:03"),
        _stats_sub(349, "SUB: Kidd-Gilchrist FOR Finney-Smith", team_id=1610612742, out_player_id=1627827, in_player_id=203077, clock="0:03"),
    ]
    for row in processor.data:
        row["PERIOD"] = 2

    processor._repair_silent_ft_rebound_windows()

    assert [row["EVENTNUM"] for row in processor.data] == [347, 348, 349, 352, 353]


def test_repair_silent_ft_rebound_windows_moves_sub_block_before_live_one_shot_ft():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0029600204"
    processor.data = [
        {
            "GAME_ID": "0029600204",
            "EVENTNUM": 147,
            "PERIOD": 2,
            "PCTIMESTRING": "9:06",
            "EVENTMSGACTIONTYPE": 2,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 679,
            "PLAYER1_TEAM_ID": 1610612741,
            "PLAYER2_ID": 423,
            "PLAYER2_TEAM_ID": 1610612742,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
            "HOMEDESCRIPTION": "",
            "VISITORDESCRIPTION": "Caffey S.FOUL (P1.T2)",
        },
        _stats_fg(148, 3, "MISS Gatling Free Throw 1 of 1", team_id=1610612742, player_id=423, action_type=10, clock="9:06"),
        _stats_sub(149, "SUB: Jackson FOR McCloud", team_id=1610612742, out_player_id=45, in_player_id=754, clock="9:06"),
        _stats_sub(150, "SUB: Jordan FOR Kukoc", team_id=1610612741, out_player_id=389, in_player_id=893, clock="9:06"),
        _stats_sub(151, "SUB: Pippen FOR Rodman", team_id=1610612741, out_player_id=23, in_player_id=937, clock="9:06"),
        _stats_sub(152, "SUB: Dumas FOR Kidd", team_id=1610612742, out_player_id=467, in_player_id=434, clock="9:06"),
        _stats_rebound(153, "Pippen REBOUND (Off:0 Def:3)", team_id=1610612741, player_id=937, clock="9:05"),
    ]

    processor._repair_silent_ft_rebound_windows()

    assert [row["EVENTNUM"] for row in processor.data] == [147, 149, 150, 151, 152, 148, 153]


def test_repair_silent_ft_rebound_windows_leaves_sub_block_when_rebounder_not_subbed_in():
    processor = object.__new__(PbpProcessor)
    processor.game_id = "0041900155"
    processor.data = [
        _stats_fg(347, 3, "Doncic Free Throw 1 of 2 (12 PTS)", team_id=1610612742, player_id=1629029, action_type=11, clock="0:03"),
        _stats_fg(352, 3, "MISS Doncic Free Throw 2 of 2", team_id=1610612742, player_id=1629029, action_type=12, clock="0:03"),
        _stats_rebound(353, "Harrell REBOUND (Off:1 Def:3)", team_id=1610612746, player_id=1626149, clock="0:01"),
        _stats_sub(348, "SUB: Mann FOR Zubac", team_id=1610612746, out_player_id=1627826, in_player_id=1629611, clock="0:03"),
    ]
    for row in processor.data:
        row["PERIOD"] = 2

    processor._repair_silent_ft_rebound_windows()

    assert [row["EVENTNUM"] for row in processor.data] == [347, 352, 353, 348]

def test_processor_uses_expanded_retry_budget(monkeypatch):
    called = []

    def fake_process(self, max_retries):
        called.append(max_retries)

    monkeypatch.setattr(PbpProcessor, "_process_with_retries", fake_process)

    PbpProcessor("0029600111", [])

    assert called == [100]
