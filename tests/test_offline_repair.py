import pandas as pd
import pytest

from pbpstats.offline.ordering import reorder_with_v3
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


def test_reorder_with_v3_preserves_existing_row_order_for_putback_cluster():
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

    ordered = reorder_with_v3(game_df, "0029601035", lambda _: v3_df)

    assert ordered["EVENTNUM"].tolist() == [463, 465, 464]


def test_reorder_with_v3_preserves_existing_row_order_for_missed_free_throw_cluster():
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

    ordered = reorder_with_v3(game_df, "0029601101", lambda _: v3_df)

    assert ordered["EVENTNUM"].tolist() == [450, 454, 451, 452, 453]


def test_reorder_with_v3_coerces_eventnum_to_int_without_resorting():
    game_df = pd.DataFrame(
        [
            {"GAME_ID": "0029601086", "EVENTNUM": "425", "EVENTMSGTYPE": 2, "PERIOD": 4},
            {"GAME_ID": "0029601086", "EVENTNUM": "427", "EVENTMSGTYPE": 4, "PERIOD": 4},
            {"GAME_ID": "0029601086", "EVENTNUM": "426", "EVENTMSGTYPE": 1, "PERIOD": 4},
        ]
    )

    ordered = reorder_with_v3(game_df, "0029601086", lambda _: pd.DataFrame())

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


def test_processor_uses_expanded_retry_budget(monkeypatch):
    called = []

    def fake_process(self, max_retries):
        called.append(max_retries)

    monkeypatch.setattr(PbpProcessor, "_process_with_retries", fake_process)

    PbpProcessor("0029600111", [])

    assert called == [100]
