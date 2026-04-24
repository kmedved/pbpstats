import pandas as pd

from pbpstats.offline.ordering import patch_start_of_periods


def _row(eventnum, period, msg_type=1, clock="11:59"):
    return {
        "GAME_ID": "0029600111",
        "EVENTNUM": eventnum,
        "PERIOD": period,
        "EVENTMSGTYPE": msg_type,
        "EVENTMSGACTIONTYPE": 0,
        "PCTIMESTRING": clock,
        "PLAYER1_ID": 0,
        "PLAYER1_TEAM_ID": 0,
        "PLAYER2_ID": 0,
        "PLAYER2_TEAM_ID": 0,
        "PLAYER3_ID": 0,
        "PLAYER3_TEAM_ID": 0,
    }


def test_patch_start_of_periods_prepends_q1_without_resorting_nonmonotonic_cluster():
    game_df = pd.DataFrame(
        [
            _row(56, 1, msg_type=10, clock="5:08"),
            _row(57, 1, msg_type=4, clock="5:06"),
            _row(58, 1, msg_type=2, clock="4:34"),
            _row(59, 1, msg_type=4, clock="4:22"),
            _row(60, 1, msg_type=1, clock="4:22"),
            _row(63, 1, msg_type=2, clock="4:17"),
            _row(62, 1, msg_type=4, clock="3:59"),
        ]
    )

    patched = patch_start_of_periods(game_df, "0029600111", fetch_pbp_v3_fn=None)

    assert patched["EVENTNUM"].tolist() == [55, 56, 57, 58, 59, 60, 63, 62]
    assert patched.iloc[0]["EVENTMSGTYPE"] == 12


def test_patch_start_of_periods_inserts_later_period_start_before_first_period_event_without_sorting():
    game_df = pd.DataFrame(
        [
            _row(200, 2, msg_type=12, clock="12:00"),
            _row(202, 2, msg_type=1, clock="11:45"),
            _row(210, 3, msg_type=2, clock="11:55"),
            _row(212, 3, msg_type=4, clock="11:40"),
            _row(211, 3, msg_type=1, clock="11:40"),
        ]
    )
    v3_df = pd.DataFrame(
        [
            {
                "actionType": "period",
                "subType": "start",
                "period": 3,
                "actionNumber": 207,
            }
        ]
    )

    patched = patch_start_of_periods(game_df, "0029600111", fetch_pbp_v3_fn=lambda _: v3_df)

    assert patched["EVENTNUM"].tolist() == [200, 202, 207, 210, 212, 211]
    assert patched.iloc[2]["EVENTMSGTYPE"] == 12
    assert patched.iloc[2]["PERIOD"] == 3


def test_patch_start_of_periods_leaves_existing_q1_start_unchanged():
    game_df = pd.DataFrame(
        [
            _row(55, 1, msg_type=12, clock="12:00"),
            _row(56, 1, msg_type=10, clock="11:59"),
            _row(57, 1, msg_type=2, clock="11:40"),
            _row(58, 1, msg_type=4, clock="11:38"),
        ]
    )

    patched = patch_start_of_periods(game_df, "0029600111", fetch_pbp_v3_fn=None)

    assert patched["EVENTNUM"].tolist() == [55, 56, 57, 58]
    assert (patched["EVENTMSGTYPE"] == 12).sum() == 1


def test_patch_start_of_periods_moves_existing_start_before_start_clock_live_action():
    game_df = pd.DataFrame(
        [
            _row(344, 2, msg_type=13, clock="0:00"),
            _row(346, 3, msg_type=2, clock="12:00"),
            _row(345, 3, msg_type=12, clock="12:00"),
            _row(347, 3, msg_type=4, clock="11:51"),
            _row(348, 3, msg_type=2, clock="11:50"),
        ]
    )

    patched = patch_start_of_periods(game_df, "0021700394", fetch_pbp_v3_fn=None)

    assert patched["EVENTNUM"].tolist() == [344, 345, 346, 347, 348]
    assert patched.iloc[1]["EVENTMSGTYPE"] == 12


def test_patch_start_of_periods_leaves_period_start_after_exact_start_technical_cluster():
    game_df = pd.DataFrame(
        [
            _row(200, 2, msg_type=6, clock="12:00"),
            _row(201, 2, msg_type=12, clock="12:00"),
            _row(202, 2, msg_type=3, clock="12:00"),
            _row(203, 2, msg_type=1, clock="11:44"),
        ]
    )
    game_df.loc[0, "EVENTMSGACTIONTYPE"] = 11

    patched = patch_start_of_periods(game_df, "0021700394", fetch_pbp_v3_fn=None)

    assert patched["EVENTNUM"].tolist() == [200, 201, 202, 203]
