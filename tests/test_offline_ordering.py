import pandas as pd

import pbpstats
from pbpstats.offline.ordering import dedupe_with_v3, patch_start_of_periods
from pbpstats.offline.row_overrides import PBP_ROW_OVERRIDE_ACTION_COLUMN


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

    patched = patch_start_of_periods(
        game_df, "0029600111", fetch_pbp_v3_fn=lambda _: v3_df
    )

    assert patched["EVENTNUM"].tolist() == [200, 202, 207, 210, 212, 211]
    assert patched.iloc[2]["EVENTMSGTYPE"] == 12
    assert patched.iloc[2]["PERIOD"] == 3


def test_patch_start_of_periods_inserts_overtime_start_with_five_minute_clock():
    game_df = pd.DataFrame(
        [
            _row(501, 5, msg_type=1, clock="4:55"),
        ]
    )
    v3_df = pd.DataFrame(
        [
            {
                "actionType": "period",
                "subType": "start",
                "period": 5,
                "actionNumber": 500,
            }
        ]
    )

    patched = patch_start_of_periods(
        game_df, "0029700001", fetch_pbp_v3_fn=lambda _: v3_df
    )

    inserted = patched[patched["EVENTNUM"] == 500].iloc[0]
    assert patched["EVENTNUM"].tolist() == [500, 501]
    assert inserted["EVENTMSGTYPE"] == 12
    assert inserted["PERIOD"] == 5
    assert inserted["PCTIMESTRING"] == "5:00"


def test_patch_start_of_periods_infers_wnba_ten_minute_regulation_clock():
    game_df = pd.DataFrame(
        [
            {
                **_row(4, 1, msg_type=10, clock="9:58"),
                "GAME_ID": "1022500234",
            },
        ]
    )

    patched = patch_start_of_periods(game_df, "1022500234", fetch_pbp_v3_fn=None)

    inserted = patched.iloc[0]
    assert inserted["EVENTMSGTYPE"] == 12
    assert inserted["PCTIMESTRING"] == "10:00"


def test_patch_start_of_periods_infers_old_wnba_twenty_minute_halves():
    game_df = pd.DataFrame(
        [
            {
                **_row(4, 1, msg_type=10, clock="19:58"),
                "GAME_ID": 1029700234.0,
            },
        ]
    )

    patched = patch_start_of_periods(game_df, 1029700234.0, fetch_pbp_v3_fn=None)

    inserted = patched.iloc[0]
    assert inserted["GAME_ID"] == "1029700234"
    assert inserted["EVENTMSGTYPE"] == 12
    assert inserted["PCTIMESTRING"] == "20:00"


def test_patch_start_of_periods_treats_old_wnba_period_three_as_overtime():
    game_df = pd.DataFrame(
        [
            {
                **_row(101, 2, msg_type=1, clock="19:45"),
                "GAME_ID": "1029700234",
            },
            {
                **_row(201, 3, msg_type=1, clock="4:55"),
                "GAME_ID": "1029700234",
            },
        ]
    )
    v3_df = pd.DataFrame(
        [
            {
                "actionType": "period",
                "subType": "start",
                "period": 2,
                "actionNumber": 100,
            },
            {
                "actionType": "period",
                "subType": "start",
                "period": 3,
                "actionNumber": 200,
            },
        ]
    )

    fetched_game_ids = []

    def fetch_v3(game_id):
        fetched_game_ids.append(game_id)
        return v3_df

    patched = patch_start_of_periods(
        game_df, "1029700234.0", fetch_pbp_v3_fn=fetch_v3
    )

    period_two_start = patched[patched["EVENTNUM"] == 100].iloc[0]
    period_three_start = patched[patched["EVENTNUM"] == 200].iloc[0]
    assert fetched_game_ids == ["1029700234"]
    assert period_two_start["PCTIMESTRING"] == "20:00"
    assert period_three_start["PCTIMESTRING"] == "5:00"


def test_patch_start_of_periods_honors_explicit_wnba_league_for_short_ids():
    game_df = pd.DataFrame(
        [
            {
                **_row(101, 2, msg_type=1, clock="9:45"),
                "GAME_ID": "22500234",
            },
        ]
    )
    v3_df = pd.DataFrame(
        [
            {
                "actionType": "period",
                "subType": "start",
                "period": 2,
                "actionNumber": 100,
            }
        ]
    )

    patched = patch_start_of_periods(
        game_df,
        "22500234",
        fetch_pbp_v3_fn=lambda _: v3_df,
        league=pbpstats.WNBA_STRING,
    )

    inserted = patched[patched["EVENTNUM"] == 100].iloc[0]
    assert inserted["EVENTMSGTYPE"] == 12
    assert inserted["PCTIMESTRING"] == "10:00"
    assert set(patched["GAME_ID"]) == {"1022500234"}


def test_patch_start_of_periods_coerces_string_period_for_q1_insert():
    game_df = pd.DataFrame(
        [
            {
                **_row(4, "1", msg_type=10, clock="9:58"),
                "GAME_ID": "1022500234",
            },
        ]
    )

    patched = patch_start_of_periods(game_df, "1022500234", fetch_pbp_v3_fn=None)

    inserted = patched.iloc[0]
    assert inserted["EVENTMSGTYPE"] == 12
    assert inserted["PERIOD"] == 1
    assert inserted["PCTIMESTRING"] == "10:00"


def test_patch_start_of_periods_inserts_later_start_before_string_period_rows():
    game_df = pd.DataFrame(
        [
            {
                **_row(101, "2", msg_type=1, clock="9:45"),
                "GAME_ID": "1022500234",
            },
        ]
    )
    v3_df = pd.DataFrame(
        [
            {
                "actionType": "period",
                "subType": "start",
                "period": 2,
                "actionNumber": 100,
            }
        ]
    )

    patched = patch_start_of_periods(
        game_df, "1022500234", fetch_pbp_v3_fn=lambda _: v3_df
    )

    assert patched["EVENTNUM"].tolist() == [100, 101]
    inserted = patched.iloc[0]
    assert inserted["EVENTMSGTYPE"] == 12
    assert inserted["PERIOD"] == 2
    assert inserted["PCTIMESTRING"] == "10:00"


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


def test_patch_start_of_periods_coerces_string_eventmsgtype_before_existing_start_check():
    game_df = pd.DataFrame(
        [
            _row(55, 1, msg_type="12", clock="10:00"),
            _row(56, 1, msg_type="10", clock="9:59"),
        ]
    )

    patched = patch_start_of_periods(
        game_df,
        "22500234",
        fetch_pbp_v3_fn=None,
        league=pbpstats.WNBA_STRING,
    )

    assert patched["EVENTNUM"].tolist() == [55, 56]
    assert patched["EVENTMSGTYPE"].tolist() == [12, 10]
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


def test_dedupe_with_v3_preserves_explicit_pbp_row_override_rows():
    game_df = pd.DataFrame(
        [
            _row(147, 2, msg_type=6, clock="7:59"),
            {
                **_row(148, 2, msg_type=8, clock="7:59"),
                PBP_ROW_OVERRIDE_ACTION_COLUMN: "insert_sub_before",
            },
            _row(149, 2, msg_type=8, clock="7:59"),
        ]
    )
    v3_df = pd.DataFrame(
        [
            {"actionNumber": 147},
            {"actionNumber": 149},
        ]
    )

    deduped = dedupe_with_v3(game_df, "0020400335", fetch_pbp_v3_fn=lambda _: v3_df)

    assert deduped["EVENTNUM"].tolist() == [147, 148, 149]
