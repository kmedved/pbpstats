from __future__ import annotations

import pandas as pd

from historic_backfill.common.player_id_normalization import normalize_single_game_player_ids


def _official_boxscore(players: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(players)


def test_repairs_primary_player_id_from_scoring_description() -> None:
    game_df = pd.DataFrame(
        [
            {
                "PLAYER1_ID": 775,
                "PLAYER1_NAME": "",
                "PLAYER1_TEAM_ID": 1610612744,
                "PLAYER2_ID": 692,
                "PLAYER2_NAME": "Andrew DeClercq",
                "PLAYER2_TEAM_ID": 1610612744,
                "PLAYER3_ID": 0,
                "PLAYER3_NAME": "",
                "PLAYER3_TEAM_ID": 0,
                "HOMEDESCRIPTION": "Booker Layup (2 PTS) (DeClercq 1 AST)",
                "VISITORDESCRIPTION": None,
                "NEUTRALDESCRIPTION": None,
            }
        ]
    )
    official_boxscore = _official_boxscore(
        [
            {"PLAYER_ID": 511, "TEAM_ID": 1610612744, "PLAYER_NAME": "Melvin Booker"},
            {"PLAYER_ID": 692, "TEAM_ID": 1610612744, "PLAYER_NAME": "Andrew DeClercq"},
        ]
    )

    normalized = normalize_single_game_player_ids(game_df, official_boxscore)

    assert normalized.loc[0, "PLAYER1_ID"] == 511
    assert normalized.loc[0, "PLAYER1_NAME"] == "Melvin Booker"


def test_repairs_secondary_player_id_from_substitution_description() -> None:
    game_df = pd.DataFrame(
        [
            {
                "PLAYER1_ID": 899,
                "PLAYER1_NAME": "Mark Price",
                "PLAYER1_TEAM_ID": 1610612744,
                "PLAYER2_ID": 775,
                "PLAYER2_NAME": "",
                "PLAYER2_TEAM_ID": 1610612744,
                "PLAYER3_ID": 0,
                "PLAYER3_NAME": "",
                "PLAYER3_TEAM_ID": 0,
                "HOMEDESCRIPTION": "SUB: Booker FOR Price",
                "VISITORDESCRIPTION": None,
                "NEUTRALDESCRIPTION": None,
            }
        ]
    )
    official_boxscore = _official_boxscore(
        [
            {"PLAYER_ID": 511, "TEAM_ID": 1610612744, "PLAYER_NAME": "Melvin Booker"},
            {"PLAYER_ID": 899, "TEAM_ID": 1610612744, "PLAYER_NAME": "Mark Price"},
        ]
    )

    normalized = normalize_single_game_player_ids(game_df, official_boxscore)

    assert normalized.loc[0, "PLAYER2_ID"] == 511
    assert normalized.loc[0, "PLAYER2_NAME"] == "Melvin Booker"


def test_repairs_consistent_off_roster_alias_to_lone_missing_roster_player() -> None:
    game_df = pd.DataFrame(
        [
            {
                "EVENTMSGTYPE": 8,
                "PLAYER1_ID": 899,
                "PLAYER1_NAME": "Mark Price",
                "PLAYER1_TEAM_ID": 1610612744,
                "PLAYER2_ID": 902,
                "PLAYER2_NAME": "Bimbo Coles",
                "PLAYER2_TEAM_ID": 1610612744,
                "PLAYER3_ID": 0,
                "PLAYER3_NAME": "",
                "PLAYER3_TEAM_ID": 0,
                "HOMEDESCRIPTION": "SUB: Coles FOR Price",
                "VISITORDESCRIPTION": None,
                "NEUTRALDESCRIPTION": None,
            },
            {
                "EVENTMSGTYPE": 2,
                "PLAYER1_ID": 902,
                "PLAYER1_NAME": "Bimbo Coles",
                "PLAYER1_TEAM_ID": 1610612744,
                "PLAYER2_ID": 0,
                "PLAYER2_NAME": "",
                "PLAYER2_TEAM_ID": 0,
                "PLAYER3_ID": 0,
                "PLAYER3_NAME": "",
                "PLAYER3_TEAM_ID": 0,
                "HOMEDESCRIPTION": "MISS Coles 3PT Jump Shot",
                "VISITORDESCRIPTION": None,
                "NEUTRALDESCRIPTION": None,
            },
        ]
    )
    official_boxscore = _official_boxscore(
        [
            {"PLAYER_ID": 899, "TEAM_ID": 1610612744, "PLAYER_NAME": "Mark Price"},
            {"PLAYER_ID": 511, "TEAM_ID": 1610612744, "PLAYER_NAME": "Melvin Booker"},
        ]
    )

    normalized = normalize_single_game_player_ids(game_df, official_boxscore)

    assert normalized.loc[0, "PLAYER2_ID"] == 511
    assert normalized.loc[0, "PLAYER2_NAME"] == "Melvin Booker"
    assert normalized.loc[1, "PLAYER1_ID"] == 511
    assert normalized.loc[1, "PLAYER1_NAME"] == "Melvin Booker"


def test_repairs_tertiary_player_id_from_block_description() -> None:
    game_df = pd.DataFrame(
        [
            {
                "PLAYER1_ID": 727,
                "PLAYER1_NAME": "Eric Snow",
                "PLAYER1_TEAM_ID": 1610612760,
                "PLAYER2_ID": 0,
                "PLAYER2_NAME": "",
                "PLAYER2_TEAM_ID": 0,
                "PLAYER3_ID": 775,
                "PLAYER3_NAME": "",
                "PLAYER3_TEAM_ID": 1610612744,
                "HOMEDESCRIPTION": "Booker BLOCK (1 BLK)",
                "VISITORDESCRIPTION": "MISS Snow 17' Jump Shot",
                "NEUTRALDESCRIPTION": None,
            }
        ]
    )
    official_boxscore = _official_boxscore(
        [
            {"PLAYER_ID": 511, "TEAM_ID": 1610612744, "PLAYER_NAME": "Melvin Booker"},
            {"PLAYER_ID": 727, "TEAM_ID": 1610612760, "PLAYER_NAME": "Eric Snow"},
        ]
    )

    normalized = normalize_single_game_player_ids(game_df, official_boxscore)

    assert normalized.loc[0, "PLAYER3_ID"] == 511
    assert normalized.loc[0, "PLAYER3_NAME"] == "Melvin Booker"


def test_ambiguous_same_team_surname_is_left_unchanged() -> None:
    game_df = pd.DataFrame(
        [
            {
                "PLAYER1_ID": 999,
                "PLAYER1_NAME": "",
                "PLAYER1_TEAM_ID": 1,
                "PLAYER2_ID": 0,
                "PLAYER2_NAME": "",
                "PLAYER2_TEAM_ID": 0,
                "PLAYER3_ID": 0,
                "PLAYER3_NAME": "",
                "PLAYER3_TEAM_ID": 0,
                "HOMEDESCRIPTION": "Smith Free Throw 1 of 2",
                "VISITORDESCRIPTION": None,
                "NEUTRALDESCRIPTION": None,
            }
        ]
    )
    official_boxscore = _official_boxscore(
        [
            {"PLAYER_ID": 11, "TEAM_ID": 1, "PLAYER_NAME": "John Smith"},
            {"PLAYER_ID": 22, "TEAM_ID": 1, "PLAYER_NAME": "Joe Smith"},
        ]
    )

    normalized = normalize_single_game_player_ids(game_df, official_boxscore)

    assert normalized.equals(game_df)


def test_repairs_preserve_string_player_id_columns() -> None:
    game_df = pd.DataFrame(
        [
            {
                "PLAYER1_ID": "775",
                "PLAYER1_NAME": "",
                "PLAYER1_TEAM_ID": "1610612744",
                "PLAYER2_ID": "692",
                "PLAYER2_NAME": "Andrew DeClercq",
                "PLAYER2_TEAM_ID": "1610612744",
                "PLAYER3_ID": "0",
                "PLAYER3_NAME": "",
                "PLAYER3_TEAM_ID": "0",
                "HOMEDESCRIPTION": "Booker Layup (2 PTS) (DeClercq 1 AST)",
                "VISITORDESCRIPTION": None,
                "NEUTRALDESCRIPTION": None,
            }
        ]
    ).astype(
        {
            "PLAYER1_ID": "string",
            "PLAYER1_TEAM_ID": "string",
            "PLAYER2_ID": "string",
            "PLAYER2_TEAM_ID": "string",
            "PLAYER3_ID": "string",
            "PLAYER3_TEAM_ID": "string",
        }
    )
    official_boxscore = _official_boxscore(
        [
            {"PLAYER_ID": 511, "TEAM_ID": 1610612744, "PLAYER_NAME": "Melvin Booker"},
            {"PLAYER_ID": 692, "TEAM_ID": 1610612744, "PLAYER_NAME": "Andrew DeClercq"},
        ]
    )

    normalized = normalize_single_game_player_ids(game_df, official_boxscore)

    assert normalized.loc[0, "PLAYER1_ID"] == "511"


def test_steal_slot_uses_event_role_to_break_same_surname_ambiguity() -> None:
    game_df = pd.DataFrame(
        [
            {
                "EVENTMSGTYPE": 5,
                "PLAYER1_ID": 181,
                "PLAYER1_NAME": "Kenny Smith",
                "PLAYER1_TEAM_ID": 1610612743,
                "PLAYER2_ID": 775,
                "PLAYER2_NAME": "",
                "PLAYER2_TEAM_ID": 1610612744,
                "PLAYER3_ID": 0,
                "PLAYER3_NAME": "",
                "PLAYER3_TEAM_ID": 0,
                "HOMEDESCRIPTION": "Booker STEAL (1 STL)",
                "VISITORDESCRIPTION": "Smith Bad Pass Turnover (P3.T12)",
                "NEUTRALDESCRIPTION": None,
            }
        ]
    )
    official_boxscore = _official_boxscore(
        [
            {"PLAYER_ID": 511, "TEAM_ID": 1610612744, "PLAYER_NAME": "Melvin Booker"},
            {"PLAYER_ID": 693, "TEAM_ID": 1610612744, "PLAYER_NAME": "Joe Smith"},
            {"PLAYER_ID": 181, "TEAM_ID": 1610612743, "PLAYER_NAME": "Kenny Smith"},
        ]
    )

    normalized = normalize_single_game_player_ids(game_df, official_boxscore)

    assert normalized.loc[0, "PLAYER2_ID"] == 511
    assert normalized.loc[0, "PLAYER2_NAME"] == "Melvin Booker"


def test_missed_shot_slot_uses_event_role_to_break_same_surname_ambiguity() -> None:
    game_df = pd.DataFrame(
        [
            {
                "EVENTMSGTYPE": 2,
                "PLAYER1_ID": 775,
                "PLAYER1_NAME": "",
                "PLAYER1_TEAM_ID": 1610612744,
                "PLAYER2_ID": 0,
                "PLAYER2_NAME": "",
                "PLAYER2_TEAM_ID": 0,
                "PLAYER3_ID": 63,
                "PLAYER3_NAME": "Michael Smith",
                "PLAYER3_TEAM_ID": 1610612758,
                "HOMEDESCRIPTION": "Smith BLOCK (1 BLK)",
                "VISITORDESCRIPTION": "MISS Booker Layup",
                "NEUTRALDESCRIPTION": None,
            }
        ]
    )
    official_boxscore = _official_boxscore(
        [
            {"PLAYER_ID": 511, "TEAM_ID": 1610612744, "PLAYER_NAME": "Melvin Booker"},
            {"PLAYER_ID": 693, "TEAM_ID": 1610612744, "PLAYER_NAME": "Joe Smith"},
            {"PLAYER_ID": 63, "TEAM_ID": 1610612758, "PLAYER_NAME": "Michael Smith"},
        ]
    )

    normalized = normalize_single_game_player_ids(game_df, official_boxscore)

    assert normalized.loc[0, "PLAYER1_ID"] == 511
    assert normalized.loc[0, "PLAYER1_NAME"] == "Melvin Booker"


def test_jump_ball_slot_uses_event_role_to_break_same_surname_ambiguity() -> None:
    game_df = pd.DataFrame(
        [
            {
                "EVENTMSGTYPE": 10,
                "PLAYER1_ID": 63,
                "PLAYER1_NAME": "Michael Smith",
                "PLAYER1_TEAM_ID": 1610612758,
                "PLAYER2_ID": 775,
                "PLAYER2_NAME": "",
                "PLAYER2_TEAM_ID": 1610612744,
                "PLAYER3_ID": 722,
                "PLAYER3_NAME": "Corliss Williamson",
                "PLAYER3_TEAM_ID": 1610612758,
                "HOMEDESCRIPTION": "Jump Ball Smith vs. Booker: Tip to Williamson",
                "VISITORDESCRIPTION": None,
                "NEUTRALDESCRIPTION": None,
            }
        ]
    )
    official_boxscore = _official_boxscore(
        [
            {"PLAYER_ID": 511, "TEAM_ID": 1610612744, "PLAYER_NAME": "Melvin Booker"},
            {"PLAYER_ID": 693, "TEAM_ID": 1610612744, "PLAYER_NAME": "Joe Smith"},
            {"PLAYER_ID": 63, "TEAM_ID": 1610612758, "PLAYER_NAME": "Michael Smith"},
            {"PLAYER_ID": 722, "TEAM_ID": 1610612758, "PLAYER_NAME": "Corliss Williamson"},
        ]
    )

    normalized = normalize_single_game_player_ids(game_df, official_boxscore)

    assert normalized.loc[0, "PLAYER2_ID"] == 511
    assert normalized.loc[0, "PLAYER2_NAME"] == "Melvin Booker"


def test_fills_missing_team_id_for_known_roster_player() -> None:
    game_df = pd.DataFrame(
        [
            {
                "EVENTMSGTYPE": 4,
                "PLAYER1_ID": 2484,
                "PLAYER1_NAME": "Devin Brown",
                "PLAYER1_TEAM_ID": None,
                "PLAYER2_ID": 0,
                "PLAYER2_NAME": "",
                "PLAYER2_TEAM_ID": 0,
                "PLAYER3_ID": 0,
                "PLAYER3_NAME": "",
                "PLAYER3_TEAM_ID": 0,
                "HOMEDESCRIPTION": "",
                "VISITORDESCRIPTION": "",
                "NEUTRALDESCRIPTION": None,
            }
        ]
    )
    official_boxscore = _official_boxscore(
        [
            {"PLAYER_ID": 2484, "TEAM_ID": 1610612740, "PLAYER_NAME": "Devin Brown"},
        ]
    )

    normalized = normalize_single_game_player_ids(game_df, official_boxscore)

    assert normalized.loc[0, "PLAYER1_ID"] == 2484
    assert normalized.loc[0, "PLAYER1_TEAM_ID"] == 1610612740


def test_fills_missing_team_id_preserving_string_dtype() -> None:
    game_df = pd.DataFrame(
        [
            {
                "EVENTMSGTYPE": 4,
                "PLAYER1_ID": "2420",
                "PLAYER1_NAME": "Nenad Krstic",
                "PLAYER1_TEAM_ID": None,
                "PLAYER2_ID": "0",
                "PLAYER2_NAME": "",
                "PLAYER2_TEAM_ID": "0",
                "PLAYER3_ID": "0",
                "PLAYER3_NAME": "",
                "PLAYER3_TEAM_ID": "0",
                "HOMEDESCRIPTION": "",
                "VISITORDESCRIPTION": "",
                "NEUTRALDESCRIPTION": None,
            }
        ]
    ).astype(
        {
            "PLAYER1_ID": "string",
            "PLAYER1_TEAM_ID": "string",
            "PLAYER2_ID": "string",
            "PLAYER2_TEAM_ID": "string",
            "PLAYER3_ID": "string",
            "PLAYER3_TEAM_ID": "string",
        }
    )
    official_boxscore = _official_boxscore(
        [
            {"PLAYER_ID": 2420, "TEAM_ID": 1610612760, "PLAYER_NAME": "Nenad Krstic"},
        ]
    )

    normalized = normalize_single_game_player_ids(game_df, official_boxscore)

    assert normalized.loc[0, "PLAYER1_TEAM_ID"] == "1610612760"
