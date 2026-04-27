from __future__ import annotations

import pandas as pd

from team_event_normalization import normalize_single_game_team_events


def test_home_side_team_rebound_becomes_real_team_row() -> None:
    game_df = pd.DataFrame(
        [
            {
                "EVENTMSGTYPE": "4",
                "PLAYER1_ID": 8,
                "PLAYER1_TEAM_ID": None,
                "HOMEDESCRIPTION": "Warriors Rebound",
                "VISITORDESCRIPTION": None,
            }
        ]
    )

    normalized = normalize_single_game_team_events(
        game_df,
        home_team_id=1610612744,
        away_team_id=1610612760,
        boxscore_player_ids={23, 34},
    )

    assert normalized.loc[0, "PLAYER1_TEAM_ID"] == 1610612744
    assert normalized.loc[0, "PLAYER1_ID"] == 0


def test_visitor_side_team_foul_becomes_real_team_row() -> None:
    game_df = pd.DataFrame(
        [
            {
                "EVENTMSGTYPE": 6,
                "PLAYER1_ID": 24,
                "PLAYER1_TEAM_ID": None,
                "HOMEDESCRIPTION": None,
                "VISITORDESCRIPTION": "SUPERSONICS Foul",
            }
        ]
    )

    normalized = normalize_single_game_team_events(
        game_df,
        home_team_id=1610612744,
        away_team_id=1610612760,
        boxscore_player_ids={20, 40},
    )

    assert normalized.loc[0, "PLAYER1_TEAM_ID"] == 1610612760
    assert normalized.loc[0, "PLAYER1_ID"] == 0


def test_real_player_id_is_preserved_when_team_id_is_missing() -> None:
    game_df = pd.DataFrame(
        [
            {
                "EVENTMSGTYPE": "6",
                "PLAYER1_ID": 201939,
                "PLAYER1_TEAM_ID": None,
                "HOMEDESCRIPTION": "Player Foul",
                "VISITORDESCRIPTION": None,
            }
        ]
    )

    normalized = normalize_single_game_team_events(
        game_df,
        home_team_id=1610612744,
        away_team_id=1610612760,
        boxscore_player_ids={201939},
    )

    assert normalized.loc[0, "PLAYER1_TEAM_ID"] == 1610612744
    assert normalized.loc[0, "PLAYER1_ID"] == 201939


def test_rows_with_both_sides_populated_are_unchanged() -> None:
    game_df = pd.DataFrame(
        [
            {
                "EVENTMSGTYPE": "5",
                "PLAYER1_ID": 8,
                "PLAYER1_TEAM_ID": None,
                "HOMEDESCRIPTION": "Warriors Turnover",
                "VISITORDESCRIPTION": "SuperSonics Steal",
            }
        ]
    )

    normalized = normalize_single_game_team_events(
        game_df,
        home_team_id=1610612744,
        away_team_id=1610612760,
        boxscore_player_ids=set(),
    )

    assert normalized.equals(game_df)


def test_rows_outside_team_style_event_types_are_unchanged() -> None:
    game_df = pd.DataFrame(
        [
            {
                "EVENTMSGTYPE": "3",
                "PLAYER1_ID": 8,
                "PLAYER1_TEAM_ID": None,
                "HOMEDESCRIPTION": "Warriors Free Throw 1 of 2",
                "VISITORDESCRIPTION": None,
            }
        ]
    )

    normalized = normalize_single_game_team_events(
        game_df,
        home_team_id=1610612744,
        away_team_id=1610612760,
        boxscore_player_ids=set(),
    )

    assert normalized.equals(game_df)


def test_zero_team_ids_short_circuit_without_changes() -> None:
    game_df = pd.DataFrame(
        [
            {
                "EVENTMSGTYPE": "4",
                "PLAYER1_ID": 8,
                "PLAYER1_TEAM_ID": None,
                "HOMEDESCRIPTION": "Warriors Rebound",
                "VISITORDESCRIPTION": None,
            }
        ]
    )

    normalized = normalize_single_game_team_events(
        game_df,
        home_team_id=0,
        away_team_id=0,
        boxscore_player_ids=set(),
    )

    assert normalized.equals(game_df)


def test_string_backed_team_id_columns_accept_normalized_values() -> None:
    game_df = pd.DataFrame(
        [
            {
                "EVENTMSGTYPE": "4",
                "PLAYER1_ID": "1610612761",
                "PLAYER1_TEAM_ID": pd.NA,
                "HOMEDESCRIPTION": "Raptors Rebound",
                "VISITORDESCRIPTION": None,
            }
        ]
    ).astype(
        {
            "PLAYER1_ID": "string[pyarrow]",
            "PLAYER1_TEAM_ID": "string[pyarrow]",
        }
    )

    normalized = normalize_single_game_team_events(
        game_df,
        home_team_id=1610612761,
        away_team_id=1610612744,
        boxscore_player_ids=set(),
    )

    assert normalized.loc[0, "PLAYER1_TEAM_ID"] == "1610612761"
    assert normalized.loc[0, "PLAYER1_ID"] == "0"


def test_object_string_team_id_columns_accept_normalized_values() -> None:
    game_df = pd.DataFrame(
        [
            {
                "EVENTMSGTYPE": "4",
                "PLAYER1_ID": "1610612761",
                "PLAYER1_TEAM_ID": "",
                "HOMEDESCRIPTION": "Raptors Rebound",
                "VISITORDESCRIPTION": None,
            }
        ]
    )

    normalized = normalize_single_game_team_events(
        game_df,
        home_team_id=1610612761,
        away_team_id=1610612744,
        boxscore_player_ids=set(),
    )

    assert normalized.loc[0, "PLAYER1_TEAM_ID"] == "1610612761"
    assert normalized.loc[0, "PLAYER1_ID"] == "0"
