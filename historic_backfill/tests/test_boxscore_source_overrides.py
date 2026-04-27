import pandas as pd

from historic_backfill.catalogs.boxscore_source_overrides import (
    BOXSCORE_SOURCE_COLUMNS,
    apply_boxscore_response_overrides,
)


def _override_row(**kwargs):
    row = {column: 0 for column in BOXSCORE_SOURCE_COLUMNS}
    row.update(
        {
            "GAME_ID": "0029600070",
            "TEAM_ID": 1610612743,
            "TEAM_ABBREVIATION": "DEN",
            "TEAM_CITY": "Denver",
            "PLAYER_ID": 36,
            "PLAYER_NAME": "Sarunas Marciulionis",
            "NICKNAME": "Sarunas",
            "START_POSITION": "",
            "COMMENT": "",
            "MIN": "18:00",
            "FGM": 3,
            "FGA": 9,
            "FG_PCT": 0.333,
            "FG3M": 1,
            "FG3A": 3,
            "FG3_PCT": 0.333,
            "FTM": 0,
            "FTA": 0,
            "FT_PCT": 0.0,
            "OREB": 2,
            "DREB": 2,
            "REB": 4,
            "AST": 2,
            "STL": 0,
            "BLK": 0,
            "TO": 2,
            "PF": 3,
            "PTS": 7,
            "PLUS_MINUS": -20,
        }
    )
    row.update(kwargs)
    return {"game_id": "0029600070", **row, "notes": "test"}


def test_apply_boxscore_response_overrides_adds_missing_player_row():
    base = {
        "resultSets": [
            {
                "headers": BOXSCORE_SOURCE_COLUMNS,
                "rowSet": [
                    [
                        "0029600070",
                        1610612743,
                        "DEN",
                        "Denver",
                        107,
                        "Dale Ellis",
                        "Dale",
                        "F",
                        "",
                        "33:29",
                        8,
                        12,
                        0.667,
                        2,
                        3,
                        0.667,
                        0,
                        0,
                        0.0,
                        2,
                        2,
                        4,
                        3,
                        0,
                        0,
                        3,
                        1,
                        18,
                        -5,
                    ]
                ],
            }
        ]
    }
    overrides = pd.DataFrame([_override_row()])

    adjusted = apply_boxscore_response_overrides("0029600070", base, overrides=overrides)

    rows = adjusted["resultSets"][0]["rowSet"]
    assert len(rows) == 2
    sarunas = pd.DataFrame(rows, columns=BOXSCORE_SOURCE_COLUMNS).set_index("PLAYER_ID").loc[36]
    assert sarunas["PLAYER_NAME"] == "Sarunas Marciulionis"
    assert sarunas["PTS"] == 7
    assert sarunas["PF"] == 3


def test_apply_boxscore_response_overrides_replaces_existing_player_row():
    base = {
        "resultSets": [
            {
                "headers": BOXSCORE_SOURCE_COLUMNS,
                "rowSet": [
                    [
                        "0029600070",
                        1610612743,
                        "DEN",
                        "Denver",
                        36,
                        "Bad Name",
                        "Bad",
                        "",
                        "",
                        "00:00",
                        0,
                        0,
                        0.0,
                        0,
                        0,
                        0.0,
                        0,
                        0,
                        0.0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                    ]
                ],
            }
        ]
    }
    overrides = pd.DataFrame([_override_row()])

    adjusted = apply_boxscore_response_overrides("0029600070", base, overrides=overrides)

    rows = adjusted["resultSets"][0]["rowSet"]
    assert len(rows) == 1
    sarunas = dict(zip(BOXSCORE_SOURCE_COLUMNS, rows[0]))
    assert sarunas["PLAYER_NAME"] == "Sarunas Marciulionis"
    assert sarunas["MIN"] == "18:00"
    assert sarunas["REB"] == 4


def test_apply_boxscore_response_overrides_can_fix_existing_row_without_changing_roster():
    base = {
        "resultSets": [
            {
                "headers": BOXSCORE_SOURCE_COLUMNS,
                "rowSet": [
                    [
                        "0029800661",
                        1610612751,
                        "NJN",
                        "New Jersey",
                        971,
                        "Mark Hendrickson",
                        "Mark",
                        "",
                        "",
                        "36",
                        5,
                        9,
                        0.556,
                        0,
                        0,
                        0.0,
                        2,
                        2,
                        1.0,
                        3,
                        5,
                        8,
                        1,
                        2,
                        0,
                        1,
                        1,
                        12,
                        0,
                    ],
                    [
                        "0029800661",
                        1610612751,
                        "NJN",
                        "New Jersey",
                        710,
                        "David Vaughn",
                        "David",
                        "",
                        "",
                        "3",
                        1,
                        1,
                        1.0,
                        0,
                        0,
                        0.0,
                        2,
                        2,
                        1.0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        4,
                        0,
                    ],
                ],
            }
        ]
    }
    overrides = pd.DataFrame(
        [
            {
                "game_id": "0029800661",
                "GAME_ID": "0029800661",
                "TEAM_ID": 1610612751,
                "TEAM_ABBREVIATION": "NJN",
                "TEAM_CITY": "New Jersey",
                "PLAYER_ID": 971,
                "PLAYER_NAME": "Mark Hendrickson",
                "NICKNAME": "Mark",
                "START_POSITION": "",
                "COMMENT": "",
                "MIN": "36",
                "FGM": 4,
                "FGA": 8,
                "FG_PCT": 0.5,
                "FG3M": 0,
                "FG3A": 0,
                "FG3_PCT": 0.0,
                "FTM": 0,
                "FTA": 0,
                "FT_PCT": 0.0,
                "OREB": 3,
                "DREB": 5,
                "REB": 8,
                "AST": 1,
                "STL": 2,
                "BLK": 0,
                "TO": 1,
                "PF": 1,
                "PTS": 8,
                "PLUS_MINUS": 0,
                "notes": "test",
            }
        ]
    )

    adjusted = apply_boxscore_response_overrides("0029800661", base, overrides=overrides)

    rows = pd.DataFrame(adjusted["resultSets"][0]["rowSet"], columns=BOXSCORE_SOURCE_COLUMNS)
    assert len(rows) == 2
    assert rows.loc[rows["PLAYER_ID"] == 971, "PTS"].item() == 8
    assert rows.loc[rows["PLAYER_ID"] == 710, "PTS"].item() == 4
    assert rows.loc[rows["TEAM_ID"] == 1610612751, "PTS"].sum() == 12
