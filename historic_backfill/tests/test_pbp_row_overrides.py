from pathlib import Path

import pandas as pd

from historic_backfill.catalogs.loader import (
    DEFAULT_HISTORIC_PBP_ROW_OVERRIDES_PATH,
    load_historic_pbp_row_overrides,
    validate_historic_pbp_row_override_catalog,
)
from pbpstats.offline.row_overrides import (
    PBP_ROW_OVERRIDE_ACTION_COLUMN,
    apply_pbp_row_overrides,
)


def _game_df(event_nums: list[int]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "GAME_ID": ["0029600442"] * len(event_nums),
            "EVENTNUM": event_nums,
            "EVENTMSGTYPE": [4] * len(event_nums),
            "PERIOD": [4] * len(event_nums),
            "PCTIMESTRING": ["3:33"] * len(event_nums),
        }
    )


def test_apply_pbp_row_overrides_moves_event_before_anchor():
    df = _game_df([376, 378, 379, 381, 382, 384, 377, 385])
    overrides = {
        "0029600442": [
            {
                "action": "move_before",
                "event_num": 377,
                "anchor_event_num": 378,
                "notes": "Move delayed Bullets rebound back before FT block",
            }
        ]
    }

    result = apply_pbp_row_overrides(df, overrides=overrides)

    assert result["EVENTNUM"].tolist() == [376, 377, 378, 379, 381, 382, 384, 385]


def test_apply_pbp_row_overrides_supports_multiple_actions_for_same_game():
    df = _game_df([408, 409, 410, 412, 411, 413, 414, 417, 415, 416, 418])
    overrides = {
        "0029600442": [
            {
                "action": "move_before",
                "event_num": 411,
                "anchor_event_num": 412,
                "notes": "Move missed layup ahead of its rebound",
            },
            {
                "action": "move_after",
                "event_num": 417,
                "anchor_event_num": 416,
                "notes": "Move team FT placeholder behind the missed free throw",
            },
        ]
    }

    result = apply_pbp_row_overrides(
        df.assign(GAME_ID="0029600396"),
        overrides={"0029600396": overrides["0029600442"]},
    )

    assert result["EVENTNUM"].tolist() == [408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418]


def test_apply_pbp_row_overrides_can_move_rebound_after_later_miss():
    df = pd.DataFrame(
        {
            "GAME_ID": ["0029600682"] * 8,
            "EVENTNUM": [354, 355, 356, 357, 361, 362, 364, 365],
            "EVENTMSGTYPE": [2, 4, 2, 4, 1, 4, 2, 13],
            "PERIOD": [3] * 8,
            "PCTIMESTRING": ["1:27", "1:24", "1:12", "1:07", "0:35", "0:16", "0:00", "0:00"],
        }
    )
    overrides = {
        "0029600682": [
            {
                "action": "move_after",
                "event_num": 362,
                "anchor_event_num": 364,
                "notes": "Move Oakley rebound behind the later Grant miss it belongs to",
            }
        ]
    }

    result = apply_pbp_row_overrides(df, overrides=overrides)

    assert result["EVENTNUM"].tolist() == [354, 355, 356, 357, 361, 364, 362, 365]


def test_apply_pbp_row_overrides_can_drop_stray_placeholder_row():
    df = _game_df([167, 168, 169, 170, 171]).assign(GAME_ID="0029600245")
    overrides = {
        "0029600245": [
            {
                "action": "drop",
                "event_num": 169,
                "anchor_event_num": None,
                "notes": "Drop dead-ball team rebound placeholder after missed FT",
            }
        ]
    }

    result = apply_pbp_row_overrides(df, overrides=overrides)

    assert result["EVENTNUM"].tolist() == [167, 168, 170, 171]


def test_apply_pbp_row_overrides_can_insert_synthetic_sub_before_anchor():
    df = pd.DataFrame(
        {
            "GAME_ID": ["0020400335"] * 4,
            "EVENTNUM": [145, 147, 149, 150],
            "EVENTMSGTYPE": [2, 6, 8, 8],
            "EVENTMSGACTIONTYPE": [1, 3, 0, 0],
            "PERIOD": [2] * 4,
            "PCTIMESTRING": ["8:00", "7:59", "7:59", "7:59"],
            "WCTIMESTRING": ["8:59 PM"] * 4,
            "HOMEDESCRIPTION": ["MISS Smith 26' 3PT Jump Shot", "Baxter L.B.FOUL (P2.T2)", "", ""],
            "NEUTRALDESCRIPTION": [""] * 4,
            "VISITORDESCRIPTION": ["", "", "SUB: Barry FOR Udrih", "SUB: Parker FOR Ginobili"],
            "SCORE": [""] * 4,
            "SCOREMARGIN": [""] * 4,
            "PLAYER1_ID": [2747, 2437, 2757, 1939],
            "PLAYER1_NAME": ["JR Smith", "Lonny Baxter", "Beno Udrih", "Manu Ginobili"],
            "PLAYER1_TEAM_ID": [1610612740, 1610612740, 1610612759, 1610612759],
            "PLAYER2_ID": [0, 0, 699, 2225],
            "PLAYER2_NAME": ["", "", "Brent Barry", "Tony Parker"],
            "PLAYER2_TEAM_ID": ["", "", 1610612759, 1610612759],
            "PLAYER3_ID": [0] * 4,
            "PLAYER3_NAME": [""] * 4,
            "PLAYER3_TEAM_ID": [""] * 4,
            "event_num": [145, 147, 149, 150],
            "period": [2] * 4,
            "clock_seconds_remaining": [480.0, 479.0, 479.0, 479.0],
            "description": [
                "MISS Smith 26' 3PT Jump Shot",
                "Baxter L.B.FOUL (P2.T2)",
                "SUB: Barry FOR Udrih",
                "SUB: Parker FOR Ginobili",
            ],
        }
    )
    overrides = {
        "0020400335": [
            {
                "action": "insert_sub_before",
                "event_num": 148,
                "anchor_event_num": 149,
                "period": "2",
                "pctimestring": "7:59",
                "player_out_id": "2747",
                "player_out_name": "JR Smith",
                "player_out_team_id": "1610612740",
                "player_in_id": "2454",
                "player_in_name": "Junior Harrington",
                "player_in_team_id": "1610612740",
                "description_side": "home",
                "notes": "Synthesize missing Q2 Harrington for Smith sub",
            }
        ]
    }

    result = apply_pbp_row_overrides(df, overrides=overrides)
    inserted = result[result["EVENTNUM"] == 148].iloc[0]

    assert result["EVENTNUM"].tolist() == [145, 147, 148, 149, 150]
    assert inserted["EVENTMSGTYPE"] == 8
    assert inserted["EVENTMSGACTIONTYPE"] == 0
    assert inserted["PLAYER1_ID"] == 2747
    assert inserted["PLAYER2_ID"] == 2454
    assert inserted["HOMEDESCRIPTION"] == "SUB: Junior Harrington FOR JR Smith"
    assert inserted["description"] == "SUB: Junior Harrington FOR JR Smith"
    assert inserted["clock_seconds_remaining"] == 479.0
    assert inserted[PBP_ROW_OVERRIDE_ACTION_COLUMN] == "insert_sub_before"


def test_historic_pbp_row_override_catalog_path_is_explicit():
    expected = Path(__file__).resolve().parents[1] / "catalogs" / "pbp_row_overrides.csv"

    assert DEFAULT_HISTORIC_PBP_ROW_OVERRIDES_PATH == expected


def test_historic_pbp_row_override_catalog_contains_synthetic_canary():
    overrides = load_historic_pbp_row_overrides()
    canaries = [
        row
        for row in overrides["0020400335"]
        if row["event_num"] == 148 and row["action"] == "insert_sub_before"
    ]

    assert len(canaries) == 1
    assert canaries[0]["anchor_event_num"] == 149
    assert canaries[0]["player_out_id"] == "2747"
    assert canaries[0]["player_in_id"] == "2454"


def test_historic_pbp_row_override_catalog_validates_strictly():
    validate_historic_pbp_row_override_catalog()
