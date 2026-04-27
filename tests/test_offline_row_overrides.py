import pandas as pd

import pbpstats.offline.row_overrides as row_overrides
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


def test_row_overrides_module_has_no_import_time_catalog_cache():
    assert not hasattr(row_overrides, "_PBP_ROW_OVERRIDES")
    assert not hasattr(row_overrides, "DEFAULT_PBP_ROW_OVERRIDES_PATH")


def test_apply_pbp_row_overrides_moves_and_drops_rows():
    df = _game_df([1, 3, 4, 2, 5])
    result = apply_pbp_row_overrides(
        df,
        {
            "0029600442": [
                {"action": "move_before", "event_num": 2, "anchor_event_num": 3},
                {"action": "drop", "event_num": 4, "anchor_event_num": None},
            ]
        },
    )

    assert result["EVENTNUM"].tolist() == [1, 2, 3, 5]


def test_apply_pbp_row_overrides_marks_synthetic_sub_rows():
    df = pd.DataFrame(
        {
            "GAME_ID": ["0020400335"] * 2,
            "EVENTNUM": [147, 149],
            "EVENTMSGTYPE": [6, 8],
            "EVENTMSGACTIONTYPE": [3, 0],
            "PERIOD": [2, 2],
            "PCTIMESTRING": ["7:59", "7:59"],
            "WCTIMESTRING": ["8:59 PM", "8:59 PM"],
            "HOMEDESCRIPTION": ["Baxter L.B.FOUL (P2.T2)", ""],
            "NEUTRALDESCRIPTION": ["", ""],
            "VISITORDESCRIPTION": ["", "SUB: Barry FOR Udrih"],
            "SCORE": ["", ""],
            "SCOREMARGIN": ["", ""],
            "PLAYER1_ID": [2437, 2757],
            "PLAYER1_NAME": ["Lonny Baxter", "Beno Udrih"],
            "PLAYER1_TEAM_ID": [1610612740, 1610612759],
            "PLAYER2_ID": [0, 699],
            "PLAYER2_NAME": ["", "Brent Barry"],
            "PLAYER2_TEAM_ID": ["", 1610612759],
            "PLAYER3_ID": [0, 0],
            "PLAYER3_NAME": ["", ""],
            "PLAYER3_TEAM_ID": ["", ""],
            "event_num": [147, 149],
            "period": [2, 2],
            "clock_seconds_remaining": [479.0, 479.0],
            "description": ["Baxter L.B.FOUL (P2.T2)", "SUB: Barry FOR Udrih"],
        }
    )
    result = apply_pbp_row_overrides(
        df,
        {
            "0020400335": [
                {
                    "action": "insert_sub_before",
                    "event_num": 148,
                    "anchor_event_num": 149,
                    "period": "2",
                    "pctimestring": "7:59",
                    "description_side": "home",
                    "player_out_id": "2747",
                    "player_out_name": "JR Smith",
                    "player_out_team_id": "1610612740",
                    "player_in_id": "2454",
                    "player_in_name": "Junior Harrington",
                    "player_in_team_id": "1610612740",
                    "notes": "Synthesize missing Q2 Harrington for Smith sub",
                }
            ]
        },
    )

    inserted = result[result["EVENTNUM"] == 148].iloc[0]
    assert inserted[PBP_ROW_OVERRIDE_ACTION_COLUMN] == "insert_sub_before"
    assert inserted["HOMEDESCRIPTION"] == "SUB: Junior Harrington FOR JR Smith"
