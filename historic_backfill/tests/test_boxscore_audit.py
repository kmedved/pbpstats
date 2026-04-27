import json

import pandas as pd
import pbpstats

from historic_backfill.audits.core.boxscore import (
    build_game_boxscore_audit,
    build_pbp_boxscore_from_stat_rows,
    write_boxscore_audit_outputs,
)


def test_build_pbp_boxscore_from_stat_rows_aggregates_core_boxscore_stats():
    stat_rows = [
        {"player_id": 11, "team_id": 1, "stat_key": f"{pbpstats.ASSISTED_STRING}{pbpstats.ARC_3_STRING}", "stat_value": 1},
        {"player_id": 11, "team_id": 1, "stat_key": f"{pbpstats.UNASSISTED_STRING}{pbpstats.AT_RIM_STRING}", "stat_value": 1},
        {"player_id": 11, "team_id": 1, "stat_key": f"{pbpstats.MISSED_STRING}{pbpstats.UNKNOWN_SHOT_DISTANCE_STRING}", "stat_value": 1},
        {"player_id": 11, "team_id": 1, "stat_key": f"{pbpstats.CORNER_3_STRING}{pbpstats.BLOCKED_STRING}", "stat_value": 1},
        {"player_id": 11, "team_id": 1, "stat_key": pbpstats.FTS_MADE_STRING, "stat_value": 1},
        {"player_id": 11, "team_id": 1, "stat_key": pbpstats.TECHNICAL_FTS_MADE_STRING, "stat_value": 1},
        {"player_id": 11, "team_id": 1, "stat_key": pbpstats.FTS_MISSED_STRING, "stat_value": 1},
        {"player_id": 11, "team_id": 1, "stat_key": f"{pbpstats.AT_RIM_STRING}{pbpstats.OFFENSIVE_ABBREVIATION_PREFIX}{pbpstats.REBOUNDS_STRING}", "stat_value": 1},
        {"player_id": 11, "team_id": 1, "stat_key": f"{pbpstats.LONG_MID_RANGE_STRING}{pbpstats.DEFENSIVE_ABBREVIATION_PREFIX}{pbpstats.REBOUNDS_STRING}", "stat_value": 2},
        {"player_id": 11, "team_id": 1, "stat_key": pbpstats.BAD_PASS_TURNOVER_STRING, "stat_value": 1},
        {"player_id": 11, "team_id": 1, "stat_key": pbpstats.PERSONAL_FOUL_TYPE_STRING, "stat_value": 1},
        {"player_id": 22, "team_id": 1, "stat_key": f"{pbpstats.ARC_3_STRING}{pbpstats.ASSISTS_STRING}", "stat_value": 1},
        {"player_id": 33, "team_id": 2, "stat_key": f"{pbpstats.BLOCKED_STRING}{pbpstats.CORNER_3_STRING}", "stat_value": 1},
        {"player_id": 44, "team_id": 2, "stat_key": pbpstats.LOST_BALL_STEAL_STRING, "stat_value": 1},
    ]

    audit_df = build_pbp_boxscore_from_stat_rows(stat_rows)
    player_11 = audit_df.set_index(["player_id", "team_id"]).loc[(11, 1)]
    player_22 = audit_df.set_index(["player_id", "team_id"]).loc[(22, 1)]
    player_33 = audit_df.set_index(["player_id", "team_id"]).loc[(33, 2)]
    player_44 = audit_df.set_index(["player_id", "team_id"]).loc[(44, 2)]

    assert player_11["FGM"] == 2
    assert player_11["FGA"] == 4
    assert player_11["3PM"] == 1
    assert player_11["3PA"] == 2
    assert player_11["FTM"] == 2
    assert player_11["FTA"] == 3
    assert player_11["PTS"] == 7
    assert player_11["OREB"] == 1
    assert player_11["DRB"] == 2
    assert player_11["REB"] == 3
    assert player_11["TOV"] == 1
    assert player_11["PF"] == 1
    assert player_22["AST"] == 1
    assert player_33["BLK"] == 1
    assert player_44["STL"] == 1


def test_build_pbp_boxscore_from_stat_rows_counts_extended_foul_types():
    stat_rows = [
        {"player_id": 55, "team_id": 1, "stat_key": pbpstats.DOUBLE_FOUL_TYPE_STRING, "stat_value": 1},
    ]

    audit_df = build_pbp_boxscore_from_stat_rows(stat_rows)
    player_55 = audit_df.set_index(["player_id", "team_id"]).loc[(55, 1)]

    assert player_55["PF"] == 1


def test_build_game_boxscore_audit_flags_team_and_player_mismatches():
    pbp_box_df = pd.DataFrame(
        [
            {"player_id": 11, "team_id": 1, "PTS": 10, "AST": 2, "STL": 0, "BLK": 0, "TOV": 1, "PF": 2, "FGA": 8, "FGM": 4, "3PA": 2, "3PM": 1, "FTA": 2, "FTM": 1, "OREB": 1, "DRB": 4, "REB": 5},
            {"player_id": 22, "team_id": 1, "PTS": 6, "AST": 3, "STL": 1, "BLK": 0, "TOV": 0, "PF": 1, "FGA": 5, "FGM": 3, "3PA": 1, "3PM": 0, "FTA": 0, "FTM": 0, "OREB": 0, "DRB": 2, "REB": 2},
        ]
    )
    official_box_df = pd.DataFrame(
        [
            {"PLAYER_ID": 11, "TEAM_ID": 1, "PLAYER_NAME": "Player One", "TEAM_ABBREVIATION": "AAA", "PTS": 10, "AST": 2, "STL": 0, "BLK": 0, "TO": 1, "PF": 2, "FGA": 8, "FGM": 4, "FG3A": 2, "FG3M": 1, "FTA": 2, "FTM": 1, "OREB": 1, "DREB": 3, "REB": 4},
            {"PLAYER_ID": 22, "TEAM_ID": 1, "PLAYER_NAME": "Player Two", "TEAM_ABBREVIATION": "AAA", "PTS": 6, "AST": 3, "STL": 1, "BLK": 0, "TO": 0, "PF": 1, "FGA": 5, "FGM": 3, "FG3A": 1, "FG3M": 0, "FTA": 0, "FTM": 0, "OREB": 0, "DREB": 2, "REB": 2},
        ]
    )

    team_audit, player_mismatches, summary = build_game_boxscore_audit(
        "0029600001",
        pbp_box_df,
        official_box_df,
        player_name_map={11: "Player One", 22: "Player Two"},
    )

    assert summary["team_rows"] == 1
    assert summary["team_rows_with_mismatch"] == 1
    assert summary["player_rows_with_mismatch"] == 1
    assert team_audit.iloc[0]["has_mismatch"]
    assert team_audit.iloc[0]["max_abs_diff"] == 1
    assert len(player_mismatches) == 1
    assert player_mismatches.iloc[0]["player_id"] == 11
    assert player_mismatches.iloc[0]["player_name"] == "Player One"
    assert player_mismatches.iloc[0]["DIFF_DRB"] == 1
    assert player_mismatches.iloc[0]["DIFF_REB"] == 1


def test_build_game_boxscore_audit_accepts_pbp_override_for_confirmed_source_anomaly():
    pbp_box_df = pd.DataFrame(
        [
            {"player_id": 11, "team_id": 1, "PTS": 10, "AST": 2, "STL": 0, "BLK": 0, "TOV": 1, "PF": 2, "FGA": 8, "FGM": 4, "3PA": 2, "3PM": 1, "FTA": 2, "FTM": 1, "OREB": 1, "DRB": 4, "REB": 5},
            {"player_id": 22, "team_id": 1, "PTS": 6, "AST": 3, "STL": 1, "BLK": 0, "TOV": 0, "PF": 1, "FGA": 5, "FGM": 3, "3PA": 1, "3PM": 0, "FTA": 0, "FTM": 0, "OREB": 0, "DRB": 2, "REB": 2},
        ]
    )
    official_box_df = pd.DataFrame(
        [
            {"PLAYER_ID": 11, "TEAM_ID": 1, "PLAYER_NAME": "Player One", "TEAM_ABBREVIATION": "AAA", "PTS": 10, "AST": 2, "STL": 0, "BLK": 0, "TO": 1, "PF": 2, "FGA": 8, "FGM": 4, "FG3A": 2, "FG3M": 1, "FTA": 2, "FTM": 1, "OREB": 0, "DREB": 5, "REB": 5},
            {"PLAYER_ID": 22, "TEAM_ID": 1, "PLAYER_NAME": "Player Two", "TEAM_ABBREVIATION": "AAA", "PTS": 6, "AST": 3, "STL": 1, "BLK": 0, "TO": 0, "PF": 1, "FGA": 5, "FGM": 3, "FG3A": 1, "FG3M": 0, "FTA": 0, "FTM": 0, "OREB": 0, "DREB": 2, "REB": 2},
        ]
    )
    audit_overrides = pd.DataFrame(
        [
            {"game_id": "0029600001", "team_id": 1, "player_id": 11, "stat": "OREB", "action": "accept_pbp", "notes": "confirmed source split anomaly"},
            {"game_id": "0029600001", "team_id": 1, "player_id": 11, "stat": "DRB", "action": "accept_pbp", "notes": "confirmed source split anomaly"},
        ]
    )

    team_audit, player_mismatches, summary = build_game_boxscore_audit(
        "0029600001",
        pbp_box_df,
        official_box_df,
        player_name_map={11: "Player One", 22: "Player Two"},
        audit_overrides=audit_overrides,
    )

    assert summary["team_rows_with_mismatch"] == 0
    assert summary["player_rows_with_mismatch"] == 0
    assert team_audit.iloc[0]["has_mismatch"] == 0
    assert player_mismatches.empty


def test_write_boxscore_audit_outputs_writes_files_and_summary(tmp_path):
    pbp_box_df = pd.DataFrame(
        [
            {"player_id": 11, "team_id": 1, "PTS": 10, "AST": 2, "STL": 0, "BLK": 0, "TOV": 1, "PF": 2, "FGA": 8, "FGM": 4, "3PA": 2, "3PM": 1, "FTA": 2, "FTM": 1, "OREB": 1, "DRB": 4, "REB": 5},
            {"player_id": 22, "team_id": 1, "PTS": 6, "AST": 3, "STL": 1, "BLK": 0, "TOV": 0, "PF": 1, "FGA": 5, "FGM": 3, "3PA": 1, "3PM": 0, "FTA": 0, "FTM": 0, "OREB": 0, "DRB": 2, "REB": 2},
        ]
    )
    official_box_df = pd.DataFrame(
        [
            {"PLAYER_ID": 11, "TEAM_ID": 1, "PLAYER_NAME": "Player One", "TEAM_ABBREVIATION": "AAA", "PTS": 10, "AST": 2, "STL": 0, "BLK": 0, "TO": 1, "PF": 2, "FGA": 8, "FGM": 4, "FG3A": 2, "FG3M": 1, "FTA": 2, "FTM": 1, "OREB": 1, "DREB": 3, "REB": 4},
            {"PLAYER_ID": 22, "TEAM_ID": 1, "PLAYER_NAME": "Player Two", "TEAM_ABBREVIATION": "AAA", "PTS": 6, "AST": 3, "STL": 1, "BLK": 0, "TO": 0, "PF": 1, "FGA": 5, "FGM": 3, "FG3A": 1, "FG3M": 0, "FTA": 0, "FTM": 0, "OREB": 0, "DREB": 2, "REB": 2},
        ]
    )

    team_audit, player_mismatches, _ = build_game_boxscore_audit(
        "0029600001",
        pbp_box_df,
        official_box_df,
        player_name_map={11: "Player One", 22: "Player Two"},
    )
    audit_errors = pd.DataFrame([{"game_id": "0029600002", "error": "missing official boxscore"}])

    summary = write_boxscore_audit_outputs(
        team_audit=team_audit,
        player_mismatches=player_mismatches,
        audit_errors=audit_errors,
        season=1997,
        output_dir=tmp_path,
        games_requested=2,
    )

    assert summary["games_requested"] == 2
    assert summary["games_audited"] == 1
    assert summary["audit_failures"] == 1
    assert summary["games_with_team_mismatch"] == 1
    assert summary["player_rows_with_mismatch"] == 1
    assert summary["team_mismatch_counts_by_stat"]["DRB"] == 1

    summary_path = tmp_path / "boxscore_audit_summary_1997.json"
    assert (tmp_path / "boxscore_team_audit_1997.csv").exists()
    assert (tmp_path / "boxscore_player_mismatches_1997.csv").exists()
    assert (tmp_path / "boxscore_audit_errors_1997.csv").exists()
    assert summary_path.exists()
    assert json.loads(summary_path.read_text(encoding="utf-8")) == summary

