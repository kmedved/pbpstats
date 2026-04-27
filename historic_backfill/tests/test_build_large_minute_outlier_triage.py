from __future__ import annotations

import json

import pandas as pd

from historic_backfill.audits.cross_source.build_large_minute_outlier_triage import (
    _build_candidate_rows_for_game,
    _build_later_sub_in_map,
    _diff_bucket_seconds,
    _write_progress_checkpoint,
)


def test_build_later_sub_in_map_excludes_period_start_subs():
    stints_df = pd.DataFrame(
        [
            {
                "team_id": 1,
                "player_id": 10,
                "start_period": 2,
                "start_clock": "12:00",
                "start_reason": "substitution_in",
            },
            {
                "team_id": 1,
                "player_id": 11,
                "start_period": 2,
                "start_clock": "10:15",
                "start_reason": "substitution_in",
            },
            {
                "team_id": 1,
                "player_id": 12,
                "start_period": 2,
                "start_clock": "9:00",
                "start_reason": "lineup_change_in",
            },
        ]
    )

    later_map = _build_later_sub_in_map(stints_df)

    assert later_map == {(2, 1): [11]}


def test_diff_bucket_seconds_distinguishes_period_sized_errors():
    assert _diff_bucket_seconds([-300.0, 300.0]) == "300"
    assert _diff_bucket_seconds([-720.0, 720.0]) == "720"
    assert _diff_bucket_seconds([-147.0, 147.0]) == "other"


def test_build_candidate_rows_marks_simple_one_for_one_case():
    outlier_game_df = pd.DataFrame(
        [
            {"game_id": "0029600001", "team_id": 1, "player_id": 5, "Minutes_diff": -5.0},
            {"game_id": "0029600001", "team_id": 1, "player_id": 9, "Minutes_diff": 5.0},
        ]
    )
    starter_audit_df = pd.DataFrame(
        [
            {
                "game_id": "0029600001",
                "period": 5,
                "team_id": 1,
                "starter_sets_match": False,
                "current_starter_ids": [1, 2, 3, 4, 9],
                "current_starter_names": ["A", "B", "C", "D", "Ninth"],
                "tpdev_starter_ids": [1, 2, 3, 4, 5],
                "tpdev_starter_names": ["A", "B", "C", "D", "Fifth"],
                "missing_from_current_ids": [5],
                "missing_from_current_names": ["Fifth"],
                "extra_in_current_ids": [9],
                "extra_in_current_names": ["Ninth"],
            }
        ]
    )
    recon_df = pd.DataFrame(
        [
            {
                "team_id": 1,
                "player_id": 5,
                "player_name": "Fifth",
                "consensus_diff_seconds": -300.0,
                "official_matches_tpdev": True,
                "official_matches_bbr": True,
            },
            {
                "team_id": 1,
                "player_id": 9,
                "player_name": "Ninth",
                "consensus_diff_seconds": 300.0,
                "official_matches_tpdev": True,
                "official_matches_bbr": True,
            },
        ]
    )
    stints_df = pd.DataFrame(
        [
            {
                "team_id": 1,
                "player_id": 9,
                "start_period": 5,
                "start_clock": "2:30",
                "start_reason": "substitution_in",
            }
        ]
    )

    candidate_df, residual_df = _build_candidate_rows_for_game(
        game_id="0029600001",
        outlier_game_df=outlier_game_df,
        starter_audit_df=starter_audit_df,
        recon_df=recon_df,
        stints_df=stints_df,
    )

    assert len(candidate_df) == 1
    row = candidate_df.iloc[0]
    assert row["diff_bucket_seconds"] == "300"
    assert bool(row["official_matches_tpdev"]) is True
    assert bool(row["official_matches_bbr"]) is True
    assert bool(row["is_simple_later_sub_in_case"]) is True
    assert residual_df.empty


def test_build_candidate_rows_leaves_non_simple_case_false_and_residual_free():
    outlier_game_df = pd.DataFrame(
        [
            {"game_id": "0029600002", "team_id": 1, "player_id": 5, "Minutes_diff": -12.0},
            {"game_id": "0029600002", "team_id": 1, "player_id": 9, "Minutes_diff": 12.0},
        ]
    )
    starter_audit_df = pd.DataFrame(
        [
            {
                "game_id": "0029600002",
                "period": 2,
                "team_id": 1,
                "starter_sets_match": False,
                "current_starter_ids": [1, 2, 3, 4, 9],
                "current_starter_names": ["A", "B", "C", "D", "Ninth"],
                "tpdev_starter_ids": [1, 2, 3, 4, 5],
                "tpdev_starter_names": ["A", "B", "C", "D", "Fifth"],
                "missing_from_current_ids": [5],
                "missing_from_current_names": ["Fifth"],
                "extra_in_current_ids": [9],
                "extra_in_current_names": ["Ninth"],
            }
        ]
    )
    recon_df = pd.DataFrame(
        [
            {
                "team_id": 1,
                "player_id": 5,
                "player_name": "Fifth",
                "consensus_diff_seconds": -720.0,
                "official_matches_tpdev": False,
                "official_matches_bbr": True,
            },
            {
                "team_id": 1,
                "player_id": 9,
                "player_name": "Ninth",
                "consensus_diff_seconds": 147.0,
                "official_matches_tpdev": False,
                "official_matches_bbr": True,
            },
        ]
    )
    stints_df = pd.DataFrame(
        [
            {
                "team_id": 1,
                "player_id": 9,
                "start_period": 2,
                "start_clock": "12:00",
                "start_reason": "substitution_in",
            }
        ]
    )

    candidate_df, residual_df = _build_candidate_rows_for_game(
        game_id="0029600002",
        outlier_game_df=outlier_game_df,
        starter_audit_df=starter_audit_df,
        recon_df=recon_df,
        stints_df=stints_df,
    )

    assert len(candidate_df) == 1
    row = candidate_df.iloc[0]
    assert row["diff_bucket_seconds"] == "other"
    assert bool(row["official_matches_tpdev"]) is False
    assert bool(row["official_matches_bbr"]) is True
    assert bool(row["is_simple_later_sub_in_case"]) is False
    assert residual_df.empty


def test_write_progress_checkpoint_writes_json_and_log(tmp_path):
    _write_progress_checkpoint(
        tmp_path,
        processed_games=12,
        total_games=90,
        current_season=1999,
        current_game_id="0029800001",
        candidate_rows=7,
        residual_rows=3,
    )

    progress_payload = json.loads((tmp_path / "progress.json").read_text(encoding="utf-8"))
    assert progress_payload["processed_games"] == 12
    assert progress_payload["total_games"] == 90
    assert progress_payload["current_season"] == 1999
    assert progress_payload["current_game_id"] == "0029800001"
    assert progress_payload["candidate_rows_so_far"] == 7
    assert progress_payload["residual_rows_so_far"] == 3
    assert progress_payload["percent_complete"] == round((12 / 90) * 100.0, 2)

    log_text = (tmp_path / "progress.log").read_text(encoding="utf-8")
    assert "12/90 games" in log_text
    assert "season=1999" in log_text
    assert "game=0029800001" in log_text
