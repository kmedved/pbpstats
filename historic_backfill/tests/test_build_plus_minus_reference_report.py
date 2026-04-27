from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_build_plus_minus_reference_report_classifies_rows_and_emits_sample(tmp_path: Path) -> None:
    residual_dir = tmp_path / "residual"
    residual_dir.mkdir()
    residual_dir_2 = tmp_path / "residual_2"
    residual_dir_2.mkdir()
    output_dir = tmp_path / "report"
    lane_map_path = tmp_path / "lane_map.csv"
    overlay_csv = tmp_path / "overlay.csv"

    _write_csv(
        lane_map_path,
        ["game_id", "lane", "recommended_next_action", "notes"],
        [
            {
                "game_id": "0029700003",
                "lane": "special_holdout_material_minute",
                "recommended_next_action": "direct_game_validation",
                "notes": "broad material-minute holdout",
            }
        ],
    )
    _write_csv(
        overlay_csv,
        [
            "policy_decision_id",
            "game_id",
            "release_gate_status",
            "release_reason_code",
            "execution_lane",
            "blocks_release",
            "research_open",
            "policy_source",
            "expected_primary_quality_status",
            "evidence_artifact",
            "reviewed_at",
            "notes",
        ],
        [
            {
                "policy_decision_id": "reviewed_release_policy_20260322_v1",
                "game_id": "0029799999",
                "release_gate_status": "documented_hold",
                "release_reason_code": "mixed_source_boundary_tail",
                "execution_lane": "documented_hold",
                "blocks_release": False,
                "research_open": True,
                "policy_source": "reviewed_override",
                "expected_primary_quality_status": "open",
                "evidence_artifact": "artifact",
                "reviewed_at": "2026-03-22",
                "notes": "",
            }
        ],
    )

    _write_csv(
        residual_dir / "plus_minus_reference_delta_register.csv",
        [
            "grain",
            "residual_source",
            "game_id",
            "season",
            "team_id",
            "player_id",
            "player_name",
            "period",
            "event_num",
            "minutes_abs_diff",
            "plus_minus_diff",
            "status_detail",
            "computed_residual_class",
            "blocking_reason",
            "is_blocking",
            "has_minutes_mismatch",
            "has_plus_minus_reference_delta",
            "is_minutes_outlier",
            "manual_annotation_id",
            "manual_residual_class",
            "manual_status",
            "manual_notes",
            "effective_residual_class",
            "effective_is_blocking",
        ],
        [
            {
                "grain": "player_game",
                "residual_source": "plus_minus_reference_delta",
                "game_id": "29700001",
                "season": 1998,
                "team_id": 1,
                "player_id": 10,
                "player_name": "Boundary",
                "period": 0,
                "event_num": 0,
                "minutes_abs_diff": 0.0,
                "plus_minus_diff": 2.0,
                "status_detail": "",
                "computed_residual_class": "candidate_boundary_difference",
                "blocking_reason": "plus_minus_reference_only",
                "is_blocking": False,
                "has_minutes_mismatch": False,
                "has_plus_minus_reference_delta": True,
                "is_minutes_outlier": False,
                "manual_annotation_id": "",
                "manual_residual_class": "",
                "manual_status": "",
                "manual_notes": "",
                "effective_residual_class": "candidate_boundary_difference",
                "effective_is_blocking": False,
            },
            {
                "grain": "player_game",
                "residual_source": "plus_minus_reference_delta",
                "game_id": "29700002",
                "season": 1998,
                "team_id": 1,
                "player_id": 11,
                "player_name": "SourceLimited",
                "period": 0,
                "event_num": 0,
                "minutes_abs_diff": 0.0,
                "plus_minus_diff": -1.0,
                "status_detail": "",
                "computed_residual_class": "unknown",
                "blocking_reason": "",
                "is_blocking": False,
                "has_minutes_mismatch": False,
                "has_plus_minus_reference_delta": True,
                "is_minutes_outlier": False,
                "manual_annotation_id": "ann1",
                "manual_residual_class": "source_limited_upstream_error",
                "manual_status": "accepted",
                "manual_notes": "",
                "effective_residual_class": "source_limited_upstream_error",
                "effective_is_blocking": False,
            },
            {
                "grain": "player_game",
                "residual_source": "plus_minus_reference_delta",
                "game_id": "29700003",
                "season": 1999,
                "team_id": 1,
                "player_id": 12,
                "player_name": "OpenGame",
                "period": 0,
                "event_num": 0,
                "minutes_abs_diff": 0.25,
                "plus_minus_diff": 3.0,
                "status_detail": "",
                "computed_residual_class": "fixable_lineup_defect",
                "blocking_reason": "",
                "is_blocking": False,
                "has_minutes_mismatch": True,
                "has_plus_minus_reference_delta": True,
                "is_minutes_outlier": False,
                "manual_annotation_id": "",
                "manual_residual_class": "",
                "manual_status": "",
                "manual_notes": "",
                "effective_residual_class": "fixable_lineup_defect",
                "effective_is_blocking": False,
            },
        ],
    )
    _write_csv(
        residual_dir / "game_quality.csv",
        [
            "game_id",
            "primary_quality_status",
            "has_material_minute_issue",
            "has_severe_minute_issue",
            "has_event_on_court_issue",
        ],
        [
            {
                "game_id": "0029700001",
                "primary_quality_status": "boundary_difference",
                "has_material_minute_issue": False,
                "has_severe_minute_issue": False,
                "has_event_on_court_issue": False,
            },
            {
                "game_id": "0029700002",
                "primary_quality_status": "source_limited",
                "has_material_minute_issue": False,
                "has_severe_minute_issue": False,
                "has_event_on_court_issue": True,
            },
            {
                "game_id": "0029700003",
                "primary_quality_status": "open",
                "has_material_minute_issue": True,
                "has_severe_minute_issue": False,
                "has_event_on_court_issue": True,
            },
        ],
    )
    _write_csv(
        residual_dir_2 / "plus_minus_reference_delta_register.csv",
        [
            "grain",
            "residual_source",
            "game_id",
            "season",
            "team_id",
            "player_id",
            "player_name",
            "period",
            "event_num",
            "minutes_abs_diff",
            "plus_minus_diff",
            "status_detail",
            "computed_residual_class",
            "blocking_reason",
            "is_blocking",
            "has_minutes_mismatch",
            "has_plus_minus_reference_delta",
            "is_minutes_outlier",
            "manual_annotation_id",
            "manual_residual_class",
            "manual_status",
            "manual_notes",
            "effective_residual_class",
            "effective_is_blocking",
        ],
        [
            {
                "grain": "player_game",
                "residual_source": "plus_minus_reference_delta",
                "game_id": "29700004",
                "season": 2000,
                "team_id": 1,
                "player_id": 13,
                "player_name": "AnotherBoundary",
                "period": 0,
                "event_num": 0,
                "minutes_abs_diff": 0.0,
                "plus_minus_diff": -4.0,
                "status_detail": "",
                "computed_residual_class": "candidate_boundary_difference",
                "blocking_reason": "plus_minus_reference_only",
                "is_blocking": False,
                "has_minutes_mismatch": False,
                "has_plus_minus_reference_delta": True,
                "is_minutes_outlier": False,
                "manual_annotation_id": "",
                "manual_residual_class": "",
                "manual_status": "",
                "manual_notes": "",
                "effective_residual_class": "candidate_boundary_difference",
                "effective_is_blocking": False,
            },
            {
                "grain": "player_game",
                "residual_source": "plus_minus_reference_delta",
                "game_id": "29700005",
                "season": 2000,
                "team_id": 1,
                "player_id": 14,
                "player_name": "SourceLimitedByGame",
                "period": 0,
                "event_num": 0,
                "minutes_abs_diff": 0.0,
                "plus_minus_diff": 2.0,
                "status_detail": "",
                "computed_residual_class": "candidate_boundary_difference",
                "blocking_reason": "plus_minus_reference_only",
                "is_blocking": False,
                "has_minutes_mismatch": False,
                "has_plus_minus_reference_delta": True,
                "is_minutes_outlier": False,
                "manual_annotation_id": "",
                "manual_residual_class": "",
                "manual_status": "",
                "manual_notes": "",
                "effective_residual_class": "candidate_boundary_difference",
                "effective_is_blocking": False,
            },
            {
                "grain": "player_game",
                "residual_source": "plus_minus_reference_delta",
                "game_id": "29700002",
                "season": 1998,
                "team_id": 1,
                "player_id": 11,
                "player_name": "SourceLimitedDuplicate",
                "period": 0,
                "event_num": 0,
                "minutes_abs_diff": 0.0,
                "plus_minus_diff": -1.0,
                "status_detail": "",
                "computed_residual_class": "unknown",
                "blocking_reason": "",
                "is_blocking": False,
                "has_minutes_mismatch": False,
                "has_plus_minus_reference_delta": True,
                "is_minutes_outlier": False,
                "manual_annotation_id": "ann1dup",
                "manual_residual_class": "source_limited_upstream_error",
                "manual_status": "accepted",
                "manual_notes": "",
                "effective_residual_class": "source_limited_upstream_error",
                "effective_is_blocking": False,
            },
        ],
    )
    _write_csv(
        residual_dir_2 / "game_quality.csv",
        [
            "game_id",
            "primary_quality_status",
            "has_material_minute_issue",
            "has_severe_minute_issue",
            "has_event_on_court_issue",
        ],
        [
            {
                "game_id": "0029700004",
                "primary_quality_status": "boundary_difference",
                "has_material_minute_issue": False,
                "has_severe_minute_issue": False,
                "has_event_on_court_issue": False,
            },
            {
                "game_id": "0029700005",
                "primary_quality_status": "source_limited",
                "has_material_minute_issue": False,
                "has_severe_minute_issue": False,
                "has_event_on_court_issue": True,
            },
            {
                "game_id": "0029700002",
                "primary_quality_status": "source_limited",
                "has_material_minute_issue": False,
                "has_severe_minute_issue": False,
                "has_event_on_court_issue": True,
            },
        ],
    )

    subprocess.run(
        [
            sys.executable,
            str(ROOT / "audits" / "core" / "build_plus_minus_reference_report.py"),
            "--residual-dir",
            str(residual_dir),
            "--residual-dir",
            str(residual_dir_2),
            "--output-dir",
            str(output_dir),
            "--lane-map-csv",
            str(lane_map_path),
            "--reviewed-policy-overlay-csv",
            str(overlay_csv),
        ],
        check=True,
        cwd=ROOT.parent,
    )

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["total_pm_reference_delta_rows"] == 5
    assert summary["reviewed_policy_overlay_version"] == "reviewed_release_policy_20260322_v1"
    assert summary["frontier_inventory_snapshot_id"] == "lane_map"
    assert summary["class_counts"] == {
        "candidate_boundary_difference": 2,
        "lineup_related": 1,
        "source_limited_upstream_error": 2,
    }
    assert summary["report_class_counts"] == [
        {"report_pm_characterization": "open_lineup_blocker", "row_count": 1, "game_count": 1},
        {"report_pm_characterization": "reference_only_boundary", "row_count": 2, "game_count": 2},
        {"report_pm_characterization": "source_limited_upstream", "row_count": 2, "game_count": 2},
    ]
    assert summary["release_class_counts"] == [
        {"release_pm_class": "open_actionable_lineup_blocker", "row_count": 1, "game_count": 1},
        {"release_pm_class": "reference_only_boundary", "row_count": 2, "game_count": 2},
        {"release_pm_class": "source_limited_upstream", "row_count": 2, "game_count": 2},
    ]
    assert summary["release_blocker_game_count"] == 1
    assert summary["release_blocking_game_ids"] == ["0029700003"]
    assert summary["reviewed_frontier_queue_game_count"] == 2

    sample_rows = list(csv.DictReader((output_dir / "candidate_boundary_difference_sample.csv").open()))
    assert len(sample_rows) == 2
    assert sample_rows[0]["player_name"] == "Boundary"
    assert {row["player_name"] for row in sample_rows} == {"Boundary", "AnotherBoundary"}

    alias_sample_rows = list(csv.DictReader((output_dir / "pm_reference_only_sample.csv").open()))
    assert {row["player_name"] for row in alias_sample_rows} == {"Boundary", "AnotherBoundary"}

    source_limited_games = list(csv.DictReader((output_dir / "pm_source_limited_games.csv").open()))
    assert {row["game_id"] for row in source_limited_games} == {"0029700002", "0029700005"}

    open_queue_rows = list(csv.DictReader((output_dir / "pm_open_game_queue.csv").open()))
    assert len(open_queue_rows) == 1
    assert open_queue_rows[0]["game_id"] == "0029700003"
    assert open_queue_rows[0]["lane"] == "special_holdout_material_minute"

    release_blocker_rows = list(csv.DictReader((output_dir / "pm_release_blocker_queue.csv").open()))
    assert len(release_blocker_rows) == 1
    assert release_blocker_rows[0]["game_id"] == "0029700003"

    reviewed_frontier_rows = list(csv.DictReader((output_dir / "pm_reviewed_frontier_queue.csv").open()))
    assert {row["game_id"] for row in reviewed_frontier_rows} == {"0029700001", "0029700004"}

    lane_summary_rows = list(csv.DictReader((output_dir / "pm_lane_summary.csv").open()))
    assert lane_summary_rows == [
        {
            "lane": "special_holdout_material_minute",
            "recommended_next_action": "direct_game_validation",
            "row_count": "1",
            "game_count": "1",
        }
    ]
    release_lane_summary_rows = list(csv.DictReader((output_dir / "pm_release_lane_summary.csv").open()))
    assert release_lane_summary_rows == lane_summary_rows
    reviewed_lane_summary_rows = list(csv.DictReader((output_dir / "pm_reviewed_frontier_lane_summary.csv").open()))
    assert reviewed_lane_summary_rows == [
        {
            "lane": "",
            "recommended_next_action": "",
            "row_count": "2",
            "game_count": "2",
        }
    ]

    characterization_rows = list(csv.DictReader((output_dir / "pm_reference_characterization.csv").open()))
    by_game = {row["game_id"]: row for row in characterization_rows}
    assert {row["pm_residual_class"] for row in characterization_rows} == {
        "candidate_boundary_difference",
        "lineup_related",
        "source_limited_upstream_error",
    }
    assert {row["state_context"] for row in characterization_rows} == {"live_state"}
    assert by_game["0029700003"]["report_pm_characterization"] == "open_lineup_blocker"
    assert by_game["0029700003"]["release_pm_class"] == "open_actionable_lineup_blocker"
    assert by_game["0029700003"]["release_gate_status"] == "open_actionable"
    assert {row["game_id"] for row in characterization_rows if row["release_pm_class"] == "open_actionable_lineup_blocker"} == {"0029700003"}
    assert {
        row["game_id"] for row in characterization_rows if row["release_pm_class"] == "open_actionable_lineup_blocker"
    }.issubset({row["game_id"] for row in characterization_rows if row["blocks_release"] == "True"})


def test_build_plus_minus_reference_report_applies_release_overlay_to_raw_open_pm_rows(tmp_path: Path) -> None:
    residual_dir = tmp_path / "residual"
    residual_dir.mkdir()
    output_dir = tmp_path / "report"

    _write_csv(
        residual_dir / "plus_minus_reference_delta_register.csv",
        [
            "grain",
            "residual_source",
            "game_id",
            "season",
            "team_id",
            "player_id",
            "player_name",
            "period",
            "event_num",
            "minutes_abs_diff",
            "plus_minus_diff",
            "status_detail",
            "computed_residual_class",
            "blocking_reason",
            "is_blocking",
            "has_minutes_mismatch",
            "has_plus_minus_reference_delta",
            "is_minutes_outlier",
            "manual_annotation_id",
            "manual_residual_class",
            "manual_status",
            "manual_notes",
            "effective_residual_class",
            "effective_is_blocking",
        ],
        [
            {
                "grain": "player_game",
                "residual_source": "plus_minus_reference_delta",
                "game_id": "29700007",
                "season": 2000,
                "team_id": 1,
                "player_id": 15,
                "player_name": "Contradiction",
                "period": 0,
                "event_num": 0,
                "minutes_abs_diff": 0.0,
                "plus_minus_diff": 2.0,
                "status_detail": "",
                "computed_residual_class": "fixable_lineup_defect",
                "blocking_reason": "",
                "is_blocking": False,
                "has_minutes_mismatch": False,
                "has_plus_minus_reference_delta": True,
                "is_minutes_outlier": False,
                "manual_annotation_id": "",
                "manual_residual_class": "",
                "manual_status": "",
                "manual_notes": "",
                "effective_residual_class": "fixable_lineup_defect",
                "effective_is_blocking": False,
            },
            {
                "grain": "player_game",
                "residual_source": "plus_minus_reference_delta",
                "game_id": "29700008",
                "season": 2000,
                "team_id": 1,
                "player_id": 16,
                "player_name": "Hold",
                "period": 0,
                "event_num": 0,
                "minutes_abs_diff": 0.25,
                "plus_minus_diff": -1.0,
                "status_detail": "",
                "computed_residual_class": "fixable_lineup_defect",
                "blocking_reason": "",
                "is_blocking": False,
                "has_minutes_mismatch": True,
                "has_plus_minus_reference_delta": True,
                "is_minutes_outlier": False,
                "manual_annotation_id": "",
                "manual_residual_class": "",
                "manual_status": "",
                "manual_notes": "",
                "effective_residual_class": "fixable_lineup_defect",
                "effective_is_blocking": False,
            },
        ],
    )
    _write_csv(
        residual_dir / "game_quality.csv",
        [
            "game_id",
            "primary_quality_status",
            "release_gate_status",
            "release_reason_code",
            "execution_lane",
            "blocks_release",
            "research_open",
            "policy_source",
            "has_material_minute_issue",
            "has_severe_minute_issue",
            "has_event_on_court_issue",
        ],
        [
            {
                "game_id": "0029700007",
                "primary_quality_status": "open",
                "release_gate_status": "accepted_unresolvable_contradiction",
                "release_reason_code": "period_start_contradiction",
                "execution_lane": "accepted_contradiction",
                "blocks_release": False,
                "research_open": False,
                "policy_source": "reviewed_override",
                "has_material_minute_issue": False,
                "has_severe_minute_issue": False,
                "has_event_on_court_issue": True,
            },
            {
                "game_id": "0029700008",
                "primary_quality_status": "open",
                "release_gate_status": "documented_hold",
                "release_reason_code": "mixed_source_boundary_tail",
                "execution_lane": "documented_hold",
                "blocks_release": False,
                "research_open": True,
                "policy_source": "reviewed_override",
                "has_material_minute_issue": True,
                "has_severe_minute_issue": False,
                "has_event_on_court_issue": True,
            },
        ],
    )

    subprocess.run(
        [
            sys.executable,
            str(ROOT / "audits" / "core" / "build_plus_minus_reference_report.py"),
            "--residual-dir",
            str(residual_dir),
            "--output-dir",
            str(output_dir),
        ],
        check=True,
        cwd=ROOT.parent,
    )

    characterization_rows = list(csv.DictReader((output_dir / "pm_reference_characterization.csv").open()))
    by_game = {row["game_id"]: row for row in characterization_rows}
    assert by_game["0029700007"]["pm_residual_class"] == "lineup_related"
    assert by_game["0029700007"]["release_pm_class"] == "accepted_contradiction"
    assert by_game["0029700007"]["execution_lane"] == "accepted_contradiction"
    assert by_game["0029700007"]["research_open"] == "False"
    assert by_game["0029700008"]["pm_residual_class"] == "lineup_related"
    assert by_game["0029700008"]["release_pm_class"] == "documented_hold"
    assert by_game["0029700008"]["blocks_release"] == "False"
    assert by_game["0029700008"]["research_open"] == "True"


def test_build_plus_minus_reference_report_fails_on_unmapped_release_pm_combination(tmp_path: Path) -> None:
    residual_dir = tmp_path / "residual"
    residual_dir.mkdir()
    output_dir = tmp_path / "report"

    _write_csv(
        residual_dir / "plus_minus_reference_delta_register.csv",
        [
            "grain",
            "residual_source",
            "game_id",
            "season",
            "team_id",
            "player_id",
            "player_name",
            "period",
            "event_num",
            "minutes_abs_diff",
            "plus_minus_diff",
            "status_detail",
            "computed_residual_class",
            "blocking_reason",
            "is_blocking",
            "has_minutes_mismatch",
            "has_plus_minus_reference_delta",
            "is_minutes_outlier",
            "manual_annotation_id",
            "manual_residual_class",
            "manual_status",
            "manual_notes",
            "effective_residual_class",
            "effective_is_blocking",
        ],
        [
            {
                "grain": "player_game",
                "residual_source": "plus_minus_reference_delta",
                "game_id": "29700009",
                "season": 2000,
                "team_id": 1,
                "player_id": 17,
                "player_name": "ImpossibleCombo",
                "period": 0,
                "event_num": 0,
                "minutes_abs_diff": 0.0,
                "plus_minus_diff": 1.0,
                "status_detail": "",
                "computed_residual_class": "candidate_boundary_difference",
                "blocking_reason": "",
                "is_blocking": False,
                "has_minutes_mismatch": False,
                "has_plus_minus_reference_delta": True,
                "is_minutes_outlier": False,
                "manual_annotation_id": "",
                "manual_residual_class": "",
                "manual_status": "",
                "manual_notes": "",
                "effective_residual_class": "candidate_boundary_difference",
                "effective_is_blocking": False,
            }
        ],
    )
    _write_csv(
        residual_dir / "game_quality.csv",
        [
            "game_id",
            "primary_quality_status",
            "release_gate_status",
            "release_reason_code",
            "execution_lane",
            "blocks_release",
            "research_open",
            "policy_source",
            "has_material_minute_issue",
            "has_severe_minute_issue",
            "has_event_on_court_issue",
        ],
        [
            {
                "game_id": "0029700009",
                "primary_quality_status": "boundary_difference",
                "release_gate_status": "open_actionable",
                "release_reason_code": "bad_test_combo",
                "execution_lane": "unreviewed_open",
                "blocks_release": True,
                "research_open": True,
                "policy_source": "reviewed_override",
                "has_material_minute_issue": False,
                "has_severe_minute_issue": False,
                "has_event_on_court_issue": False,
            }
        ],
    )

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "audits" / "core" / "build_plus_minus_reference_report.py"),
            "--residual-dir",
            str(residual_dir),
            "--output-dir",
            str(output_dir),
        ],
        cwd=ROOT.parent,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "Unmapped raw PM class x release_gate_status combination" in result.stderr
