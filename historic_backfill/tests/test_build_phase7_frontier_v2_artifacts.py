from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def test_build_phase7_frontier_v2_artifacts_writes_seed_checkpoint(tmp_path: Path) -> None:
    raw_game_quality_csv = tmp_path / "game_quality.csv"
    base_inventory_csv = tmp_path / "inventory_v1.csv"
    base_shortlist_csv = tmp_path / "shortlist_v1.csv"
    base_overlay_csv = tmp_path / "overlay_v1.csv"
    additions_csv = tmp_path / "additions.csv"
    output_inventory_csv = tmp_path / "inventory_v2.csv"
    output_shortlist_csv = tmp_path / "shortlist_v2.csv"
    output_overlay_csv = tmp_path / "overlay_v2.csv"
    coverage_summary_json = tmp_path / "seed_checkpoint.json"

    _write_csv(
        raw_game_quality_csv,
        [
            {
                "game_id": "0020000628",
                "primary_quality_status": "open",
                "has_event_on_court_issue": True,
                "has_material_minute_issue": True,
                "has_severe_minute_issue": False,
                "n_actionable_event_rows": 1,
                "max_abs_minute_diff": 0.25,
                "n_pm_reference_delta_rows": 0,
            },
            {
                "game_id": "0020900189",
                "primary_quality_status": "open",
                "has_event_on_court_issue": True,
                "has_material_minute_issue": False,
                "has_severe_minute_issue": False,
                "n_actionable_event_rows": 1,
                "max_abs_minute_diff": 0.0,
                "n_pm_reference_delta_rows": 2,
            },
            {
                "game_id": "0021700236",
                "primary_quality_status": "open",
                "has_event_on_court_issue": True,
                "has_material_minute_issue": False,
                "has_severe_minute_issue": False,
                "n_actionable_event_rows": 2,
                "max_abs_minute_diff": 0.0016666666666651,
                "n_pm_reference_delta_rows": 2,
            },
            {
                "game_id": "0029600070",
                "primary_quality_status": "open",
                "has_event_on_court_issue": False,
                "has_material_minute_issue": True,
                "has_severe_minute_issue": False,
                "n_actionable_event_rows": 0,
                "max_abs_minute_diff": 0.3666666666666671,
                "n_pm_reference_delta_rows": 0,
            },
        ],
    )
    _write_csv(
        base_inventory_csv,
        [
            {
                "game_id": "0020000628",
                "block_key": "B",
                "season": 2001,
                "lane": "contradiction_mixed_source_case",
                "recommended_next_action": "keep_open",
                "has_event_on_court_issue": True,
                "has_material_minute_issue": True,
                "has_severe_minute_issue": False,
                "n_actionable_event_rows": 1,
                "max_abs_minute_diff": 0.25,
                "n_pm_reference_delta_rows": 0,
                "notes": "mixed source tail",
            },
            {
                "game_id": "0020900189",
                "block_key": "C",
                "season": 2010,
                "lane": "contradiction_period_start_boundary",
                "recommended_next_action": "keep_open",
                "has_event_on_court_issue": True,
                "has_material_minute_issue": False,
                "has_severe_minute_issue": False,
                "n_actionable_event_rows": 1,
                "max_abs_minute_diff": 0.0,
                "n_pm_reference_delta_rows": 2,
                "notes": "accepted contradiction",
            },
        ],
    )
    _write_csv(
        base_shortlist_csv,
        [
            {
                "game_id": "0020000628",
                "block_key": "B",
                "current_lane": "contradiction_mixed_source_case",
                "current_blocker_status": "open",
                "recommended_execution_lane": "documented_hold",
                "next_step": "keep_open",
                "evidence_basis": "test",
            },
            {
                "game_id": "0020900189",
                "block_key": "C",
                "current_lane": "contradiction_period_start_boundary",
                "current_blocker_status": "open",
                "recommended_execution_lane": "accepted_contradiction",
                "next_step": "keep_open",
                "evidence_basis": "test",
            },
        ],
    )
    _write_csv(
        base_overlay_csv,
        [
            {
                "policy_decision_id": "reviewed_release_policy_20260322_v1",
                "game_id": "0020000628",
                "release_gate_status": "documented_hold",
                "release_reason_code": "mixed_source_boundary_tail",
                "execution_lane": "documented_hold",
                "blocks_release": False,
                "research_open": True,
                "policy_source": "reviewed_override",
                "expected_primary_quality_status": "open",
                "evidence_artifact": "artifact",
                "reviewed_at": "2026-03-22",
                "notes": "mixed source tail",
            },
            {
                "policy_decision_id": "reviewed_release_policy_20260322_v1",
                "game_id": "0020900189",
                "release_gate_status": "accepted_unresolvable_contradiction",
                "release_reason_code": "period_start_contradiction",
                "execution_lane": "accepted_contradiction",
                "blocks_release": False,
                "research_open": False,
                "policy_source": "reviewed_override",
                "expected_primary_quality_status": "open",
                "evidence_artifact": "artifact",
                "reviewed_at": "2026-03-22",
                "notes": "accepted contradiction",
            },
        ],
    )
    _write_csv(
        additions_csv,
        [
            {
                "game_id": "0021700236",
                "block_key": "E",
                "season": 2018,
                "lane": "same_clock_control",
                "recommended_next_action": "keep_reviewed_same_clock_control_do_not_retry",
                "notes": "same-clock control anti-canary",
                "current_blocker_status": "open_same_clock_convention_difference",
                "recommended_execution_lane": "policy_frontier_non_local",
                "next_step": "keep reviewed as same-clock control",
                "evidence_basis": "golden canary anti-canary",
                "release_gate_status": "accepted_boundary_difference",
                "release_reason_code": "same_clock_control",
                "execution_lane": "policy_frontier_non_local",
                "blocks_release": False,
                "research_open": False,
                "expected_primary_quality_status": "open",
            },
        ],
    )

    result = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve().parents[1] / "audits" / "core" / "build_phase7_frontier_v2_artifacts.py"),
            "--raw-game-quality-csv",
            str(raw_game_quality_csv),
            "--base-inventory-csv",
            str(base_inventory_csv),
            "--base-shortlist-csv",
            str(base_shortlist_csv),
            "--base-overlay-csv",
            str(base_overlay_csv),
            "--additions-csv",
            str(additions_csv),
            "--output-inventory-csv",
            str(output_inventory_csv),
            "--output-shortlist-csv",
            str(output_shortlist_csv),
            "--output-overlay-csv",
            str(output_overlay_csv),
            "--coverage-summary-json",
            str(coverage_summary_json),
            "--policy-decision-id",
            "reviewed_release_policy_20260323_v2",
            "--reviewed-at",
            "2026-03-23",
            "--expected-overlay-row-count",
            "3",
            "--expected-covered-count",
            "3",
            "--expected-uncovered-game-ids",
            "0029600070",
        ],
        cwd=Path(__file__).resolve().parents[2],
        text=True,
        capture_output=True,
        check=True,
    )

    coverage_summary = json.loads(coverage_summary_json.read_text(encoding="utf-8"))
    overlay_df = pd.read_csv(output_overlay_csv, dtype={"game_id": str})

    assert result.returncode == 0
    assert coverage_summary["overlay_row_count"] == 3
    assert coverage_summary["reviewed_overlay_row_count"] == 3
    assert coverage_summary["raw_open_game_count"] == 4
    assert coverage_summary["covered_raw_open_count"] == 3
    assert coverage_summary["uncovered_raw_open_game_ids"] == ["0029600070"]
    assert coverage_summary["release_blocking_game_ids"] == []
    assert coverage_summary["research_open_game_ids"] == ["0020000628"]
    assert overlay_df["game_id"].tolist() == ["0020000628", "0020900189", "0021700236"]
