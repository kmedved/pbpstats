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


def test_build_reviewed_frontier_inventory_emits_raw_release_and_reviewed_views(tmp_path: Path) -> None:
    residual_a = tmp_path / "A"
    residual_b = tmp_path / "B"
    residual_a.mkdir()
    residual_b.mkdir()
    output_dir = tmp_path / "frontier"

    game_quality_fields = [
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
        "n_actionable_event_rows",
        "max_abs_minute_diff",
        "n_pm_reference_delta_rows",
    ]
    _write_csv(
        residual_a / "game_quality.csv",
        game_quality_fields,
        [
            {
                "game_id": "0029700159",
                "primary_quality_status": "open",
                "release_gate_status": "documented_hold",
                "release_reason_code": "source_limited_tradeoff_hold",
                "execution_lane": "documented_hold",
                "blocks_release": False,
                "research_open": True,
                "policy_source": "reviewed_override",
                "has_material_minute_issue": True,
                "has_severe_minute_issue": True,
                "has_event_on_court_issue": True,
                "n_actionable_event_rows": 0,
                "max_abs_minute_diff": 1.85,
                "n_pm_reference_delta_rows": 3,
            }
        ],
    )
    _write_csv(
        residual_b / "game_quality.csv",
        game_quality_fields,
        [
            {
                "game_id": "0021700337",
                "primary_quality_status": "open",
                "release_gate_status": "accepted_boundary_difference",
                "release_reason_code": "same_clock_control",
                "execution_lane": "policy_frontier_non_local",
                "blocks_release": False,
                "research_open": False,
                "policy_source": "reviewed_override",
                "has_material_minute_issue": False,
                "has_severe_minute_issue": False,
                "has_event_on_court_issue": True,
                "n_actionable_event_rows": 2,
                "max_abs_minute_diff": 0.0,
                "n_pm_reference_delta_rows": 2,
            }
        ],
    )
    for residual_dir in [residual_a, residual_b]:
        (residual_dir / "summary.json").write_text(
            json.dumps(
                {
                    "raw_counts": {
                        "failed_games": 0,
                        "event_stats_errors": 0,
                    }
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    authoritative_path = tmp_path / "authoritative.csv"
    _write_csv(
        authoritative_path,
        [
            "game_id",
            "block_key",
            "season",
            "lane",
            "recommended_next_action",
            "has_event_on_court_issue",
            "has_material_minute_issue",
            "has_severe_minute_issue",
            "n_actionable_event_rows",
            "max_abs_minute_diff",
            "n_pm_reference_delta_rows",
            "notes",
        ],
        [
            {
                "game_id": "0029700159",
                "block_key": "A",
                "season": 1998,
                "lane": "special_holdout_material_minute",
                "recommended_next_action": "hold_open_mixed_minute_tradeoff",
                "has_event_on_court_issue": True,
                "has_material_minute_issue": True,
                "has_severe_minute_issue": True,
                "n_actionable_event_rows": 0,
                "max_abs_minute_diff": 1.85,
                "n_pm_reference_delta_rows": 3,
                "notes": "live tradeoff hold",
            },
            {
                "game_id": "0021700337",
                "block_key": "E",
                "season": 2018,
                "lane": "same_clock_control_guardrail",
                "recommended_next_action": "keep_as_same_clock_control",
                "has_event_on_court_issue": True,
                "has_material_minute_issue": False,
                "has_severe_minute_issue": False,
                "n_actionable_event_rows": 2,
                "max_abs_minute_diff": 0.0,
                "n_pm_reference_delta_rows": 2,
                "notes": "same-clock control",
            },
        ],
    )

    shortlist_path = tmp_path / "shortlist.csv"
    _write_csv(
        shortlist_path,
        [
            "game_id",
            "block_key",
            "current_lane",
            "current_blocker_status",
            "recommended_execution_lane",
            "next_step",
            "evidence_basis",
        ],
        [
            {
                "game_id": "0029700159",
                "block_key": "A",
                "current_lane": "special_holdout_material_minute",
                "current_blocker_status": "open_material_minute_tradeoff",
                "recommended_execution_lane": "documented_hold",
                "next_step": "keep live state",
                "evidence_basis": "paired comparison",
            },
            {
                "game_id": "0021700337",
                "block_key": "E",
                "current_lane": "same_clock_control_guardrail",
                "current_blocker_status": "open_event_only_control_case",
                "recommended_execution_lane": "policy_frontier_non_local",
                "next_step": "keep as control",
                "evidence_basis": "same-clock guardrail",
            },
        ],
    )
    overlay_path = tmp_path / "reviewed_overlay.csv"
    _write_csv(
        overlay_path,
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
                "game_id": "0029700159",
                "release_gate_status": "documented_hold",
                "release_reason_code": "source_limited_tradeoff_hold",
                "execution_lane": "documented_hold",
                "blocks_release": False,
                "research_open": True,
                "policy_source": "reviewed_override",
                "expected_primary_quality_status": "open",
                "evidence_artifact": str(shortlist_path),
                "reviewed_at": "2026-03-22",
                "notes": "live tradeoff hold",
            },
            {
                "policy_decision_id": "reviewed_release_policy_20260322_v1",
                "game_id": "0021700337",
                "release_gate_status": "accepted_boundary_difference",
                "release_reason_code": "same_clock_control",
                "execution_lane": "policy_frontier_non_local",
                "blocks_release": False,
                "research_open": False,
                "policy_source": "reviewed_override",
                "expected_primary_quality_status": "open",
                "evidence_artifact": str(shortlist_path),
                "reviewed_at": "2026-03-22",
                "notes": "same-clock control",
            },
        ],
    )
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "audits" / "core" / "build_reviewed_frontier_inventory.py"),
            "--residual-dir",
            str(residual_a),
            "--residual-dir",
            str(residual_b),
            "--inventory-csv",
            str(authoritative_path),
            "--shortlist-csv",
            str(shortlist_path),
            "--reviewed-policy-overlay-csv",
            str(overlay_path),
            "--output-dir",
            str(output_dir),
        ],
        check=True,
        cwd=ROOT.parent,
    )

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["total_live_raw_open_games"] == 2
    assert summary["release_blocking_game_count"] == 0
    assert summary["release_blocking_game_ids"] == []
    assert summary["research_open_game_count"] == 1
    assert summary["research_open_game_ids"] == ["0029700159"]
    assert summary["reviewed_policy_overlay_version"] == "reviewed_release_policy_20260322_v1"
    assert summary["frontier_inventory_snapshot_id"] == "authoritative"
    assert summary["release_gate_status_counts"] == {
        "accepted_boundary_difference": 1,
        "documented_hold": 1,
    }
    assert summary["execution_lane_counts"] == {
        "documented_hold": 1,
        "policy_frontier_non_local": 1,
    }

    reviewed_rows = list(csv.DictReader((output_dir / "reviewed_frontier_inventory.csv").open()))
    by_game = {row["game_id"]: row for row in reviewed_rows}
    assert by_game["0029700159"]["release_reason_code"] == "source_limited_tradeoff_hold"
    assert by_game["0021700337"]["recommended_next_action"] == "keep_as_same_clock_control"
    assert summary["tier1_release_ready"] is True
    assert summary["tier2_frontier_closed"] is False

    raw_rows = list(csv.DictReader((output_dir / "raw_open_inventory.csv").open()))
    assert len(raw_rows) == 2
    by_game = {row["game_id"]: row for row in raw_rows}
    assert by_game["0029700159"]["release_gate_status"] == "documented_hold"
    assert by_game["0021700337"]["release_gate_status"] == "accepted_boundary_difference"

    release_rows = list(csv.DictReader((output_dir / "release_blocker_inventory.csv").open()))
    assert release_rows == []

    reviewed_rows = list(csv.DictReader((output_dir / "reviewed_frontier_inventory.csv").open()))
    assert len(reviewed_rows) == 2
