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


def test_build_reviewed_release_quality_sidecar_combines_sparse_game_quality_outputs(tmp_path: Path) -> None:
    residual_a = tmp_path / "A_1998-2000"
    residual_b = tmp_path / "E_2017-2020"
    residual_a.mkdir()
    residual_b.mkdir()
    output_dir = tmp_path / "sidecar"
    overlay_csv = tmp_path / "overlay.csv"
    frontier_inventory_csv = tmp_path / "phase6_open_blocker_inventory_20260322_v1.csv"

    fields = [
        "game_id",
        "primary_quality_status",
        "release_gate_status",
        "release_reason_code",
        "execution_lane",
        "blocks_release",
        "research_open",
        "policy_source",
        "has_active_correction",
        "has_open_actionable_residual",
        "has_source_limited_residual",
        "has_boundary_difference",
        "has_material_minute_issue",
        "has_severe_minute_issue",
        "has_event_on_court_issue",
        "n_active_corrections",
        "n_actionable_event_rows",
        "max_abs_minute_diff",
        "sum_abs_minute_diff_over_0_1",
        "n_pm_reference_delta_rows",
    ]

    _write_csv(
        residual_a / "game_quality.csv",
        fields,
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
                "has_active_correction": False,
                "has_open_actionable_residual": True,
                "has_source_limited_residual": False,
                "has_boundary_difference": True,
                "has_material_minute_issue": True,
                "has_severe_minute_issue": True,
                "has_event_on_court_issue": True,
                "n_active_corrections": 0,
                "n_actionable_event_rows": 0,
                "max_abs_minute_diff": 1.85,
                "sum_abs_minute_diff_over_0_1": 1.85,
                "n_pm_reference_delta_rows": 3,
            }
        ],
    )
    _write_csv(
        residual_b / "game_quality.csv",
        fields,
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
                "has_active_correction": False,
                "has_open_actionable_residual": True,
                "has_source_limited_residual": False,
                "has_boundary_difference": True,
                "has_material_minute_issue": False,
                "has_severe_minute_issue": False,
                "has_event_on_court_issue": True,
                "n_active_corrections": 0,
                "n_actionable_event_rows": 2,
                "max_abs_minute_diff": 0.0,
                "sum_abs_minute_diff_over_0_1": 0.0,
                "n_pm_reference_delta_rows": 2,
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
                "game_id": "0029700159",
                "release_gate_status": "documented_hold",
                "release_reason_code": "source_limited_tradeoff_hold",
                "execution_lane": "documented_hold",
                "blocks_release": False,
                "research_open": True,
                "policy_source": "reviewed_override",
                "expected_primary_quality_status": "open",
                "evidence_artifact": "artifact_a",
                "reviewed_at": "2026-03-22",
                "notes": "",
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
                "evidence_artifact": "artifact_b",
                "reviewed_at": "2026-03-22",
                "notes": "",
            },
        ],
    )
    frontier_inventory_csv.write_text("game_id\n0029700159\n", encoding="utf-8")

    subprocess.run(
        [
            sys.executable,
            str(ROOT / "build_reviewed_release_quality_sidecar.py"),
            "--residual-dir",
            str(residual_a),
            "--residual-dir",
            str(residual_b),
            "--reviewed-policy-overlay-csv",
            str(overlay_csv),
            "--frontier-inventory-csv",
            str(frontier_inventory_csv),
            "--output-dir",
            str(output_dir),
        ],
        check=True,
        cwd=ROOT,
    )

    rows = list(csv.DictReader((output_dir / "game_quality_sparse.csv").open()))
    assert len(rows) == 2
    by_game = {row["game_id"]: row for row in rows}
    assert by_game["0029700159"]["block_key"] == "A"
    assert by_game["0029700159"]["season_group"] == "1998-2000"
    assert by_game["0021700337"]["block_key"] == "E"
    assert by_game["0021700337"]["release_gate_status"] == "accepted_boundary_difference"

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["row_count"] == 2
    assert summary["unique_game_count"] == 2
    assert summary["coverage"] == "sparse_problem_or_reviewed_games_only"
    assert summary["reviewed_policy_overlay_version"] == "reviewed_release_policy_20260322_v1"
    assert summary["frontier_inventory_snapshot_id"] == "phase6_open_blocker_inventory_20260322_v1"
    assert summary["release_blocking_game_count"] == 0
    assert summary["release_blocking_game_ids"] == []
    assert summary["research_open_game_count"] == 1
    assert summary["research_open_game_ids"] == ["0029700159"]
    assert summary["reviewed_override_game_count"] == 2
    assert summary["default_absent_row_values"]["release_gate_status"] == "exact"

    join_contract = json.loads((output_dir / "join_contract.json").read_text(encoding="utf-8"))
    assert join_contract["join_key"] == "game_id"
    assert join_contract["join_strategy"] == "left_join_sparse_game_quality_sidecar"
    assert join_contract["default_absent_row_values"]["blocks_release"] is False

    notes = (output_dir / "integration_notes.md").read_text(encoding="utf-8")
    assert "left join on `game_id`" in notes
    assert "default exact / non-blocking case" in notes
