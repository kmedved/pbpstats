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


def test_build_reviewed_release_policy_bundle_applies_overlay_to_existing_residual_bundle(tmp_path: Path) -> None:
    source_dir = tmp_path / "source_bundle"
    source_dir.mkdir()
    output_dir = tmp_path / "output_bundle"
    overlay_path = tmp_path / "overlay.csv"

    _write_csv(
        source_dir / "game_quality.csv",
        [
            "game_id",
            "primary_quality_status",
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
        ],
        [
            {
                "game_id": "0029700159",
                "primary_quality_status": "open",
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
            },
            {
                "game_id": "0021700337",
                "primary_quality_status": "boundary_difference",
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
            },
        ],
    )
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
                "evidence_artifact": "shortlist.csv",
                "reviewed_at": "2026-03-22",
                "notes": "least-bad tradeoff",
            }
        ],
    )
    (source_dir / "summary.json").write_text(
        json.dumps(
            {
                "run_dir": "/tmp/source_bundle",
                "manifest_path": "/tmp/correction_manifest.json",
                "raw_counts": {"failed_games": 0, "event_stats_errors": 0},
                "blocker_counts": {"actionable_residual_rows": 1},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_csv(
        source_dir / "plus_minus_reference_delta_register.csv",
        ["game_id", "plus_minus_diff"],
        [{"game_id": "0029700159", "plus_minus_diff": 3}],
    )

    subprocess.run(
        [
            sys.executable,
            str(ROOT / "audits" / "core" / "build_reviewed_release_policy_bundle.py"),
            "--source-bundle-dir",
            str(source_dir),
            "--reviewed-policy-overlay-csv",
            str(overlay_path),
            "--output-dir",
            str(output_dir),
        ],
        check=True,
        cwd=ROOT.parent,
    )

    rows = list(csv.DictReader((output_dir / "game_quality.csv").open()))
    by_game = {row["game_id"]: row for row in rows}
    assert by_game["0029700159"]["release_reason_code"] == "source_limited_tradeoff_hold"
    assert by_game["0029700159"]["execution_lane"] == "documented_hold"
    assert by_game["0029700159"]["blocks_release"] == "False"
    assert by_game["0029700159"]["research_open"] == "True"
    assert by_game["0021700337"]["release_gate_status"] == "accepted_boundary_difference"
    assert by_game["0021700337"]["policy_source"] == "auto_default"

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["reviewed_policy_overlay_version"] == "reviewed_release_policy_20260322_v1"
    assert summary["release_blocking_game_count"] == 0
    assert summary["release_blocking_game_ids"] == []
    assert summary["research_open_game_ids"] == ["0029700159"]

    passthrough_rows = list(csv.DictReader((output_dir / "plus_minus_reference_delta_register.csv").open()))
    assert passthrough_rows == [{"game_id": "0029700159", "plus_minus_diff": "3"}]
