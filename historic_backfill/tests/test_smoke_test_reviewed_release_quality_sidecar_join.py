from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_smoke_test_reviewed_release_quality_sidecar_join_uses_real_join_defaults_and_reviewed_rows(tmp_path: Path) -> None:
    darko_path = tmp_path / "darko_sample.parquet"
    sidecar_dir = tmp_path / "sidecar"
    sidecar_dir.mkdir()
    output_dir = tmp_path / "smoke"

    pd.DataFrame(
        [
            {"Game_SingleGame": 29700159, "Player_SingleGame": 1},
            {"Game_SingleGame": 29700159, "Player_SingleGame": 2},
            {"Game_SingleGame": 29700337, "Player_SingleGame": 3},
            {"Game_SingleGame": 29600001, "Player_SingleGame": 4},
        ]
    ).to_parquet(darko_path, index=False)

    _write_csv(
        sidecar_dir / "game_quality_sparse.csv",
        [
            "state_context",
            "game_id",
            "block_key",
            "season_group",
            "source_bundle",
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
        ],
        [
            {
                "state_context": "live_state",
                "game_id": "0029700159",
                "block_key": "A",
                "season_group": "1998-2000",
                "source_bundle": "A_1998-2000",
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
                "n_actionable_event_rows": 1,
                "max_abs_minute_diff": 1.85,
                "sum_abs_minute_diff_over_0_1": 1.85,
                "n_pm_reference_delta_rows": 3,
            },
            {
                "state_context": "live_state",
                "game_id": "0029700337",
                "block_key": "A",
                "season_group": "1998-2000",
                "source_bundle": "A_1998-2000",
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
            },
        ],
    )

    (sidecar_dir / "summary.json").write_text(
        json.dumps(
            {
                "reviewed_policy_overlay_version": "reviewed_release_policy_20260322_v1",
                "frontier_inventory_snapshot_id": "phase6_open_blocker_inventory_20260322_v1",
                "research_open_game_ids": ["0029700159"],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (sidecar_dir / "join_contract.json").write_text(
        json.dumps(
            {
                "default_absent_row_values": {
                    "primary_quality_status": "exact",
                    "release_gate_status": "exact",
                    "release_reason_code": "exact",
                    "execution_lane": "exact",
                    "blocks_release": False,
                    "research_open": False,
                    "policy_source": "auto_default",
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    subprocess.run(
        [
            sys.executable,
            str(ROOT / "audits" / "core" / "smoke_test_reviewed_release_quality_sidecar_join.py"),
            "--darko-parquet",
            str(darko_path),
            "--sidecar-csv",
            str(sidecar_dir / "game_quality_sparse.csv"),
            "--sidecar-summary-json",
            str(sidecar_dir / "summary.json"),
            "--join-contract-json",
            str(sidecar_dir / "join_contract.json"),
            "--output-dir",
            str(output_dir),
        ],
        check=True,
        cwd=ROOT.parent,
    )

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["reviewed_policy_overlay_version"] == "reviewed_release_policy_20260322_v1"
    assert summary["frontier_inventory_snapshot_id"] == "phase6_open_blocker_inventory_20260322_v1"
    assert summary["reviewed_override_game_count"] == 2
    assert summary["research_open_game_ids"] == ["0029700159"]
    assert summary["absent_game_id"] == "0029600001"
    assert summary["absent_defaults_verified"] is True
    assert summary["reviewed_rows_survive_join_unchanged"] is True
    assert summary["join_passed"] is True

    rows = list(csv.DictReader((output_dir / "joined_sample.csv").open()))
    absent_rows = [row for row in rows if row["game_id"] == "0029600001"]
    assert absent_rows
    assert {row["release_gate_status"] for row in absent_rows} == {"exact"}
    reviewed_rows = [row for row in rows if row["game_id"] == "0029700159"]
    assert reviewed_rows
    assert {row["release_reason_code"] for row in reviewed_rows} == {"source_limited_tradeoff_hold"}
