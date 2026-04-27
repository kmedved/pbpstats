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


def test_build_reviewed_frontier_policy_overlay_maps_lanes_to_release_policy(tmp_path: Path) -> None:
    inventory_path = tmp_path / "inventory.csv"
    shortlist_path = tmp_path / "shortlist.csv"
    output_path = tmp_path / "overlay.csv"

    _write_csv(
        inventory_path,
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
                "game_id": "0020900189",
                "block_key": "C",
                "season": 2010,
                "lane": "contradiction_period_start_boundary",
                "recommended_next_action": "keep_open_contradiction_case",
                "has_event_on_court_issue": True,
                "has_material_minute_issue": False,
                "has_severe_minute_issue": False,
                "n_actionable_event_rows": 1,
                "max_abs_minute_diff": 0.0016666667,
                "n_pm_reference_delta_rows": 2,
                "notes": "period-start contradiction",
            },
            {
                "game_id": "0029701075",
                "block_key": "A",
                "season": 1998,
                "lane": "candidate_systematic_defect",
                "recommended_next_action": "hold_open_candidate_systematic_defect",
                "has_event_on_court_issue": True,
                "has_material_minute_issue": True,
                "has_severe_minute_issue": True,
                "n_actionable_event_rows": 13,
                "max_abs_minute_diff": 1.0333333333,
                "n_pm_reference_delta_rows": 0,
                "notes": "block A scrambled period",
            },
            {
                "game_id": "0029700159",
                "block_key": "A",
                "season": 1998,
                "lane": "special_holdout_material_minute",
                "recommended_next_action": "keep_live_state_tradeoff_hold",
                "has_event_on_court_issue": True,
                "has_material_minute_issue": True,
                "has_severe_minute_issue": True,
                "n_actionable_event_rows": 1,
                "max_abs_minute_diff": 1.85,
                "n_pm_reference_delta_rows": 0,
                "notes": "source-limited tradeoff hold",
            },
        ],
    )
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
                "game_id": "0020900189",
                "block_key": "C",
                "current_lane": "contradiction_period_start_boundary",
                "current_blocker_status": "open_boundary_contradiction",
                "recommended_execution_lane": "documented_hold",
                "next_step": "keep contradiction",
                "evidence_basis": "starter source split",
            },
            {
                "game_id": "0029701075",
                "block_key": "A",
                "current_lane": "candidate_systematic_defect",
                "current_blocker_status": "open_candidate_systematic_defect",
                "recommended_execution_lane": "documented_hold",
                "next_step": "keep systematic defect hold",
                "evidence_basis": "contradictory local probes",
            },
            {
                "game_id": "0029700159",
                "block_key": "A",
                "current_lane": "special_holdout_material_minute",
                "current_blocker_status": "open_material_minute_tradeoff",
                "recommended_execution_lane": "documented_hold",
                "next_step": "keep least-bad live state",
                "evidence_basis": "source-limited tradeoff comparison",
            },
        ],
    )

    subprocess.run(
        [
            sys.executable,
            str(ROOT / "build_reviewed_frontier_policy_overlay.py"),
            "--inventory-csv",
            str(inventory_path),
            "--shortlist-csv",
            str(shortlist_path),
            "--output-csv",
            str(output_path),
        ],
        check=True,
        cwd=ROOT,
    )

    rows = list(csv.DictReader(output_path.open()))
    by_game = {row["game_id"]: row for row in rows}
    assert by_game["0020900189"]["release_gate_status"] == "accepted_unresolvable_contradiction"
    assert by_game["0020900189"]["execution_lane"] == "accepted_contradiction"
    assert by_game["0020900189"]["blocks_release"] == "False"
    assert by_game["0020900189"]["research_open"] == "False"
    assert by_game["0029701075"]["release_gate_status"] == "documented_hold"
    assert by_game["0029701075"]["release_reason_code"] == "scrambled_pbp_missing_subs_blockA"
    assert by_game["0029701075"]["execution_lane"] == "documented_hold"
    assert by_game["0029701075"]["blocks_release"] == "False"
    assert by_game["0029701075"]["research_open"] == "True"
    assert by_game["0029700159"]["release_reason_code"] == "source_limited_tradeoff_hold"
    assert by_game["0029700159"]["execution_lane"] == "documented_hold"
    assert by_game["0029700159"]["research_open"] == "True"

    summary = json.loads(output_path.with_suffix(".summary.json").read_text(encoding="utf-8"))
    assert summary["overlay_row_count"] == 3
    assert summary["reviewed_policy_overlay_version"] == "reviewed_release_policy_20260322_v1"
    assert summary["frontier_inventory_snapshot_id"] == "inventory"
    assert summary["execution_lane_counts"] == {
        "accepted_contradiction": 1,
        "documented_hold": 2,
    }
    assert summary["release_blocking_game_ids"] == []
    assert summary["research_open_game_ids"] == ["0029700159", "0029701075"]
