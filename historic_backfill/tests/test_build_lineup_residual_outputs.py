from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path


RESIDUAL_OUTPUT_COLUMNS = [
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
]

GAME_QUALITY_COLUMNS = [
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


def _run_builder(
    repo_root: Path,
    run_dir: Path,
    output_dir: Path,
    manifest_path: Path,
    reviewed_policy_path: Path | None = None,
    seasons: list[int] | None = None,
) -> None:
    command = [
        sys.executable,
        str(repo_root / "build_lineup_residual_outputs.py"),
        "--run-dir",
        str(run_dir),
        "--output-dir",
        str(output_dir),
        "--manifest-path",
        str(manifest_path),
    ]
    if seasons is not None:
        command.extend(["--seasons", *[str(season) for season in seasons]])
    if reviewed_policy_path is not None:
        command.extend(["--reviewed-policy-overlay-csv", str(reviewed_policy_path)])
    subprocess.run(command, check=True, cwd=repo_root)


def _read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        raise ValueError("rows must not be empty")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_build_lineup_residual_outputs_handles_empty_run_dir(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parent.parent
    run_dir = tmp_path / "run"
    output_dir = tmp_path / "output"
    run_dir.mkdir()

    manifest_path = tmp_path / "correction_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": "test",
                "corrections": [],
                "residual_annotations": [],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    _run_builder(repo_root, run_dir, output_dir, manifest_path)

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert set(summary.keys()) == {
        "blocker_counts",
        "execution_lane_counts",
        "manifest_path",
        "quality_status_counts",
        "raw_counts",
        "raw_quality_status_counts",
        "release_blocking_game_count",
        "release_blocking_game_ids",
        "release_gate_status_counts",
        "research_open_game_count",
        "research_open_game_ids",
        "reviewed_policy_overlay_csv",
        "reviewed_policy_overlay_version",
        "run_dir",
        "selected_seasons",
        "tier1_release_ready",
        "tier2_frontier_closed",
    }
    assert summary["raw_counts"]["failed_games"] == 0
    assert summary["raw_counts"]["event_on_court_issue_rows"] == 0
    assert summary["blocker_counts"]["actionable_residual_rows"] == 0
    assert summary["quality_status_counts"] == {}
    assert summary["raw_quality_status_counts"] == {}
    assert summary["release_gate_status_counts"] == {}
    assert summary["execution_lane_counts"] == {}
    assert summary["release_blocking_game_count"] == 0
    assert summary["release_blocking_game_ids"] == []
    assert summary["tier1_release_ready"] is True
    assert summary["tier2_frontier_closed"] is True
    assert summary["research_open_game_ids"] == []
    assert summary["research_open_game_count"] == 0
    assert summary["reviewed_policy_overlay_version"] == ""
    assert summary["selected_seasons"] == []
    assert (output_dir / "actionable_queue.csv").exists()
    assert (output_dir / "source_limited_residuals.csv").exists()
    assert (output_dir / "boundary_difference_residuals.csv").exists()
    assert (output_dir / "residual_annotations.csv").exists()
    assert (output_dir / "plus_minus_reference_delta_register.csv").exists()
    assert (output_dir / "game_quality.csv").exists()
    assert (output_dir / "raw_open_games.csv").exists()


def test_build_lineup_residual_outputs_writes_full_phase4_bundle_and_expected_shapes(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parent.parent
    run_dir = tmp_path / "run"
    output_dir = tmp_path / "output"
    run_dir.mkdir()

    manifest_path = tmp_path / "correction_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": "test",
                "corrections": [
                    {
                        "correction_id": "starter__0029800004__p1__t400",
                        "episode_id": "period_start__0029700004__p1__t400__legacy",
                        "status": "active",
                        "domain": "lineup",
                        "scope_type": "period_start",
                        "authoring_mode": "explicit",
                        "game_id": "0029700004",
                        "period": 1,
                        "team_id": 400,
                        "lineup_player_ids": [401, 402, 403, 404, 405],
                        "reason_code": "legacy",
                        "evidence_summary": "legacy starter correction",
                        "source_primary": "raw_pbp",
                        "source_secondary": "unknown",
                        "preferred_source": "raw_pbp",
                        "confidence": "legacy",
                        "validation_artifacts": [],
                        "supersedes": [],
                        "date_added": "2026-03-22",
                        "notes": "",
                    }
                ],
                "residual_annotations": [
                    {
                        "annotation_id": "source_limited__0000000002__p1__e7",
                        "game_id": "0000000002",
                        "team_id": 200,
                        "player_id": 202,
                        "period": 1,
                        "event_num": 7,
                        "residual_class": "source_limited_upstream_error",
                        "status": "accepted",
                        "source_primary": "bbr",
                        "source_secondary": "raw_pbp",
                        "preferred_source": "bbr",
                        "confidence": "high",
                        "validation_artifacts": [],
                        "notes": "confirmed upstream source conflict",
                    }
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    _write_csv(
        run_dir / "event_player_on_court_issues_1998.csv",
        [
            {
                "game_id": 1,
                "team_id": 100,
                "player_id": 101,
                "player_name": "Alpha",
                "period": 2,
                "event_num": 5,
                "status": "off_court_credit",
            },
            {
                "game_id": 2,
                "team_id": 200,
                "player_id": 202,
                "player_name": "Beta",
                "period": 1,
                "event_num": 7,
                "status": "off_court_credit",
            },
        ],
    )
    _write_csv(
        run_dir / "minutes_plus_minus_audit_1998.csv",
        [
            {
                "game_id": 1,
                "team_id": 100,
                "player_id": 101,
                "player_name": "Alpha",
                "Minutes_abs_diff": 0.75,
                "Plus_Minus_diff": 0.0,
                "has_minutes_mismatch": True,
                "has_plus_minus_mismatch": False,
                "is_minutes_outlier": True,
            },
            {
                "game_id": 3,
                "team_id": 300,
                "player_id": 303,
                "player_name": "Gamma",
                "Minutes_abs_diff": 0.0,
                "Plus_Minus_diff": 2.0,
                "has_minutes_mismatch": False,
                "has_plus_minus_mismatch": True,
                "is_minutes_outlier": False,
            },
        ],
    )
    (run_dir / "summary_1998.json").write_text(
        json.dumps({"failed_games": 1, "event_stats_errors": 2}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    _run_builder(repo_root, run_dir, output_dir, manifest_path)

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert set(summary.keys()) == {
        "blocker_counts",
        "execution_lane_counts",
        "manifest_path",
        "quality_status_counts",
        "raw_counts",
        "raw_quality_status_counts",
        "release_blocking_game_count",
        "release_blocking_game_ids",
        "release_gate_status_counts",
        "research_open_game_count",
        "research_open_game_ids",
        "reviewed_policy_overlay_csv",
        "reviewed_policy_overlay_version",
        "run_dir",
        "selected_seasons",
        "tier1_release_ready",
        "tier2_frontier_closed",
    }
    assert summary["raw_counts"] == {
        "candidate_boundary_difference_rows": 1,
        "event_on_court_issue_rows": 2,
        "event_stats_errors": 2,
        "failed_games": 1,
        "minutes_mismatch_rows": 1,
        "minutes_outlier_rows": 1,
        "plus_minus_reference_delta_rows": 1,
        "source_limited_residual_rows": 1,
    }
    assert summary["blocker_counts"] == {
        "actionable_event_on_court_rows": 1,
        "actionable_residual_rows": 2,
        "event_stats_errors": 2,
        "failed_games": 1,
        "material_minute_rows": 1,
        "severe_minute_rows": 1,
    }
    assert summary["quality_status_counts"] == {
        "boundary_difference": 1,
        "open": 1,
        "override_corrected": 1,
        "source_limited": 1,
    }
    assert summary["raw_quality_status_counts"] == summary["quality_status_counts"]
    assert summary["release_gate_status_counts"] == {
        "accepted_boundary_difference": 1,
        "open_actionable": 1,
        "override_corrected": 1,
        "source_limited_upstream_error": 1,
    }
    assert summary["execution_lane_counts"] == {
        "override_corrected": 1,
        "policy_frontier_non_local": 1,
        "source_limited": 1,
        "unreviewed_open": 1,
    }
    assert summary["release_blocking_game_count"] == 1
    assert summary["release_blocking_game_ids"] == ["0000000001"]
    assert summary["tier1_release_ready"] is False
    assert summary["tier2_frontier_closed"] is False
    assert summary["research_open_game_ids"] == ["0000000001"]
    assert summary["research_open_game_count"] == 1
    assert summary["reviewed_policy_overlay_version"] == ""
    assert summary["selected_seasons"] == [1998]

    header, rows = _read_csv_rows(output_dir / "residual_annotations.csv")
    assert header == RESIDUAL_OUTPUT_COLUMNS
    assert len(rows) == 4

    header, rows = _read_csv_rows(output_dir / "actionable_queue.csv")
    assert header == RESIDUAL_OUTPUT_COLUMNS
    assert len(rows) == 2
    assert {row["effective_residual_class"] for row in rows} == {"fixable_lineup_defect"}

    header, rows = _read_csv_rows(output_dir / "source_limited_residuals.csv")
    assert header == RESIDUAL_OUTPUT_COLUMNS
    assert len(rows) == 1
    assert rows[0]["game_id"] == "0000000002"
    assert rows[0]["effective_residual_class"] == "source_limited_upstream_error"
    assert rows[0]["effective_is_blocking"] == "False"

    header, rows = _read_csv_rows(output_dir / "boundary_difference_residuals.csv")
    assert header == RESIDUAL_OUTPUT_COLUMNS
    assert len(rows) == 1
    assert rows[0]["game_id"] == "0000000003"
    assert rows[0]["effective_residual_class"] == "candidate_boundary_difference"

    header, rows = _read_csv_rows(output_dir / "plus_minus_reference_delta_register.csv")
    assert header == RESIDUAL_OUTPUT_COLUMNS
    assert len(rows) == 1
    assert rows[0]["residual_source"] == "plus_minus_reference_delta"
    assert rows[0]["game_id"] == "0000000003"

    header, rows = _read_csv_rows(output_dir / "game_quality.csv")
    assert header == GAME_QUALITY_COLUMNS
    assert [row["primary_quality_status"] for row in rows] == [
        "open",
        "source_limited",
        "boundary_difference",
        "override_corrected",
    ]
    by_game = {row["game_id"]: row for row in rows}
    assert by_game["0000000001"]["release_gate_status"] == "open_actionable"
    assert by_game["0000000001"]["execution_lane"] == "unreviewed_open"
    assert by_game["0000000001"]["blocks_release"] == "True"
    assert by_game["0000000002"]["release_gate_status"] == "source_limited_upstream_error"
    assert by_game["0000000003"]["release_gate_status"] == "accepted_boundary_difference"
    assert by_game["0029700004"]["release_gate_status"] == "override_corrected"
    assert by_game["0000000001"]["has_open_actionable_residual"] == "True"
    assert by_game["0000000001"]["has_severe_minute_issue"] == "True"
    assert by_game["0000000002"]["has_source_limited_residual"] == "True"
    assert by_game["0000000003"]["has_boundary_difference"] == "True"
    assert by_game["0029700004"]["has_active_correction"] == "True"


def test_build_lineup_residual_outputs_applies_reviewed_policy_overlay_and_detects_stale_rows(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parent.parent
    run_dir = tmp_path / "run"
    output_dir = tmp_path / "output"
    stale_output_dir = tmp_path / "stale_output"
    run_dir.mkdir()

    manifest_path = tmp_path / "correction_manifest.json"
    manifest_path.write_text(
        json.dumps({"manifest_version": "test", "corrections": [], "residual_annotations": []}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    reviewed_policy_path = tmp_path / "reviewed_policy.csv"
    reviewed_policy_path.write_text(
        "\n".join(
            [
                "policy_decision_id,game_id,release_gate_status,release_reason_code,execution_lane,blocks_release,research_open,policy_source,expected_primary_quality_status,evidence_artifact,reviewed_at,notes",
                "reviewed__0000000001,0000000001,documented_hold,mixed_source_boundary_tail,documented_hold,false,true,reviewed_override,open,/tmp/evidence,2026-03-22,reviewed hold",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    stale_policy_path = tmp_path / "stale_reviewed_policy.csv"
    stale_policy_path.write_text(
        "\n".join(
            [
                "policy_decision_id,game_id,release_gate_status,release_reason_code,execution_lane,blocks_release,research_open,policy_source,expected_primary_quality_status,evidence_artifact,reviewed_at,notes",
                "reviewed__0000000001,0000000001,documented_hold,mixed_source_boundary_tail,documented_hold,false,true,reviewed_override,boundary_difference,/tmp/evidence,2026-03-22,stale reviewed hold",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    _write_csv(
        run_dir / "event_player_on_court_issues_1998.csv",
        [
            {
                "game_id": 1,
                "team_id": 100,
                "player_id": 101,
                "player_name": "Alpha",
                "period": 2,
                "event_num": 5,
                "status": "off_court_credit",
            }
        ],
    )

    _run_builder(repo_root, run_dir, output_dir, manifest_path, reviewed_policy_path)

    header, rows = _read_csv_rows(output_dir / "game_quality.csv")
    assert header == GAME_QUALITY_COLUMNS
    assert len(rows) == 1
    assert rows[0]["primary_quality_status"] == "open"
    assert rows[0]["release_gate_status"] == "documented_hold"
    assert rows[0]["execution_lane"] == "documented_hold"
    assert rows[0]["blocks_release"] == "False"
    assert rows[0]["research_open"] == "True"
    assert rows[0]["policy_source"] == "reviewed_override"

    completed = subprocess.run(
        [
            sys.executable,
            str(repo_root / "build_lineup_residual_outputs.py"),
            "--run-dir",
            str(run_dir),
            "--output-dir",
            str(stale_output_dir),
            "--manifest-path",
            str(manifest_path),
            "--reviewed-policy-overlay-csv",
            str(stale_policy_path),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0
    assert "Stale reviewed policy override" in (completed.stderr or completed.stdout)


def test_build_lineup_residual_outputs_filters_to_requested_season(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parent.parent
    run_dir = tmp_path / "run"
    output_dir = tmp_path / "output"
    run_dir.mkdir()

    manifest_path = tmp_path / "correction_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": "test",
                "corrections": [
                    {
                        "correction_id": "starter__0029600004__p1__t400",
                        "episode_id": "period_start__0029600004__p1__t400__legacy",
                        "status": "active",
                        "domain": "lineup",
                        "scope_type": "period_start",
                        "authoring_mode": "explicit",
                        "game_id": "0029600004",
                        "period": 1,
                        "team_id": 400,
                        "lineup_player_ids": [401, 402, 403, 404, 405],
                        "reason_code": "legacy",
                        "evidence_summary": "legacy starter correction",
                        "source_primary": "raw_pbp",
                        "source_secondary": "unknown",
                        "preferred_source": "raw_pbp",
                        "confidence": "legacy",
                        "validation_artifacts": [],
                        "supersedes": [],
                        "date_added": "2026-03-22",
                        "notes": "",
                    },
                    {
                        "correction_id": "starter__0029700004__p1__t500",
                        "episode_id": "period_start__0029700004__p1__t500__legacy",
                        "status": "active",
                        "domain": "lineup",
                        "scope_type": "period_start",
                        "authoring_mode": "explicit",
                        "game_id": "0029700004",
                        "period": 1,
                        "team_id": 500,
                        "lineup_player_ids": [501, 502, 503, 504, 505],
                        "reason_code": "legacy",
                        "evidence_summary": "legacy starter correction",
                        "source_primary": "raw_pbp",
                        "source_secondary": "unknown",
                        "preferred_source": "raw_pbp",
                        "confidence": "legacy",
                        "validation_artifacts": [],
                        "supersedes": [],
                        "date_added": "2026-03-22",
                        "notes": "",
                    },
                ],
                "residual_annotations": [],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    _write_csv(
        run_dir / "event_player_on_court_issues_1997.csv",
        [
            {
                "game_id": 29600070,
                "team_id": 100,
                "player_id": 101,
                "player_name": "Alpha",
                "period": 2,
                "event_num": 5,
                "status": "off_court_credit",
            }
        ],
    )
    _write_csv(
        run_dir / "event_player_on_court_issues_1998.csv",
        [
            {
                "game_id": 29700070,
                "team_id": 200,
                "player_id": 201,
                "player_name": "Beta",
                "period": 3,
                "event_num": 8,
                "status": "off_court_credit",
            }
        ],
    )
    (run_dir / "summary_1997.json").write_text(
        json.dumps({"failed_games": 0, "event_stats_errors": 0}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (run_dir / "summary_1998.json").write_text(
        json.dumps({"failed_games": 1, "event_stats_errors": 2}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    _run_builder(repo_root, run_dir, output_dir, manifest_path, seasons=[1997])

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["selected_seasons"] == [1997]
    assert summary["raw_counts"]["failed_games"] == 0
    assert summary["raw_counts"]["event_stats_errors"] == 0

    _, rows = _read_csv_rows(output_dir / "game_quality.csv")
    assert sorted(row["game_id"] for row in rows) == ["0029600004", "0029600070"]

    _, open_rows = _read_csv_rows(output_dir / "raw_open_games.csv")
    assert [row["game_id"] for row in open_rows] == ["0029600070"]
