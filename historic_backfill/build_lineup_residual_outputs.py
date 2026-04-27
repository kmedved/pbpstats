from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from lineup_correction_manifest import DEFAULT_MANIFEST_PATH, RESIDUAL_CLASS_VALUES, load_manifest
from reviewed_release_policy import (
    RELEASE_POLICY_OUTPUT_COLUMNS,
    apply_release_policy,
    load_reviewed_policy_overlay,
)


ROOT = Path(__file__).resolve().parent
QUALITY_PRECEDENCE = ["open", "source_limited", "boundary_difference", "override_corrected", "exact"]
RESIDUAL_BASE_COLUMNS = [
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
]
RESIDUAL_OUTPUT_COLUMNS = RESIDUAL_BASE_COLUMNS + [
    "manual_annotation_id",
    "manual_residual_class",
    "manual_status",
    "manual_notes",
    "effective_residual_class",
    "effective_is_blocking",
]
GAME_QUALITY_OUTPUT_COLUMNS = [
    "game_id",
    "primary_quality_status",
    *RELEASE_POLICY_OUTPUT_COLUMNS,
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


def _normalize_game_id(value: Any) -> str:
    return str(int(value)).zfill(10)


def _infer_season_from_game_id(game_id: str) -> int:
    normalized = _normalize_game_id(game_id)
    season_suffix = int(normalized[3:5])
    return (1900 + season_suffix + 1) if season_suffix >= 96 else (2000 + season_suffix + 1)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build residual annotation, blocker-count, and per-game quality outputs from a rerun/block directory."
    )
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--reviewed-policy-overlay-csv", type=Path)
    parser.add_argument("--seasons", nargs="*", type=int)
    parser.add_argument("--material-minute-threshold", type=float, default=0.1)
    parser.add_argument("--severe-minute-threshold", type=float, default=0.5)
    return parser.parse_args()


def _active_corrections_by_game(manifest: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for correction in manifest.get("corrections", []):
        if correction.get("status") != "active":
            continue
        if correction.get("domain") != "lineup":
            continue
        result.setdefault(_normalize_game_id(correction["game_id"]), []).append(correction)
    return result


def _run_seasons(run_dir: Path, requested_seasons: set[int] | None = None) -> set[int]:
    seasons: set[int] = set()
    for pattern in ("event_player_on_court_issues_*.csv", "minutes_plus_minus_audit_*.csv", "summary_*.json"):
        for path in sorted(run_dir.glob(pattern)):
            try:
                season = int(path.stem.rsplit("_", 1)[-1])
            except ValueError:
                continue
            if requested_seasons is not None and season not in requested_seasons:
                continue
            seasons.add(season)
    return seasons


def _annotation_applies(row: dict[str, Any], annotation: dict[str, Any]) -> bool:
    if annotation.get("status") == "rejected":
        return False
    if _normalize_game_id(annotation["game_id"]) != row["game_id"]:
        return False
    optional_matches = {
        "period": row.get("period"),
        "team_id": row.get("team_id"),
        "player_id": row.get("player_id"),
        "event_num": row.get("event_num"),
    }
    for key, row_value in optional_matches.items():
        annotation_value = annotation.get(key)
        if annotation_value in (None, "", 0):
            continue
        if int(annotation_value) != int(row_value or 0):
            return False
    return True


def _apply_annotations(
    row: dict[str, Any],
    annotations: list[dict[str, Any]],
) -> dict[str, Any]:
    manual = None
    for annotation in annotations:
        if _annotation_applies(row, annotation):
            manual = annotation
            break
    result = dict(row)
    result["manual_annotation_id"] = manual.get("annotation_id") if manual is not None else ""
    result["manual_residual_class"] = manual.get("residual_class") if manual is not None else ""
    result["manual_status"] = manual.get("status") if manual is not None else ""
    result["manual_notes"] = manual.get("notes") if manual is not None else ""
    result["effective_residual_class"] = (
        result["manual_residual_class"] or result["computed_residual_class"]
    )
    return result


def _event_rows_from_run(run_dir: Path, selected_seasons: set[int] | None = None) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for csv_path in sorted(run_dir.glob("event_player_on_court_issues_*.csv")):
        season = int(csv_path.stem.rsplit("_", 1)[-1])
        if selected_seasons is not None and season not in selected_seasons:
            continue
        df = pd.read_csv(csv_path)
        if df.empty:
            continue
        for record in df.to_dict(orient="records"):
            rows.append(
                {
                    "grain": "event",
                    "residual_source": "event_on_court",
                    "game_id": _normalize_game_id(record["game_id"]),
                    "season": season,
                    "team_id": int(record.get("team_id") or 0),
                    "player_id": int(record.get("player_id") or 0),
                    "player_name": str(record.get("player_name") or ""),
                    "period": int(record.get("period") or 0),
                    "event_num": int(record.get("event_num") or 0),
                    "minutes_abs_diff": 0.0,
                    "plus_minus_diff": 0.0,
                    "status_detail": str(record.get("status") or ""),
                    "computed_residual_class": "fixable_lineup_defect",
                    "blocking_reason": "event_on_court",
                    "is_blocking": True,
                    "has_minutes_mismatch": False,
                    "has_plus_minus_reference_delta": False,
                    "is_minutes_outlier": False,
                }
            )
    return pd.DataFrame(rows)


def _minutes_rows_from_run(
    run_dir: Path,
    *,
    selected_seasons: set[int] | None,
    material_minute_threshold: float,
    severe_minute_threshold: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    pm_rows: list[dict[str, Any]] = []
    for csv_path in sorted(run_dir.glob("minutes_plus_minus_audit_*.csv")):
        season = int(csv_path.stem.rsplit("_", 1)[-1])
        if selected_seasons is not None and season not in selected_seasons:
            continue
        df = pd.read_csv(csv_path)
        if df.empty:
            continue
        for record in df.to_dict(orient="records"):
            game_id = _normalize_game_id(record["game_id"])
            minutes_abs_diff = float(record.get("Minutes_abs_diff") or 0.0)
            plus_minus_diff = float(record.get("Plus_Minus_diff") or 0.0)
            has_minutes_mismatch = bool(record.get("has_minutes_mismatch"))
            has_plus_minus_reference_delta = bool(record.get("has_plus_minus_mismatch"))
            is_minutes_outlier = bool(record.get("is_minutes_outlier"))
            if not (has_minutes_mismatch or has_plus_minus_reference_delta or is_minutes_outlier):
                continue

            blocking_reason = ""
            computed_residual_class = "unknown"
            is_blocking = False
            if is_minutes_outlier or minutes_abs_diff > severe_minute_threshold:
                blocking_reason = "severe_minute_issue"
                computed_residual_class = "fixable_lineup_defect"
                is_blocking = True
            elif minutes_abs_diff > material_minute_threshold:
                blocking_reason = "material_minute_issue"
                computed_residual_class = "fixable_lineup_defect"
                is_blocking = True
            elif has_plus_minus_reference_delta:
                blocking_reason = "plus_minus_reference_only"
                computed_residual_class = "candidate_boundary_difference"

            base_row = {
                "grain": "player_game",
                "residual_source": "minutes_plus_minus",
                "game_id": game_id,
                "season": season,
                "team_id": int(record.get("team_id") or 0),
                "player_id": int(record.get("player_id") or 0),
                "player_name": str(record.get("player_name") or ""),
                "period": 0,
                "event_num": 0,
                "minutes_abs_diff": minutes_abs_diff,
                "plus_minus_diff": plus_minus_diff,
                "status_detail": "",
                "computed_residual_class": computed_residual_class,
                "blocking_reason": blocking_reason,
                "is_blocking": is_blocking,
                "has_minutes_mismatch": has_minutes_mismatch,
                "has_plus_minus_reference_delta": has_plus_minus_reference_delta,
                "is_minutes_outlier": is_minutes_outlier,
            }
            rows.append(base_row)
            if has_plus_minus_reference_delta:
                pm_rows.append(
                    {
                        **base_row,
                        "residual_source": "plus_minus_reference_delta",
                    }
                )
    return pd.DataFrame(rows), pd.DataFrame(pm_rows)


def _load_run_level_counts(run_dir: Path, selected_seasons: set[int] | None = None) -> dict[str, int]:
    result = {"failed_games": 0, "event_stats_errors": 0}
    for path in sorted(run_dir.glob("summary_*.json")):
        season = int(path.stem.rsplit("_", 1)[-1])
        if selected_seasons is not None and season not in selected_seasons:
            continue
        summary = _load_json(path)
        result["failed_games"] += int(summary.get("failed_games", 0) or 0)
        result["event_stats_errors"] += int(summary.get("event_stats_errors", 0) or 0)
    return result


def _compute_primary_quality_status(row: dict[str, Any]) -> str:
    if row["has_open_actionable_residual"]:
        return "open"
    if row["has_source_limited_residual"]:
        return "source_limited"
    if row["has_boundary_difference"]:
        return "boundary_difference"
    if row["has_active_correction"]:
        return "override_corrected"
    return "exact"


def main() -> int:
    args = parse_args()
    run_dir = args.run_dir.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(args.manifest_path.resolve())
    reviewed_policy = load_reviewed_policy_overlay(args.reviewed_policy_overlay_csv)
    annotations = list(manifest.get("residual_annotations") or [])
    active_corrections = _active_corrections_by_game(manifest)
    requested_seasons = set(args.seasons or []) or None
    run_seasons = _run_seasons(run_dir, requested_seasons)
    if requested_seasons is not None and run_seasons != requested_seasons:
        missing = sorted(requested_seasons - run_seasons)
        if missing:
            raise ValueError(f"Requested seasons missing from run dir: {missing}")
    if run_seasons:
        active_corrections = {
            game_id: corrections
            for game_id, corrections in active_corrections.items()
            if _infer_season_from_game_id(game_id) in run_seasons
        }

    event_df = _event_rows_from_run(run_dir, run_seasons if run_seasons else requested_seasons)
    minutes_df, pm_df = _minutes_rows_from_run(
        run_dir,
        selected_seasons=run_seasons if run_seasons else requested_seasons,
        material_minute_threshold=float(args.material_minute_threshold),
        severe_minute_threshold=float(args.severe_minute_threshold),
    )

    residual_df = pd.concat([event_df, minutes_df], ignore_index=True) if not event_df.empty or not minutes_df.empty else pd.DataFrame(
        columns=RESIDUAL_BASE_COLUMNS
    )

    residual_rows = [_apply_annotations(row, annotations) for row in residual_df.to_dict(orient="records")]
    for row in residual_rows:
        effective_class = row["effective_residual_class"]
        if effective_class not in RESIDUAL_CLASS_VALUES:
            raise ValueError(f"Unsupported effective residual class: {effective_class}")
        row["effective_is_blocking"] = bool(row["is_blocking"])
        if effective_class in {"source_limited_upstream_error", "boundary_difference", "candidate_boundary_difference"}:
            row["effective_is_blocking"] = False
    residual_df = pd.DataFrame(residual_rows, columns=RESIDUAL_OUTPUT_COLUMNS)
    if residual_df.empty:
        residual_df = pd.DataFrame(columns=RESIDUAL_OUTPUT_COLUMNS)

    if not pm_df.empty:
        pm_rows = [_apply_annotations(row, annotations) for row in pm_df.to_dict(orient="records")]
        for row in pm_rows:
            if row["effective_residual_class"] == "unknown":
                row["effective_residual_class"] = "candidate_boundary_difference"
            row["effective_is_blocking"] = False
        pm_df = pd.DataFrame(pm_rows, columns=RESIDUAL_OUTPUT_COLUMNS)
    else:
        pm_df = pd.DataFrame(columns=RESIDUAL_OUTPUT_COLUMNS)

    residual_df.to_csv(output_dir / "residual_annotations.csv", index=False)
    residual_df.loc[residual_df["effective_is_blocking"].fillna(False)].to_csv(
        output_dir / "actionable_queue.csv",
        index=False,
    )
    residual_df.loc[
        residual_df["effective_residual_class"] == "source_limited_upstream_error"
    ].to_csv(output_dir / "source_limited_residuals.csv", index=False)
    residual_df.loc[
        residual_df["effective_residual_class"].isin(["candidate_boundary_difference", "boundary_difference"])
    ].to_csv(output_dir / "boundary_difference_residuals.csv", index=False)
    pm_df.to_csv(output_dir / "plus_minus_reference_delta_register.csv", index=False)

    grouped_game_metrics = pd.DataFrame()
    if not residual_df.empty:
        grouped_source = residual_df.copy()
        grouped_source["effective_is_blocking_bool"] = grouped_source["effective_is_blocking"].fillna(False).astype(bool)
        grouped_source["source_limited_flag"] = (
            grouped_source["effective_residual_class"] == "source_limited_upstream_error"
        )
        grouped_source["boundary_difference_flag"] = grouped_source["effective_residual_class"].isin(
            ["candidate_boundary_difference", "boundary_difference"]
        )
        grouped_source["material_minute_flag"] = grouped_source["minutes_abs_diff"] > args.material_minute_threshold
        grouped_source["severe_minute_flag"] = grouped_source["minutes_abs_diff"] > args.severe_minute_threshold
        grouped_source["event_on_court_flag"] = grouped_source["residual_source"] == "event_on_court"
        grouped_source["actionable_event_flag"] = (
            grouped_source["event_on_court_flag"] & grouped_source["effective_is_blocking_bool"]
        )
        grouped_source["minute_diff_over_material"] = grouped_source["minutes_abs_diff"].where(
            grouped_source["material_minute_flag"],
            0.0,
        )
        grouped_source["pm_reference_flag"] = grouped_source["has_plus_minus_reference_delta"].fillna(False).astype(bool)

        grouped_game_metrics = (
            grouped_source.groupby("game_id", sort=True)
            .agg(
                has_open_actionable_residual=("effective_is_blocking_bool", "any"),
                has_source_limited_residual=("source_limited_flag", "any"),
                has_boundary_difference=("boundary_difference_flag", "any"),
                has_material_minute_issue=("material_minute_flag", "any"),
                has_severe_minute_issue=("severe_minute_flag", "any"),
                has_event_on_court_issue=("event_on_court_flag", "any"),
                n_actionable_event_rows=("actionable_event_flag", "sum"),
                max_abs_minute_diff=("minutes_abs_diff", "max"),
                sum_abs_minute_diff_over_0_1=("minute_diff_over_material", "sum"),
                n_pm_reference_delta_rows=("pm_reference_flag", "sum"),
            )
            .reset_index()
        )

    if not grouped_game_metrics.empty:
        grouped_game_metrics = grouped_game_metrics.set_index("game_id")
    else:
        grouped_game_metrics = pd.DataFrame(index=pd.Index([], name="game_id"))

    game_rows: list[dict[str, Any]] = []
    all_game_ids = sorted(
        set(grouped_game_metrics.index.tolist())
        | set(active_corrections.keys())
    )
    for game_id in all_game_ids:
        active = active_corrections.get(game_id, [])
        metrics = grouped_game_metrics.loc[game_id].to_dict() if game_id in grouped_game_metrics.index else {}
        row = {
            "game_id": game_id,
            "primary_quality_status": "exact",
            "has_active_correction": bool(active),
            "has_open_actionable_residual": bool(metrics.get("has_open_actionable_residual", False)),
            "has_source_limited_residual": bool(metrics.get("has_source_limited_residual", False)),
            "has_boundary_difference": bool(metrics.get("has_boundary_difference", False)),
            "has_material_minute_issue": bool(metrics.get("has_material_minute_issue", False)),
            "has_severe_minute_issue": bool(metrics.get("has_severe_minute_issue", False)),
            "has_event_on_court_issue": bool(metrics.get("has_event_on_court_issue", False)),
            "n_active_corrections": len(active),
            "n_actionable_event_rows": int(metrics.get("n_actionable_event_rows", 0) or 0),
            "max_abs_minute_diff": float(metrics.get("max_abs_minute_diff", 0.0) or 0.0),
            "sum_abs_minute_diff_over_0_1": float(metrics.get("sum_abs_minute_diff_over_0_1", 0.0) or 0.0),
            "n_pm_reference_delta_rows": int(metrics.get("n_pm_reference_delta_rows", 0) or 0),
        }
        row["primary_quality_status"] = _compute_primary_quality_status(row)
        row.update(apply_release_policy(row, reviewed_policy))
        game_rows.append(row)

    game_quality_df = pd.DataFrame(game_rows, columns=GAME_QUALITY_OUTPUT_COLUMNS)
    if not game_quality_df.empty:
        order_map = {status: index for index, status in enumerate(QUALITY_PRECEDENCE)}
        game_quality_df["__quality_order"] = game_quality_df["primary_quality_status"].map(order_map)
        game_quality_df = game_quality_df.sort_values(["__quality_order", "game_id"]).drop(columns="__quality_order")
    game_quality_df.to_csv(output_dir / "game_quality.csv", index=False)
    raw_open_games_df = game_quality_df.loc[game_quality_df["primary_quality_status"] == "open"].copy()
    raw_open_games_df.to_csv(output_dir / "raw_open_games.csv", index=False)

    run_counts = _load_run_level_counts(run_dir, run_seasons if run_seasons else requested_seasons)
    raw_counts = {
        "failed_games": run_counts["failed_games"],
        "event_stats_errors": run_counts["event_stats_errors"],
        "event_on_court_issue_rows": int((residual_df["residual_source"] == "event_on_court").sum()) if not residual_df.empty else 0,
        "minutes_mismatch_rows": int(residual_df["has_minutes_mismatch"].fillna(False).sum()) if not residual_df.empty else 0,
        "minutes_outlier_rows": int(residual_df["is_minutes_outlier"].fillna(False).sum()) if not residual_df.empty else 0,
        "plus_minus_reference_delta_rows": int(residual_df["has_plus_minus_reference_delta"].fillna(False).sum()) if not residual_df.empty else 0,
        "candidate_boundary_difference_rows": int(
            (residual_df["effective_residual_class"] == "candidate_boundary_difference").sum()
        ) if not residual_df.empty else 0,
        "source_limited_residual_rows": int(
            (residual_df["effective_residual_class"] == "source_limited_upstream_error").sum()
        ) if not residual_df.empty else 0,
    }
    blocker_counts = {
        "failed_games": run_counts["failed_games"],
        "event_stats_errors": run_counts["event_stats_errors"],
        "actionable_residual_rows": int(residual_df["effective_is_blocking"].fillna(False).sum()) if not residual_df.empty else 0,
        "actionable_event_on_court_rows": int(
            residual_df.loc[
                (residual_df["residual_source"] == "event_on_court")
                & (residual_df["effective_is_blocking"].fillna(False))
            ].shape[0]
        ) if not residual_df.empty else 0,
        "severe_minute_rows": int(
            residual_df.loc[
                (residual_df["minutes_abs_diff"] > args.severe_minute_threshold)
                & (residual_df["effective_is_blocking"].fillna(False))
            ].shape[0]
        ) if not residual_df.empty else 0,
        "material_minute_rows": int(
            residual_df.loc[
                (residual_df["minutes_abs_diff"] > args.material_minute_threshold)
                & (residual_df["effective_is_blocking"].fillna(False))
            ].shape[0]
        ) if not residual_df.empty else 0,
    }
    summary = {
        "run_dir": str(run_dir),
        "manifest_path": str(args.manifest_path.resolve()),
        "selected_seasons": sorted(run_seasons) if run_seasons else [],
        "reviewed_policy_overlay_csv": (
            str(args.reviewed_policy_overlay_csv.resolve()) if args.reviewed_policy_overlay_csv is not None else ""
        ),
        "reviewed_policy_overlay_version": "",
        "raw_counts": raw_counts,
        "blocker_counts": blocker_counts,
        "quality_status_counts": game_quality_df["primary_quality_status"].value_counts().sort_index().to_dict() if not game_quality_df.empty else {},
        "raw_quality_status_counts": game_quality_df["primary_quality_status"].value_counts().sort_index().to_dict() if not game_quality_df.empty else {},
        "release_gate_status_counts": game_quality_df["release_gate_status"].value_counts().sort_index().to_dict() if not game_quality_df.empty else {},
        "execution_lane_counts": game_quality_df["execution_lane"].value_counts().sort_index().to_dict() if not game_quality_df.empty else {},
        "release_blocking_game_count": int(game_quality_df["blocks_release"].fillna(False).sum()) if not game_quality_df.empty else 0,
        "release_blocking_game_ids": (
            sorted(game_quality_df.loc[game_quality_df["blocks_release"].fillna(False), "game_id"].astype(str).tolist())
            if not game_quality_df.empty
            else []
        ),
        "research_open_game_count": int(game_quality_df["research_open"].fillna(False).sum()) if not game_quality_df.empty else 0,
        "research_open_game_ids": (
            sorted(game_quality_df.loc[game_quality_df["research_open"].fillna(False), "game_id"].astype(str).tolist())
            if not game_quality_df.empty
            else []
        ),
        "tier1_release_ready": bool(
            run_counts["failed_games"] == 0
            and run_counts["event_stats_errors"] == 0
            and (int(game_quality_df["blocks_release"].fillna(False).sum()) if not game_quality_df.empty else 0) == 0
        ),
        "tier2_frontier_closed": bool(
            run_counts["failed_games"] == 0
            and run_counts["event_stats_errors"] == 0
            and (int(game_quality_df["research_open"].fillna(False).sum()) if not game_quality_df.empty else 0) == 0
            and (int(game_quality_df["blocks_release"].fillna(False).sum()) if not game_quality_df.empty else 0) == 0
        ),
    }
    overlay_versions = sorted(
        {
            str(policy.get("policy_decision_id") or "")
            for policy in reviewed_policy.values()
            if str(policy.get("policy_decision_id") or "")
        }
    )
    if len(overlay_versions) > 1:
        raise ValueError(f"Expected at most one reviewed policy overlay version, found {overlay_versions}")
    summary["reviewed_policy_overlay_version"] = overlay_versions[0] if overlay_versions else ""
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
