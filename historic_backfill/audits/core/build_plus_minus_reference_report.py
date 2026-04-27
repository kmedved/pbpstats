from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from historic_backfill.audits.core.reviewed_release_policy import apply_release_policy, ensure_release_policy_columns, load_reviewed_policy_overlay


REPORT_CLASS_ALIAS = {
    "candidate_boundary_difference": "reference_only_boundary",
    "source_limited_upstream_error": "source_limited_upstream",
    "lineup_related": "open_lineup_blocker",
}


def _overlay_version(overlay_csv: Path | None) -> str:
    reviewed_policy = load_reviewed_policy_overlay(overlay_csv)
    versions = sorted(
        {
            str(policy.get("policy_decision_id") or "")
            for policy in reviewed_policy.values()
            if str(policy.get("policy_decision_id") or "")
        }
    )
    if len(versions) > 1:
        raise ValueError(f"Expected exactly one reviewed policy overlay version, found {versions}")
    return versions[0] if versions else ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a phase-5 plus-minus reference characterization report from a residual bundle."
    )
    parser.add_argument("--residual-dir", type=Path, required=True, action="append")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--material-minute-threshold", type=float, default=0.1)
    parser.add_argument("--sample-per-bucket", type=int, default=1)
    parser.add_argument("--lane-map-csv", type=Path)
    parser.add_argument("--reviewed-policy-overlay-csv", type=Path)
    return parser.parse_args()


def _era_bucket(season: int) -> str:
    if season <= 2000:
        return "1997-2000"
    if season <= 2005:
        return "2001-2005"
    if season <= 2010:
        return "2006-2010"
    if season <= 2016:
        return "2011-2016"
    return "2017-2020+"


def _abs_pm_bucket(value: float) -> str:
    absolute = abs(value)
    if absolute <= 1:
        return "1"
    if absolute <= 2:
        return "2"
    if absolute <= 4:
        return "3-4"
    return "5+"


def _game_type(game_id: str) -> str:
    if str(game_id).startswith("004"):
        return "playoffs"
    return "regular_season"


def _classify_row(row: pd.Series, *, material_minute_threshold: float) -> str:
    effective_class = str(row.get("effective_residual_class") or "")
    if effective_class == "source_limited_upstream_error":
        return "source_limited_upstream_error"

    primary_quality_status = str(row.get("primary_quality_status") or "")
    if primary_quality_status == "source_limited":
        return "source_limited_upstream_error"

    minutes_abs_diff = float(row.get("minutes_abs_diff") or 0.0)
    if (
        minutes_abs_diff > material_minute_threshold
        or bool(row.get("has_material_minute_issue"))
        or bool(row.get("has_severe_minute_issue"))
        or bool(row.get("has_event_on_court_issue"))
        or primary_quality_status == "open"
    ):
        return "lineup_related"
    if effective_class in {"candidate_boundary_difference", "boundary_difference"} or primary_quality_status == "boundary_difference":
        return "candidate_boundary_difference"
    return "unknown"


def _alias_class(value: str) -> str:
    return REPORT_CLASS_ALIAS.get(str(value), str(value))


def _release_pm_class(row: pd.Series) -> str:
    raw_pm_class = str(row.get("pm_residual_class") or row.get("pm_characterization") or "")
    release_gate_status = str(row.get("release_gate_status") or "")
    if raw_pm_class == "source_limited_upstream_error":
        return "source_limited_upstream"
    if raw_pm_class in {"candidate_boundary_difference", "lineup_related"} and release_gate_status == "accepted_boundary_difference":
        return "reference_only_boundary"
    if raw_pm_class in {"candidate_boundary_difference", "lineup_related"} and release_gate_status == "accepted_unresolvable_contradiction":
        return "accepted_contradiction"
    if raw_pm_class in {"candidate_boundary_difference", "lineup_related"} and release_gate_status == "documented_hold":
        return "documented_hold"
    if release_gate_status == "open_actionable" and raw_pm_class == "lineup_related":
        return "open_actionable_lineup_blocker"
    raise ValueError(
        "Unmapped raw PM class x release_gate_status combination: "
        f"raw_pm_class={raw_pm_class!r}, release_gate_status={release_gate_status!r}, "
        f"game_id={row.get('game_id')!r}, player_id={row.get('player_id')!r}"
    )


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    residual_dirs = [path.resolve() for path in args.residual_dir]
    reviewed_policy_overlay_version = _overlay_version(
        args.reviewed_policy_overlay_csv.resolve() if args.reviewed_policy_overlay_csv is not None else None
    )
    frontier_inventory_snapshot_id = args.lane_map_csv.resolve().stem if args.lane_map_csv is not None else ""

    lane_map_df = pd.DataFrame()
    if args.lane_map_csv is not None:
        lane_map_df = pd.read_csv(args.lane_map_csv)
        if not lane_map_df.empty:
            lane_map_df["game_id"] = lane_map_df["game_id"].astype(str).str.zfill(10)

    pm_frames: list[pd.DataFrame] = []
    game_quality_frames: list[pd.DataFrame] = []
    for bundle_order, residual_dir in enumerate(residual_dirs):
        pm_path = residual_dir / "plus_minus_reference_delta_register.csv"
        game_quality_path = residual_dir / "game_quality.csv"
        pm_df = pd.read_csv(pm_path)
        game_quality_df = pd.read_csv(game_quality_path)
        game_quality_df = ensure_release_policy_columns(game_quality_df)
        if not pm_df.empty:
            pm_df["residual_bundle"] = residual_dir.name
            pm_df["bundle_order"] = bundle_order
            pm_frames.append(pm_df)
        if not game_quality_df.empty:
            game_quality_df["residual_bundle"] = residual_dir.name
            game_quality_df["bundle_order"] = bundle_order
            game_quality_frames.append(game_quality_df)

    pm_df = pd.concat(pm_frames, ignore_index=True) if pm_frames else pd.DataFrame()
    game_quality_df = pd.concat(game_quality_frames, ignore_index=True) if game_quality_frames else pd.DataFrame()

    if pm_df.empty:
        summary = {
            "residual_dirs": [str(path) for path in residual_dirs],
            "reviewed_policy_overlay_version": reviewed_policy_overlay_version,
            "frontier_inventory_snapshot_id": frontier_inventory_snapshot_id,
            "total_pm_reference_delta_rows": 0,
            "class_counts": {},
            "report_class_counts": [],
            "release_class_counts": [],
            "era_counts": {},
            "sample_row_count": 0,
            "release_blocker_game_count": 0,
            "reviewed_frontier_queue_game_count": 0,
        }
        (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        pd.DataFrame().to_csv(output_dir / "pm_reference_characterization.csv", index=False)
        pd.DataFrame().to_csv(output_dir / "candidate_boundary_difference_sample.csv", index=False)
        pd.DataFrame().to_csv(output_dir / "pm_reference_only_sample.csv", index=False)
        pd.DataFrame().to_csv(output_dir / "pm_source_limited_games.csv", index=False)
        pd.DataFrame().to_csv(output_dir / "pm_open_game_queue.csv", index=False)
        pd.DataFrame().to_csv(output_dir / "pm_release_blocker_queue.csv", index=False)
        pd.DataFrame().to_csv(output_dir / "pm_reviewed_frontier_queue.csv", index=False)
        pd.DataFrame().to_csv(output_dir / "pm_lane_summary.csv", index=False)
        pd.DataFrame().to_csv(output_dir / "pm_release_lane_summary.csv", index=False)
        pd.DataFrame().to_csv(output_dir / "pm_reviewed_frontier_lane_summary.csv", index=False)
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0

    pm_df["game_id"] = pm_df["game_id"].astype(str).str.zfill(10)
    game_quality_df["game_id"] = game_quality_df["game_id"].astype(str).str.zfill(10)
    pm_df = (
        pm_df.sort_values("bundle_order")
        .drop_duplicates(
            subset=[
                "grain",
                "residual_source",
                "game_id",
                "season",
                "team_id",
                "player_id",
                "period",
                "event_num",
            ],
            keep="last",
        )
        .reset_index(drop=True)
    )
    game_quality_df = (
        game_quality_df.sort_values("bundle_order")
        .drop_duplicates(subset=["residual_bundle", "game_id"], keep="last")
        .reset_index(drop=True)
    )
    merged = pm_df.merge(
        game_quality_df[
            [
                "residual_bundle",
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
            ]
        ],
        on=["residual_bundle", "game_id"],
        how="left",
    )

    merged["season"] = merged["season"].astype(int)
    merged["minutes_abs_diff"] = merged["minutes_abs_diff"].astype(float)
    merged["plus_minus_diff"] = merged["plus_minus_diff"].astype(float)
    merged["abs_plus_minus_diff"] = merged["plus_minus_diff"].abs()
    merged["era_bucket"] = merged["season"].map(_era_bucket)
    merged["abs_plus_minus_bucket"] = merged["abs_plus_minus_diff"].map(_abs_pm_bucket)
    merged["game_type"] = merged["game_id"].map(_game_type)
    merged["pm_characterization"] = merged.apply(
        lambda row: _classify_row(row, material_minute_threshold=float(args.material_minute_threshold)),
        axis=1,
    )
    merged["pm_residual_class"] = merged["pm_characterization"]
    merged["report_pm_characterization"] = merged["pm_characterization"].map(_alias_class)
    merged["state_context"] = "live_state"

    if args.reviewed_policy_overlay_csv is not None:
        reviewed_policy = load_reviewed_policy_overlay(args.reviewed_policy_overlay_csv.resolve())
        updated_rows = []
        for row in merged.to_dict(orient="records"):
            row.update(apply_release_policy(row, reviewed_policy))
            updated_rows.append(row)
        merged = pd.DataFrame(updated_rows)

    merged["release_pm_class"] = merged.apply(_release_pm_class, axis=1)

    merged.to_csv(output_dir / "pm_reference_characterization.csv", index=False)

    boundary_sample = (
        merged.loc[
            (merged["pm_characterization"] == "candidate_boundary_difference")
            & (merged["primary_quality_status"] == "boundary_difference")
        ]
        .sort_values(
            [
                "era_bucket",
                "abs_plus_minus_bucket",
                "game_type",
                "abs_plus_minus_diff",
                "game_id",
                "team_id",
                "player_id",
            ],
            ascending=[True, True, True, False, True, True, True],
        )
        .groupby(["era_bucket", "abs_plus_minus_bucket", "game_type"], as_index=False, sort=True)
        .head(int(args.sample_per_bucket))
    )
    boundary_sample.to_csv(output_dir / "candidate_boundary_difference_sample.csv", index=False)
    boundary_sample.to_csv(output_dir / "pm_reference_only_sample.csv", index=False)

    def _build_game_summary(frame: pd.DataFrame) -> pd.DataFrame:
        summary = (
            frame.groupby("game_id", as_index=False)
            .agg(
                season=("season", "min"),
                pm_row_count=("game_id", "size"),
                max_abs_pm_diff=("abs_plus_minus_diff", "max"),
                max_abs_minute_diff=("minutes_abs_diff", "max"),
                has_material_minute_issue=("has_material_minute_issue", "max"),
                has_event_on_court_issue=("has_event_on_court_issue", "max"),
            )
        )
        return summary.sort_values(["pm_row_count", "max_abs_pm_diff", "game_id"], ascending=[False, False, True])

    source_limited_games = _build_game_summary(
        merged.loc[merged["report_pm_characterization"] == "source_limited_upstream"]
    )
    source_limited_games.to_csv(output_dir / "pm_source_limited_games.csv", index=False)

    open_game_queue = _build_game_summary(
        merged.loc[merged["report_pm_characterization"] == "open_lineup_blocker"]
    )
    if not lane_map_df.empty:
        open_game_queue = open_game_queue.merge(lane_map_df, on="game_id", how="left")
    else:
        open_game_queue["lane"] = ""
        open_game_queue["recommended_next_action"] = ""
        open_game_queue["notes"] = ""
    open_game_queue.to_csv(output_dir / "pm_open_game_queue.csv", index=False)

    release_blocker_queue = _build_game_summary(
        merged.loc[merged["release_pm_class"] == "open_actionable_lineup_blocker"]
    )
    if not lane_map_df.empty:
        release_blocker_queue = release_blocker_queue.merge(lane_map_df, on="game_id", how="left")
    else:
        release_blocker_queue["lane"] = ""
        release_blocker_queue["recommended_next_action"] = ""
        release_blocker_queue["notes"] = ""
    release_blocker_queue.to_csv(output_dir / "pm_release_blocker_queue.csv", index=False)

    reviewed_frontier_queue = _build_game_summary(
        merged.loc[merged["release_pm_class"].isin(["reference_only_boundary", "accepted_contradiction", "documented_hold"])]
    )
    if not lane_map_df.empty:
        reviewed_frontier_queue = reviewed_frontier_queue.merge(lane_map_df, on="game_id", how="left")
        contradiction_games = set(
            merged.loc[merged["release_pm_class"] == "accepted_contradiction", "game_id"].astype(str).unique().tolist()
        )
        contradiction_mask = reviewed_frontier_queue["game_id"].astype(str).isin(contradiction_games)
        reviewed_frontier_queue.loc[contradiction_mask, "recommended_next_action"] = "accept_unresolvable_contradiction"
        reviewed_frontier_queue.loc[
            contradiction_mask,
            "notes",
        ] = "Reviewed accepted contradiction; final classification, not a research-open hold."
    else:
        reviewed_frontier_queue["lane"] = ""
        reviewed_frontier_queue["recommended_next_action"] = ""
        reviewed_frontier_queue["notes"] = ""
    reviewed_frontier_queue.to_csv(output_dir / "pm_reviewed_frontier_queue.csv", index=False)

    if not open_game_queue.empty:
        lane_summary_source = open_game_queue.fillna({"lane": "", "recommended_next_action": ""})
        lane_summary = (
            lane_summary_source.groupby(["lane"], dropna=False, as_index=False)
            .agg(
                recommended_next_action=(
                    "recommended_next_action",
                    lambda values: " | ".join(sorted({str(value) for value in values if str(value)})),
                ),
                row_count=("pm_row_count", "sum"),
                game_count=("game_id", "nunique"),
            )
            .sort_values(["row_count", "game_count", "lane"], ascending=[False, False, True])
        )
    else:
        lane_summary = pd.DataFrame(columns=["lane", "recommended_next_action", "row_count", "game_count"])
    lane_summary.to_csv(output_dir / "pm_lane_summary.csv", index=False)

    def _build_lane_summary(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=["lane", "recommended_next_action", "row_count", "game_count"])
        lane_summary_source = frame.fillna({"lane": "", "recommended_next_action": ""})
        return (
            lane_summary_source.groupby(["lane"], dropna=False, as_index=False)
            .agg(
                recommended_next_action=(
                    "recommended_next_action",
                    lambda values: " | ".join(sorted({str(value) for value in values if str(value)})),
                ),
                row_count=("pm_row_count", "sum"),
                game_count=("game_id", "nunique"),
            )
            .sort_values(["row_count", "game_count", "lane"], ascending=[False, False, True])
        )

    release_lane_summary = _build_lane_summary(release_blocker_queue)
    release_lane_summary.to_csv(output_dir / "pm_release_lane_summary.csv", index=False)

    reviewed_frontier_lane_summary = _build_lane_summary(reviewed_frontier_queue)
    reviewed_frontier_lane_summary.to_csv(output_dir / "pm_reviewed_frontier_lane_summary.csv", index=False)

    class_counts = merged["pm_characterization"].value_counts().sort_index().to_dict()
    report_class_counts = (
        merged.groupby("report_pm_characterization")
        .agg(row_count=("game_id", "size"), game_count=("game_id", "nunique"))
        .reset_index()
        .sort_values("report_pm_characterization")
        .to_dict(orient="records")
    )
    release_class_counts = (
        merged.groupby("release_pm_class")
        .agg(row_count=("game_id", "size"), game_count=("game_id", "nunique"))
        .reset_index()
        .sort_values("release_pm_class")
        .to_dict(orient="records")
    )
    era_counts = (
        merged.groupby(["era_bucket", "pm_characterization"]).size().rename("row_count").reset_index().to_dict(orient="records")
    )
    game_type_counts = (
        merged.groupby(["game_type", "pm_characterization"]).size().rename("row_count").reset_index().to_dict(orient="records")
    )
    summary = {
        "residual_dirs": [str(path) for path in residual_dirs],
        "reviewed_policy_overlay_csv": (
            str(args.reviewed_policy_overlay_csv.resolve()) if args.reviewed_policy_overlay_csv is not None else ""
        ),
        "reviewed_policy_overlay_version": reviewed_policy_overlay_version,
        "frontier_inventory_snapshot_id": frontier_inventory_snapshot_id,
        "total_pm_reference_delta_rows": int(len(merged)),
        "class_counts": {str(key): int(value) for key, value in class_counts.items()},
        "report_class_counts": report_class_counts,
        "release_class_counts": release_class_counts,
        "era_counts": era_counts,
        "game_type_counts": game_type_counts,
        "bundle_counts": (
            merged.groupby(["residual_bundle", "pm_characterization"]).size().rename("row_count").reset_index().to_dict(orient="records")
        ),
        "sample_row_count": int(len(boundary_sample)),
        "lane_summary_count": int(len(lane_summary)),
        "source_limited_game_count": int(len(source_limited_games)),
        "open_queue_game_count": int(len(open_game_queue)),
        "release_blocker_game_count": int(len(release_blocker_queue)),
        "release_blocking_game_ids": sorted(release_blocker_queue["game_id"].astype(str).tolist()),
        "reviewed_frontier_queue_game_count": int(len(reviewed_frontier_queue)),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
