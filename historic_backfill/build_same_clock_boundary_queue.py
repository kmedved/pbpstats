from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_SAME_CLOCK_CANARY_MANIFEST_PATH = (
    ROOT / "same_clock_canary_manifest_20260320_v1" / "same_clock_canary_manifest.json"
)
DEFAULT_NON_OPENING_FT_MANIFEST_PATH = (
    ROOT / "same_clock_canary_manifest_non_opening_ft_sub_20260320_v1.json"
)


def _normalize_game_id(value: Any) -> str:
    return str(int(value)).zfill(10)


def _as_int(value: Any, default: int = 0) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    try:
        return int(float(value))
    except Exception:
        return default


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the next same-clock boundary review queue from the event-on-court register."
    )
    parser.add_argument("--event-on-court-family-register-dir", type=Path, required=True)
    parser.add_argument("--same-clock-register-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-per-family", type=int, default=5)
    parser.add_argument(
        "--same-clock-canary-manifest-path",
        type=Path,
        default=DEFAULT_SAME_CLOCK_CANARY_MANIFEST_PATH,
    )
    parser.add_argument(
        "--non-opening-ft-manifest-path",
        type=Path,
        default=DEFAULT_NON_OPENING_FT_MANIFEST_PATH,
    )
    return parser.parse_args()


def _load_lane_table(event_dir: Path) -> pd.DataFrame:
    path = event_dir / "event_on_court_game_period_team_summary.csv"
    df = pd.read_csv(path) if path.exists() else pd.DataFrame()
    if df.empty:
        return df
    df = df.copy()
    df["game_id"] = df["game_id"].map(_normalize_game_id)
    df["period"] = pd.to_numeric(df["period"], errors="coerce").fillna(0).astype(int)
    df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").fillna(0).astype(int)
    df["issue_rows"] = pd.to_numeric(df["issue_rows"], errors="coerce").fillna(0).astype(int)
    return df


def _load_same_clock_lane_meta(register_dir: Path) -> pd.DataFrame:
    path = register_dir / "same_clock_attribution_register.csv"
    df = pd.read_csv(path) if path.exists() else pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["game_id"] = df["game_id"].map(_normalize_game_id)
    df["period"] = pd.to_numeric(df["period"], errors="coerce").fillna(0).astype(int)
    df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").fillna(0).astype(int)
    grouped = (
        df.groupby(["game_id", "period", "team_id"], as_index=False)
        .agg(
            same_clock_family=("same_clock_family", "first"),
            source_families=("source_family", lambda values: json.dumps(sorted(set(str(v) for v in values if str(v))))),
            candidate_rows=("same_clock_family", "size"),
            max_local_confidence_score=("local_confidence_score", lambda values: float(pd.to_numeric(values, errors="coerce").fillna(0).max())),
            max_game_plus_minus_mismatch_rows=("game_plus_minus_mismatch_rows", lambda values: int(pd.to_numeric(values, errors="coerce").fillna(0).max())),
            max_game_event_issue_rows=("game_event_issue_rows", lambda values: int(pd.to_numeric(values, errors="coerce").fillna(0).max())),
            max_game_minutes_abs_diff=("game_max_minutes_abs_diff", lambda values: float(pd.to_numeric(values, errors="coerce").fillna(0).max())),
        )
    )
    return grouped


def _load_positive_canary_keys(
    same_clock_manifest_path: Path,
    ft_manifest_path: Path,
) -> tuple[set[tuple[str, int, int]], set[tuple[str, int]]]:
    exact_keys: set[tuple[str, int, int]] = set()
    loose_keys: set[tuple[str, int]] = set()

    if same_clock_manifest_path.exists():
        payload = json.loads(same_clock_manifest_path.read_text(encoding="utf-8"))
        positive_canaries = payload.get("positive_canaries", {}) if isinstance(payload, dict) else {}
        if isinstance(positive_canaries, dict):
            for items in positive_canaries.values():
                if not isinstance(items, list):
                    continue
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    try:
                        exact_keys.add(
                            (
                                _normalize_game_id(item["game_id"]),
                                _as_int(item.get("period")),
                                _as_int(item.get("team_id")),
                            )
                        )
                    except Exception:
                        continue

    if ft_manifest_path.exists():
        payload = json.loads(ft_manifest_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            for key in ["positive_core_canaries"]:
                items = payload.get(key, [])
                if not isinstance(items, list):
                    continue
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    try:
                        loose_keys.add(
                            (
                                _normalize_game_id(item["game_id"]),
                                _as_int(item.get("period")),
                            )
                        )
                    except Exception:
                        continue
    return exact_keys, loose_keys


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    lane_df = _load_lane_table(args.event_on_court_family_register_dir.resolve())
    if lane_df.empty:
        empty_df = pd.DataFrame()
        empty_df.to_csv(output_dir / "same_clock_boundary_queue.csv", index=False)
        summary = {"rows": 0, "families": {}, "next_canaries_by_family": {}}
        (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
        return 0

    lane_df = lane_df.loc[lane_df["actionability"] == "event_ordering_queue"].copy()
    if lane_df.empty:
        empty_df = pd.DataFrame()
        empty_df.to_csv(output_dir / "same_clock_boundary_queue.csv", index=False)
        summary = {"rows": 0, "families": {}, "next_canaries_by_family": {}}
        (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
        return 0

    same_clock_meta = _load_same_clock_lane_meta(args.same_clock_register_dir.resolve())
    exact_positive_keys, loose_positive_keys = _load_positive_canary_keys(
        args.same_clock_canary_manifest_path.resolve(),
        args.non_opening_ft_manifest_path.resolve(),
    )
    if not same_clock_meta.empty:
        lane_df = lane_df.merge(
            same_clock_meta,
            on=["game_id", "period", "team_id"],
            how="left",
            suffixes=("", "_same_clock"),
        )
    else:
        lane_df["source_families"] = "[]"
        lane_df["candidate_rows"] = 0
        lane_df["max_local_confidence_score"] = 0.0
        lane_df["max_game_plus_minus_mismatch_rows"] = 0
        lane_df["max_game_event_issue_rows"] = 0
        lane_df["max_game_minutes_abs_diff"] = 0.0

    lane_df["same_clock_family"] = lane_df["same_clock_family"].fillna("event_ordering_candidate")
    lane_df["is_manifest_positive"] = lane_df.apply(
        lambda row: (
            (str(row["game_id"]), int(row["period"]), int(row["team_id"])) in exact_positive_keys
            or (str(row["game_id"]), int(row["period"])) in loose_positive_keys
        ),
        axis=1,
    )
    lane_df["has_period_start_clock_issue"] = (
        pd.to_numeric(lane_df.get("has_period_start_clock_issue"), errors="coerce")
        .fillna(0)
        .astype(bool)
    )
    lane_df["technical_or_ejection_like"] = (
        pd.to_numeric(lane_df.get("technical_or_ejection_like"), errors="coerce")
        .fillna(0)
        .astype(bool)
    )
    lane_df["is_opening_cluster_control"] = (
        lane_df["has_period_start_clock_issue"] & lane_df["technical_or_ejection_like"]
    )
    lane_df["queue_role"] = lane_df["is_opening_cluster_control"].map(
        lambda value: "opening_cluster_control" if value else "active_same_clock_review"
    )
    lane_df["queue_notes"] = lane_df["is_opening_cluster_control"].map(
        lambda value: (
            "opening-cluster control case; keep for non-regression, not as an unresolved teaching lane"
            if value
            else ""
        )
    )
    lane_df["review_priority_score"] = (
        lane_df["issue_rows"].astype(float) * 1000.0
        + pd.to_numeric(lane_df["max_game_plus_minus_mismatch_rows"], errors="coerce").fillna(0.0) * 10.0
        + pd.to_numeric(lane_df["candidate_rows"], errors="coerce").fillna(0.0)
    )

    all_lane_df = lane_df.sort_values(
        [
            "queue_role",
            "same_clock_family",
            "is_manifest_positive",
            "review_priority_score",
            "season",
            "game_id",
            "period",
            "team_id",
        ],
        ascending=[True, True, False, False, True, True, True, True],
    ).reset_index(drop=True)
    active_lane_df = all_lane_df.loc[all_lane_df["queue_role"] == "active_same_clock_review"].copy()
    control_lane_df = all_lane_df.loc[all_lane_df["queue_role"] == "opening_cluster_control"].copy()

    active_lane_df["family_rank"] = active_lane_df.groupby("same_clock_family").cumcount() + 1
    control_lane_df["family_rank"] = control_lane_df.groupby("same_clock_family").cumcount() + 1

    all_lane_df.to_csv(output_dir / "same_clock_boundary_queue_all.csv", index=False)
    active_lane_df.to_csv(output_dir / "same_clock_boundary_queue.csv", index=False)
    control_lane_df.to_csv(output_dir / "same_clock_boundary_controls.csv", index=False)

    next_canaries_by_family: dict[str, list[dict[str, Any]]] = {}
    for family, group in active_lane_df.groupby("same_clock_family", dropna=False):
        top = group.head(int(args.max_per_family))
        next_canaries_by_family[str(family)] = top[
            [
                "block_key",
                "season",
                "game_id",
                "period",
                "team_id",
                "issue_rows",
                "family",
                "notes",
                "candidate_rows",
                "is_manifest_positive",
                "max_local_confidence_score",
                "max_game_plus_minus_mismatch_rows",
                "max_game_minutes_abs_diff",
                "source_families",
                "queue_role",
                "queue_notes",
                "family_rank",
            ]
        ].to_dict("records")

    control_cases_by_family: dict[str, list[dict[str, Any]]] = {}
    for family, group in control_lane_df.groupby("same_clock_family", dropna=False):
        top = group.head(int(args.max_per_family))
        control_cases_by_family[str(family)] = top[
            [
                "block_key",
                "season",
                "game_id",
                "period",
                "team_id",
                "issue_rows",
                "family",
                "notes",
                "candidate_rows",
                "is_manifest_positive",
                "max_local_confidence_score",
                "max_game_plus_minus_mismatch_rows",
                "max_game_minutes_abs_diff",
                "source_families",
                "queue_role",
                "queue_notes",
                "family_rank",
            ]
        ].to_dict("records")

    (output_dir / "next_canaries_by_family.json").write_text(
        json.dumps(next_canaries_by_family, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (output_dir / "control_cases_by_family.json").write_text(
        json.dumps(control_cases_by_family, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary = {
        "rows": int(len(active_lane_df)),
        "control_rows": int(len(control_lane_df)),
        "families": active_lane_df["same_clock_family"].value_counts().sort_index().to_dict(),
        "control_families": control_lane_df["same_clock_family"].value_counts().sort_index().to_dict(),
        "next_canaries_by_family": next_canaries_by_family,
        "control_cases_by_family": control_cases_by_family,
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
