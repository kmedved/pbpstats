from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent
DEFAULT_MANIFEST_PATH = ROOT / "intraperiod_canary_manifest_1998_2020.json"
DEFAULT_MANUAL_REVIEW_REGISTRY_PATH = ROOT / "intraperiod_manual_review_registry.json"


def _safe_literal(value: Any) -> Any:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (dict, list)):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        try:
            return ast.literal_eval(text)
        except Exception:
            return None


def _normalize_game_id(value: Any) -> str:
    return str(int(value)).zfill(10)


def _as_int(value: Any, default: int = 0) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    try:
        return int(float(value))
    except Exception:
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    try:
        return float(value)
    except Exception:
        return default


def classify_family(row: pd.Series) -> str:
    decision = str(row.get("promotion_decision") or "")
    apply_position = str(row.get("deadball_apply_position") or "")
    choice_kind = str(row.get("deadball_choice_kind") or "")
    contradiction_counts = _safe_literal(row.get("contradiction_status_counts")) or {}

    if bool(row.get("auto_apply")):
        return "auto_apply"
    if decision == "broken_substitution_context":
        return "broken_substitution_context"
    if decision == "zero_duration_window":
        return "zero_duration_window"
    if decision == "invalid_base_lineup":
        return "invalid_base_lineup"
    if decision == "introduces_new_contradiction":
        return "forward_regression_candidate"
    if decision == "ambiguous_runner_up":
        return "ambiguous_runner_up"
    if decision == "no_candidate":
        if contradiction_counts:
            statuses = sorted(contradiction_counts.keys())
            if statuses == ["off_court_event_credit"]:
                return "no_deadball_local_signal"
        return "no_candidate"
    if decision == "insufficient_local_context":
        if apply_position == "after_window_end" and choice_kind in {"latest_winning", "only_winning"}:
            if _as_int(row.get("player_in_later_sub_out_support")) > 0 or _as_int(
                row.get("matching_sub_out_candidate_support")
            ) > 0:
                return "same_clock_reentry_candidate"
            return "after_cluster_low_confidence"
        if _as_int(row.get("period_repeat_contradiction_support")) > 0 or _as_int(
            row.get("prior_repeat_swap_support")
        ) > 0:
            return "repeat_swap_low_confidence"
        if _as_int(row.get("later_explicit_reentry_support")) > 0 or _as_int(
            row.get("player_in_later_sub_out_support")
        ) > 0:
            return "manual_review_reentry_candidate"
        if _as_int(row.get("incoming_missing_current_support")) > 0:
            return "incoming_missing_current_candidate"
        return "insufficient_local_context"
    return decision or "unclassified"


def _manual_review_bucket(family: str) -> bool:
    return family in {
        "same_clock_reentry_candidate",
        "manual_review_reentry_candidate",
        "repeat_swap_low_confidence",
        "incoming_missing_current_candidate",
        "ambiguous_runner_up",
        "after_cluster_low_confidence",
    }


def _load_manual_review_registry(path: Path) -> dict[tuple[str, int, int], dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    entries = payload.get("entries", []) if isinstance(payload, dict) else []
    mapping: dict[tuple[str, int, int], dict[str, Any]] = {}
    for entry in entries:
        try:
            key = (
                _normalize_game_id(entry["game_id"]),
                _as_int(entry.get("period")),
                _as_int(entry.get("team_id")),
            )
        except Exception:
            continue
        mapping[key] = {
            "manual_review_disposition": str(entry.get("disposition") or ""),
            "manual_review_recommended_next_track": str(entry.get("recommended_next_track") or ""),
            "manual_review_notes": str(entry.get("notes") or ""),
            "manual_review_source_case_summary_path": str(entry.get("source_case_summary_path") or ""),
            "is_reviewed_manual_reject": str(entry.get("disposition") or "").startswith("rejected_"),
        }
    return mapping


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a family register from intraperiod candidate outputs."
    )
    parser.add_argument("--loop-output-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument(
        "--manual-review-registry-path",
        type=Path,
        default=DEFAULT_MANUAL_REVIEW_REGISTRY_PATH,
    )
    return parser.parse_args()


def _build_game_metrics(loop_output_dir: Path) -> dict[tuple[str, str], dict[str, Any]]:
    metrics: dict[tuple[str, str], dict[str, Any]] = {}
    for block_dir in sorted(loop_output_dir.glob("blocks/*")):
        if not block_dir.is_dir():
            continue
        block_key = block_dir.name
        for csv_path in sorted(block_dir.glob("minutes_plus_minus_audit_*.csv")):
            df = pd.read_csv(csv_path)
            if df.empty:
                continue
            temp = df.copy()
            temp["game_id"] = temp["game_id"].apply(_normalize_game_id)
            grouped = temp.groupby("game_id", dropna=False)
            for game_id, group in grouped:
                key = (block_key, game_id)
                row = metrics.setdefault(key, {"block_key": block_key, "game_id": game_id})
                row["game_minutes_mismatch_rows"] = int(group["has_minutes_mismatch"].fillna(False).sum())
                row["game_plus_minus_mismatch_rows"] = int(group["has_plus_minus_mismatch"].fillna(False).sum())
                row["game_minute_outlier_rows"] = int(group["is_minutes_outlier"].fillna(False).sum())
                row["game_max_minutes_abs_diff"] = float(
                    pd.to_numeric(group["Minutes_abs_diff"], errors="coerce").fillna(0.0).max()
                )
        for csv_path in sorted(block_dir.glob("event_player_on_court_issues_*.csv")):
            df = pd.read_csv(csv_path)
            if df.empty:
                continue
            temp = df.copy()
            temp["game_id"] = temp["game_id"].apply(_normalize_game_id)
            grouped = temp.groupby("game_id", dropna=False)
            for game_id, group in grouped:
                key = (block_key, game_id)
                row = metrics.setdefault(key, {"block_key": block_key, "game_id": game_id})
                row["game_event_issue_rows"] = int(len(group))
                row["game_event_issue_statuses"] = json.dumps(
                    group["status"].fillna("").value_counts().sort_index().to_dict(),
                    sort_keys=True,
                )
    return metrics


def main() -> int:
    args = parse_args()
    loop_output_dir = args.loop_output_dir.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = args.manifest_path.resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    manual_review_registry = _load_manual_review_registry(args.manual_review_registry_path.resolve())
    game_metrics = _build_game_metrics(loop_output_dir)
    micro_index = {
        _normalize_game_id(item["game_id"]): {
            "manifest_family": str(item.get("family") or ""),
            "manifest_role": str(item.get("role") or ""),
            "manifest_target_type": str(item.get("target_type") or ""),
        }
        for item in manifest.get("micro_canaries", [])
    }

    rows: list[dict[str, Any]] = []
    for csv_path in sorted(loop_output_dir.glob("blocks/*/candidates/intraperiod_missing_sub_candidates.csv")):
        block_key = csv_path.parent.parent.name
        df = pd.read_csv(csv_path)
        if df.empty:
            continue
        for _, row in df.iterrows():
            family = classify_family(row)
            local_score = _as_float(row.get("local_confidence_score"))
            runner_up_score = _as_float(row.get("runner_up_local_confidence_score"))
            record = {
                "block_key": block_key,
                "game_id": _normalize_game_id(row["game_id"]),
                "period": _as_int(row.get("period")),
                "team_id": _as_int(row.get("team_id")),
                "player_in_id": _as_int(row.get("player_in_id")),
                "player_out_id": _as_int(row.get("player_out_id")),
                "promotion_decision": str(row.get("promotion_decision") or ""),
                "family": family,
                "manual_review_bucket": _manual_review_bucket(family),
                "auto_apply": bool(row.get("auto_apply")),
                "deadball_choice_kind": str(row.get("deadball_choice_kind") or ""),
                "deadball_apply_position": str(row.get("deadball_apply_position") or ""),
                "local_confidence_score": local_score,
                "runner_up_local_confidence_score": runner_up_score,
                "confidence_gap": _as_float(row.get("best_vs_runner_up_confidence_gap")),
                "forward_simulation_contradiction_delta": _as_int(
                    row.get("forward_simulation_contradiction_delta")
                ),
                "contradictions_removed": _as_int(row.get("contradictions_removed")),
                "new_contradictions_introduced": _as_int(row.get("new_contradictions_introduced")),
                "approx_window_seconds": _as_float(row.get("approx_window_seconds")),
                "later_explicit_reentry_support": _as_int(row.get("later_explicit_reentry_support")),
                "player_in_later_sub_out_support": _as_int(row.get("player_in_later_sub_out_support")),
                "player_out_silence_support": _as_int(row.get("player_out_silence_support")),
                "incoming_missing_current_support": _as_int(row.get("incoming_missing_current_support")),
                "matching_sub_out_candidate_support": _as_int(
                    row.get("matching_sub_out_candidate_support")
                ),
                "period_repeat_contradiction_support": _as_int(
                    row.get("period_repeat_contradiction_support")
                ),
                "prior_repeat_swap_support": _as_int(row.get("prior_repeat_swap_support")),
                "same_clock_cluster_consistency": _as_int(row.get("same_clock_cluster_consistency")),
                "lineup_size_consistency": _as_int(row.get("lineup_size_consistency")),
                "not_nearest_like_choice": str(row.get("deadball_choice_kind") or "") == "latest_winning"
                or str(row.get("deadball_apply_position") or "") == "after_window_end",
                "contradiction_status_counts": json.dumps(
                    _safe_literal(row.get("contradiction_status_counts")) or {},
                    sort_keys=True,
                ),
                "evidence_event_nums": json.dumps(_safe_literal(row.get("evidence_event_nums")) or []),
            }
            manifest_meta = micro_index.get(record["game_id"], {})
            record.update(
                {
                    "manifest_family": manifest_meta.get("manifest_family", ""),
                    "manifest_role": manifest_meta.get("manifest_role", ""),
                    "manifest_target_type": manifest_meta.get("manifest_target_type", ""),
                    "is_known_negative_tripwire": manifest_meta.get("manifest_role") == "negative",
                }
            )
            metrics = game_metrics.get((record["block_key"], record["game_id"]), {})
            record.update(
                {
                    "game_minutes_mismatch_rows": int(metrics.get("game_minutes_mismatch_rows", 0)),
                    "game_plus_minus_mismatch_rows": int(metrics.get("game_plus_minus_mismatch_rows", 0)),
                    "game_minute_outlier_rows": int(metrics.get("game_minute_outlier_rows", 0)),
                    "game_max_minutes_abs_diff": float(metrics.get("game_max_minutes_abs_diff", 0.0)),
                    "game_event_issue_rows": int(metrics.get("game_event_issue_rows", 0)),
                    "game_event_issue_statuses": metrics.get("game_event_issue_statuses", "{}"),
                }
            )
            manual_review_meta = manual_review_registry.get(
                (record["game_id"], record["period"], record["team_id"]),
                {},
            )
            record.update(
                {
                    "manual_review_disposition": manual_review_meta.get(
                        "manual_review_disposition", ""
                    ),
                    "manual_review_recommended_next_track": manual_review_meta.get(
                        "manual_review_recommended_next_track", ""
                    ),
                    "manual_review_notes": manual_review_meta.get("manual_review_notes", ""),
                    "manual_review_source_case_summary_path": manual_review_meta.get(
                        "manual_review_source_case_summary_path", ""
                    ),
                    "is_reviewed_manual_reject": bool(
                        manual_review_meta.get("is_reviewed_manual_reject", False)
                    ),
                }
            )
            record["high_signal_game"] = (
                record["game_minute_outlier_rows"] > 0
                or record["game_max_minutes_abs_diff"] > 0.5
                or record["game_plus_minus_mismatch_rows"] >= 2
            )
            rows.append(record)

    register_df = pd.DataFrame(rows)
    register_path = output_dir / "intraperiod_family_register.csv"
    if register_df.empty:
        register_df.to_csv(register_path, index=False)
        summary = {
            "rows": 0,
            "family_counts": {},
            "block_family_counts": {},
            "manual_review_rows": 0,
            "auto_apply_rows": 0,
        }
    else:
        register_df = register_df.sort_values(
            [
                "is_known_negative_tripwire",
                "is_reviewed_manual_reject",
                "manual_review_bucket",
                "auto_apply",
                "local_confidence_score",
                "confidence_gap",
                "forward_simulation_contradiction_delta",
            ],
            ascending=[True, True, False, False, False, False, False],
        ).reset_index(drop=True)
        register_df.to_csv(register_path, index=False)

        family_counts = register_df["family"].value_counts().sort_index().to_dict()
        block_family_counts = (
            register_df.groupby(["block_key", "family"]).size().rename("rows").reset_index().to_dict("records")
        )
        manual_review_df = register_df[register_df["manual_review_bucket"]].head(25)
        manual_review_path = output_dir / "intraperiod_manual_review_shortlist.csv"
        manual_review_df.to_csv(manual_review_path, index=False)
        actionable_df = register_df[
            register_df["manual_review_bucket"]
            & (~register_df["is_known_negative_tripwire"])
            & (~register_df["is_reviewed_manual_reject"])
            & (register_df["high_signal_game"])
        ].head(25)
        actionable_path = output_dir / "intraperiod_actionable_shortlist.csv"
        actionable_df.to_csv(actionable_path, index=False)
        reviewed_reject_df = register_df[register_df["is_reviewed_manual_reject"]].head(25)
        reviewed_reject_path = output_dir / "intraperiod_reviewed_manual_rejections.csv"
        reviewed_reject_df.to_csv(reviewed_reject_path, index=False)
        summary = {
            "rows": int(len(register_df)),
            "family_counts": family_counts,
            "block_family_counts": block_family_counts,
            "manual_review_rows": int(register_df["manual_review_bucket"].sum()),
            "auto_apply_rows": int(register_df["auto_apply"].sum()),
            "not_nearest_like_rows": int(register_df["not_nearest_like_choice"].sum()),
            "known_negative_tripwire_rows": int(register_df["is_known_negative_tripwire"].sum()),
            "reviewed_manual_reject_rows": int(register_df["is_reviewed_manual_reject"].sum()),
            "high_signal_rows": int(register_df["high_signal_game"].sum()),
            "top_manual_review_games": manual_review_df[
                [
                    "block_key",
                    "game_id",
                    "period",
                    "team_id",
                    "family",
                    "local_confidence_score",
                    "manifest_target_type",
                    "game_max_minutes_abs_diff",
                    "game_event_issue_rows",
                ]
            ].to_dict("records"),
            "top_actionable_games": actionable_df[
                [
                    "block_key",
                    "game_id",
                    "period",
                    "team_id",
                    "family",
                    "local_confidence_score",
                    "game_max_minutes_abs_diff",
                    "game_event_issue_rows",
                ]
            ].to_dict("records"),
            "top_reviewed_manual_rejections": reviewed_reject_df[
                [
                    "block_key",
                    "game_id",
                    "period",
                    "team_id",
                    "family",
                    "manual_review_disposition",
                    "manual_review_recommended_next_track",
                    "game_max_minutes_abs_diff",
                    "game_event_issue_rows",
                ]
            ].to_dict("records"),
        }

    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
