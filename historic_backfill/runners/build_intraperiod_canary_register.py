from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError


ROOT = Path(__file__).resolve().parent
DEFAULT_MANIFEST_PATH = ROOT / "intraperiod_canary_manifest_1998_2020.json"


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _as_int(value: Any, default: int = 0) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    try:
        return int(float(value))
    except Exception:
        return default


def _classify_candidate_family(row: pd.Series) -> str:
    decision = str(row.get("promotion_decision") or "")
    if _as_int(row.get("broken_substitution_context")) > 0:
        return "broken_substitution_context"
    if (
        _as_int(row.get("incoming_missing_current_support")) > 0
        or _as_int(row.get("player_in_later_sub_out_support")) > 0
        or _as_int(row.get("prior_repeat_swap_support")) > 0
        or _as_int(row.get("matching_sub_out_candidate_support")) > 0
    ):
        return "missing_reentry"
    if _as_int(row.get("same_clock_cluster_consistency")) > 0:
        return "same_clock_sub_cluster"
    if decision in {"invalid_base_lineup", "zero_duration_window"}:
        return "unresolved_structural"
    return "source_conflict_or_unresolved"


def _best_candidate_row(candidate_df: pd.DataFrame, game_id: str) -> pd.Series | None:
    if candidate_df.empty:
        return None
    subset = candidate_df.loc[candidate_df["game_id"] == game_id].copy()
    if subset.empty:
        return None
    subset = subset.sort_values(
        [
            "auto_apply",
            "local_confidence_score",
            "contradictions_removed",
            "best_vs_runner_up_confidence_gap",
        ],
        ascending=[False, False, False, False],
    )
    return subset.iloc[0]


def _load_candidate_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()
    if df.empty:
        return df
    df["game_id"] = df["game_id"].map(_normalize_game_id)
    return df


def _load_micro_runtime_metrics(group_dir: Path) -> dict[str, dict[str, int]]:
    metrics: dict[str, dict[str, int]] = {}

    for csv_path in sorted(group_dir.glob("minutes_plus_minus_audit_*.csv")):
        if not csv_path.exists():
            continue
        df = pd.read_csv(csv_path)
        if df.empty or "game_id" not in df.columns:
            continue
        normalized_ids = df["game_id"].map(_normalize_game_id)
        for game_id, subset in df.assign(game_id_norm=normalized_ids).groupby("game_id_norm"):
            entry = metrics.setdefault(
                str(game_id),
                {
                    "runtime_minutes_mismatches": 0,
                    "runtime_minutes_outliers": 0,
                    "runtime_plus_minus_mismatches": 0,
                    "runtime_event_on_court_issue_rows": 0,
                    "runtime_event_on_court_issue_games": 0,
                },
            )
            if "has_minutes_mismatch" in subset.columns:
                entry["runtime_minutes_mismatches"] += int(subset["has_minutes_mismatch"].fillna(False).sum())
            if "is_minutes_outlier" in subset.columns:
                entry["runtime_minutes_outliers"] += int(subset["is_minutes_outlier"].fillna(False).sum())
            if "has_plus_minus_mismatch" in subset.columns:
                entry["runtime_plus_minus_mismatches"] += int(
                    subset["has_plus_minus_mismatch"].fillna(False).sum()
                )

    for csv_path in sorted(group_dir.glob("event_player_on_court_issues_*.csv")):
        if not csv_path.exists():
            continue
        df = pd.read_csv(csv_path)
        if df.empty or "game_id" not in df.columns:
            continue
        normalized_ids = df["game_id"].map(_normalize_game_id)
        for game_id, subset in df.assign(game_id_norm=normalized_ids).groupby("game_id_norm"):
            entry = metrics.setdefault(
                str(game_id),
                {
                    "runtime_minutes_mismatches": 0,
                    "runtime_minutes_outliers": 0,
                    "runtime_plus_minus_mismatches": 0,
                    "runtime_event_on_court_issue_rows": 0,
                    "runtime_event_on_court_issue_games": 0,
                },
            )
            entry["runtime_event_on_court_issue_rows"] += int(len(subset))
            entry["runtime_event_on_court_issue_games"] = 1

    return metrics


def _micro_register_rows(loop_output_dir: Path, manifest: dict[str, Any]) -> list[dict[str, Any]]:
    positive_dir = loop_output_dir / "micro" / "positive"
    negative_dir = loop_output_dir / "micro" / "negative"
    positive_df = _load_candidate_csv(
        positive_dir / "candidates" / "intraperiod_missing_sub_candidates.csv"
    )
    negative_df = _load_candidate_csv(
        negative_dir / "candidates" / "intraperiod_missing_sub_candidates.csv"
    )
    combined = pd.concat([positive_df, negative_df], ignore_index=True) if not positive_df.empty or not negative_df.empty else pd.DataFrame()
    positive_runtime = _load_micro_runtime_metrics(positive_dir)
    negative_runtime = _load_micro_runtime_metrics(negative_dir)

    rows: list[dict[str, Any]] = []
    for item in manifest.get("micro_canaries", []):
        game_id = _normalize_game_id(item["game_id"])
        role = str(item.get("role"))
        best_row = _best_candidate_row(combined, game_id)
        runtime_entry = (
            positive_runtime.get(game_id, {})
            if role == "positive"
            else negative_runtime.get(game_id, {})
        )
        runtime_issue_rows = int(runtime_entry.get("runtime_event_on_court_issue_rows", 0))
        runtime_minutes_outliers = int(runtime_entry.get("runtime_minutes_outliers", 0))
        if role == "positive":
            if runtime_issue_rows == 0:
                runtime_status = "runtime_clean"
            else:
                runtime_status = "runtime_issue_rows_present"
        else:
            runtime_status = "runtime_flat_or_pending" if runtime_issue_rows >= 0 else "runtime_missing"
        rows.append(
            {
                "row_type": "micro_canary",
                "game_id": game_id,
                "family": str(item["family"]),
                "role": role,
                "target_type": str(item["target_type"]),
                "block_id": None,
                "block_label": None,
                "runtime_status": runtime_status,
                "runtime_minutes_mismatches": int(runtime_entry.get("runtime_minutes_mismatches", 0)),
                "runtime_minutes_outliers": runtime_minutes_outliers,
                "runtime_plus_minus_mismatches": int(
                    runtime_entry.get("runtime_plus_minus_mismatches", 0)
                ),
                "runtime_event_on_court_issue_rows": runtime_issue_rows,
                "runtime_event_on_court_issue_games": int(
                    runtime_entry.get("runtime_event_on_court_issue_games", 0)
                ),
                "promotion_decision": None if best_row is None else str(best_row.get("promotion_decision")),
                "auto_apply": False if best_row is None else bool(best_row.get("auto_apply")),
                "local_confidence_score": None if best_row is None else best_row.get("local_confidence_score"),
                "runner_up_local_confidence_score": None if best_row is None else best_row.get("runner_up_local_confidence_score"),
                "best_vs_runner_up_confidence_gap": None if best_row is None else best_row.get("best_vs_runner_up_confidence_gap"),
                "deadball_choice_kind": None if best_row is None else best_row.get("deadball_choice_kind"),
                "deadball_event_num": None if best_row is None else best_row.get("deadball_event_num"),
                "runner_up_deadball_event_num": None if best_row is None else best_row.get("runner_up_deadball_event_num"),
                "forward_simulation_contradiction_delta": None if best_row is None else best_row.get("forward_simulation_contradiction_delta"),
                "notes": str(item.get("notes") or ""),
            }
        )
    return rows


def _block_register_rows(loop_output_dir: Path, manifest: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    blocks_dir = loop_output_dir / "blocks"
    for block in manifest.get("blocks", []):
        block_id = str(block["block_id"])
        block_label = str(block["label"])
        block_dir = blocks_dir / f"{block_id}_{block_label.replace(' ', '_')}"
        candidate_df = _load_candidate_csv(
            block_dir / "candidates" / "intraperiod_missing_sub_candidates.csv"
        )
        if candidate_df.empty:
            continue
        for row in candidate_df.itertuples(index=False):
            row_series = pd.Series(row._asdict())
            rows.append(
                {
                    "row_type": "block_candidate",
                    "game_id": str(row.game_id),
                    "family": _classify_candidate_family(row_series),
                    "role": "discovered",
                    "target_type": "heuristic"
                    if str(row.promotion_decision) not in {"invalid_base_lineup", "zero_duration_window"}
                    else "manual_only",
                    "block_id": block_id,
                    "block_label": block_label,
                    "promotion_decision": str(row.promotion_decision),
                    "auto_apply": bool(row.auto_apply),
                    "local_confidence_score": row.local_confidence_score,
                    "runner_up_local_confidence_score": row.runner_up_local_confidence_score,
                    "best_vs_runner_up_confidence_gap": getattr(row, "best_vs_runner_up_confidence_gap", None),
                    "deadball_choice_kind": getattr(row, "deadball_choice_kind", None),
                    "deadball_event_num": getattr(row, "deadball_event_num", None),
                    "runner_up_deadball_event_num": getattr(row, "runner_up_deadball_event_num", None),
                    "forward_simulation_contradiction_delta": getattr(
                        row, "forward_simulation_contradiction_delta", None
                    ),
                    "notes": "",
                }
            )
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a cross-block intraperiod canary and candidate register."
    )
    parser.add_argument("--loop-output-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = _load_json(args.manifest_path.resolve())

    rows = _micro_register_rows(args.loop_output_dir.resolve(), manifest)
    rows.extend(_block_register_rows(args.loop_output_dir.resolve(), manifest))
    register_df = pd.DataFrame(rows)

    if register_df.empty:
        summary = {
            "rows": 0,
            "micro_canaries": 0,
            "block_candidates": 0,
            "family_counts": {},
            "promotion_decision_counts": {},
            "auto_apply_rows": 0,
            "not_nearest_wins": 0,
        }
    else:
        runtime_status_counts = (
            register_df.loc[register_df["row_type"] == "micro_canary", "runtime_status"]
            .fillna("missing")
            .value_counts()
            .sort_index()
            .to_dict()
        )
        summary = {
            "rows": int(len(register_df)),
            "micro_canaries": int((register_df["row_type"] == "micro_canary").sum()),
            "block_candidates": int((register_df["row_type"] == "block_candidate").sum()),
            "micro_runtime_status_counts": {
                str(key): int(value) for key, value in runtime_status_counts.items()
            },
            "family_counts": {
                str(key): int(value)
                for key, value in register_df["family"].value_counts().sort_index().to_dict().items()
            },
            "promotion_decision_counts": {
                str(key): int(value)
                for key, value in register_df["promotion_decision"].fillna("missing").value_counts().sort_index().to_dict().items()
            },
            "auto_apply_rows": int(register_df["auto_apply"].fillna(False).sum()),
            "not_nearest_wins": int(
                register_df["deadball_choice_kind"]
                .fillna("")
                .isin(["earliest_winning", "middle_winning"])
                .sum()
            ),
        }
        register_df = register_df.sort_values(
            ["row_type", "family", "block_id", "game_id"],
            na_position="last",
        ).reset_index(drop=True)

    register_df.to_csv(args.output_dir / "intraperiod_canary_register.csv", index=False)
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
