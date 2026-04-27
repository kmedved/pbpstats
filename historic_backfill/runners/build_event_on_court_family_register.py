from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_PERIOD_STARTER_AUDIT_PATH = (
    ROOT / "period_starters_vs_tpdev_20260316_v1" / "period_starter_audit.csv"
)
DEFAULT_LARGE_MINUTE_REGISTER_PATH = (
    ROOT / "large_minute_outlier_family_register_20260316_v2" / "large_minute_outlier_family_register.csv"
)
DEFAULT_PERIOD_STARTER_OVERRIDES_PATH = ROOT / "overrides" / "period_starters_overrides.json"
DEFAULT_MANUAL_REVIEW_REGISTRY_PATH = ROOT / "intraperiod_manual_review_registry.json"
DEFAULT_SAME_CLOCK_CANARY_MANIFEST_PATH = (
    ROOT / "same_clock_canary_manifest_20260320_v1" / "same_clock_canary_manifest.json"
)
DEFAULT_NON_OPENING_FT_MANIFEST_PATH = (
    ROOT / "same_clock_canary_manifest_non_opening_ft_sub_20260320_v1.json"
)
UF_DATALESS_FLAG = 0x40000000

FAMILY_ACTIONABILITY = {
    "opening_cluster_period_starter": "immediate_fix_queue",
    "period_starter_boundary": "immediate_fix_queue",
    "intraperiod_missing_sub_candidate": "manual_or_local_override_queue",
    "same_clock_boundary_conflict": "event_ordering_queue",
    "event_ordering_candidate": "event_ordering_queue",
    "source_conflict_or_missing_source": "document_only",
    "already_reviewed_negative": "document_only",
    "documented_residual": "document_only",
}

FAMILY_PRIORITY = {
    "opening_cluster_period_starter": "high",
    "period_starter_boundary": "high",
    "intraperiod_missing_sub_candidate": "high",
    "same_clock_boundary_conflict": "medium",
    "event_ordering_candidate": "medium",
    "source_conflict_or_missing_source": "low",
    "already_reviewed_negative": "low",
    "documented_residual": "low",
}

FAMILY_ACTION = {
    "opening_cluster_period_starter": "validate_existing_opening_cluster_fix_or_targeted_period_starter_override",
    "period_starter_boundary": "period_starter_override_first_then_narrow_start_of_period_logic",
    "intraperiod_missing_sub_candidate": "manual_intraperiod_review_queue_or_lineup_window_override",
    "same_clock_boundary_conflict": "same_clock_boundary_review_before_any_direct_fix",
    "event_ordering_candidate": "row_order_repair_or_repeated_ordering_rule",
    "source_conflict_or_missing_source": "document_source_conflict_or_missing_source",
    "already_reviewed_negative": "keep_blocked_unless_new_source_evidence",
    "documented_residual": "document_residual_no_new_fix",
}


def _normalize_game_id(value: Any) -> str:
    return str(int(value)).zfill(10)


def _as_int(value: Any, default: int = 0) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    try:
        return int(float(value))
    except Exception:
        return default


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


def _list_text(value: Any) -> str:
    parsed = _safe_literal(value)
    if isinstance(parsed, list):
        return json.dumps(parsed)
    if parsed is None:
        return "[]"
    return json.dumps([parsed])


def _normalize_clock(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "." in text:
        text = text.split(".", 1)[0]
    return text


def _is_period_start_clock(clock: Any, period: int) -> bool:
    normalized = _normalize_clock(clock)
    if not normalized:
        return False
    expected = "12:00" if int(period) <= 4 else "5:00"
    if normalized == expected:
        return True
    if expected == "5:00" and normalized == "05:00":
        return True
    return False


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_period_starter_override_keys(path: Path) -> set[tuple[str, int, int]]:
    if not path.exists():
        return set()
    payload = _load_json(path)
    if not isinstance(payload, dict):
        return set()
    keys: set[tuple[str, int, int]] = set()
    for game_id, period_map in payload.items():
        if not isinstance(period_map, dict):
            continue
        for period, team_map in period_map.items():
            if not isinstance(team_map, dict):
                continue
            for team_id in team_map:
                try:
                    keys.add((_normalize_game_id(game_id), int(period), int(team_id)))
                except Exception:
                    continue
    return keys


def _load_manual_review_registry(path: Path) -> dict[tuple[str, int, int], dict[str, Any]]:
    if not path.exists():
        return {}
    payload = _load_json(path)
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
        disposition = str(entry.get("disposition") or "")
        mapping[key] = {
            "manual_review_disposition": disposition,
            "manual_review_recommended_next_track": str(entry.get("recommended_next_track") or ""),
            "manual_review_notes": str(entry.get("notes") or ""),
            "is_reviewed_manual_reject": disposition.startswith("rejected_"),
        }
    return mapping


def _load_same_clock_negative_index(
    primary_manifest_path: Path,
    ft_manifest_path: Path,
) -> tuple[dict[tuple[str, int], dict[str, Any]], dict[tuple[str, int, int], dict[str, Any]]]:
    negative_by_game_period: dict[tuple[str, int], dict[str, Any]] = {}
    reviewed_reject_by_key: dict[tuple[str, int, int], dict[str, Any]] = {}

    if primary_manifest_path.exists():
        payload = _load_json(primary_manifest_path)
        for item in payload.get("reviewed_manual_rejects", []) if isinstance(payload, dict) else []:
            try:
                key = (
                    _normalize_game_id(item["game_id"]),
                    _as_int(item.get("period")),
                    _as_int(item.get("team_id")),
                )
            except Exception:
                continue
            reviewed_reject_by_key[key] = {
                "same_clock_manual_reject_disposition": str(item.get("disposition") or ""),
                "same_clock_manual_reject_note": str(item.get("notes") or ""),
                "same_clock_manual_reject_track": str(item.get("recommended_next_track") or ""),
            }

    if ft_manifest_path.exists():
        payload = _load_json(ft_manifest_path)
        for item in payload.get("guardrails", []) if isinstance(payload, dict) else []:
            try:
                key = (
                    _normalize_game_id(item["game_id"]),
                    _as_int(item.get("period")),
                )
            except Exception:
                continue
            negative_by_game_period[key] = {
                "same_clock_guardrail_role": str(item.get("role") or ""),
                "same_clock_guardrail_note": str(item.get("note") or ""),
            }
    return negative_by_game_period, reviewed_reject_by_key


def _is_dataless(path: Path) -> bool:
    try:
        return bool(getattr(path.stat(), "st_flags", 0) & UF_DATALESS_FLAG)
    except Exception:
        return False


def _load_event_issue_rows(loop_output_dir: Path) -> tuple[pd.DataFrame, list[str], list[str]]:
    frames: list[pd.DataFrame] = []
    skipped_dataless_files: list[str] = []
    read_error_files: list[str] = []
    for csv_path in sorted(loop_output_dir.glob("blocks/*/event_player_on_court_issues_*.csv")):
        if _is_dataless(csv_path):
            skipped_dataless_files.append(str(csv_path))
            continue
        try:
            df = pd.read_csv(csv_path)
        except Exception:
            read_error_files.append(str(csv_path))
            continue
        if df.empty:
            continue
        season_text = csv_path.stem.rsplit("_", 1)[-1]
        df = df.copy()
        df["block_key"] = csv_path.parent.name
        df["season"] = _as_int(season_text)
        df["game_id"] = df["game_id"].map(_normalize_game_id)
        df["period"] = pd.to_numeric(df["period"], errors="coerce").fillna(0).astype(int)
        df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").fillna(0).astype(int)
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").fillna(0).astype(int)
        df["event_num"] = pd.to_numeric(df["event_num"], errors="coerce").fillna(0).astype(int)
        df["status"] = df["status"].fillna("").astype(str)
        df["clock"] = df["clock"].fillna("").astype(str)
        df["player_field"] = df["player_field"].fillna("").astype(str)
        df["event_class"] = df["event_class"].fillna("").astype(str)
        df["player_name"] = df["player_name"].fillna("").astype(str)
        df["event_description"] = df["event_description"].fillna("").astype(str)
        frames.append(df)
    if not frames:
        return pd.DataFrame(), skipped_dataless_files, read_error_files
    return pd.concat(frames, ignore_index=True), skipped_dataless_files, read_error_files


def _load_period_starter_index(path: Path) -> dict[tuple[str, int, int], dict[str, Any]]:
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    if df.empty:
        return {}
    df = df.copy()
    df["game_id"] = df["game_id"].map(_normalize_game_id)
    df["period"] = pd.to_numeric(df["period"], errors="coerce").fillna(0).astype(int)
    df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").fillna(0).astype(int)
    mapping: dict[tuple[str, int, int], dict[str, Any]] = {}
    for row in df.itertuples(index=False):
        missing_ids = _safe_literal(getattr(row, "missing_from_current_ids", None)) or []
        extra_ids = _safe_literal(getattr(row, "extra_in_current_ids", None)) or []
        starter_sets_match = bool(getattr(row, "starter_sets_match", False))
        mapping[(str(row.game_id), int(row.period), int(row.team_id))] = {
            "starter_sets_match": starter_sets_match,
            "missing_from_current_ids": json.dumps(missing_ids),
            "extra_in_current_ids": json.dumps(extra_ids),
            "current_starter_ids": _list_text(getattr(row, "current_starter_ids", None)),
            "tpdev_starter_ids": _list_text(getattr(row, "tpdev_starter_ids", None)),
            "has_period_starter_boundary_issue": (not starter_sets_match)
            or bool(missing_ids)
            or bool(extra_ids),
        }
    return mapping


def _load_intraperiod_index(
    family_register_dir: Path,
) -> dict[tuple[str, int, int], dict[str, Any]]:
    path = family_register_dir / "intraperiod_family_register.csv"
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    if df.empty:
        return {}
    df = df.copy()
    df["game_id"] = df["game_id"].map(_normalize_game_id)
    df["period"] = pd.to_numeric(df["period"], errors="coerce").fillna(0).astype(int)
    df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").fillna(0).astype(int)
    df["local_confidence_score"] = pd.to_numeric(
        df.get("local_confidence_score"), errors="coerce"
    ).fillna(0.0)
    mapping: dict[tuple[str, int, int], dict[str, Any]] = {}
    for key, group in df.groupby(["game_id", "period", "team_id"], dropna=False):
        ordered = group.sort_values(
            [
                "is_reviewed_manual_reject",
                "is_known_negative_tripwire",
                "high_signal_game",
                "local_confidence_score",
            ],
            ascending=[True, True, False, False],
        )
        top = ordered.iloc[0]
        families = sorted(set(group["family"].fillna("").astype(str)) - {""})
        negative_rows = group[group["is_known_negative_tripwire"].fillna(False)]
        reviewed_reject_rows = group[group["is_reviewed_manual_reject"].fillna(False)]
        mapping[(str(key[0]), int(key[1]), int(key[2]))] = {
            "intraperiod_families": json.dumps(families),
            "intraperiod_top_family": str(top.get("family") or ""),
            "intraperiod_manual_review_bucket": bool(top.get("manual_review_bucket", False)),
            "intraperiod_manifest_role": (
                str(negative_rows.iloc[0].get("manifest_role") or "")
                if not negative_rows.empty
                else str(top.get("manifest_role") or "")
            ),
            "intraperiod_manifest_target_type": (
                str(negative_rows.iloc[0].get("manifest_target_type") or "")
                if not negative_rows.empty
                else str(top.get("manifest_target_type") or "")
            ),
            "intraperiod_is_known_negative_tripwire": bool(
                group["is_known_negative_tripwire"].fillna(False).any()
            ),
            "intraperiod_is_reviewed_manual_reject": bool(
                group["is_reviewed_manual_reject"].fillna(False).any()
            ),
            "intraperiod_manual_review_disposition": str(
                (
                    reviewed_reject_rows.iloc[0].get("manual_review_disposition")
                    if not reviewed_reject_rows.empty
                    else top.get("manual_review_disposition")
                )
                or ""
            ),
            "intraperiod_manual_review_recommended_next_track": str(
                (
                    reviewed_reject_rows.iloc[0].get("manual_review_recommended_next_track")
                    if not reviewed_reject_rows.empty
                    else top.get("manual_review_recommended_next_track")
                )
                or ""
            ),
            "intraperiod_game_max_minutes_abs_diff": float(
                top.get("game_max_minutes_abs_diff") or 0.0
            ),
            "intraperiod_game_event_issue_rows": _as_int(top.get("game_event_issue_rows")),
        }
    return mapping


def _load_same_clock_index(
    same_clock_register_dir: Path,
) -> dict[tuple[str, int, int], dict[str, Any]]:
    path = same_clock_register_dir / "same_clock_attribution_register.csv"
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    if df.empty:
        return {}
    df = df.copy()
    df["game_id"] = df["game_id"].map(_normalize_game_id)
    df["period"] = pd.to_numeric(df["period"], errors="coerce").fillna(0).astype(int)
    df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").fillna(0).astype(int)
    mapping: dict[tuple[str, int, int], dict[str, Any]] = {}
    for key, group in df.groupby(["game_id", "period", "team_id"], dropna=False):
        families = sorted(set(group["same_clock_family"].fillna("").astype(str)) - {""})
        ordered = (
            group["same_clock_family"]
            .fillna("")
            .astype(str)
            .value_counts()
            .sort_values(ascending=False)
        )
        mapping[(str(key[0]), int(key[1]), int(key[2]))] = {
            "same_clock_family": ordered.index[0] if not ordered.empty else "",
            "same_clock_families": json.dumps(families),
            "same_clock_candidate_rows": int(len(group)),
        }
    return mapping


def _load_large_minute_index(
    path: Path,
) -> dict[tuple[str, int, int], dict[str, Any]]:
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    if df.empty or "game_id" not in df.columns or "team_id" not in df.columns or "player_id" not in df.columns:
        return {}
    df = df.copy()
    df["game_id"] = df["game_id"].map(_normalize_game_id)
    df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").fillna(0).astype(int)
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").fillna(0).astype(int)
    mapping: dict[tuple[str, int, int], dict[str, Any]] = {}
    family_col = "family" if "family" in df.columns else ""
    for key, group in df.groupby(["game_id", "team_id", "player_id"], dropna=False):
        families = (
            sorted(set(group[family_col].fillna("").astype(str)) - {""}) if family_col else []
        )
        mapping[(str(key[0]), int(key[1]), int(key[2]))] = {
            "minute_families": json.dumps(families),
            "minute_top_family": families[0] if families else "",
        }
    return mapping


def _event_description_flags(event_class_values: list[str], description_values: list[str]) -> dict[str, bool]:
    text = " ".join(description_values).lower()
    event_classes = {value.lower() for value in event_class_values}
    return {
        "technical_or_ejection_like": (
            "technical" in text
            or "ejection" in text
            or any("technical" in value or "ejection" in value for value in event_classes)
        )
    }


def _classify_group(row: pd.Series) -> tuple[str, str]:
    if str(row.get("same_clock_family") or "") == "scorer_sub_same_clock_ordering":
        return (
            "already_reviewed_negative",
            "scorer/sub same-clock bucket is currently frozen as a blocked tripwire family",
        )

    if str(row.get("same_clock_guardrail_role") or "") == "negative_tripwire":
        return (
            "already_reviewed_negative",
            str(row.get("same_clock_guardrail_note") or "same-clock guardrail blocks this lane"),
        )

    if str(row.get("same_clock_manual_reject_disposition") or ""):
        disposition = str(row.get("same_clock_manual_reject_disposition") or "same_clock_manual_reject")
        return (
            "already_reviewed_negative",
            f"same-clock manual review already rejected this lane ({disposition})",
        )

    if bool(row.get("is_known_negative_tripwire")):
        target_type = str(row.get("intraperiod_manifest_target_type") or "negative_tripwire")
        return "already_reviewed_negative", f"manifest marks this lane as a blocked negative canary ({target_type})"

    if bool(row.get("is_reviewed_manual_reject")):
        note = str(row.get("manual_review_disposition") or "reviewed_manual_reject")
        return "already_reviewed_negative", f"manual review previously rejected this lane ({note})"

    if bool(row.get("has_period_starter_boundary_issue")) and bool(row.get("has_period_start_clock_issue")):
        return (
            "opening_cluster_period_starter",
            "period-starter disagreement shows up inside the opening cluster",
        )

    if bool(row.get("has_period_starter_boundary_issue")):
        return (
            "period_starter_boundary",
            "period-starter audit disagrees on this game/period/team boundary",
        )

    if bool(row.get("has_same_clock_conflict")):
        family = str(row.get("same_clock_family") or "")
        if family:
            return (
                "same_clock_boundary_conflict",
                f"same-clock register flags {family} for this boundary",
            )
        return (
            "same_clock_boundary_conflict",
            "same-clock boundary conflict appears in the event-on-court issues",
        )

    intraperiod_top_family = str(row.get("intraperiod_top_family") or "")
    minute_top_family = str(row.get("minute_top_family") or "")
    if (
        intraperiod_top_family == "insufficient_local_context"
        and minute_top_family == "v3_ordering_candidate"
        and not bool(row.get("has_same_clock_conflict"))
    ):
        return (
            "documented_residual",
            "weak intraperiod signal plus thin v3-ordering evidence is not strong enough to keep this lane in the active fix queue",
        )

    if intraperiod_top_family:
        return (
            "intraperiod_missing_sub_candidate",
            f"intraperiod family register flags {intraperiod_top_family}",
        )

    minute_top_family = str(row.get("minute_top_family") or "")
    if minute_top_family == "v3_ordering_candidate":
        return (
            "event_ordering_candidate",
            "large-minute family register points to an event-ordering mismatch",
        )
    if minute_top_family == "source_conflict_or_missing_source":
        return (
            "source_conflict_or_missing_source",
            "large-minute family register marks this player as source conflict or missing source",
        )

    if bool(row.get("technical_or_ejection_like")):
        return (
            "documented_residual",
            "technical/ejection-style event credit likely reflects an audit false positive rather than a lineup bug",
        )

    return (
        "documented_residual",
        "no stronger upstream fix family matched this event-on-court issue",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an upstream-first family register for event-on-court issues."
    )
    parser.add_argument("--loop-output-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--family-register-dir", type=Path)
    parser.add_argument("--same-clock-register-dir", type=Path)
    parser.add_argument(
        "--period-starter-audit-path",
        type=Path,
        default=DEFAULT_PERIOD_STARTER_AUDIT_PATH,
    )
    parser.add_argument(
        "--large-minute-register-path",
        type=Path,
        default=DEFAULT_LARGE_MINUTE_REGISTER_PATH,
    )
    parser.add_argument(
        "--period-starter-overrides-path",
        type=Path,
        default=DEFAULT_PERIOD_STARTER_OVERRIDES_PATH,
    )
    parser.add_argument(
        "--manual-review-registry-path",
        type=Path,
        default=DEFAULT_MANUAL_REVIEW_REGISTRY_PATH,
    )
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


def main() -> int:
    args = parse_args()
    loop_output_dir = args.loop_output_dir.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    family_register_dir = (
        args.family_register_dir.resolve()
        if args.family_register_dir is not None
        else loop_output_dir / "family_register"
    )
    same_clock_register_dir = (
        args.same_clock_register_dir.resolve()
        if args.same_clock_register_dir is not None
        else loop_output_dir / "same_clock_attribution"
    )

    event_df, skipped_dataless_files, read_error_files = _load_event_issue_rows(loop_output_dir)
    if event_df.empty:
        empty_df = pd.DataFrame()
        empty_df.to_csv(output_dir / "event_on_court_family_register.csv", index=False)
        empty_df.to_csv(output_dir / "event_on_court_game_period_team_summary.csv", index=False)
        summary = {
            "rows": 0,
            "games": 0,
            "issue_rows_total": 0,
            "group_rows": 0,
            "counts_by_family": {},
            "counts_by_actionability": {},
            "counts_by_season": {},
            "counts_by_family_and_season": [],
            "unclassified_rows": 0,
            "skipped_dataless_file_count": len(skipped_dataless_files),
            "skipped_dataless_files": skipped_dataless_files,
            "read_error_file_count": len(read_error_files),
            "read_error_files": read_error_files,
            "top_actionable_groups": [],
        }
        (output_dir / "summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0

    period_starter_index = _load_period_starter_index(args.period_starter_audit_path.resolve())
    intraperiod_index = _load_intraperiod_index(family_register_dir)
    same_clock_index = _load_same_clock_index(same_clock_register_dir)
    large_minute_index = _load_large_minute_index(args.large_minute_register_path.resolve())
    period_starter_override_keys = _load_period_starter_override_keys(
        args.period_starter_overrides_path.resolve()
    )
    manual_review_registry = _load_manual_review_registry(args.manual_review_registry_path.resolve())
    same_clock_negative_by_game_period, same_clock_reviewed_reject_by_key = (
        _load_same_clock_negative_index(
            args.same_clock_canary_manifest_path.resolve(),
            args.non_opening_ft_manifest_path.resolve(),
        )
    )

    join_keys = ["game_id", "period", "team_id"]
    event_df["group_key"] = event_df.apply(
        lambda row: f"{row['game_id']}|{int(row['period'])}|{int(row['team_id'])}", axis=1
    )

    grouped_rows: list[dict[str, Any]] = []
    for key, group in event_df.groupby(join_keys, dropna=False):
        game_id, period, team_id = str(key[0]), int(key[1]), int(key[2])
        starter_meta = period_starter_index.get((game_id, period, team_id), {})
        intraperiod_meta = intraperiod_index.get((game_id, period, team_id), {})
        same_clock_meta = same_clock_index.get((game_id, period, team_id), {})
        manual_meta = manual_review_registry.get((game_id, period, team_id), {})
        same_clock_guardrail_meta = same_clock_negative_by_game_period.get((game_id, period), {})
        same_clock_reviewed_reject_meta = same_clock_reviewed_reject_by_key.get(
            (game_id, period, team_id), {}
        )

        minute_meta_matches = []
        for player_id in sorted(set(group["player_id"].astype(int))):
            player_meta = large_minute_index.get((game_id, team_id, int(player_id)))
            if player_meta:
                minute_meta_matches.append(player_meta)
        minute_families: set[str] = set()
        for player_meta in minute_meta_matches:
            minute_families.update(_safe_literal(player_meta.get("minute_families")) or [])
        minute_top_family = ""
        if minute_meta_matches:
            minute_top_family = str(minute_meta_matches[0].get("minute_top_family") or "")

        event_class_values = sorted(set(group["event_class"].fillna("").astype(str)) - {""})
        description_values = [str(value) for value in group["event_description"].fillna("").tolist()]
        flags = _event_description_flags(event_class_values, description_values)
        has_period_start_clock_issue = any(
            _is_period_start_clock(clock, period) for clock in group["clock"].fillna("")
        )
        statuses = sorted(set(group["status"].fillna("").astype(str)) - {""})
        has_same_clock_conflict = (
            "same_clock_boundary_conflict" in statuses or bool(same_clock_meta)
        )
        grouped = {
            "block_key": str(group["block_key"].iloc[0]),
            "season": int(group["season"].iloc[0]),
            "game_id": game_id,
            "period": period,
            "team_id": team_id,
            "issue_rows": int(len(group)),
            "status_counts": json.dumps(group["status"].value_counts().sort_index().to_dict(), sort_keys=True),
            "statuses": json.dumps(statuses),
            "player_ids": json.dumps(sorted(set(group["player_id"].astype(int)))),
            "player_names": json.dumps(sorted(set(group["player_name"].fillna("").astype(str)) - {""})),
            "event_nums": json.dumps(sorted(set(group["event_num"].astype(int)))),
            "event_classes": json.dumps(event_class_values),
            "clocks": json.dumps(sorted(set(group["clock"].fillna("").astype(str)) - {""})),
            "has_period_start_clock_issue": has_period_start_clock_issue,
            "starter_sets_match": bool(starter_meta.get("starter_sets_match", True)),
            "has_period_starter_boundary_issue": bool(
                starter_meta.get("has_period_starter_boundary_issue", False)
            ),
            "period_starter_missing_from_current_ids": starter_meta.get(
                "missing_from_current_ids", "[]"
            ),
            "period_starter_extra_in_current_ids": starter_meta.get(
                "extra_in_current_ids", "[]"
            ),
            "period_starter_current_starter_ids": starter_meta.get("current_starter_ids", "[]"),
            "period_starter_tpdev_starter_ids": starter_meta.get("tpdev_starter_ids", "[]"),
            "has_local_period_starter_override": (
                (game_id, period, team_id) in period_starter_override_keys
            ),
            "intraperiod_families": intraperiod_meta.get("intraperiod_families", "[]"),
            "intraperiod_top_family": intraperiod_meta.get("intraperiod_top_family", ""),
            "intraperiod_manual_review_bucket": bool(
                intraperiod_meta.get("intraperiod_manual_review_bucket", False)
            ),
            "intraperiod_manifest_role": intraperiod_meta.get("intraperiod_manifest_role", ""),
            "intraperiod_manifest_target_type": intraperiod_meta.get(
                "intraperiod_manifest_target_type", ""
            ),
            "is_known_negative_tripwire": bool(
                intraperiod_meta.get("intraperiod_is_known_negative_tripwire", False)
            ),
            "same_clock_family": same_clock_meta.get("same_clock_family", ""),
            "same_clock_families": same_clock_meta.get("same_clock_families", "[]"),
            "same_clock_candidate_rows": int(same_clock_meta.get("same_clock_candidate_rows", 0)),
            "same_clock_guardrail_role": same_clock_guardrail_meta.get(
                "same_clock_guardrail_role", ""
            ),
            "same_clock_guardrail_note": same_clock_guardrail_meta.get(
                "same_clock_guardrail_note", ""
            ),
            "same_clock_manual_reject_disposition": same_clock_reviewed_reject_meta.get(
                "same_clock_manual_reject_disposition", ""
            ),
            "same_clock_manual_reject_note": same_clock_reviewed_reject_meta.get(
                "same_clock_manual_reject_note", ""
            ),
            "same_clock_manual_reject_track": same_clock_reviewed_reject_meta.get(
                "same_clock_manual_reject_track", ""
            ),
            "has_same_clock_conflict": has_same_clock_conflict,
            "minute_families": json.dumps(sorted(minute_families)),
            "minute_top_family": minute_top_family,
            "manual_review_disposition": manual_meta.get(
                "manual_review_disposition",
                intraperiod_meta.get("intraperiod_manual_review_disposition", ""),
            ),
            "manual_review_recommended_next_track": manual_meta.get(
                "manual_review_recommended_next_track",
                intraperiod_meta.get("intraperiod_manual_review_recommended_next_track", ""),
            ),
            "manual_review_notes": manual_meta.get("manual_review_notes", ""),
            "is_reviewed_manual_reject": bool(
                manual_meta.get(
                    "is_reviewed_manual_reject",
                    intraperiod_meta.get("intraperiod_is_reviewed_manual_reject", False),
                )
            ),
            "technical_or_ejection_like": flags["technical_or_ejection_like"],
        }
        family, notes = _classify_group(pd.Series(grouped))
        grouped["family"] = family
        grouped["priority"] = FAMILY_PRIORITY[family]
        grouped["actionability"] = FAMILY_ACTIONABILITY[family]
        grouped["recommended_next_action"] = FAMILY_ACTION[family]
        grouped["notes"] = notes
        grouped_rows.append(grouped)

    group_df = pd.DataFrame(grouped_rows)
    group_df = group_df.sort_values(
        ["priority", "actionability", "issue_rows", "season", "game_id", "period", "team_id"],
        ascending=[True, True, False, True, True, True, True],
        key=lambda col: col.map({"high": 0, "medium": 1, "low": 2}) if col.name == "priority" else col,
    ).reset_index(drop=True)

    group_lookup = {
        (str(row.game_id), int(row.period), int(row.team_id)): row._asdict()
        for row in group_df.itertuples(index=False)
    }

    row_records: list[dict[str, Any]] = []
    for row in event_df.itertuples(index=False):
        group_meta = group_lookup[(str(row.game_id), int(row.period), int(row.team_id))]
        row_records.append(
            {
                "block_key": str(row.block_key),
                "season": int(row.season),
                "game_id": str(row.game_id),
                "period": int(row.period),
                "team_id": int(row.team_id),
                "event_num": int(row.event_num),
                "clock": str(row.clock),
                "event_class": str(row.event_class),
                "player_field": str(row.player_field),
                "player_id": int(row.player_id),
                "player_name": str(row.player_name),
                "status": str(row.status),
                "event_description": str(row.event_description),
                "family": group_meta["family"],
                "priority": group_meta["priority"],
                "actionability": group_meta["actionability"],
                "recommended_next_action": group_meta["recommended_next_action"],
                "notes": group_meta["notes"],
                "has_period_starter_boundary_issue": bool(
                    group_meta["has_period_starter_boundary_issue"]
                ),
                "has_period_start_clock_issue": bool(group_meta["has_period_start_clock_issue"]),
                "has_same_clock_conflict": bool(group_meta["has_same_clock_conflict"]),
                "intraperiod_top_family": str(group_meta["intraperiod_top_family"]),
                "same_clock_family": str(group_meta["same_clock_family"]),
                "minute_top_family": str(group_meta["minute_top_family"]),
                "is_reviewed_manual_reject": bool(group_meta["is_reviewed_manual_reject"]),
            }
        )

    row_df = pd.DataFrame(row_records)
    row_df.to_csv(output_dir / "event_on_court_family_register.csv", index=False)
    group_df.to_csv(output_dir / "event_on_court_game_period_team_summary.csv", index=False)

    actionable_df = group_df[
        group_df["actionability"].isin(
            ["immediate_fix_queue", "manual_or_local_override_queue", "event_ordering_queue"]
        )
    ].copy()
    actionable_df.to_csv(output_dir / "event_on_court_actionable_shortlist.csv", index=False)

    family_counts = group_df["family"].value_counts().sort_index().to_dict()
    actionability_counts = group_df["actionability"].value_counts().sort_index().to_dict()
    season_counts = row_df["season"].value_counts().sort_index().to_dict()
    family_season_counts = (
        row_df.groupby(["season", "family"]).size().rename("rows").reset_index().to_dict("records")
    )
    summary = {
        "rows": int(len(row_df)),
        "games": int(row_df["game_id"].nunique()),
        "issue_rows_total": int(len(row_df)),
        "group_rows": int(len(group_df)),
        "counts_by_family": family_counts,
        "counts_by_actionability": actionability_counts,
        "counts_by_season": {str(key): int(value) for key, value in season_counts.items()},
        "counts_by_family_and_season": family_season_counts,
        "unclassified_rows": int((row_df["family"] == "").sum()),
        "skipped_dataless_file_count": len(skipped_dataless_files),
        "skipped_dataless_files": skipped_dataless_files,
        "read_error_file_count": len(read_error_files),
        "read_error_files": read_error_files,
        "top_actionable_groups": actionable_df[
            [
                "block_key",
                "season",
                "game_id",
                "period",
                "team_id",
                "issue_rows",
                "family",
                "priority",
                "actionability",
                "recommended_next_action",
                "notes",
            ]
        ]
        .head(25)
        .to_dict("records"),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
