from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


RELEASE_GATE_STATUS_VALUES = {
    "exact",
    "override_corrected",
    "source_limited_upstream_error",
    "accepted_boundary_difference",
    "accepted_unresolvable_contradiction",
    "documented_hold",
    "open_actionable",
}

EXECUTION_LANE_VALUES = {
    "policy_frontier_non_local",
    "accepted_contradiction",
    "documented_hold",
    "source_limited",
    "override_corrected",
    "exact",
    "unreviewed_open",
    "local_override_chosen",
    "synthetic_sub_chosen",
    "systematic_rule_chosen",
    "status_quo_chosen",
    "policy_overlay_chosen",
}

POLICY_SOURCE_VALUES = {"auto_default", "reviewed_override"}

REVIEWED_POLICY_COLUMNS = [
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
]

RELEASE_POLICY_OUTPUT_COLUMNS = [
    "release_gate_status",
    "release_reason_code",
    "execution_lane",
    "blocks_release",
    "research_open",
    "policy_source",
]


def normalize_game_id(value: Any) -> str:
    return str(int(value)).zfill(10)


def _is_missing(value: Any) -> bool:
    return pd.isna(value) or value in (None, "")


def parse_boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n", ""}:
        return False
    raise ValueError(f"Unsupported boolean value: {value!r}")


def default_release_policy(primary_quality_status: str) -> dict[str, Any]:
    normalized = str(primary_quality_status or "")
    if normalized == "exact":
        return {
            "release_gate_status": "exact",
            "release_reason_code": "exact",
            "execution_lane": "exact",
            "blocks_release": False,
            "research_open": False,
            "policy_source": "auto_default",
        }
    if normalized == "override_corrected":
        return {
            "release_gate_status": "override_corrected",
            "release_reason_code": "override_corrected",
            "execution_lane": "override_corrected",
            "blocks_release": False,
            "research_open": False,
            "policy_source": "auto_default",
        }
    if normalized == "source_limited":
        return {
            "release_gate_status": "source_limited_upstream_error",
            "release_reason_code": "source_limited_upstream_error",
            "execution_lane": "source_limited",
            "blocks_release": False,
            "research_open": False,
            "policy_source": "auto_default",
        }
    if normalized == "boundary_difference":
        return {
            "release_gate_status": "accepted_boundary_difference",
            "release_reason_code": "general_boundary_difference",
            "execution_lane": "policy_frontier_non_local",
            "blocks_release": False,
            "research_open": False,
            "policy_source": "auto_default",
        }
    return {
        "release_gate_status": "open_actionable",
        "release_reason_code": "unreviewed_open_actionable",
        "execution_lane": "unreviewed_open",
        "blocks_release": True,
        "research_open": True,
        "policy_source": "auto_default",
    }


def load_reviewed_policy_overlay(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    overlay_path = path.resolve()
    if not overlay_path.exists():
        raise FileNotFoundError(f"Reviewed policy overlay not found: {overlay_path}")

    df = pd.read_csv(overlay_path, dtype=str).fillna("")
    if df.empty:
        return {}
    missing = [column for column in REVIEWED_POLICY_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Reviewed policy overlay missing columns: {missing}")

    result: dict[str, dict[str, Any]] = {}
    for record in df.to_dict(orient="records"):
        game_id = normalize_game_id(record["game_id"])
        if game_id in result:
            raise ValueError(f"Duplicate reviewed policy row for game_id={game_id}")
        release_gate_status = str(record["release_gate_status"] or "")
        execution_lane = str(record["execution_lane"] or "")
        policy_source = str(record["policy_source"] or "")
        if release_gate_status not in RELEASE_GATE_STATUS_VALUES:
            raise ValueError(f"Unsupported release_gate_status for {game_id}: {release_gate_status}")
        if execution_lane not in EXECUTION_LANE_VALUES:
            raise ValueError(f"Unsupported execution_lane for {game_id}: {execution_lane}")
        if policy_source not in POLICY_SOURCE_VALUES:
            raise ValueError(f"Unsupported policy_source for {game_id}: {policy_source}")
        if policy_source == "reviewed_override" and not str(record["expected_primary_quality_status"] or ""):
            raise ValueError(f"Reviewed override row requires expected_primary_quality_status for {game_id}")
        result[game_id] = {
            "policy_decision_id": str(record["policy_decision_id"] or ""),
            "game_id": game_id,
            "release_gate_status": release_gate_status,
            "release_reason_code": str(record["release_reason_code"] or ""),
            "execution_lane": execution_lane,
            "blocks_release": parse_boolish(record["blocks_release"]),
            "research_open": parse_boolish(record["research_open"]),
            "policy_source": policy_source,
            "expected_primary_quality_status": str(record["expected_primary_quality_status"] or ""),
            "evidence_artifact": str(record["evidence_artifact"] or ""),
            "reviewed_at": str(record["reviewed_at"] or ""),
            "notes": str(record["notes"] or ""),
        }
    return result


def apply_release_policy(game_row: dict[str, Any], overlay_by_game: dict[str, dict[str, Any]]) -> dict[str, Any]:
    game_id = normalize_game_id(game_row["game_id"])
    primary_quality_status = str(game_row.get("primary_quality_status") or "")
    policy = default_release_policy(primary_quality_status)
    overlay = overlay_by_game.get(game_id)
    if overlay is not None:
        expected = str(overlay.get("expected_primary_quality_status") or "")
        if expected and expected != primary_quality_status:
            raise ValueError(
                f"Stale reviewed policy override for {game_id}: expected primary_quality_status={expected}, found {primary_quality_status}"
            )
        for key in RELEASE_POLICY_OUTPUT_COLUMNS:
            policy[key] = overlay[key]
    return policy


def ensure_release_policy_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        for column in RELEASE_POLICY_OUTPUT_COLUMNS:
            df[column] = pd.Series(dtype="object")
        return df

    rows: list[dict[str, Any]] = []
    for record in df.to_dict(orient="records"):
        primary_quality_status = str(record.get("primary_quality_status") or "")
        policy = default_release_policy(primary_quality_status)
        for column in RELEASE_POLICY_OUTPUT_COLUMNS:
            if column not in record or _is_missing(record[column]):
                continue
            if column in {"blocks_release", "research_open"}:
                policy[column] = parse_boolish(record[column])
            else:
                policy[column] = str(record[column])
        record.update(policy)
        rows.append(record)
    return pd.DataFrame(rows)
