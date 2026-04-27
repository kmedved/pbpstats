from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_V6_PATH = ROOT / "period_starters_v6.parquet"
DEFAULT_V5_PATH = ROOT / "period_starters_v5.parquet"
DEFAULT_V6_UNRESOLVED_PATH = ROOT / "period_starters_unresolved_v6.parquet"
DEFAULT_V6_FAILURES_PATH = ROOT / "period_starters_failures_v6.parquet"
DEFAULT_V5_UNRESOLVED_PATH = ROOT / "period_starters_unresolved_v5.parquet"
DEFAULT_V5_FAILURES_PATH = ROOT / "period_starters_failures_v5.parquet"
DEFAULT_V5_QUEUE_PATH = ROOT / "period_starters_v5_rescrape_queue.parquet"
DEFAULT_OVERRIDES_PATH = ROOT / "overrides" / "period_starters_overrides.json"


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _normalize_key(game_id: str | int, period: Any) -> tuple[str, int]:
    return (_normalize_game_id(game_id), int(period))


def _load_resolved_rows(path: Path) -> dict[tuple[str, int], dict[str, Any]]:
    if not path.exists():
        return {}
    df = pd.read_parquet(path)
    if "resolved" in df.columns:
        df = df[df["resolved"] == True]  # noqa: E712
    rows: dict[tuple[str, int], dict[str, Any]] = {}
    for row in df.itertuples(index=False):
        key = _normalize_key(row.game_id, row.period)
        rows[key] = {
            "game_id": key[0],
            "period": key[1],
            "away_team_id": int(row.away_team_id),
            "home_team_id": int(row.home_team_id),
            "away_players": tuple(
                sorted(int(getattr(row, f"away_player{i}")) for i in range(1, 6))
            ),
            "home_players": tuple(
                sorted(int(getattr(row, f"home_player{i}")) for i in range(1, 6))
            ),
            "resolver_mode": getattr(row, "resolver_mode", None),
        }
    return rows


def _load_key_set(path: Path) -> set[tuple[str, int]]:
    if not path.exists():
        return set()
    df = pd.read_parquet(path, columns=["game_id", "period"])
    return {_normalize_key(row.game_id, row.period) for row in df.itertuples(index=False)}


def _load_override_map(path: Path) -> dict[tuple[str, int], dict[str, Any]]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    overrides: dict[tuple[str, int], dict[str, Any]] = {}
    for raw_game_id, periods in raw.items():
        game_id = _normalize_game_id(raw_game_id)
        if not isinstance(periods, dict):
            continue
        for raw_period, teams in periods.items():
            key = (game_id, int(raw_period))
            team_ids = []
            if isinstance(teams, dict):
                for raw_team_id in teams:
                    try:
                        team_ids.append(int(raw_team_id))
                    except (TypeError, ValueError):
                        continue
            overrides[key] = {
                "override_present": True,
                "override_team_ids": tuple(sorted(team_ids)),
                "override_team_count": len(team_ids),
            }
    return overrides


def _starter_rows_match(v6_row: dict[str, Any], v5_row: dict[str, Any]) -> bool:
    return (
        v6_row["away_team_id"] == v5_row["away_team_id"]
        and v6_row["home_team_id"] == v5_row["home_team_id"]
        and v6_row["away_players"] == v5_row["away_players"]
        and v6_row["home_players"] == v5_row["home_players"]
    )


def _status_for_key(
    key: tuple[str, int],
    *,
    resolved: dict[tuple[str, int], dict[str, Any]],
    unresolved: set[tuple[str, int]],
    failures: set[tuple[str, int]],
    queued: set[tuple[str, int]] | None = None,
) -> str:
    if key in resolved:
        return "resolved"
    if key in unresolved:
        return "unresolved"
    if key in failures:
        return "failure"
    if queued is not None and key in queued:
        return "queued"
    return "missing"


def _category_for_row(
    key: tuple[str, int],
    *,
    v6_resolved: dict[tuple[str, int], dict[str, Any]],
    v5_resolved: dict[tuple[str, int], dict[str, Any]],
    v6_status: str,
    v5_status: str,
) -> str:
    if v6_status == "resolved" and v5_status == "resolved":
        if _starter_rows_match(v6_resolved[key], v5_resolved[key]):
            return "both_resolved_match"
        return "both_resolved_disagree"
    if v6_status == "resolved":
        return f"v6_resolved_v5_{v5_status}"
    if v5_status == "resolved":
        return f"v6_{v6_status}_v5_resolved"
    return f"v6_{v6_status}_v5_{v5_status}"


def build_gap_report(
    *,
    v6_path: Path,
    v5_path: Path,
    v6_unresolved_path: Path,
    v6_failures_path: Path,
    v5_unresolved_path: Path,
    v5_failures_path: Path,
    v5_queue_path: Path,
    overrides_path: Path,
) -> pd.DataFrame:
    v6_resolved = _load_resolved_rows(v6_path)
    v5_resolved = _load_resolved_rows(v5_path)
    v6_unresolved = _load_key_set(v6_unresolved_path)
    v6_failures = _load_key_set(v6_failures_path)
    v5_unresolved = _load_key_set(v5_unresolved_path)
    v5_failures = _load_key_set(v5_failures_path)
    v5_queue = _load_key_set(v5_queue_path)
    overrides = _load_override_map(overrides_path)

    all_keys = (
        set(v6_resolved)
        | set(v5_resolved)
        | v6_unresolved
        | v6_failures
        | v5_unresolved
        | v5_failures
        | v5_queue
        | set(overrides)
    )

    rows = []
    for key in sorted(all_keys):
        v6_status = _status_for_key(
            key,
            resolved=v6_resolved,
            unresolved=v6_unresolved,
            failures=v6_failures,
        )
        v5_status = _status_for_key(
            key,
            resolved=v5_resolved,
            unresolved=v5_unresolved,
            failures=v5_failures,
            queued=v5_queue,
        )
        category = _category_for_row(
            key,
            v6_resolved=v6_resolved,
            v5_resolved=v5_resolved,
            v6_status=v6_status,
            v5_status=v5_status,
        )
        override_info = overrides.get(
            key,
            {
                "override_present": False,
                "override_team_ids": tuple(),
                "override_team_count": 0,
            },
        )
        v6_row = v6_resolved.get(key)
        v5_row = v5_resolved.get(key)
        rows.append(
            {
                "game_id": key[0],
                "period": key[1],
                "category": category,
                "v6_status": v6_status,
                "v5_status": v5_status,
                "v6_resolver_mode": (v6_row or {}).get("resolver_mode"),
                "v5_resolver_mode": (v5_row or {}).get("resolver_mode"),
                "both_resolved_match": bool(
                    v6_status == "resolved"
                    and v5_status == "resolved"
                    and _starter_rows_match(v6_row, v5_row)
                ),
                "override_present": bool(override_info["override_present"]),
                "override_team_count": int(override_info["override_team_count"]),
                "override_team_ids": list(override_info["override_team_ids"]),
            }
        )

    report_df = pd.DataFrame(rows)
    if report_df.empty:
        return report_df
    return report_df.sort_values(["category", "game_id", "period"]).reset_index(drop=True)


def summarize_gap_report(report_df: pd.DataFrame) -> dict[str, Any]:
    if report_df.empty:
        return {"rows": 0, "actionable_rows": 0}

    actionable_df = report_df[report_df["category"] != "both_resolved_match"].copy()
    return {
        "rows": int(len(report_df)),
        "actionable_rows": int(len(actionable_df)),
        "category_counts": dict(Counter(actionable_df["category"])),
        "v6_status_counts_actionable": dict(Counter(actionable_df["v6_status"])),
        "v5_status_counts_actionable": dict(Counter(actionable_df["v5_status"])),
        "override_rows_actionable": int(actionable_df["override_present"].sum()),
        "both_resolved_match_rows": int((report_df["category"] == "both_resolved_match").sum()),
        "both_resolved_disagree_rows": int((report_df["category"] == "both_resolved_disagree").sum()),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report where period_starters_v6 is resolved, missing, or deferred to v5."
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--v6-path", type=Path, default=DEFAULT_V6_PATH)
    parser.add_argument("--v5-path", type=Path, default=DEFAULT_V5_PATH)
    parser.add_argument("--v6-unresolved-path", type=Path, default=DEFAULT_V6_UNRESOLVED_PATH)
    parser.add_argument("--v6-failures-path", type=Path, default=DEFAULT_V6_FAILURES_PATH)
    parser.add_argument("--v5-unresolved-path", type=Path, default=DEFAULT_V5_UNRESOLVED_PATH)
    parser.add_argument("--v5-failures-path", type=Path, default=DEFAULT_V5_FAILURES_PATH)
    parser.add_argument("--v5-queue-path", type=Path, default=DEFAULT_V5_QUEUE_PATH)
    parser.add_argument("--overrides-path", type=Path, default=DEFAULT_OVERRIDES_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    report_df = build_gap_report(
        v6_path=args.v6_path,
        v5_path=args.v5_path,
        v6_unresolved_path=args.v6_unresolved_path,
        v6_failures_path=args.v6_failures_path,
        v5_unresolved_path=args.v5_unresolved_path,
        v5_failures_path=args.v5_failures_path,
        v5_queue_path=args.v5_queue_path,
        overrides_path=args.overrides_path,
    )
    summary = summarize_gap_report(report_df)

    actionable_df = report_df[report_df["category"] != "both_resolved_match"].copy()
    report_df.to_parquet(args.output_dir / "period_starter_source_gap_report_full.parquet", index=False)
    report_df.to_csv(args.output_dir / "period_starter_source_gap_report_full.csv", index=False)
    actionable_df.to_parquet(args.output_dir / "period_starter_source_gap_report_actionable.parquet", index=False)
    actionable_df.to_csv(args.output_dir / "period_starter_source_gap_report_actionable.csv", index=False)
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
