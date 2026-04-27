from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_mapping_arg(values: list[str]) -> dict[int, Path]:
    result: dict[int, Path] = {}
    for value in values:
        season_text, path_text = value.split("=", 1)
        result[int(season_text)] = Path(path_text).resolve()
    return result


def _summary_path(path: Path, season: int) -> Path:
    if path.is_dir():
        return path / f"summary_{season}.json"
    return path


def _extract_metrics(summary: dict[str, Any]) -> dict[str, int]:
    lineup = summary.get("lineup_audit", {})
    minutes = lineup.get("minutes_plus_minus", {})
    event_on_court = lineup.get("event_on_court", {})
    boxscore = summary.get("boxscore_audit", {})
    return {
        "failed_games": int(summary.get("failed_games", 0) or 0),
        "event_stats_errors": int(summary.get("event_stats_errors", 0) or 0),
        "audit_failures": int(boxscore.get("audit_failures", 0) or 0),
        "team_rows_with_mismatch": int(boxscore.get("team_rows_with_mismatch", 0) or 0),
        "player_rows_with_mismatch": int(boxscore.get("player_rows_with_mismatch", 0) or 0),
        "minutes_mismatches": int(minutes.get("minutes_mismatches", 0) or 0),
        "minutes_outliers": int(minutes.get("minutes_outliers", 0) or 0),
        "plus_minus_mismatches": int(minutes.get("plus_minus_mismatches", 0) or 0),
        "problem_games": int(lineup.get("problem_games", 0) or 0),
        "event_on_court_issue_rows": int(event_on_court.get("issue_rows", 0) or 0),
        "event_on_court_issue_games": int(event_on_court.get("issue_games", 0) or 0),
    }


def _delta_metrics(current: dict[str, int], baseline: dict[str, int]) -> dict[str, int]:
    return {key: int(current[key]) - int(baseline[key]) for key in current}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize opening-cluster Pass 0 season proofs against locked baseline season summaries."
    )
    parser.add_argument("--result", action="append", default=[], help="season=/path/to/season_output_dir_or_summary.json")
    parser.add_argument("--baseline", action="append", default=[], help="season=/path/to/baseline_summary.json")
    parser.add_argument("--canary-summary", type=Path, help="Optional rerun_selected_games summary.json for opening-cluster canaries")
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result_map = _parse_mapping_arg(args.result)
    baseline_map = _parse_mapping_arg(args.baseline)
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    seasons = sorted(result_map)
    missing = [season for season in seasons if season not in baseline_map]
    if missing:
        raise SystemExit(f"Missing baseline summaries for seasons: {missing}")

    season_rows: list[dict[str, Any]] = []
    pass0_ok = True
    for season in seasons:
        result_summary = _load_json(_summary_path(result_map[season], season))
        baseline_summary = _load_json(_summary_path(baseline_map[season], season))
        result_metrics = _extract_metrics(result_summary)
        baseline_metrics = _extract_metrics(baseline_summary)
        deltas = _delta_metrics(result_metrics, baseline_metrics)
        fail_reasons: list[str] = []
        if result_metrics["failed_games"] != 0:
            fail_reasons.append(f"failed_games={result_metrics['failed_games']}")
        if result_metrics["event_stats_errors"] != 0:
            fail_reasons.append(f"event_stats_errors={result_metrics['event_stats_errors']}")
        if result_metrics["audit_failures"] != 0:
            fail_reasons.append(f"audit_failures={result_metrics['audit_failures']}")
        if result_metrics["team_rows_with_mismatch"] != 0:
            fail_reasons.append(f"team_rows_with_mismatch={result_metrics['team_rows_with_mismatch']}")
        if result_metrics["player_rows_with_mismatch"] != 0:
            fail_reasons.append(f"player_rows_with_mismatch={result_metrics['player_rows_with_mismatch']}")
        if deltas["minutes_outliers"] > 0:
            fail_reasons.append(f"minutes_outliers_delta={deltas['minutes_outliers']}")
        row = {
            "season": season,
            "pass": len(fail_reasons) == 0,
            "fail_reasons": fail_reasons,
            "result_metrics": result_metrics,
            "baseline_metrics": baseline_metrics,
            "deltas": deltas,
        }
        if fail_reasons:
            pass0_ok = False
        season_rows.append(row)

    canary_summary = None
    if args.canary_summary is not None:
        canary_summary = _load_json(args.canary_summary.resolve())

    summary = {
        "pass0_ok": pass0_ok,
        "seasons": season_rows,
        "canary_summary_path": str(args.canary_summary.resolve()) if args.canary_summary else "",
        "canary_summary": canary_summary,
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
