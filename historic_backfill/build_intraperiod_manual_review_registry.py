from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_REGISTRY_PATH = ROOT / "intraperiod_manual_review_registry.json"
DEFAULT_QUEUE_GLOB = "intraperiod_manual_review_queue_20260320*"


def _normalize_game_id(value: Any) -> str:
    return str(int(value)).zfill(10)


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def _as_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def _load_case_summaries(queue_output_dirs: list[Path]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    seen_case_dirs: set[Path] = set()
    for queue_dir in queue_output_dirs:
        if not queue_dir.exists():
            continue
        for summary_path in sorted(queue_dir.glob("*/summary.json")):
            if summary_path.parent in seen_case_dirs:
                continue
            try:
                payload = json.loads(summary_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if "case_slug" not in payload:
                continue
            payload["_summary_path"] = str(summary_path)
            payload["_queue_output_dir"] = str(queue_dir)
            seen_case_dirs.add(summary_path.parent)
            summaries.append(payload)
    return sorted(summaries, key=lambda item: str(item.get("case_slug") or ""))


def _disposition_for_case(case_summary: dict[str, Any]) -> tuple[str, str, str]:
    acceptance = case_summary.get("acceptance") or {}
    candidate = case_summary.get("candidate") or {}
    game_delta = case_summary.get("game_delta") or {}

    accepted = bool(acceptance.get("accepted"))
    manual_only = bool(case_summary.get("manual_only", False))
    confidence_gap = _as_float(
        candidate.get("confidence_gap", candidate.get("best_vs_runner_up_confidence_gap"))
    )
    event_delta = _as_int(game_delta.get("event_issue_rows_delta"))
    minute_outlier_delta = _as_int(game_delta.get("minute_outlier_rows_delta"))
    plus_minus_delta = _as_int(game_delta.get("plus_minus_mismatch_rows_delta"))
    max_minute_delta = _as_float(game_delta.get("game_max_minutes_abs_diff_delta"))

    if accepted:
        return (
            "accepted_manual_override_candidate",
            "promote_manual_override",
            "Passed one-game and block acceptance gates.",
        )
    if manual_only or confidence_gap <= 0.0:
        return (
            "rejected_manual_only_ambiguous",
            "leave_unresolved_or_manual_only",
            "Candidate remained ambiguous under local evidence or was pre-designated manual-only.",
        )
    if event_delta < 0 and (
        minute_outlier_delta > 0 or plus_minus_delta > 0 or max_minute_delta > 0.0
    ):
        return (
            "rejected_same_clock_attribution_candidate",
            "same_clock_attribution_program",
            "Reduced contradiction rows but worsened minute and/or plus-minus residue.",
        )
    if event_delta >= 0:
        return (
            "rejected_no_contradiction_improvement",
            "leave_unresolved",
            "Did not improve the event-on-court contradiction signal.",
        )
    return (
        "rejected_manual_override_candidate",
        "leave_unresolved",
        "Failed the manual override gates without a clean same-clock promotion signal.",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate intraperiod manual-review queue results into a reusable registry."
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--queue-output-dir",
        type=Path,
        action="append",
        help="Manual-review queue output directory. May be passed multiple times.",
    )
    parser.add_argument(
        "--queue-output-glob",
        type=str,
        default=DEFAULT_QUEUE_GLOB,
        help="Glob under repo root used when --queue-output-dir is omitted.",
    )
    parser.add_argument("--registry-path", type=Path, default=DEFAULT_REGISTRY_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    queue_output_dirs = [path.resolve() for path in (args.queue_output_dir or [])]
    if not queue_output_dirs:
        queue_output_dirs = sorted(ROOT.glob(args.queue_output_glob))

    case_summaries = _load_case_summaries(queue_output_dirs)

    rows: list[dict[str, Any]] = []
    registry_entries: list[dict[str, Any]] = []
    for case_summary in case_summaries:
        candidate = case_summary.get("candidate") or {}
        diagnostics = case_summary.get("diagnostics") or {}
        before_game = case_summary.get("before_game") or {}
        after_game = case_summary.get("after_game") or {}
        game_delta = case_summary.get("game_delta") or {}
        disposition, next_track, disposition_notes = _disposition_for_case(case_summary)

        game_id = _normalize_game_id(case_summary["game_id"])
        period = _as_int(case_summary.get("period"))
        team_id = _as_int(candidate.get("team_id", diagnostics.get("team_id")))
        player_in_id = _as_int(candidate.get("player_in_id", diagnostics.get("player_in_id")))
        player_out_id = _as_int(candidate.get("player_out_id", diagnostics.get("player_out_id")))
        case_key = f"{game_id}:P{period}:T{team_id}"

        row = {
            "case_key": case_key,
            "game_id": game_id,
            "period": period,
            "team_id": team_id,
            "player_in_id": player_in_id,
            "player_out_id": player_out_id,
            "block_key": str(case_summary.get("block_key") or ""),
            "candidate_family": str(candidate.get("family") or ""),
            "manual_only": bool(case_summary.get("manual_only", False)),
            "accepted": bool((case_summary.get("acceptance") or {}).get("accepted")),
            "disposition": disposition,
            "recommended_next_track": next_track,
            "disposition_notes": disposition_notes,
            "event_issue_rows_before": _as_int(
                ((before_game.get("event_on_court") or {}).get("issue_rows"))
            ),
            "event_issue_rows_after": _as_int(
                ((after_game.get("event_on_court") or {}).get("issue_rows"))
            ),
            "event_issue_rows_delta": _as_int(game_delta.get("event_issue_rows_delta")),
            "minutes_mismatch_rows_before": _as_int(
                ((before_game.get("minutes_plus_minus") or {}).get("minutes_mismatch_rows"))
            ),
            "minutes_mismatch_rows_after": _as_int(
                ((after_game.get("minutes_plus_minus") or {}).get("minutes_mismatch_rows"))
            ),
            "minute_outlier_rows_before": _as_int(
                ((before_game.get("minutes_plus_minus") or {}).get("minute_outlier_rows"))
            ),
            "minute_outlier_rows_after": _as_int(
                ((after_game.get("minutes_plus_minus") or {}).get("minute_outlier_rows"))
            ),
            "plus_minus_mismatch_rows_before": _as_int(
                ((before_game.get("minutes_plus_minus") or {}).get("plus_minus_mismatch_rows"))
            ),
            "plus_minus_mismatch_rows_after": _as_int(
                ((after_game.get("minutes_plus_minus") or {}).get("plus_minus_mismatch_rows"))
            ),
            "game_max_minutes_abs_diff_before": _as_float(
                ((before_game.get("minutes_plus_minus") or {}).get("game_max_minutes_abs_diff"))
            ),
            "game_max_minutes_abs_diff_after": _as_float(
                ((after_game.get("minutes_plus_minus") or {}).get("game_max_minutes_abs_diff"))
            ),
            "game_max_minutes_abs_diff_delta": _as_float(
                game_delta.get("game_max_minutes_abs_diff_delta")
            ),
            "local_confidence_score": _as_float(
                candidate.get("local_confidence_score", diagnostics.get("local_confidence_score"))
            ),
            "confidence_gap": _as_float(
                candidate.get("confidence_gap", candidate.get("best_vs_runner_up_confidence_gap"))
            ),
            "queue_output_dir": str(case_summary.get("_queue_output_dir") or ""),
            "case_summary_path": str(case_summary.get("_summary_path") or ""),
            "date_reviewed": "2026-03-20",
        }
        rows.append(row)
        registry_entries.append(
            {
                "game_id": game_id,
                "period": period,
                "team_id": team_id,
                "player_in_id": player_in_id,
                "player_out_id": player_out_id,
                "case_key": case_key,
                "disposition": disposition,
                "recommended_next_track": next_track,
                "notes": disposition_notes,
                "source_case_summary_path": row["case_summary_path"],
                "source_queue_output_dir": row["queue_output_dir"],
                "date_reviewed": row["date_reviewed"],
            }
        )

    rows = sorted(rows, key=lambda item: (item["game_id"], item["period"], item["team_id"]))
    registry_entries = sorted(
        registry_entries,
        key=lambda item: (item["game_id"], item["period"], item["team_id"]),
    )

    csv_path = output_dir / "intraperiod_manual_review_registry.csv"
    if rows:
        with csv_path.open("w", encoding="utf-8", newline="") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    else:
        csv_path.write_text("", encoding="utf-8")

    registry_payload = {"entries": registry_entries}
    registry_path = args.registry_path.resolve()
    registry_path.write_text(json.dumps(registry_payload, indent=2), encoding="utf-8")

    disposition_counts: dict[str, int] = {}
    next_track_counts: dict[str, int] = {}
    for row in rows:
        disposition_counts[row["disposition"]] = disposition_counts.get(row["disposition"], 0) + 1
        next_track_counts[row["recommended_next_track"]] = (
            next_track_counts.get(row["recommended_next_track"], 0) + 1
        )

    summary = {
        "rows": len(rows),
        "disposition_counts": disposition_counts,
        "recommended_next_track_counts": next_track_counts,
        "registry_path": str(registry_path),
        "top_rows": rows[:10],
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
