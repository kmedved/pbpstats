from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_SAME_CLOCK_DIR = (
    ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "same_clock_attribution"
)
DEFAULT_MANUAL_REVIEW_REGISTRY_PATH = ROOT / "intraperiod_manual_review_registry.json"
DEFAULT_INTRAPERIOD_MANIFEST_PATH = ROOT / "intraperiod_canary_manifest_1998_2020.json"


def _normalize_game_id(value: Any) -> str:
    return str(int(value)).zfill(10)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as infile:
        return list(csv.DictReader(infile))


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a fork-focused same-clock canary manifest from current proving artifacts."
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--same-clock-dir", type=Path, default=DEFAULT_SAME_CLOCK_DIR)
    parser.add_argument(
        "--manual-review-registry-path",
        type=Path,
        default=DEFAULT_MANUAL_REVIEW_REGISTRY_PATH,
    )
    parser.add_argument(
        "--intraperiod-manifest-path",
        type=Path,
        default=DEFAULT_INTRAPERIOD_MANIFEST_PATH,
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    same_clock_dir = args.same_clock_dir.resolve()
    shortlist_rows = _read_csv(same_clock_dir / "same_clock_positive_shortlist.csv")
    registry_payload = _load_json(args.manual_review_registry_path.resolve())
    intraperiod_manifest = _load_json(args.intraperiod_manifest_path.resolve())

    family_buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in shortlist_rows:
        if _as_bool(row.get("is_known_negative_tripwire")) or _as_bool(
            row.get("is_reviewed_manual_reject")
        ):
            continue
        family_buckets[str(row.get("same_clock_family") or "unknown")].append(
            {
                "block_key": str(row.get("block_key") or ""),
                "game_id": _normalize_game_id(row["game_id"]),
                "period": int(float(row["period"])),
                "team_id": int(float(row["team_id"])),
                "player_in_id": int(float(row.get("player_in_id") or 0)),
                "player_out_id": int(float(row.get("player_out_id") or 0)),
                "source_family": str(row.get("source_family") or ""),
                "game_event_issue_rows": int(float(row.get("game_event_issue_rows") or 0)),
                "game_plus_minus_mismatch_rows": int(
                    float(row.get("game_plus_minus_mismatch_rows") or 0)
                ),
                "game_max_minutes_abs_diff": float(
                    row.get("game_max_minutes_abs_diff") or 0.0
                ),
            }
        )

    positive_canaries = {
        family: rows[:3] if family != "foul_free_throw_sub_same_clock_ordering" else rows[:5]
        for family, rows in sorted(family_buckets.items())
    }

    reviewed_rejects = []
    for entry in registry_payload.get("entries", []):
        reviewed_rejects.append(
            {
                "game_id": _normalize_game_id(entry["game_id"]),
                "period": int(entry["period"]),
                "team_id": int(entry["team_id"]),
                "player_in_id": int(entry.get("player_in_id") or 0),
                "player_out_id": int(entry.get("player_out_id") or 0),
                "disposition": str(entry.get("disposition") or ""),
                "recommended_next_track": str(entry.get("recommended_next_track") or ""),
                "notes": str(entry.get("notes") or ""),
            }
        )

    negative_micro_canaries = [
        {
            "game_id": _normalize_game_id(item["game_id"]),
            "family": str(item.get("family") or ""),
            "target_type": str(item.get("target_type") or ""),
            "notes": str(item.get("notes") or ""),
        }
        for item in intraperiod_manifest.get("micro_canaries", [])
        if str(item.get("role") or "") == "negative"
    ]

    manifest_payload = {
        "positive_canaries": positive_canaries,
        "reviewed_manual_rejects": reviewed_rejects,
        "negative_micro_canaries": negative_micro_canaries,
        "notes": {
            "cluster_start_vs_cluster_end_timing": "First-pass timing family; prioritize older block A/B cases first.",
            "foul_free_throw_sub_same_clock_ordering": "Keep timeout/technical/flagrant variants visible; do not assume one universal sub ordering yet.",
            "scorer_sub_same_clock_ordering": "Edge family; require repeated evidence before broadening.",
        },
    }

    (output_dir / "same_clock_canary_manifest.json").write_text(
        json.dumps(manifest_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    summary = {
        "positive_canary_counts": {
            family: len(rows) for family, rows in sorted(positive_canaries.items())
        },
        "reviewed_manual_reject_count": len(reviewed_rejects),
        "negative_micro_canary_count": len(negative_micro_canaries),
        "first_pass_cluster_timing": positive_canaries.get(
            "cluster_start_vs_cluster_end_timing", []
        ),
        "first_pass_foul_free_throw_sub": positive_canaries.get(
            "foul_free_throw_sub_same_clock_ordering", []
        ),
        "first_pass_scorer_sub": positive_canaries.get("scorer_sub_same_clock_ordering", []),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
