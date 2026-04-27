from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "historical_baseline_manifest_20260315_v1"
DEFAULT_SEASON_START = 1997
DEFAULT_SEASON_STOP = 2020

_DATE_VERSION_RE = re.compile(r"_(\d{8})_v(\d+)$")


@dataclass(frozen=True)
class CleanSeasonCandidate:
    season: int
    output_dir: Path
    summary_path: Path
    summary: dict[str, Any]
    rebound_deletions: int
    event_stats_error_rows: int
    failed_games: int
    team_mismatch_games: int
    player_mismatch_rows: int
    audit_failures: int
    player_rows: int


def _count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        return max(sum(1 for _ in reader) - 1, 0)


def _sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _date_version_key(path: Path) -> tuple[str, int, float, str]:
    match = _DATE_VERSION_RE.search(path.name)
    if match:
        return (match.group(1), int(match.group(2)), path.stat().st_mtime, path.name)
    return ("00000000", 0, path.stat().st_mtime, path.name)


def _load_clean_candidate(output_dir: Path, season: int) -> CleanSeasonCandidate | None:
    summary_path = output_dir / f"summary_{season}.json"
    if not summary_path.exists():
        return None

    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None

    boxscore_audit = summary.get("boxscore_audit") or {}
    rebound_deletions = _count_csv_rows(output_dir / f"rebound_fallback_deletions_{season}.csv")
    event_stats_error_rows = _count_csv_rows(output_dir / f"event_stats_errors_{season}.csv")
    failed_games = int(summary.get("failed_games", 0) or 0)
    team_mismatch_games = int(boxscore_audit.get("games_with_team_mismatch", 0) or 0)
    player_mismatch_rows = int(boxscore_audit.get("player_rows_with_mismatch", 0) or 0)
    audit_failures = int(boxscore_audit.get("audit_failures", 0) or 0)
    player_rows = int(summary.get("player_rows", 0) or 0)

    if any(
        [
            failed_games,
            event_stats_error_rows,
            rebound_deletions,
            team_mismatch_games,
            player_mismatch_rows,
            audit_failures,
        ]
    ):
        return None

    return CleanSeasonCandidate(
        season=season,
        output_dir=output_dir,
        summary_path=summary_path,
        summary=summary,
        rebound_deletions=rebound_deletions,
        event_stats_error_rows=event_stats_error_rows,
        failed_games=failed_games,
        team_mismatch_games=team_mismatch_games,
        player_mismatch_rows=player_mismatch_rows,
        audit_failures=audit_failures,
        player_rows=player_rows,
    )


def _artifact_entry(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "size_bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def build_manifest(*, root: Path, season_start: int, season_stop: int) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    missing: list[int] = []
    for season in range(season_start, season_stop + 1):
        candidates = []
        for output_dir in root.iterdir():
            if not output_dir.is_dir() or not output_dir.name.startswith("audit_"):
                continue
            candidate = _load_clean_candidate(output_dir, season)
            if candidate is not None:
                candidates.append(candidate)

        if not candidates:
            missing.append(season)
            continue

        chosen = max(candidates, key=lambda candidate: _date_version_key(candidate.output_dir))
        boxscore_summary_path = chosen.output_dir / f"boxscore_audit_summary_{season}.json"
        parquet_path = chosen.output_dir / f"darko_{season}.parquet"
        artifacts = {
            "summary_json": _artifact_entry(chosen.summary_path),
            "boxscore_audit_summary_json": _artifact_entry(boxscore_summary_path),
            "darko_parquet": _artifact_entry(parquet_path),
        }

        result = {
            "season": season,
            "output_dir": str(chosen.output_dir),
            "selected_from_clean_candidates": len(candidates),
            "selection_key": {
                "date_version": _date_version_key(chosen.output_dir)[0],
                "version": _date_version_key(chosen.output_dir)[1],
            },
            "metrics": {
                "player_rows": chosen.player_rows,
                "failed_games": chosen.failed_games,
                "event_stats_errors": chosen.event_stats_error_rows,
                "rebound_fallback_deletions": chosen.rebound_deletions,
                "audit_failures": chosen.audit_failures,
                "games_with_team_mismatch": chosen.team_mismatch_games,
                "player_rows_with_mismatch": chosen.player_mismatch_rows,
            },
            "artifacts": artifacts,
            "candidate_dirs": [candidate.output_dir.name for candidate in sorted(candidates, key=lambda c: _date_version_key(c.output_dir))],
        }
        results.append(result)

    return {
        "season_range": [season_start, season_stop],
        "seasons_frozen": len(results),
        "missing_clean_seasons": missing,
        "seasons": results,
    }


def write_manifest(manifest: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "historical_baseline_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=False),
        encoding="utf-8",
    )

    csv_rows = []
    for row in manifest["seasons"]:
        csv_rows.append(
            {
                "season": row["season"],
                "output_dir": row["output_dir"],
                "selected_from_clean_candidates": row["selected_from_clean_candidates"],
                "player_rows": row["metrics"]["player_rows"],
                "failed_games": row["metrics"]["failed_games"],
                "event_stats_errors": row["metrics"]["event_stats_errors"],
                "rebound_fallback_deletions": row["metrics"]["rebound_fallback_deletions"],
                "audit_failures": row["metrics"]["audit_failures"],
                "games_with_team_mismatch": row["metrics"]["games_with_team_mismatch"],
                "player_rows_with_mismatch": row["metrics"]["player_rows_with_mismatch"],
                "darko_parquet_sha256": row["artifacts"]["darko_parquet"]["sha256"],
                "summary_json_sha256": row["artifacts"]["summary_json"]["sha256"],
                "boxscore_audit_summary_sha256": row["artifacts"]["boxscore_audit_summary_json"]["sha256"],
            }
        )

    import pandas as pd

    pd.DataFrame(csv_rows).to_csv(output_dir / "historical_baseline_manifest.csv", index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Freeze a clean 1997-2020 baseline manifest from audited output dirs")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--season-start", type=int, default=DEFAULT_SEASON_START)
    parser.add_argument("--season-stop", type=int, default=DEFAULT_SEASON_STOP)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    manifest = build_manifest(
        root=args.root.resolve(),
        season_start=args.season_start,
        season_stop=args.season_stop,
    )
    write_manifest(manifest, args.output_dir.resolve())
    print(json.dumps({"seasons_frozen": manifest["seasons_frozen"], "missing_clean_seasons": manifest["missing_clean_seasons"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
