from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

COUNTING_STAT_COLUMNS = [
    "PTS",
    "AST",
    "STL",
    "BLK",
    "TOV",
    "PF",
    "FGM",
    "FGA",
    "3PM",
    "3PA",
    "FTM",
    "FTA",
    "OREB",
    "DRB",
    "REB",
]
INVALID_TEAM_TECH_NORMALIZATION = "invalid_team_tech"


@dataclass
class SeasonRunMetrics:
    season: int
    output_dir: str
    player_rows: int | None
    darko_rows: int | None
    darko_games: int | None
    failed_games: int
    event_stats_errors: int
    rebound_fallback_deletions: int
    audit_failures: int | None = None
    games_with_team_mismatch: int | None = None
    team_rows_with_mismatch: int | None = None
    games_with_player_mismatch: int | None = None
    player_rows_with_mismatch: int | None = None
    team_mismatch_counts_by_stat: dict[str, int] = field(default_factory=dict)
    player_mismatch_counts_by_stat: dict[str, int] = field(default_factory=dict)
    missing_files: list[str] = field(default_factory=list)
    raw_player_rows: int | None = None
    raw_darko_rows: int | None = None
    raw_darko_games: int | None = None
    normalization_profile: str | None = None
    normalized_filtered_row_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def raw_metrics_dict(self) -> dict[str, Any]:
        return {
            "player_rows": self.raw_player_rows,
            "darko_rows": self.raw_darko_rows,
            "darko_games": self.raw_darko_games,
        }

    def normalized_metrics_dict(self) -> dict[str, Any]:
        return {
            "player_rows": self.player_rows,
            "darko_rows": self.darko_rows,
            "darko_games": self.darko_games,
            "normalization_profile": self.normalization_profile,
            "filtered_row_count": int(self.normalized_filtered_row_count),
        }


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(encoding="utf-8") as handle:
        line_count = sum(1 for _ in handle)
    return max(line_count - 1, 0)


def _load_darko_metrics(path: Path) -> tuple[int | None, int | None]:
    if not path.exists():
        return None, None

    parquet_file = pq.ParquetFile(path)
    darko_rows = int(parquet_file.metadata.num_rows)
    schema_names = set(parquet_file.schema_arrow.names)
    darko_games = None
    if "Game_SingleGame" in schema_names:
        game_id_table = pq.read_table(path, columns=["Game_SingleGame"])
        darko_games = int(game_id_table.column("Game_SingleGame").to_pandas().nunique())
    return darko_rows, darko_games


def _count_invalid_team_tech_rows(path: Path) -> tuple[int | None, int | None, int]:
    if not path.exists():
        return None, None, 0

    schema_names = set(pq.ParquetFile(path).schema_arrow.names)
    counting_stat_columns = [column for column in COUNTING_STAT_COLUMNS if column in schema_names]
    columns = [
        "Game_SingleGame",
        "NbaDotComID",
        "Team_SingleGame",
        "FullName",
        "h_tm_id",
        "v_tm_id",
        "TECH",
        "FLAGRANT",
        *counting_stat_columns,
    ]
    frame = pd.read_parquet(path, columns=columns)
    if frame.empty:
        return 0, 0, 0

    numeric_team = pd.to_numeric(frame["Team_SingleGame"], errors="coerce")
    home_team = pd.to_numeric(frame["h_tm_id"], errors="coerce")
    away_team = pd.to_numeric(frame["v_tm_id"], errors="coerce")
    tech = pd.to_numeric(frame["TECH"], errors="coerce").fillna(0)
    flagrant = pd.to_numeric(frame["FLAGRANT"], errors="coerce").fillna(0)
    if counting_stat_columns:
        counting_zero = (
            frame[counting_stat_columns]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0)
            .eq(0)
            .all(axis=1)
        )
    else:
        counting_zero = pd.Series(True, index=frame.index)

    invalid_team_tech_mask = (
        pd.to_numeric(frame["NbaDotComID"], errors="coerce").fillna(0).eq(0)
        & frame["FullName"].fillna("").astype(str).str.match(r"^Team Stats \(.+\)$")
        & numeric_team.ne(home_team)
        & numeric_team.ne(away_team)
        & tech.gt(0)
        & flagrant.eq(0)
        & counting_zero
    )
    anonymous_team_placeholder_mask = (
        pd.to_numeric(frame["NbaDotComID"], errors="coerce").fillna(0).eq(0)
        & frame["FullName"].fillna("").astype(str).eq("Team Stats (0)")
        & numeric_team.fillna(0).eq(0)
        & tech.eq(0)
        & flagrant.eq(0)
        & counting_zero
    )
    normalized_mask = invalid_team_tech_mask | anonymous_team_placeholder_mask

    filtered_frame = frame.loc[~normalized_mask, ["Game_SingleGame"]]
    normalized_rows = int(len(filtered_frame))
    normalized_games = int(filtered_frame["Game_SingleGame"].nunique())
    return normalized_rows, normalized_games, int(normalized_mask.sum())


def summarize_output_dir(
    output_dir: Path,
    season: int,
    *,
    normalization_profile: str | None = None,
) -> SeasonRunMetrics:
    summary_path = output_dir / f"summary_{season}.json"
    errors_path = output_dir / f"errors_{season}.csv"
    event_errors_path = output_dir / f"event_stats_errors_{season}.csv"
    rebound_path = output_dir / f"rebound_fallback_deletions_{season}.csv"
    darko_path = output_dir / f"darko_{season}.parquet"
    audit_summary_path = output_dir / f"boxscore_audit_summary_{season}.json"

    missing_files: list[str] = []
    for path in [summary_path, darko_path, event_errors_path, rebound_path]:
        if not path.exists():
            missing_files.append(path.name)

    summary = _load_json(summary_path)
    audit_summary = _load_json(audit_summary_path)
    raw_darko_rows, raw_darko_games = _load_darko_metrics(darko_path)
    raw_player_rows = summary.get("player_rows", raw_darko_rows)

    darko_rows = raw_darko_rows
    darko_games = raw_darko_games
    normalized_filtered_row_count = 0
    if normalization_profile == INVALID_TEAM_TECH_NORMALIZATION:
        (
            darko_rows,
            darko_games,
            normalized_filtered_row_count,
        ) = _count_invalid_team_tech_rows(darko_path)
    elif normalization_profile is not None:
        raise ValueError(f"Unknown normalization profile: {normalization_profile}")

    return SeasonRunMetrics(
        season=season,
        output_dir=str(output_dir),
        player_rows=darko_rows if normalization_profile is not None else raw_player_rows,
        darko_rows=darko_rows,
        darko_games=darko_games,
        failed_games=int(summary.get("failed_games", _count_csv_rows(errors_path))),
        event_stats_errors=int(summary.get("event_stats_errors", _count_csv_rows(event_errors_path))),
        rebound_fallback_deletions=_count_csv_rows(rebound_path),
        audit_failures=audit_summary.get("audit_failures"),
        games_with_team_mismatch=audit_summary.get("games_with_team_mismatch"),
        team_rows_with_mismatch=audit_summary.get("team_rows_with_mismatch"),
        games_with_player_mismatch=audit_summary.get("games_with_player_mismatch"),
        player_rows_with_mismatch=audit_summary.get("player_rows_with_mismatch"),
        team_mismatch_counts_by_stat={
            str(k): int(v) for k, v in audit_summary.get("team_mismatch_counts_by_stat", {}).items()
        },
        player_mismatch_counts_by_stat={
            str(k): int(v) for k, v in audit_summary.get("player_mismatch_counts_by_stat", {}).items()
        },
        missing_files=missing_files,
        raw_player_rows=raw_player_rows,
        raw_darko_rows=raw_darko_rows,
        raw_darko_games=raw_darko_games,
        normalization_profile=normalization_profile,
        normalized_filtered_row_count=normalized_filtered_row_count,
    )


def _fmt_delta(candidate: int | None, baseline: int | None) -> str:
    if candidate is None or baseline is None:
        return "n/a"
    delta = candidate - baseline
    sign = "+" if delta > 0 else ""
    return f"{sign}{delta}"


def compare_runs(
    baseline: SeasonRunMetrics,
    candidate: SeasonRunMetrics,
) -> tuple[list[str], list[str], list[str]]:
    regressions: list[str] = []
    improvements: list[str] = []
    notes: list[str] = []

    def lower_is_better(name: str, base: int | None, cand: int | None) -> None:
        if base is None or cand is None:
            return
        if cand > base:
            regressions.append(f"{name} regressed: {cand} vs {base} ({_fmt_delta(cand, base)})")
        elif cand < base:
            improvements.append(f"{name} improved: {cand} vs {base} ({_fmt_delta(cand, base)})")

    def higher_is_better(name: str, base: int | None, cand: int | None) -> None:
        if base is None or cand is None:
            return
        if cand < base:
            regressions.append(f"{name} regressed: {cand} vs {base} ({_fmt_delta(cand, base)})")
        elif cand > base:
            improvements.append(f"{name} improved: {cand} vs {base} ({_fmt_delta(cand, base)})")

    higher_is_better("player_rows", baseline.player_rows, candidate.player_rows)
    higher_is_better("darko_rows", baseline.darko_rows, candidate.darko_rows)
    lower_is_better("failed_games", baseline.failed_games, candidate.failed_games)
    lower_is_better("event_stats_errors", baseline.event_stats_errors, candidate.event_stats_errors)
    lower_is_better(
        "rebound_fallback_deletions",
        baseline.rebound_fallback_deletions,
        candidate.rebound_fallback_deletions,
    )
    lower_is_better("audit_failures", baseline.audit_failures, candidate.audit_failures)
    lower_is_better(
        "games_with_team_mismatch",
        baseline.games_with_team_mismatch,
        candidate.games_with_team_mismatch,
    )
    lower_is_better(
        "player_rows_with_mismatch",
        baseline.player_rows_with_mismatch,
        candidate.player_rows_with_mismatch,
    )

    if baseline.darko_games is not None and candidate.darko_games is not None and baseline.darko_games != candidate.darko_games:
        notes.append(
            f"darko_games changed: {candidate.darko_games} vs {baseline.darko_games} ({_fmt_delta(candidate.darko_games, baseline.darko_games)})"
        )

    if baseline.team_mismatch_counts_by_stat and candidate.team_mismatch_counts_by_stat:
        changed_team_stats = sorted(
            stat
            for stat in set(baseline.team_mismatch_counts_by_stat) | set(candidate.team_mismatch_counts_by_stat)
            if baseline.team_mismatch_counts_by_stat.get(stat) != candidate.team_mismatch_counts_by_stat.get(stat)
        )
        if changed_team_stats:
            notes.append(f"team mismatch stats changed: {', '.join(changed_team_stats)}")
    elif bool(baseline.team_mismatch_counts_by_stat) != bool(candidate.team_mismatch_counts_by_stat):
        notes.append("integrated team audit present in only one of the two runs")

    if baseline.player_mismatch_counts_by_stat and candidate.player_mismatch_counts_by_stat:
        changed_player_stats = sorted(
            stat
            for stat in set(baseline.player_mismatch_counts_by_stat) | set(candidate.player_mismatch_counts_by_stat)
            if baseline.player_mismatch_counts_by_stat.get(stat) != candidate.player_mismatch_counts_by_stat.get(stat)
        )
        if changed_player_stats:
            notes.append(f"player mismatch stats changed: {', '.join(changed_player_stats)}")
    elif bool(baseline.player_mismatch_counts_by_stat) != bool(candidate.player_mismatch_counts_by_stat):
        notes.append("integrated player audit present in only one of the two runs")

    if candidate.missing_files:
        notes.append(f"candidate missing files: {', '.join(candidate.missing_files)}")

    if baseline.normalization_profile is not None or candidate.normalization_profile is not None:
        notes.append(
            "normalization applied: "
            f"{candidate.normalization_profile or baseline.normalization_profile}"
        )
        notes.append(
            "filtered normalization rows: "
            f"baseline={baseline.normalized_filtered_row_count}, "
            f"candidate={candidate.normalized_filtered_row_count}"
        )
        if baseline.raw_player_rows is not None and candidate.raw_player_rows is not None:
            notes.append(
                "raw_player_rows changed before normalization: "
                f"{candidate.raw_player_rows} vs {baseline.raw_player_rows} "
                f"({_fmt_delta(candidate.raw_player_rows, baseline.raw_player_rows)})"
            )
        if baseline.raw_darko_rows is not None and candidate.raw_darko_rows is not None:
            notes.append(
                "raw_darko_rows changed before normalization: "
                f"{candidate.raw_darko_rows} vs {baseline.raw_darko_rows} "
                f"({_fmt_delta(candidate.raw_darko_rows, baseline.raw_darko_rows)})"
            )

    return regressions, improvements, notes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare two season run output directories")
    parser.add_argument("--baseline-dir", type=Path, required=True)
    parser.add_argument("--candidate-dir", type=Path, required=True)
    parser.add_argument("--seasons", nargs="+", type=int, required=True)
    parser.add_argument("--json", action="store_true", default=False, dest="as_json")
    parser.add_argument(
        "--normalization-profile",
        choices=[INVALID_TEAM_TECH_NORMALIZATION],
        default=None,
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    payload: dict[str, Any] = {
        "baseline_dir": str(args.baseline_dir.resolve()),
        "candidate_dir": str(args.candidate_dir.resolve()),
        "normalization_profile": args.normalization_profile,
        "seasons": [],
    }
    has_regression = False

    for season in args.seasons:
        baseline = summarize_output_dir(
            args.baseline_dir.resolve(),
            season,
            normalization_profile=args.normalization_profile,
        )
        candidate = summarize_output_dir(
            args.candidate_dir.resolve(),
            season,
            normalization_profile=args.normalization_profile,
        )
        regressions, improvements, notes = compare_runs(baseline, candidate)
        has_regression = has_regression or bool(regressions)
        season_payload = {
            "season": season,
            "baseline": baseline.to_dict(),
            "candidate": candidate.to_dict(),
            "regressions": regressions,
            "improvements": improvements,
            "notes": notes,
        }
        if args.normalization_profile is not None:
            season_payload["baseline_raw"] = baseline.raw_metrics_dict()
            season_payload["candidate_raw"] = candidate.raw_metrics_dict()
            season_payload["baseline_normalized"] = baseline.normalized_metrics_dict()
            season_payload["candidate_normalized"] = candidate.normalized_metrics_dict()
        payload["seasons"].append(season_payload)

    if args.as_json:
        print(json.dumps(payload, indent=2))
    else:
        for season_payload in payload["seasons"]:
            season = season_payload["season"]
            print(f"[season {season}]")
            print(
                f"baseline rows={season_payload['baseline']['player_rows']} "
                f"failed={season_payload['baseline']['failed_games']} "
                f"event_errors={season_payload['baseline']['event_stats_errors']} "
                f"rebound_deletions={season_payload['baseline']['rebound_fallback_deletions']}"
            )
            print(
                f"candidate rows={season_payload['candidate']['player_rows']} "
                f"failed={season_payload['candidate']['failed_games']} "
                f"event_errors={season_payload['candidate']['event_stats_errors']} "
                f"rebound_deletions={season_payload['candidate']['rebound_fallback_deletions']}"
            )
            if args.normalization_profile is not None:
                print(
                    "raw rows: "
                    f"baseline={season_payload['baseline_raw']['player_rows']} "
                    f"candidate={season_payload['candidate_raw']['player_rows']}"
                )
                print(
                    "normalized rows filtered: "
                    f"baseline={season_payload['baseline_normalized']['filtered_row_count']} "
                    f"candidate={season_payload['candidate_normalized']['filtered_row_count']}"
                )
            if season_payload["regressions"]:
                print("regressions:")
                for item in season_payload["regressions"]:
                    print(f"  - {item}")
            if season_payload["improvements"]:
                print("improvements:")
                for item in season_payload["improvements"]:
                    print(f"  - {item}")
            if season_payload["notes"]:
                print("notes:")
                for item in season_payload["notes"]:
                    print(f"  - {item}")
            if not season_payload["regressions"] and not season_payload["improvements"] and not season_payload["notes"]:
                print("no material differences")

    return 1 if has_regression else 0


if __name__ == "__main__":
    raise SystemExit(main())
