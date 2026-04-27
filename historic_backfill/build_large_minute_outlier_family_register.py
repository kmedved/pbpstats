from __future__ import annotations

import argparse
import ast
import json
import sqlite3
import zlib
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

from audit_period_starters_against_tpdev import _normalize_game_id
from cautious_rerun import load_v9b_namespace


ROOT = Path(__file__).resolve().parent
DEFAULT_TRIAGE_DIR = ROOT / "large_minute_outlier_triage_baseline_20260316_v1"
DEFAULT_DB_PATH = ROOT / "nba_raw.db"
DEFAULT_PARQUET_PATH = ROOT / "playbyplayv2.parq"
DEFAULT_CROSS_SOURCE_DATE = "20260316"
DEFAULT_MINUTE_THRESHOLD = 2.0
PERIOD_SIZED_SECONDS = {300, 720}
KNOWN_V3_ORDERING_GAMES = {"0029600585", "0020000383"}

TRIAGE_LIST_COLUMNS = [
    "affected_player_ids",
    "affected_player_names",
    "consensus_diff_seconds",
    "current_starter_ids",
    "current_starter_names",
    "tpdev_starter_ids",
    "tpdev_starter_names",
    "missing_from_current_ids",
    "missing_from_current_names",
    "extra_in_current_ids",
    "extra_in_current_names",
    "later_sub_in_ids",
]


def _parse_list_like(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, float) and pd.isna(value):
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            parsed = ast.literal_eval(stripped)
        except (ValueError, SyntaxError):
            return [stripped]
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, tuple):
            return list(parsed)
        return [parsed]
    return [value]


def _load_triage_df(path: Path) -> pd.DataFrame:
    triage_df = pd.read_csv(path).copy()
    triage_df["game_id"] = triage_df["game_id"].apply(_normalize_game_id)
    for column in TRIAGE_LIST_COLUMNS:
        if column in triage_df.columns:
            triage_df[column] = triage_df[column].apply(_parse_list_like)
    return triage_df


def _build_triage_player_index(triage_df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for row in triage_df.itertuples(index=False):
        affected_ids = [int(player_id) for player_id in _parse_list_like(row.affected_player_ids)]
        affected_names = [str(name) for name in _parse_list_like(row.affected_player_names)]
        name_map = {
            affected_ids[index]: affected_names[index] if index < len(affected_names) else str(affected_ids[index])
            for index in range(len(affected_ids))
        }
        for player_id in affected_ids:
            rows.append(
                {
                    "game_id": row.game_id,
                    "season": int(row.season),
                    "period": int(row.period),
                    "team_id": int(row.team_id),
                    "player_id": int(player_id),
                    "player_name_triage": name_map.get(player_id, str(player_id)),
                    "triage_diff_bucket_seconds": str(row.diff_bucket_seconds),
                    "triage_is_simple_later_sub_in_case": bool(row.is_simple_later_sub_in_case),
                    "triage_missing_from_current_ids": _parse_list_like(row.missing_from_current_ids),
                    "triage_extra_in_current_ids": _parse_list_like(row.extra_in_current_ids),
                    "triage_later_sub_in_ids": _parse_list_like(row.later_sub_in_ids),
                    "triage_official_matches_tpdev": bool(row.official_matches_tpdev),
                    "triage_official_matches_bbr": bool(row.official_matches_bbr),
                }
            )
    if not rows:
        return pd.DataFrame()

    indexed = pd.DataFrame(rows)
    grouped = (
        indexed.groupby(["game_id", "season", "team_id", "player_id"], as_index=False)
        .agg(
            player_name_triage=("player_name_triage", "first"),
            triage_periods=("period", lambda values: sorted(set(int(value) for value in values))),
            triage_diff_buckets=("triage_diff_bucket_seconds", lambda values: sorted(set(str(value) for value in values))),
            triage_simple_case=("triage_is_simple_later_sub_in_case", "any"),
            triage_period_sized_candidate=(
                "triage_diff_bucket_seconds",
                lambda values: any(str(value) in {"300", "720"} for value in values),
            ),
            triage_official_matches_tpdev=("triage_official_matches_tpdev", "all"),
            triage_official_matches_bbr=("triage_official_matches_bbr", "all"),
        )
    )
    return grouped


def _find_latest_cross_source_dir(season: int, cross_source_date: str) -> Path | None:
    candidates = list(ROOT.glob(f"minutes_cross_source_{season}_{cross_source_date}_*"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _load_cross_source_large_outliers(
    *,
    season: int,
    cross_source_date: str,
    minute_threshold: float,
) -> pd.DataFrame:
    report_dir = _find_latest_cross_source_dir(season, cross_source_date)
    if report_dir is None:
        return pd.DataFrame()

    path = report_dir / "minutes_cross_source_mismatches.csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path).copy()
    if df.empty:
        return pd.DataFrame()

    df["season"] = season
    df["game_id"] = df["game_id"].apply(_normalize_game_id)
    df["Minutes_diff_seconds"] = (df["Minutes_diff_vs_official"] * 60.0).round(3)
    df["Minutes_abs_diff_seconds"] = df["Minutes_diff_seconds"].abs()
    df["cross_source_report_dir"] = str(report_dir)
    return df[df["Minutes_abs_diff_vs_official"] > float(minute_threshold)].copy()


def _load_raw_response(db_path: Path, game_id: str, endpoint: str) -> Dict[str, Any] | None:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        row = conn.execute(
            "SELECT data FROM raw_responses WHERE game_id=? AND endpoint=? AND team_id IS NULL",
            (_normalize_game_id(game_id), endpoint),
        ).fetchone()
        if not row:
            return None
        blob = row[0]
        try:
            return json.loads(zlib.decompress(blob).decode())
        except (zlib.error, TypeError):
            if isinstance(blob, bytes):
                return json.loads(blob.decode())
            return json.loads(blob)
    finally:
        conn.close()


def _build_v3_period_counts(db_path: Path, game_id: str) -> Dict[int, int]:
    data = _load_raw_response(db_path, game_id, "pbpv3")
    actions = data.get("game", {}).get("actions", []) if isinstance(data, dict) else []
    counts: Dict[int, int] = {}
    for action in actions:
        try:
            period = int(action.get("period"))
        except (TypeError, ValueError):
            continue
        counts[period] = counts.get(period, 0) + 1
    return counts


def _build_v2_period_counts(namespace: Dict[str, Any], parquet_path: Path, season: int, game_ids: Iterable[str]) -> Dict[str, Dict[int, int]]:
    season_df = namespace["load_pbp_from_parquet"](str(parquet_path), season=season)
    if season_df.empty:
        return {}
    counts_df = season_df[["GAME_ID", "PERIOD"]].copy()
    counts_df["game_id"] = counts_df["GAME_ID"].apply(_normalize_game_id)
    counts_df["PERIOD"] = pd.to_numeric(counts_df["PERIOD"], errors="coerce").fillna(0).astype(int)
    game_id_set = {_normalize_game_id(game_id) for game_id in game_ids}
    counts_df = counts_df[counts_df["game_id"].isin(game_id_set)].copy()
    results: Dict[str, Dict[int, int]] = {}
    for game_id, game_df in counts_df.groupby("game_id"):
        results[game_id] = {
            int(period): int(count)
            for period, count in game_df["PERIOD"].value_counts().sort_index().to_dict().items()
        }
    return results


def _build_period_count_frame(
    *,
    game_ids_by_season: Dict[int, List[str]],
    db_path: Path,
    parquet_path: Path,
) -> pd.DataFrame:
    namespace = load_v9b_namespace()
    rows: List[Dict[str, Any]] = []
    for season, game_ids in sorted(game_ids_by_season.items()):
        v2_counts_by_game = _build_v2_period_counts(namespace, parquet_path, season, game_ids)
        for game_id in sorted({_normalize_game_id(game_id) for game_id in game_ids}):
            v2_counts = v2_counts_by_game.get(game_id, {})
            v3_counts = _build_v3_period_counts(db_path, game_id)
            periods = sorted(set(v2_counts) | set(v3_counts))
            period_diffs = {
                str(period): int(v3_counts.get(period, 0) - v2_counts.get(period, 0))
                for period in periods
                if v2_counts.get(period, 0) != v3_counts.get(period, 0)
            }
            rows.append(
                {
                    "game_id": game_id,
                    "season": int(season),
                    "v2_period_counts": json.dumps(v2_counts, sort_keys=True),
                    "v3_period_counts": json.dumps(v3_counts, sort_keys=True),
                    "v2_v3_period_count_diffs": json.dumps(period_diffs, sort_keys=True),
                    "v2_v3_periods_with_diff": int(len(period_diffs)),
                    "v2_v3_total_count_diff": int(sum(abs(value) for value in period_diffs.values())),
                }
            )
    return pd.DataFrame(rows)


def _consensus_label(row: pd.Series) -> str:
    official_tpdev = bool(row.get("Official_minutes_match_vs_tpdev_box", False))
    official_bbr = bool(row.get("Official_minutes_match_vs_bbr_box", False))
    if official_tpdev and official_bbr:
        return "official_tpdev_bbr"
    if official_tpdev:
        return "official_tpdev"
    if official_bbr:
        return "official_bbr"
    return "split"


def _classify_large_outlier_row(row: pd.Series) -> str:
    abs_seconds = int(round(abs(float(row["Minutes_diff_seconds"]))))
    consensus = _consensus_label(row)
    if row.get("triage_period_sized_candidate", False) and consensus != "split":
        if row.get("triage_simple_case", False):
            return "starter_simple_candidate"
        return "starter_complex_candidate"
    if abs_seconds in PERIOD_SIZED_SECONDS and consensus != "split":
        return "period_sized_residual"
    if row["game_id"] in KNOWN_V3_ORDERING_GAMES:
        return "v3_ordering_candidate"
    if abs_seconds not in PERIOD_SIZED_SECONDS and row.get("v2_v3_periods_with_diff", 0) > 0 and consensus != "split":
        return "v3_ordering_candidate"
    if consensus == "split":
        return "source_conflict_or_missing_source"
    return "other_large_outlier"


def _recommended_next_action(family: str) -> str:
    if family == "starter_simple_candidate":
        return "explicit_period_starter_override"
    if family == "starter_complex_candidate":
        return "manual_period_lineup_trace"
    if family == "period_sized_residual":
        return "manual_period_trace"
    if family == "v3_ordering_candidate":
        return "inspect_v3_ordering_and_dedupe"
    if family == "source_conflict_or_missing_source":
        return "source_conflict_review"
    return "manual_game_trace"


def _choose_primary_family(families: Iterable[str]) -> str:
    priority = [
        "v3_ordering_candidate",
        "starter_simple_candidate",
        "starter_complex_candidate",
        "period_sized_residual",
        "source_conflict_or_missing_source",
        "other_large_outlier",
    ]
    family_set = set(families)
    for family in priority:
        if family in family_set:
            return family
    return "other_large_outlier"


def build_large_minute_outlier_family_register(
    *,
    triage_dir: Path,
    db_path: Path,
    parquet_path: Path,
    cross_source_date: str,
    minute_threshold: float,
) -> tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    triage_df = _load_triage_df(triage_dir / "large_minute_outlier_triage.csv")
    triage_index_df = _build_triage_player_index(triage_df)

    cross_source_frames = []
    for season in range(1997, 2021):
        season_df = _load_cross_source_large_outliers(
            season=season,
            cross_source_date=cross_source_date,
            minute_threshold=minute_threshold,
        )
        if not season_df.empty:
            cross_source_frames.append(season_df)

    if not cross_source_frames:
        empty = pd.DataFrame()
        return empty, empty, {
            "rows": 0,
            "games": 0,
            "minute_threshold": minute_threshold,
        }

    register_df = pd.concat(cross_source_frames, ignore_index=True)
    register_df["game_id"] = register_df["game_id"].apply(_normalize_game_id)
    register_df["player_id"] = pd.to_numeric(register_df["player_id"], errors="coerce").fillna(0).astype(int)
    register_df["team_id"] = pd.to_numeric(register_df["team_id"], errors="coerce").fillna(0).astype(int)

    if not triage_index_df.empty:
        register_df = register_df.merge(
            triage_index_df,
            on=["game_id", "season", "team_id", "player_id"],
            how="left",
        )
    else:
        register_df["triage_periods"] = [[] for _ in range(len(register_df))]
        register_df["triage_diff_buckets"] = [[] for _ in range(len(register_df))]
        register_df["triage_simple_case"] = False
        register_df["triage_period_sized_candidate"] = False

    register_df["triage_periods"] = register_df["triage_periods"].apply(_parse_list_like)
    register_df["triage_diff_buckets"] = register_df["triage_diff_buckets"].apply(_parse_list_like)
    register_df["triage_simple_case"] = register_df["triage_simple_case"].fillna(False).astype(bool)
    register_df["triage_period_sized_candidate"] = (
        register_df["triage_period_sized_candidate"].fillna(False).astype(bool)
    )

    game_ids_by_season = {
        int(season): sorted({_normalize_game_id(game_id) for game_id in game_ids})
        for season, game_ids in register_df.groupby("season")["game_id"].unique().to_dict().items()
    }
    period_count_df = _build_period_count_frame(
        game_ids_by_season=game_ids_by_season,
        db_path=db_path,
        parquet_path=parquet_path,
    )
    register_df = register_df.merge(
        period_count_df,
        on=["game_id", "season"],
        how="left",
    )
    register_df["v2_v3_periods_with_diff"] = (
        pd.to_numeric(register_df["v2_v3_periods_with_diff"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    register_df["v2_v3_total_count_diff"] = (
        pd.to_numeric(register_df["v2_v3_total_count_diff"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    register_df["consensus_label"] = register_df.apply(_consensus_label, axis=1)
    register_df["family"] = register_df.apply(_classify_large_outlier_row, axis=1)
    register_df["recommended_next_action"] = register_df["family"].apply(_recommended_next_action)
    register_df["Minutes_abs_diff_seconds"] = register_df["Minutes_diff_seconds"].abs()
    register_df = register_df.sort_values(
        ["season", "game_id", "team_id", "player_id"]
    ).reset_index(drop=True)

    game_summary_df = (
        register_df.groupby(["game_id", "season"], as_index=False)
        .agg(
            player_rows=("player_id", "size"),
            max_abs_diff_seconds=("Minutes_abs_diff_seconds", "max"),
            families=("family", lambda values: sorted(set(str(value) for value in values))),
            consensus_labels=("consensus_label", lambda values: sorted(set(str(value) for value in values))),
            v2_v3_periods_with_diff=("v2_v3_periods_with_diff", "max"),
            v2_v3_total_count_diff=("v2_v3_total_count_diff", "max"),
            cross_source_report_dirs=("cross_source_report_dir", lambda values: sorted(set(str(value) for value in values))),
        )
    )
    game_summary_df["primary_family"] = game_summary_df["families"].apply(_choose_primary_family)
    game_summary_df["recommended_next_action"] = game_summary_df["primary_family"].apply(
        _recommended_next_action
    )
    game_summary_df = game_summary_df.sort_values(["season", "game_id"]).reset_index(drop=True)

    summary = {
        "rows": int(len(register_df)),
        "games": int(register_df["game_id"].nunique()),
        "minute_threshold": float(minute_threshold),
        "family_counts": {
            str(family): int(count)
            for family, count in register_df["family"].value_counts().to_dict().items()
        },
        "primary_family_counts": {
            str(family): int(count)
            for family, count in game_summary_df["primary_family"].value_counts().to_dict().items()
        },
    }
    return register_df, game_summary_df, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a row-level family register for large historical minute outliers."
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--triage-dir", type=Path, default=DEFAULT_TRIAGE_DIR)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--cross-source-date", type=str, default=DEFAULT_CROSS_SOURCE_DATE)
    parser.add_argument("--minute-threshold", type=float, default=DEFAULT_MINUTE_THRESHOLD)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    register_df, game_summary_df, summary = build_large_minute_outlier_family_register(
        triage_dir=args.triage_dir,
        db_path=args.db_path,
        parquet_path=args.parquet_path,
        cross_source_date=args.cross_source_date,
        minute_threshold=args.minute_threshold,
    )

    register_df.to_csv(args.output_dir / "large_minute_outlier_family_register.csv", index=False)
    game_summary_df.to_csv(args.output_dir / "large_minute_outlier_game_summary.csv", index=False)
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
