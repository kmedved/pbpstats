from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

from historic_backfill.runners.cautious_rerun import install_local_boxscore_wrapper, load_v9b_namespace


ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = ROOT / "data"
DEFAULT_PARQUET_PATH = DATA_ROOT / "playbyplayv2.parq"
DEFAULT_DB_PATH = DATA_ROOT / "nba_raw.db"
DEFAULT_TPDEV_PBP_PATH = (
    DATA_ROOT / "tpdev" / "full_pbp_new.parq"
)
CURRENT_STARTER_COLUMNS = [
    "game_id",
    "period",
    "team_id",
    "current_starter_ids",
    "current_starter_names",
]
TPDEV_STARTER_COLUMNS = [
    "game_id",
    "period",
    "team_id",
    "tpdev_starter_ids",
    "tpdev_starter_names",
]


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _season_from_game_id(game_id: str | int) -> int:
    gid = _normalize_game_id(game_id)
    yy = int(gid[3:5])
    return 1901 + yy if yy >= 50 else 2001 + yy


def _load_current_game_possessions(namespace: dict, game_id: str, parquet_path: Path):
    season = _season_from_game_id(game_id)
    season_df = namespace["load_pbp_from_parquet"](str(parquet_path), season=season)
    single_game_df = season_df[season_df["GAME_ID"] == game_id].copy()
    if single_game_df.empty:
        raise ValueError(f"Game {game_id} not found in parquet")

    df_box = namespace["fetch_boxscore_stats"](game_id)
    summary = namespace["fetch_game_summary"](game_id)
    h_tm_id, v_tm_id = namespace["_resolve_game_team_ids"](summary, df_box)
    if h_tm_id and v_tm_id:
        single_game_df = namespace["normalize_single_game_team_events"](
            single_game_df,
            home_team_id=h_tm_id,
            away_team_id=v_tm_id,
            boxscore_player_ids=df_box["PLAYER_ID"].tolist(),
        )
    single_game_df = namespace["normalize_single_game_player_ids"](
        single_game_df,
        official_boxscore=df_box,
    )
    single_game_df = namespace["apply_pbp_row_overrides"](single_game_df)
    possessions = namespace["get_possessions_from_df"](
        single_game_df,
        fetch_pbp_v3_fn=namespace["fetch_pbp_v3"],
    )
    name_map = {
        int(pid): str(name)
        for pid, name in zip(df_box["PLAYER_ID"].astype(int), df_box["PLAYER_NAME"])
    }
    return possessions, name_map


def _extract_current_period_starters(possessions, name_map: Dict[int, str]) -> List[dict]:
    rows: List[dict] = []
    seen_periods = set()
    for possession in possessions.items:
        for event in possession.events:
            if not event.__class__.__name__.endswith("StartOfPeriod"):
                continue
            if event.period in seen_periods:
                continue
            seen_periods.add(event.period)
            for team_id, starters in event.period_starters.items():
                starter_ids = [int(pid) for pid in starters]
                rows.append(
                    {
                        "game_id": _normalize_game_id(event.game_id),
                        "period": int(event.period),
                        "team_id": int(team_id),
                        "current_starter_ids": starter_ids,
                        "current_starter_names": [name_map.get(pid, str(pid)) for pid in starter_ids],
                    }
                )
    return rows


def _load_tpdev_period_starters(
    tpdev_pbp_path: Path,
    game_id: str,
    name_map: Dict[int, str],
) -> List[dict]:
    game_int = int(game_id)
    cols = [
        "game_id",
        "Quarter",
        "TimeRemainingStart",
        "event_id",
        "h_tm_id",
        "v_tm_id",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "v1",
        "v2",
        "v3",
        "v4",
        "v5",
    ]
    df = pd.read_parquet(
        tpdev_pbp_path,
        filters=[("game_id", "==", game_int)],
        columns=cols,
    )
    if df.empty:
        return []

    rows: List[dict] = []
    for quarter, quarter_df in df.groupby("Quarter"):
        first_row = quarter_df.sort_values(
            ["TimeRemainingStart", "event_id"],
            ascending=[False, True],
        ).iloc[0]
        for team_key, tm_id_key in [("h", "h_tm_id"), ("v", "v_tm_id")]:
            team_id = int(float(first_row[tm_id_key]))
            starter_ids = [
                int(float(first_row[f"{team_key}{slot}"]))
                for slot in range(1, 6)
                if pd.notna(first_row[f"{team_key}{slot}"])
            ]
            rows.append(
                {
                    "game_id": _normalize_game_id(game_id),
                    "period": int(quarter),
                    "team_id": team_id,
                    "tpdev_starter_ids": starter_ids,
                    "tpdev_starter_names": [name_map.get(pid, str(pid)) for pid in starter_ids],
                }
            )
    return rows


def _coerce_id_list(value) -> List[int]:
    if isinstance(value, list):
        return [int(pid) for pid in value]
    if pd.isna(value):
        return []
    return list(value)


def build_period_starter_audit(
    game_ids: Iterable[str | int],
    parquet_path: Path = DEFAULT_PARQUET_PATH,
    db_path: Path = DEFAULT_DB_PATH,
    tpdev_pbp_path: Path = DEFAULT_TPDEV_PBP_PATH,
) -> pd.DataFrame:
    namespace = load_v9b_namespace()
    install_local_boxscore_wrapper(namespace, db_path)

    audit_rows: List[dict] = []
    for raw_game_id in game_ids:
        game_id = _normalize_game_id(raw_game_id)
        possessions, name_map = _load_current_game_possessions(
            namespace, game_id, parquet_path
        )
        current_rows = _extract_current_period_starters(possessions, name_map)
        tpdev_rows = _load_tpdev_period_starters(tpdev_pbp_path, game_id, name_map)
        current_df = pd.DataFrame(current_rows, columns=CURRENT_STARTER_COLUMNS)
        tpdev_df = pd.DataFrame(tpdev_rows, columns=TPDEV_STARTER_COLUMNS)
        if current_df.empty and tpdev_df.empty:
            continue
        merged = current_df.merge(
            tpdev_df,
            on=["game_id", "period", "team_id"],
            how="outer",
        )
        for row in merged.to_dict(orient="records"):
            current_ids = _coerce_id_list(row.get("current_starter_ids"))
            tpdev_ids = _coerce_id_list(row.get("tpdev_starter_ids"))
            current_set = set(current_ids)
            tpdev_set = set(tpdev_ids)
            missing_from_current = sorted(tpdev_set - current_set)
            extra_in_current = sorted(current_set - tpdev_set)
            audit_rows.append(
                {
                    "game_id": row["game_id"],
                    "period": int(row["period"]),
                    "team_id": int(row["team_id"]),
                    "current_starter_ids": current_ids,
                    "current_starter_names": row.get("current_starter_names") or [],
                    "tpdev_starter_ids": tpdev_ids,
                    "tpdev_starter_names": row.get("tpdev_starter_names") or [],
                    "starter_sets_match": current_set == tpdev_set,
                    "missing_from_current_ids": missing_from_current,
                    "missing_from_current_names": [name_map.get(pid, str(pid)) for pid in missing_from_current],
                    "extra_in_current_ids": extra_in_current,
                    "extra_in_current_names": [name_map.get(pid, str(pid)) for pid in extra_in_current],
                }
            )
    return pd.DataFrame(audit_rows).sort_values(["game_id", "period", "team_id"]).reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare current period starters against original tpdev possession lineups."
    )
    parser.add_argument("--game-ids", nargs="+", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--tpdev-pbp-path", type=Path, default=DEFAULT_TPDEV_PBP_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    audit_df = build_period_starter_audit(
        game_ids=args.game_ids,
        parquet_path=args.parquet_path,
        db_path=args.db_path,
        tpdev_pbp_path=args.tpdev_pbp_path,
    )
    audit_df.to_json(
        args.output_dir / "period_starter_audit.json",
        orient="records",
        indent=2,
    )
    audit_df.to_csv(args.output_dir / "period_starter_audit.csv", index=False)
    summary = {
        "rows": int(len(audit_df)),
        "mismatch_rows": int((~audit_df["starter_sets_match"]).sum()) if not audit_df.empty else 0,
        "games": int(audit_df["game_id"].nunique()) if not audit_df.empty else 0,
    }
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
