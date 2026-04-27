from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

from audit_minutes_plus_minus import (
    MINUTE_OUTLIER_THRESHOLD,
    build_minutes_plus_minus_audit,
)
from audit_period_starters_against_tpdev import (
    DEFAULT_DB_PATH,
    DEFAULT_PARQUET_PATH,
    DEFAULT_TPDEV_PBP_PATH,
    CURRENT_STARTER_COLUMNS,
    TPDEV_STARTER_COLUMNS,
    _extract_current_period_starters,
    _load_tpdev_period_starters,
    _normalize_game_id,
)
from bbr_boxscore_loader import (
    DEFAULT_BBR_DB_PATH,
    DEFAULT_PLAYER_CROSSWALK_PATH,
)
from cautious_rerun import install_local_boxscore_wrapper, load_v9b_namespace
from trace_player_stints_game import (
    DEFAULT_PBPSTATS_BOX_PATH,
    DEFAULT_TPDEV_BOX_PATH,
    _build_player_minutes_recon,
    _build_player_stints,
    _build_starter_mismatch_maps,
    _collect_game_events,
    _count_same_clock_substitution_scoring_events,
    _period_length_seconds,
    _period_start_clock,
    _season_from_game_id,
)


ROOT = Path(__file__).resolve().parent


def _write_progress_checkpoint(
    output_dir: Path,
    *,
    processed_games: int,
    total_games: int,
    current_season: int | None,
    current_game_id: str | None,
    candidate_rows: int,
    residual_rows: int,
) -> None:
    progress_payload = {
        "processed_games": int(processed_games),
        "total_games": int(total_games),
        "percent_complete": round((processed_games / total_games) * 100.0, 2)
        if total_games
        else 100.0,
        "current_season": int(current_season) if current_season is not None else None,
        "current_game_id": current_game_id,
        "candidate_rows_so_far": int(candidate_rows),
        "residual_rows_so_far": int(residual_rows),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    (output_dir / "progress.json").write_text(
        json.dumps(progress_payload, indent=2),
        encoding="utf-8",
    )
    with (output_dir / "progress.log").open("a", encoding="utf-8") as progress_log:
        progress_log.write(
            "[progress] "
            f"{processed_games}/{total_games} games "
            f"({progress_payload['percent_complete']:.2f}%) "
            f"season={current_season} game={current_game_id} "
            f"candidate_rows={candidate_rows} residual_rows={residual_rows}\n"
        )


def _load_current_game_possessions_from_season_df(
    namespace: Dict[str, Any],
    game_id: str,
    season_df: pd.DataFrame,
):
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


def _json_ready_list(values: Iterable[Any]) -> List[Any]:
    ready: List[Any] = []
    for value in values:
        if pd.isna(value):
            continue
        if isinstance(value, (int, float)) and float(value).is_integer():
            ready.append(int(value))
        else:
            ready.append(value)
    return ready


def _coerce_int_list(value: Any) -> List[int]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        return [int(item) for item in value]
    return [int(item) for item in value]


def _coerce_list(value: Any) -> List[Any]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        return value
    return list(value)


def _build_later_sub_in_map(stints_df: pd.DataFrame) -> Dict[Tuple[int, int], List[int]]:
    if stints_df.empty:
        return {}

    filtered = stints_df[stints_df["start_reason"] == "substitution_in"].copy()
    if filtered.empty:
        return {}

    later_map: Dict[Tuple[int, int], List[int]] = {}
    for row in filtered.itertuples(index=False):
        start_period = int(row.start_period)
        if str(row.start_clock) == _period_start_clock(start_period):
            continue
        key = (start_period, int(row.team_id))
        later_map.setdefault(key, [])
        player_id = int(row.player_id)
        if player_id not in later_map[key]:
            later_map[key].append(player_id)
    return later_map


def _diff_bucket_seconds(consensus_diffs: Iterable[float]) -> str:
    rounded_abs = {
        int(round(abs(float(diff))))
        for diff in consensus_diffs
        if pd.notna(diff)
    }
    if rounded_abs == {300}:
        return "300"
    if rounded_abs == {720}:
        return "720"
    return "other"


def _build_candidate_rows_for_game(
    *,
    game_id: str,
    outlier_game_df: pd.DataFrame,
    starter_audit_df: pd.DataFrame,
    recon_df: pd.DataFrame,
    stints_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    later_sub_in_map = _build_later_sub_in_map(stints_df)
    candidate_rows: List[Dict[str, Any]] = []
    assigned_pairs: set[Tuple[int, int]] = set()

    for row in starter_audit_df.itertuples(index=False):
        if bool(getattr(row, "starter_sets_match", True)):
            continue

        team_id = int(row.team_id)
        period = int(row.period)
        missing_ids = _coerce_int_list(getattr(row, "missing_from_current_ids", []))
        extra_ids = _coerce_int_list(getattr(row, "extra_in_current_ids", []))
        affected_ids = sorted(
            set(missing_ids + extra_ids)
            & set(outlier_game_df.loc[outlier_game_df["team_id"] == team_id, "player_id"].astype(int).tolist())
        )
        if not affected_ids:
            continue

        recon_subset = recon_df[
            (recon_df["team_id"] == team_id)
            & (recon_df["player_id"].astype(int).isin(affected_ids))
        ].copy()
        if recon_subset.empty:
            continue

        for player_id in affected_ids:
            assigned_pairs.add((team_id, int(player_id)))

        consensus_diffs = recon_subset["consensus_diff_seconds"].dropna().astype(float).tolist()
        later_sub_in_ids = later_sub_in_map.get((period, team_id), [])
        current_starter_ids = _coerce_int_list(getattr(row, "current_starter_ids", []))
        tpdev_starter_ids = _coerce_int_list(getattr(row, "tpdev_starter_ids", []))

        candidate_rows.append(
            {
                "game_id": game_id,
                "season": _season_from_game_id(game_id),
                "period": period,
                "team_id": team_id,
                "period_length_seconds": _period_length_seconds(period),
                "affected_player_ids": affected_ids,
                "affected_player_names": recon_subset["player_name"].fillna("").astype(str).tolist(),
                "consensus_diff_seconds": _json_ready_list(consensus_diffs),
                "diff_bucket_seconds": _diff_bucket_seconds(consensus_diffs),
                "current_starter_ids": current_starter_ids,
                "current_starter_names": _coerce_list(getattr(row, "current_starter_names", [])),
                "tpdev_starter_ids": tpdev_starter_ids,
                "tpdev_starter_names": _coerce_list(getattr(row, "tpdev_starter_names", [])),
                "missing_from_current_ids": missing_ids,
                "missing_from_current_names": _coerce_list(getattr(row, "missing_from_current_names", [])),
                "extra_in_current_ids": extra_ids,
                "extra_in_current_names": _coerce_list(getattr(row, "extra_in_current_names", [])),
                "later_sub_in_ids": later_sub_in_ids,
                "official_matches_tpdev": bool(recon_subset["official_matches_tpdev"].fillna(False).all()),
                "official_matches_bbr": bool(recon_subset["official_matches_bbr"].fillna(False).all()),
                "is_simple_later_sub_in_case": (
                    period > 1
                    and len(current_starter_ids) == 5
                    and len(tpdev_starter_ids) == 5
                    and len(missing_ids) == 1
                    and len(extra_ids) == 1
                    and extra_ids[0] in later_sub_in_ids
                ),
            }
        )

    candidate_df = pd.DataFrame(candidate_rows)
    if not candidate_df.empty:
        candidate_df = candidate_df.sort_values(
            ["game_id", "period", "team_id"]
        ).reset_index(drop=True)

    residual_df = outlier_game_df.copy()
    residual_df["season"] = residual_df["game_id"].map(_season_from_game_id)
    residual_df = residual_df[
        ~residual_df.apply(
            lambda row: (int(row["team_id"]), int(row["player_id"])) in assigned_pairs,
            axis=1,
        )
    ].copy()

    if not residual_df.empty and not recon_df.empty:
        residual_df = residual_df.merge(
            recon_df[
                [
                    "team_id",
                    "player_id",
                    "consensus_diff_seconds",
                    "official_matches_tpdev",
                    "official_matches_bbr",
                ]
            ],
            on=["team_id", "player_id"],
            how="left",
        )
    residual_df["Minutes_diff_seconds"] = (residual_df["Minutes_diff"] * 60.0).round(3)
    if not residual_df.empty:
        residual_df = residual_df.sort_values(
            ["game_id", "team_id", "player_id"]
        ).reset_index(drop=True)
    return candidate_df, residual_df


def build_large_minute_outlier_triage(
    darko_df: pd.DataFrame,
    *,
    db_path: Path = DEFAULT_DB_PATH,
    parquet_path: Path = DEFAULT_PARQUET_PATH,
    tpdev_box_path: Path = DEFAULT_TPDEV_BOX_PATH,
    tpdev_pbp_path: Path = DEFAULT_TPDEV_PBP_PATH,
    bbr_db_path: Path = DEFAULT_BBR_DB_PATH,
    player_crosswalk_path: Path = DEFAULT_PLAYER_CROSSWALK_PATH,
    minute_outlier_threshold: float = MINUTE_OUTLIER_THRESHOLD,
    output_dir: Path | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    minutes_audit_df = build_minutes_plus_minus_audit(
        darko_df=darko_df,
        db_path=db_path,
        minute_outlier_threshold=minute_outlier_threshold,
    )
    outlier_df = minutes_audit_df[minutes_audit_df["is_minutes_outlier"]].copy()
    if outlier_df.empty:
        empty = pd.DataFrame()
        return empty, empty, {
            "outlier_rows": 0,
            "outlier_games": 0,
            "candidate_rows": 0,
            "simple_candidate_rows": 0,
            "residual_rows": 0,
        }

    namespace = load_v9b_namespace()
    install_local_boxscore_wrapper(namespace, db_path)
    darko_df = darko_df.copy()
    darko_df["normalized_game_id"] = darko_df["Game_SingleGame"].apply(_normalize_game_id)

    candidate_frames: List[pd.DataFrame] = []
    residual_frames: List[pd.DataFrame] = []
    game_ids_by_season = (
        outlier_df.assign(season=outlier_df["game_id"].map(_season_from_game_id))
        .groupby("season")["game_id"]
        .unique()
        .to_dict()
    )
    total_games = int(outlier_df["game_id"].nunique())
    processed_games = 0
    if output_dir is not None:
        _write_progress_checkpoint(
            output_dir,
            processed_games=0,
            total_games=total_games,
            current_season=None,
            current_game_id=None,
            candidate_rows=0,
            residual_rows=0,
        )
    for season, game_ids in sorted(game_ids_by_season.items()):
        season_df = namespace["load_pbp_from_parquet"](str(parquet_path), season=season)
        if output_dir is not None:
            print(
                f"[progress] loaded season {season} with {len(game_ids)} outlier games",
                flush=True,
            )
        for game_id in sorted(game_ids):
            normalized_game_id = _normalize_game_id(game_id)
            game_darko_df = darko_df[
                darko_df["normalized_game_id"] == normalized_game_id
            ].copy()
            game_outlier_df = outlier_df[outlier_df["game_id"] == normalized_game_id].copy()
            if game_darko_df.empty or game_outlier_df.empty:
                continue

            possessions, name_map = _load_current_game_possessions_from_season_df(
                namespace,
                normalized_game_id,
                season_df,
            )
            events = _collect_game_events(possessions)
            stints_df = _build_player_stints(events, {})
            current_rows = _extract_current_period_starters(possessions, name_map)
            tpdev_rows = _load_tpdev_period_starters(tpdev_pbp_path, normalized_game_id, name_map)
            current_df = pd.DataFrame(current_rows, columns=CURRENT_STARTER_COLUMNS)
            tpdev_df = pd.DataFrame(tpdev_rows, columns=TPDEV_STARTER_COLUMNS)
            starter_audit_df = current_df.merge(
                tpdev_df,
                on=["game_id", "period", "team_id"],
                how="outer",
            )
            if not starter_audit_df.empty:
                starter_audit_df["current_starter_ids"] = starter_audit_df["current_starter_ids"].apply(
                    lambda value: value if isinstance(value, list) else []
                )
                starter_audit_df["tpdev_starter_ids"] = starter_audit_df["tpdev_starter_ids"].apply(
                    lambda value: value if isinstance(value, list) else []
                )
                starter_audit_df["starter_sets_match"] = starter_audit_df.apply(
                    lambda row: set(row["current_starter_ids"]) == set(row["tpdev_starter_ids"]),
                    axis=1,
                )
                starter_audit_df["missing_from_current_ids"] = starter_audit_df.apply(
                    lambda row: sorted(set(row["tpdev_starter_ids"]) - set(row["current_starter_ids"])),
                    axis=1,
                )
                starter_audit_df["extra_in_current_ids"] = starter_audit_df.apply(
                    lambda row: sorted(set(row["current_starter_ids"]) - set(row["tpdev_starter_ids"])),
                    axis=1,
                )
                starter_audit_df["missing_from_current_names"] = starter_audit_df["missing_from_current_ids"].apply(
                    lambda ids: [name_map.get(player_id, str(player_id)) for player_id in ids]
                )
                starter_audit_df["extra_in_current_names"] = starter_audit_df["extra_in_current_ids"].apply(
                    lambda ids: [name_map.get(player_id, str(player_id)) for player_id in ids]
                )
            missing_starter_players, extra_starter_players = _build_starter_mismatch_maps(starter_audit_df)
            recon_df = _build_player_minutes_recon(
                darko_df=game_darko_df,
                stints_df=stints_df,
                game_id=normalized_game_id,
                db_path=db_path,
                tpdev_box_path=tpdev_box_path,
                tpdev_pbp_path=tpdev_pbp_path,
                pbpstats_box_path=DEFAULT_PBPSTATS_BOX_PATH,
                bbr_db_path=bbr_db_path,
                player_crosswalk_path=player_crosswalk_path,
                same_clock_substitution_scoring_events=_count_same_clock_substitution_scoring_events(events),
                missing_starter_players=missing_starter_players,
                extra_starter_players=extra_starter_players,
            )

            candidate_df, residual_df = _build_candidate_rows_for_game(
                game_id=normalized_game_id,
                outlier_game_df=game_outlier_df,
                starter_audit_df=starter_audit_df,
                recon_df=recon_df,
                stints_df=stints_df,
            )
            if not candidate_df.empty:
                candidate_frames.append(candidate_df)
            if not residual_df.empty:
                residual_frames.append(residual_df)

            processed_games += 1
            candidate_rows_so_far = sum(len(frame) for frame in candidate_frames)
            residual_rows_so_far = sum(len(frame) for frame in residual_frames)
            if output_dir is not None:
                _write_progress_checkpoint(
                    output_dir,
                    processed_games=processed_games,
                    total_games=total_games,
                    current_season=season,
                    current_game_id=normalized_game_id,
                    candidate_rows=candidate_rows_so_far,
                    residual_rows=residual_rows_so_far,
                )
                print(
                    f"[progress] {processed_games}/{total_games} games "
                    f"(season {season}, game {normalized_game_id}) "
                    f"candidate_rows={candidate_rows_so_far} residual_rows={residual_rows_so_far}",
                    flush=True,
                )

    candidate_df = (
        pd.concat(candidate_frames, ignore_index=True)
        if candidate_frames
        else pd.DataFrame()
    )
    residual_df = (
        pd.concat(residual_frames, ignore_index=True)
        if residual_frames
        else pd.DataFrame()
    )

    summary = {
        "outlier_rows": int(len(outlier_df)),
        "outlier_games": int(outlier_df["game_id"].nunique()),
        "candidate_rows": int(len(candidate_df)),
        "simple_candidate_rows": int(candidate_df["is_simple_later_sub_in_case"].sum())
        if not candidate_df.empty
        else 0,
        "residual_rows": int(len(residual_df)),
    }
    if not candidate_df.empty:
        summary["candidate_diff_bucket_counts"] = {
            str(bucket): int(count)
            for bucket, count in candidate_df["diff_bucket_seconds"].value_counts().to_dict().items()
        }
    return candidate_df, residual_df, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a period/team triage artifact for large historical minute outliers."
    )
    parser.add_argument("--darko-parquet", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--tpdev-box-path", type=Path, default=DEFAULT_TPDEV_BOX_PATH)
    parser.add_argument("--tpdev-pbp-path", type=Path, default=DEFAULT_TPDEV_PBP_PATH)
    parser.add_argument("--bbr-db-path", type=Path, default=DEFAULT_BBR_DB_PATH)
    parser.add_argument("--player-crosswalk-path", type=Path, default=DEFAULT_PLAYER_CROSSWALK_PATH)
    parser.add_argument("--minute-outlier-threshold", type=float, default=MINUTE_OUTLIER_THRESHOLD)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    darko_df = pd.read_parquet(args.darko_parquet)
    candidate_df, residual_df, summary = build_large_minute_outlier_triage(
        darko_df=darko_df,
        db_path=args.db_path,
        parquet_path=args.parquet_path,
        tpdev_box_path=args.tpdev_box_path,
        tpdev_pbp_path=args.tpdev_pbp_path,
        bbr_db_path=args.bbr_db_path,
        player_crosswalk_path=args.player_crosswalk_path,
        minute_outlier_threshold=args.minute_outlier_threshold,
        output_dir=args.output_dir,
    )

    candidate_df.to_csv(args.output_dir / "large_minute_outlier_triage.csv", index=False)
    candidate_df.to_json(
        args.output_dir / "large_minute_outlier_triage.json",
        orient="records",
        indent=2,
    )
    residual_df.to_csv(args.output_dir / "large_minute_outlier_residuals.csv", index=False)
    residual_df.to_json(
        args.output_dir / "large_minute_outlier_residuals.json",
        orient="records",
        indent=2,
    )
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
