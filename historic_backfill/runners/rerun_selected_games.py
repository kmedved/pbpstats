from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Iterable

import pandas as pd

from historic_backfill.runners.cautious_rerun import (
    AUDIT_PROFILES,
    DEFAULT_DB,
    DEFAULT_FILE_DIRECTORY,
    DEFAULT_OVERRIDES,
    DEFAULT_PARQUET,
    DEFAULT_RUNTIME_INPUT_CACHE_MODE,
    _current_peak_rss_mb,
    install_local_boxscore_wrapper,
    load_v9b_namespace,
    prepare_local_runtime_inputs,
    run_lineup_audits,
    RUNTIME_INPUT_CACHE_MODES,
)
import time


ROOT = Path(__file__).resolve().parent


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _season_from_game_id(game_id: str | int) -> int:
    gid = _normalize_game_id(game_id)
    yy = int(gid[3:5])
    return 1901 + yy if yy >= 50 else 2001 + yy


def _expand_game_id_tokens(values: Iterable[str]) -> list[str]:
    expanded: list[str] = []
    for value in values:
        expanded.extend(token.strip() for token in str(value).split(",") if token.strip())
    return expanded


def _load_game_ids(args: argparse.Namespace) -> list[str]:
    raw_ids: list[str] = []
    if args.game_ids:
        raw_ids.extend(_expand_game_id_tokens(args.game_ids))
    if args.game_ids_file is not None:
        raw_ids.extend(_expand_game_id_tokens(
            line.strip()
            for line in args.game_ids_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ))
    if not raw_ids:
        raise ValueError("Provide --game-ids and/or --game-ids-file")
    return sorted({_normalize_game_id(game_id) for game_id in raw_ids})


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rerun a selected set of games through the offline historical pipeline."
    )
    parser.add_argument("--game-ids", nargs="*")
    parser.add_argument("--game-ids-file", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET)
    parser.add_argument("--overrides-path", type=Path, default=DEFAULT_OVERRIDES)
    parser.add_argument("--file-directory", type=Path, default=DEFAULT_FILE_DIRECTORY)
    parser.add_argument("--strict-mode", action="store_true", default=False)
    parser.add_argument("--tolerance", type=int, default=2)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--run-boxscore-audit", action="store_true", default=False)
    parser.add_argument("--skip-lineup-audit", action="store_true", default=False)
    parser.add_argument("--audit-profile", choices=sorted(AUDIT_PROFILES), default="full")
    parser.add_argument("--allow-unreadable-csv-fallback", action="store_true", default=False)
    parser.add_argument(
        "--runtime-input-cache-mode",
        choices=sorted(RUNTIME_INPUT_CACHE_MODES),
        default=DEFAULT_RUNTIME_INPUT_CACHE_MODE,
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    overall_start = time.perf_counter()
    runtime_input_start = time.perf_counter()
    runtime_inputs = prepare_local_runtime_inputs(
        output_dir / "_local_runtime_cache",
        db_path=args.db_path.resolve(),
        parquet_path=args.parquet_path.resolve(),
        overrides_path=args.overrides_path.resolve(),
        allow_unreadable_csv_fallback=args.allow_unreadable_csv_fallback,
        file_directory=args.file_directory.resolve(),
        runtime_input_cache_mode=args.runtime_input_cache_mode,
    )
    (output_dir / "runtime_input_provenance.json").write_text(
        json.dumps(runtime_inputs["runtime_input_provenance"], indent=2),
        encoding="utf-8",
    )
    runtime_input_seconds = round(time.perf_counter() - runtime_input_start, 6)

    game_ids = _load_game_ids(args)
    seasons_to_game_ids: dict[int, list[str]] = defaultdict(list)
    for game_id in game_ids:
        seasons_to_game_ids[_season_from_game_id(game_id)].append(game_id)

    namespace_load_start = time.perf_counter()
    namespace = load_v9b_namespace(
        notebook_dump_path=runtime_inputs["notebook_dump_path"],
        preload_module_paths=runtime_inputs["preload_module_paths"],
    )
    namespace["DB_PATH"] = runtime_inputs["db_path"]
    install_local_boxscore_wrapper(
        namespace,
        runtime_inputs["db_path"],
        file_directory=runtime_inputs["file_directory"],
        allowed_seasons=sorted(seasons_to_game_ids),
        allowed_game_ids=game_ids,
        period_starter_parquet_paths=runtime_inputs["period_starter_parquet_paths"],
    )
    namespace_load_seconds = round(time.perf_counter() - namespace_load_start, 6)
    overrides = namespace["load_validation_overrides"](str(runtime_inputs["overrides_path"]))

    all_frames: list[pd.DataFrame] = []
    season_summaries: list[dict] = []

    (output_dir / "selected_game_ids.txt").write_text(
        "\n".join(game_ids) + "\n",
        encoding="utf-8",
    )

    for season in sorted(seasons_to_game_ids):
        season_game_ids = seasons_to_game_ids[season]
        season_start = time.perf_counter()
        namespace["clear_event_stats_errors"]()
        namespace["clear_rebound_fallback_deletions"]()
        pbp_load_start = time.perf_counter()
        season_df = namespace["load_pbp_from_parquet"](str(runtime_inputs["parquet_path"]), season=season)
        pbp_load_seconds = round(time.perf_counter() - pbp_load_start, 6)
        game_processing_start = time.perf_counter()
        combined_df, error_df, team_audit_df, player_mismatch_df, audit_error_df = namespace[
            "process_games_parallel"
        ](
            season_game_ids,
            season_df,
            max_workers=args.max_workers,
            validate=True,
            tolerance=args.tolerance,
            overrides=overrides,
            strict_mode=args.strict_mode,
            run_boxscore_audit=args.run_boxscore_audit,
        )
        game_processing_seconds = round(time.perf_counter() - game_processing_start, 6)

        parquet_write_start = time.perf_counter()
        if not combined_df.empty:
            season_path = output_dir / f"darko_{season}.parquet"
            combined_df.to_parquet(season_path, index=False)
            all_frames.append(combined_df)
        parquet_write_seconds = round(time.perf_counter() - parquet_write_start, 6)

        error_write_start = time.perf_counter()
        if not error_df.empty:
            error_df.to_csv(output_dir / f"errors_{season}.csv", index=False)
        error_write_seconds = round(time.perf_counter() - error_write_start, 6)

        rebound_export_start = time.perf_counter()
        if namespace.get("_event_stats_errors"):
            namespace["export_event_stats_errors"](str(output_dir / f"event_stats_errors_{season}.csv"))
        namespace["export_rebound_fallback_deletions"](
            str(output_dir / f"rebound_fallback_deletions_{season}.csv")
        )
        rebound_export_seconds = round(time.perf_counter() - rebound_export_start, 6)

        audit_summary = None
        lineup_audit_summary = None
        boxscore_audit_seconds = 0.0
        if args.run_boxscore_audit:
            boxscore_audit_start = time.perf_counter()
            audit_summary = namespace["write_boxscore_audit_outputs"](
                team_audit=team_audit_df,
                player_mismatches=player_mismatch_df,
                audit_errors=audit_error_df,
                season=season,
                output_dir=output_dir,
                games_requested=len(season_game_ids),
            )
            boxscore_audit_seconds = round(time.perf_counter() - boxscore_audit_start, 6)
        if args.audit_profile == "full" and not args.skip_lineup_audit:
            lineup_audit_summary = run_lineup_audits(
                combined_df=combined_df,
                season=season,
                output_dir=output_dir,
                db_path=runtime_inputs["db_path"],
                parquet_path=runtime_inputs["parquet_path"],
                file_directory=runtime_inputs["file_directory"],
            )

        summary = {
            "season": season,
            "games_requested": len(season_game_ids),
            "player_rows": int(len(combined_df)),
            "failed_games": int(len(error_df)),
            "event_stats_errors": int(len(namespace.get("_event_stats_errors", []))),
            "strict_mode": args.strict_mode,
            "tolerance": args.tolerance,
            "audit_profile": args.audit_profile,
            "boxscore_audit": audit_summary,
            "lineup_audit": lineup_audit_summary,
            "timings": {
                "pbp_load_seconds": pbp_load_seconds,
                "game_processing_seconds": game_processing_seconds,
                "parquet_write_seconds": parquet_write_seconds,
                "error_write_seconds": error_write_seconds,
                "rebound_export_seconds": rebound_export_seconds,
                "boxscore_audit_seconds": boxscore_audit_seconds,
                "lineup_audit_seconds": (
                    float(lineup_audit_summary.get("timings", {}).get("total_wall_seconds", 0.0))
                    if lineup_audit_summary is not None
                    else 0.0
                ),
                "total_wall_seconds": round(time.perf_counter() - season_start, 6),
            },
            "resource_usage": {
                "peak_rss_mb": _current_peak_rss_mb(),
            },
        }
        season_summaries.append(summary)
        (output_dir / f"summary_{season}.json").write_text(
            json.dumps(summary, indent=2),
            encoding="utf-8",
        )

    combined_all = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
    if not combined_all.empty:
        combined_all.to_parquet(output_dir / "darko_selected_games.parquet", index=False)

    final_summary = {
        "games_requested": len(game_ids),
        "games_completed": int(combined_all["Game_SingleGame"].nunique()) if not combined_all.empty else 0,
        "player_rows": int(len(combined_all)),
        "failed_games": int(sum(item["failed_games"] for item in season_summaries)),
        "event_stats_errors": int(sum(item["event_stats_errors"] for item in season_summaries)),
        "audit_profile": args.audit_profile,
        "runtime_input_cache_mode": args.runtime_input_cache_mode,
        "runtime_input_provenance_path": str((output_dir / "runtime_input_provenance.json").resolve()),
        "runtime_file_directory": str(runtime_inputs["file_directory"]),
        "timings": {
            "runtime_input_prep_seconds": runtime_input_seconds,
            "namespace_load_seconds": namespace_load_seconds,
            "total_wall_seconds": round(time.perf_counter() - overall_start, 6),
        },
        "resource_usage": {
            "peak_rss_mb": _current_peak_rss_mb(),
        },
        "seasons": season_summaries,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(final_summary, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(final_summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
