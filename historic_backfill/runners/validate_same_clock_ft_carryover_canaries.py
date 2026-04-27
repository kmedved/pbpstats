from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

from historic_backfill.audits.core.event_player_on_court import _build_player_team_map, _check_event_players
from historic_backfill.audits.core.minutes_plus_minus import build_minutes_plus_minus_audit
from historic_backfill.audits.cross_source.period_starters import DEFAULT_DB_PATH, DEFAULT_PARQUET_PATH, _normalize_game_id
from historic_backfill.runners.cautious_rerun import (
    DEFAULT_FILE_DIRECTORY,
    install_local_boxscore_wrapper,
    load_v9b_namespace,
    prepare_local_runtime_inputs,
)
from historic_backfill.audits.cross_source.trace_player_stints_game import _collect_game_events

ROOT = Path(__file__).resolve().parent
DEFAULT_MANIFEST_PATH = ROOT / "same_clock_canary_manifest_non_opening_ft_sub_20260320_v1.json"
DEFAULT_REGISTER_PATH = (
    ROOT
    / "intraperiod_proving_1998_2020_20260319_v2"
    / "same_clock_attribution"
    / "same_clock_attribution_register.csv"
)
DEFAULT_BASELINE_DIRS = {
    2000: ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "micro" / "negative",
    2018: ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "blocks" / "E_2017-2020",
    2020: ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "blocks" / "E_2017-2020",
}
ISSUE_COLUMNS = [
    "game_id",
    "event_num",
    "period",
    "clock",
    "event_class",
    "player_field",
    "player_id",
    "player_name",
    "team_id",
    "status",
    "on_current_lineup",
    "on_previous_lineup",
    "current_team_lineup",
    "previous_team_lineup",
    "event_description",
]


def _season_from_game_id(game_id: str | int) -> int:
    gid = _normalize_game_id(game_id)
    yy = int(gid[3:5])
    return 1901 + yy if yy >= 50 else 2001 + yy


def _load_filtered_pbp(parquet_path: Path, season: int, game_ids: Iterable[str]) -> pd.DataFrame:
    game_id_ints = sorted({int(_normalize_game_id(game_id)) for game_id in game_ids})
    try:
        df = pd.read_parquet(
            parquet_path,
            filters=[("SEASON", "==", season), ("GAME_ID", "in", game_id_ints)],
        )
    except Exception:
        df = pd.read_parquet(parquet_path, filters=[("SEASON", "==", season)])
        df = df[df["GAME_ID"].astype(int).isin(game_id_ints)].copy()

    df.columns = [str(column).upper() for column in df.columns]
    if "WCTIMESTRING" not in df.columns:
        df["WCTIMESTRING"] = "00:00 AM"

    for col in [
        "HOMEDESCRIPTION",
        "VISITORDESCRIPTION",
        "NEUTRALSITEDESCRIPTION",
        "PLAYER1_NAME",
        "PLAYER2_NAME",
        "PLAYER3_NAME",
    ]:
        if col in df.columns:
            df[col] = df[col].fillna("")

    if "GAME_ID" in df.columns:
        df["GAME_ID"] = df["GAME_ID"].astype(str).str.zfill(10)

    for col in ["EVENTNUM", "EVENTMSGTYPE", "EVENTMSGACTIONTYPE", "PERIOD"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in [
        "PLAYER1_ID",
        "PLAYER2_ID",
        "PLAYER3_ID",
        "PLAYER1_TEAM_ID",
        "PLAYER2_TEAM_ID",
        "PLAYER3_TEAM_ID",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def _load_manifest(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows: list[dict[str, Any]] = []
    for section in ("positive_core_canaries", "companion_canaries"):
        for item in payload.get(section, []):
            row = dict(item)
            row["game_id"] = _normalize_game_id(row["game_id"])
            row["season"] = _season_from_game_id(row["game_id"])
            row["is_negative_tripwire"] = bool(row.get("guardrail", False))
            rows.append(row)
    negative_keys = {
        (_normalize_game_id(item["game_id"]), int(item["period"]))
        for item in payload.get("guardrails", [])
    }
    for row in rows:
        row["is_negative_tripwire"] = row["is_negative_tripwire"] or (
            row["game_id"], int(row["period"])
        ) in negative_keys
    return rows


def _load_register_rows(path: Path, canaries: list[dict[str, Any]]) -> dict[tuple[str, int], dict[str, Any]]:
    df = pd.read_csv(path)
    df["game_id"] = df["game_id"].astype(str).str.zfill(10)
    keyed: dict[tuple[str, int], dict[str, Any]] = {}
    for row in canaries:
        subset = df[
            (df["game_id"] == row["game_id"])
            & (df["period"] == int(row["period"]))
            & (df["same_clock_family"] == "foul_free_throw_sub_same_clock_ordering")
        ]
        if subset.empty:
            raise ValueError(f"No same-clock register row found for {row['game_id']} P{row['period']}")
        keyed[(row["game_id"], int(row["period"]))] = subset.iloc[0].to_dict()
    return keyed


def _event_num(event: dict[str, Any]) -> int:
    return int(event.get("event_num") or 0)


def _parse_cluster_row(register_row: dict[str, Any]) -> dict[str, Any]:
    cluster_events = json.loads(register_row["cluster_events_json"])
    current_outcome = json.loads(register_row["current_parser_ordering_outcome_json"])
    sub_event = next(
        event for event in cluster_events if event.get("event_class") == "StatsSubstitution"
    )
    later_events = [
        event for event in cluster_events if _event_num(event) > _event_num(sub_event)
    ]
    outgoing_player_id = int(sub_event.get("outgoing_player_id") or 0)
    incoming_player_id = int(sub_event.get("incoming_player_id") or 0)
    later_outgoing_credit_events = [
        event
        for event in later_events
        if event.get("event_class") in {"StatsFoul", "StatsFreeThrow"}
        and int(event.get("player1_id") or 0) == outgoing_player_id
    ]
    later_incoming_credit_events = [
        event
        for event in later_events
        if event.get("event_class") in {"StatsFoul", "StatsFreeThrow"}
        and int(event.get("player1_id") or 0) == incoming_player_id
    ]
    return {
        "team_id": int(register_row["team_id"]),
        "cluster_clock": register_row["cluster_clock"],
        "cluster_start_event_num": int(register_row["cluster_start_event_num"]),
        "cluster_end_event_num": int(register_row["cluster_end_event_num"]),
        "incoming_player_id": incoming_player_id,
        "outgoing_player_id": outgoing_player_id,
        "later_outgoing_credit_event_nums": [_event_num(event) for event in later_outgoing_credit_events],
        "later_incoming_credit_event_nums": [_event_num(event) for event in later_incoming_credit_events],
        "cluster_events": cluster_events,
        "baseline_contradiction_counts": current_outcome.get("contradiction_status_counts", {}),
    }


def _baseline_paths_for_season(season: int) -> tuple[Path | None, Path | None]:
    base_dir = DEFAULT_BASELINE_DIRS.get(int(season))
    if base_dir is None:
        return None, None
    minutes_path = base_dir / f"minutes_plus_minus_audit_{season}.csv"
    issues_path = base_dir / f"event_player_on_court_issues_{season}.csv"
    return (minutes_path if minutes_path.exists() else None, issues_path if issues_path.exists() else None)


def _load_baseline_minutes_row(minutes_path: Path | None, game_id: str, player_id: int) -> dict[str, Any] | None:
    if minutes_path is None:
        return None
    df = pd.read_csv(minutes_path)
    df["game_id"] = df["game_id"].astype(str).str.zfill(10)
    subset = df[(df["game_id"] == game_id) & (df["player_id"] == int(player_id))]
    if subset.empty:
        return None
    return subset.iloc[0].to_dict()


def _load_baseline_issue_rows(
    issues_path: Path | None,
    game_id: str,
    team_id: int,
    event_nums: list[int],
    player_ids: list[int],
) -> pd.DataFrame:
    if issues_path is None:
        return pd.DataFrame()
    df = pd.read_csv(issues_path)
    if df.empty:
        return df
    df["game_id"] = df["game_id"].astype(str).str.zfill(10)
    return df[
        (df["game_id"] == game_id)
        & (df["team_id"] == int(team_id))
        & (df["event_num"].isin(event_nums))
        & (df["player_id"].isin(player_ids))
    ].copy()


def _player_name_lookup(df: pd.DataFrame, player_id: int) -> str:
    subset = df[df["player_id"] == int(player_id)]
    if subset.empty:
        return str(player_id)
    value = subset["player_name"].iloc[0]
    return str(value) if pd.notna(value) else str(player_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate non-opening same-clock FT carryover canaries with a direct filtered-game path."
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--register-path", type=Path, default=DEFAULT_REGISTER_PATH)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--file-directory", type=Path, default=DEFAULT_FILE_DIRECTORY)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    canaries = _load_manifest(args.manifest_path.resolve())
    register_rows = _load_register_rows(args.register_path.resolve(), canaries)
    parsed_rows = {
        key: _parse_cluster_row(value)
        for key, value in register_rows.items()
    }

    runtime_inputs = prepare_local_runtime_inputs(
        output_dir / "_local_runtime_cache",
        allow_unreadable_csv_fallback=True,
    )
    namespace = load_v9b_namespace()
    namespace["DB_PATH"] = args.db_path.resolve()

    game_ids = [row["game_id"] for row in canaries]
    seasons_to_game_ids: dict[int, list[str]] = defaultdict(list)
    for row in canaries:
        seasons_to_game_ids[int(row["season"])].append(row["game_id"])

    install_local_boxscore_wrapper(
        namespace,
        args.db_path.resolve(),
        file_directory=args.file_directory.resolve(),
        allowed_seasons=sorted(seasons_to_game_ids),
        allowed_game_ids=game_ids,
    )
    overrides = namespace["load_validation_overrides"](str(runtime_inputs["overrides_path"]))

    all_combined_frames: list[pd.DataFrame] = []
    all_minutes_frames: list[pd.DataFrame] = []
    all_issue_frames: list[pd.DataFrame] = []
    event_trace_rows: list[dict[str, Any]] = []
    per_game_results: list[dict[str, Any]] = []

    season_pbp_frames: dict[int, pd.DataFrame] = {}
    season_team_audit: dict[int, pd.DataFrame] = {}
    season_player_mismatch: dict[int, pd.DataFrame] = {}
    season_audit_errors: dict[int, pd.DataFrame] = {}
    season_error_df: dict[int, pd.DataFrame] = {}
    season_event_stats_errors: dict[int, list[dict[str, Any]]] = {}

    for season, season_game_ids in sorted(seasons_to_game_ids.items()):
        print(f"[VALIDATOR] processing season {season} for {len(season_game_ids)} canaries", flush=True)
        season_df = _load_filtered_pbp(args.parquet_path.resolve(), season, season_game_ids)
        season_pbp_frames[season] = season_df
        namespace["clear_event_stats_errors"]()
        namespace["clear_rebound_fallback_deletions"]()
        combined_df, error_df, team_audit_df, player_mismatch_df, audit_error_df = namespace[
            "process_games_parallel"
        ](
            season_game_ids,
            season_df,
            max_workers=1,
            validate=True,
            tolerance=2,
            overrides=overrides,
            strict_mode=False,
            run_boxscore_audit=True,
        )
        season_error_df[season] = error_df.copy()
        season_team_audit[season] = team_audit_df.copy()
        season_player_mismatch[season] = player_mismatch_df.copy()
        season_audit_errors[season] = audit_error_df.copy()
        season_event_stats_errors[season] = list(namespace.get("_event_stats_errors", []))
        if not combined_df.empty:
            all_combined_frames.append(combined_df)
            combined_df.to_parquet(output_dir / f"darko_{season}.parquet", index=False)

    combined_all = pd.concat(all_combined_frames, ignore_index=True) if all_combined_frames else pd.DataFrame()
    if combined_all.empty:
        raise RuntimeError("Direct validator produced no output rows.")

    minutes_audit_df = build_minutes_plus_minus_audit(combined_all, db_path=args.db_path.resolve())
    minutes_audit_df.to_csv(output_dir / "minutes_plus_minus_postpatch.csv", index=False)
    all_minutes_frames.append(minutes_audit_df)

    for row in canaries:
        game_id = row["game_id"]
        season = int(row["season"])
        print(f"[VALIDATOR] analyzing {game_id} P{row['period']}", flush=True)
        parsed_row = parsed_rows[(game_id, int(row["period"]))]
        game_namespace = load_v9b_namespace()
        game_namespace["DB_PATH"] = args.db_path.resolve()
        install_local_boxscore_wrapper(
            game_namespace,
            args.db_path.resolve(),
            file_directory=args.file_directory.resolve(),
            allowed_seasons=[season],
            allowed_game_ids=[game_id],
        )
        game_df = _load_filtered_pbp(args.parquet_path.resolve(), season, [game_id])
        darko_df, possessions = game_namespace["generate_darko_hybrid"](game_id, game_df)
        issues_df = _check_event_players(game_id, _collect_game_events(possessions), _build_player_team_map(darko_df))
        if issues_df.empty:
            issues_df = pd.DataFrame(columns=ISSUE_COLUMNS)
        if not issues_df.empty:
            all_issue_frames.append(issues_df)

        cluster_event_nums = [
            int(event["event_num"])
            for event in parsed_row["cluster_events"]
            if parsed_row["cluster_start_event_num"] <= int(event["event_num"]) <= parsed_row["cluster_end_event_num"]
        ]
        target_player_ids = [
            player_id
            for player_id in [parsed_row["outgoing_player_id"], parsed_row["incoming_player_id"]]
            if int(player_id) > 0
        ]
        targeted_issue_rows = issues_df[
            (issues_df["team_id"] == parsed_row["team_id"])
            & (issues_df["event_num"].isin(cluster_event_nums))
            & (issues_df["player_id"].isin(target_player_ids))
        ].copy()

        events = _collect_game_events(possessions)
        events_by_num = {int(getattr(event, "event_num", 0) or 0): event for event in events}
        for cluster_event_num in cluster_event_nums:
            event = events_by_num.get(cluster_event_num)
            if event is None:
                continue
            current_lineup = getattr(event, "current_players", {})
            previous_lineup = getattr(getattr(event, "previous_event", None), "current_players", {})
            event_trace_rows.append(
                {
                    "game_id": game_id,
                    "period": int(getattr(event, "period", 0) or 0),
                    "clock": str(getattr(event, "clock", "") or ""),
                    "event_num": cluster_event_num,
                    "event_class": event.__class__.__name__,
                    "description": str(getattr(event, "description", "") or ""),
                    "team_id": int(parsed_row["team_id"]),
                    "player1_id": int(getattr(event, "player1_id", 0) or 0),
                    "player2_id": int(getattr(event, "player2_id", 0) or 0),
                    "incoming_player_id": int(getattr(event, "incoming_player_id", 0) or 0),
                    "outgoing_player_id": int(getattr(event, "outgoing_player_id", 0) or 0),
                    "current_team_lineup_json": json.dumps(list(current_lineup.get(parsed_row["team_id"], []))),
                    "previous_team_lineup_json": json.dumps(list(previous_lineup.get(parsed_row["team_id"], []))),
                }
            )

        minutes_rows = minutes_audit_df[minutes_audit_df["game_id"].astype(str).str.zfill(10) == game_id].copy()
        incoming_row = minutes_rows[minutes_rows["player_id"] == int(parsed_row["incoming_player_id"])].head(1)
        outgoing_row = minutes_rows[minutes_rows["player_id"] == int(parsed_row["outgoing_player_id"])].head(1)

        minutes_path, issues_path = _baseline_paths_for_season(season)
        baseline_incoming = _load_baseline_minutes_row(minutes_path, game_id, int(parsed_row["incoming_player_id"]))
        baseline_outgoing = _load_baseline_minutes_row(minutes_path, game_id, int(parsed_row["outgoing_player_id"]))
        baseline_target_issues = _load_baseline_issue_rows(
            issues_path,
            game_id,
            int(parsed_row["team_id"]),
            cluster_event_nums,
            target_player_ids,
        )

        team_audit_df = season_team_audit.get(season, pd.DataFrame()).copy()
        if not team_audit_df.empty and "game_id" in team_audit_df.columns:
            team_audit_df["game_id"] = team_audit_df["game_id"].astype(str).str.zfill(10)
        team_totals_ok = True
        if not team_audit_df.empty:
            team_totals_ok = not bool(team_audit_df[team_audit_df["game_id"] == game_id]["has_mismatch"].any())

        player_mismatch_df = season_player_mismatch.get(season, pd.DataFrame()).copy()
        if not player_mismatch_df.empty and "game_id" in player_mismatch_df.columns:
            player_mismatch_df["game_id"] = player_mismatch_df["game_id"].astype(str).str.zfill(10)

        audit_error_df = season_audit_errors.get(season, pd.DataFrame()).copy()
        if not audit_error_df.empty and "game_id" in audit_error_df.columns:
            audit_error_df["game_id"] = audit_error_df["game_id"].astype(str).str.zfill(10)

        error_df = season_error_df.get(season, pd.DataFrame()).copy()
        if not error_df.empty and "game_id" in error_df.columns:
            error_df["game_id"] = error_df["game_id"].astype(str).str.zfill(10)

        event_stats_error_count = 0
        for item in season_event_stats_errors.get(season, []):
            if _normalize_game_id(item.get("game_id", "")) == game_id:
                event_stats_error_count += 1

        outgoing_name = _player_name_lookup(minutes_rows, int(parsed_row["outgoing_player_id"]))
        incoming_name = _player_name_lookup(minutes_rows, int(parsed_row["incoming_player_id"]))

        latest_credit_player_id = 0
        latest_credit_player_name = None
        if parsed_row["later_outgoing_credit_event_nums"]:
            latest_credit_player_id = int(parsed_row["outgoing_player_id"])
            latest_credit_player_name = outgoing_name
        elif parsed_row["later_incoming_credit_event_nums"]:
            latest_credit_player_id = int(parsed_row["incoming_player_id"])
            latest_credit_player_name = incoming_name

        per_game_results.append(
            {
                "game_id": game_id,
                "period": int(row["period"]),
                "role": row["role"],
                "is_negative_tripwire": bool(row["is_negative_tripwire"]),
                "cluster_clock": parsed_row["cluster_clock"],
                "team_id": int(parsed_row["team_id"]),
                "outgoing_player_id": int(parsed_row["outgoing_player_id"]),
                "outgoing_player_name": outgoing_name,
                "incoming_player_id": int(parsed_row["incoming_player_id"]),
                "incoming_player_name": incoming_name,
                "baseline_target_issue_rows": int(len(baseline_target_issues)),
                "postpatch_target_issue_rows": int(len(targeted_issue_rows)),
                "targeted_off_court_contradiction_cleared": bool(
                    len(targeted_issue_rows[targeted_issue_rows["status"] == "off_court_event_credit"]) == 0
                ),
                "baseline_outgoing_plus_minus_diff": None if baseline_outgoing is None else float(baseline_outgoing["Plus_Minus_diff"]),
                "baseline_incoming_plus_minus_diff": None if baseline_incoming is None else float(baseline_incoming["Plus_Minus_diff"]),
                "postpatch_outgoing_plus_minus_diff": None if outgoing_row.empty else float(outgoing_row.iloc[0]["Plus_Minus_diff"]),
                "postpatch_incoming_plus_minus_diff": None if incoming_row.empty else float(incoming_row.iloc[0]["Plus_Minus_diff"]),
                "baseline_outgoing_minutes_diff": None if baseline_outgoing is None else float(baseline_outgoing["Minutes_diff"]),
                "baseline_incoming_minutes_diff": None if baseline_incoming is None else float(baseline_incoming["Minutes_diff"]),
                "postpatch_outgoing_minutes_diff": None if outgoing_row.empty else float(outgoing_row.iloc[0]["Minutes_diff"]),
                "postpatch_incoming_minutes_diff": None if incoming_row.empty else float(incoming_row.iloc[0]["Minutes_diff"]),
                "team_totals_ok": bool(team_totals_ok),
                "player_mismatch_rows": int(len(player_mismatch_df[player_mismatch_df["game_id"] == game_id])) if not player_mismatch_df.empty else 0,
                "audit_error_rows": int(len(audit_error_df[audit_error_df["game_id"] == game_id])) if not audit_error_df.empty else 0,
                "failed_game_rows": int(len(error_df[error_df["game_id"] == game_id])) if not error_df.empty else 0,
                "event_stats_errors": int(event_stats_error_count),
                "negative_tripwire_stays_correct": bool(
                    not row["is_negative_tripwire"]
                    or (
                        latest_credit_player_id > 0
                        and latest_credit_player_name == incoming_name
                        and len(targeted_issue_rows[targeted_issue_rows["player_id"] == latest_credit_player_id]) == 0
                    )
                ),
            }
        )

    issues_all = (
        pd.concat(all_issue_frames, ignore_index=True)
        if all_issue_frames
        else pd.DataFrame()
    )
    issues_all.to_csv(output_dir / "event_player_on_court_postpatch.csv", index=False)
    pd.DataFrame(event_trace_rows).to_csv(output_dir / "same_clock_ft_carryover_event_trace.csv", index=False)

    summary = {
        "manifest_path": str(args.manifest_path.resolve()),
        "register_path": str(args.register_path.resolve()),
        "games_requested": len(canaries),
        "games_completed": int(combined_all["Game_SingleGame"].nunique()) if "Game_SingleGame" in combined_all.columns else 0,
        "per_game_results": per_game_results,
    }
    (output_dir / "same_clock_ft_carryover_validation.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
