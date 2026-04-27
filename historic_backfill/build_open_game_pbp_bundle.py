from __future__ import annotations

import argparse
import json
import sqlite3
import zlib
from collections import Counter
from pathlib import Path
from typing import Iterable

import pandas as pd

from bbr_pbp_lookup import (
    DEFAULT_BBR_DB_PATH,
    DEFAULT_NBA_RAW_DB_PATH,
    find_bbr_game_for_nba_game,
    load_bbr_play_by_play_rows,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_INVENTORY_PATH = (
    ROOT
    / "phase7_reviewed_frontier_inventory_20260323_v4"
    / "raw_open_inventory.csv"
)
DEFAULT_ACTIONABLE_QUEUE_PATH = ROOT / "H_1997-2020_20260323_v4" / "actionable_queue.csv"
DEFAULT_RAW_PBP_PATH = ROOT / "playbyplayv2.parq"
DEFAULT_TPDEV_PBP_PATH = (
    ROOT.parent / "fixed_data" / "raw_input_data" / "tpdev_data" / "full_pbp_new.parq"
)
DEFAULT_OUTPUT_DIR = ROOT / "open_game_pbp_bundle_20260423_v1"

RAW_PBP_COLUMNS = [
    "GAME_ID",
    "EVENTNUM",
    "EVENTMSGTYPE",
    "EVENTMSGACTIONTYPE",
    "PERIOD",
    "PCTIMESTRING",
    "WCTIMESTRING",
    "HOMEDESCRIPTION",
    "NEUTRALDESCRIPTION",
    "VISITORDESCRIPTION",
    "SCORE",
    "SCOREMARGIN",
    "PLAYER1_ID",
    "PLAYER1_NAME",
    "PLAYER1_TEAM_ID",
    "PLAYER2_ID",
    "PLAYER2_NAME",
    "PLAYER2_TEAM_ID",
    "PLAYER3_ID",
    "PLAYER3_NAME",
    "PLAYER3_TEAM_ID",
    "SEASON",
]

TPDEV_PBP_COLUMNS = [
    "game_id",
    "Quarter",
    "TimeRemainingStart",
    "TimeElapsedStart",
    "LengthInSeconds",
    "event_id",
    "PossString",
    "HomePoints",
    "AwayPoints",
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
    "pts",
    "poss",
    "season",
]


def _normalize_game_id(value: object) -> str:
    return str(int(str(value))).zfill(10)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


def _safe_int(value: object, default: int = 0) -> int:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return default
    return int(number)


def _clock_to_seconds_remaining(clock: object) -> float | None:
    text = str(clock or "").strip()
    if not text:
        return None
    if ":" not in text:
        try:
            return float(text)
        except ValueError:
            return None
    minutes, seconds = text.split(":", 1)
    try:
        return int(minutes) * 60 + float(seconds)
    except ValueError:
        return None


def _description(row: pd.Series) -> str:
    for col in ("HOMEDESCRIPTION", "NEUTRALDESCRIPTION", "VISITORDESCRIPTION"):
        value = str(row.get(col, "") or "").strip()
        if value:
            return value
    return ""


def _load_filtered_parquet(path: Path, columns: list[str], filters: list[tuple]) -> pd.DataFrame:
    try:
        return pd.read_parquet(path, columns=columns, filters=filters)
    except Exception:
        df = pd.read_parquet(path, columns=columns)
        for column, op, value in filters:
            if op != "in":
                raise ValueError(f"Unsupported fallback filter op: {op}")
            df = df[df[column].isin(value)]
        return df


def _load_raw_pbp_v2(path: Path, game_ids: list[str]) -> pd.DataFrame:
    # The migrated NBA v2 parquet stores GAME_ID as an unpadded string such as
    # "29600070", while the rest of this project uses "0029600070".
    raw_game_ids = [str(int(game_id)) for game_id in game_ids]
    df = _load_filtered_parquet(path, RAW_PBP_COLUMNS, [("GAME_ID", "in", raw_game_ids)])
    if df.empty:
        return df
    df = df.copy()
    df["GAME_ID"] = df["GAME_ID"].map(_normalize_game_id)
    df["event_num"] = pd.to_numeric(df["EVENTNUM"], errors="coerce").fillna(-1).astype(int)
    df["period"] = pd.to_numeric(df["PERIOD"], errors="coerce").fillna(-1).astype(int)
    df["clock_seconds_remaining"] = df["PCTIMESTRING"].map(_clock_to_seconds_remaining)
    df["description"] = df.apply(_description, axis=1)
    return df.sort_values(["GAME_ID", "period", "event_num"])


def _load_tpdev_pbp(path: Path, game_ids: list[str]) -> pd.DataFrame:
    int_ids = [int(game_id) for game_id in game_ids]
    df = _load_filtered_parquet(path, TPDEV_PBP_COLUMNS, [("game_id", "in", int_ids)])
    if df.empty:
        return df
    df = df.copy()
    df["GAME_ID"] = df["game_id"].map(_normalize_game_id)
    df["period"] = pd.to_numeric(df["Quarter"], errors="coerce").fillna(-1).astype(int)
    df["clock_seconds_remaining"] = pd.to_numeric(df["TimeRemainingStart"], errors="coerce")
    return df.sort_values(["GAME_ID", "period", "clock_seconds_remaining", "event_id"], ascending=[True, True, False, True])


def _load_json_blob(raw_value: bytes | str | memoryview) -> dict:
    if isinstance(raw_value, memoryview):
        raw_value = raw_value.tobytes()
    if isinstance(raw_value, bytes):
        try:
            raw_value = zlib.decompress(raw_value).decode("utf-8")
        except zlib.error:
            raw_value = raw_value.decode("utf-8")
    if isinstance(raw_value, str):
        return json.loads(raw_value)
    raise TypeError(f"Unsupported raw response type: {type(raw_value)!r}")


def _load_pbpv3(db_path: Path, game_ids: Iterable[str]) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    try:
        for game_id in game_ids:
            record = conn.execute(
                "SELECT data FROM raw_responses WHERE game_id = ? AND endpoint = 'pbpv3' LIMIT 1",
                (game_id,),
            ).fetchone()
            if record is None:
                continue
            payload = _load_json_blob(record[0])
            actions = payload.get("game", {}).get("actions", [])
            if not actions:
                continue
            frame = pd.DataFrame(actions)
            frame.insert(0, "GAME_ID", game_id)
            rows.append(frame)
    finally:
        conn.close()
    if not rows:
        return pd.DataFrame()
    df = pd.concat(rows, ignore_index=True)
    sort_cols = [col for col in ("GAME_ID", "period", "actionNumber") if col in df.columns]
    return df.sort_values(sort_cols)


def _load_bbr_pbp(nba_raw_db_path: Path, bbr_db_path: Path, game_ids: Iterable[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_rows: list[dict] = []
    match_rows: list[dict] = []
    for game_id in game_ids:
        try:
            context, matches = find_bbr_game_for_nba_game(
                game_id,
                nba_raw_db_path=nba_raw_db_path,
                bbr_db_path=bbr_db_path,
            )
        except Exception as exc:
            match_rows.append({"GAME_ID": game_id, "match_status": "error", "error": str(exc)})
            continue
        if len(matches) != 1:
            match_rows.append(
                {
                    "GAME_ID": game_id,
                    "match_status": f"matches_{len(matches)}",
                    "game_date": context.game_date.isoformat(),
                    "home_team_id": context.home_team_id,
                    "away_team_id": context.away_team_id,
                    "bbr_game_id": "",
                }
            )
            continue
        match = matches[0]
        match_rows.append(
            {
                "GAME_ID": game_id,
                "match_status": "matched",
                "game_date": context.game_date.isoformat(),
                "home_team_id": context.home_team_id,
                "away_team_id": context.away_team_id,
                "home_team": match.home_team,
                "away_team": match.away_team,
                "bbr_game_id": match.bbr_game_id,
                "game_url": match.game_url,
            }
        )
        for row in load_bbr_play_by_play_rows(match.bbr_game_id, bbr_db_path=bbr_db_path):
            all_rows.append({"GAME_ID": game_id, "bbr_game_id": match.bbr_game_id, **row})
    return pd.DataFrame(all_rows), pd.DataFrame(match_rows)


def _event_rows(actionable: pd.DataFrame, game_ids: set[str]) -> pd.DataFrame:
    if actionable.empty:
        return pd.DataFrame()
    df = actionable.copy()
    df["GAME_ID"] = df["game_id"].map(_normalize_game_id)
    df["period_int"] = pd.to_numeric(df.get("period", ""), errors="coerce").fillna(0).astype(int)
    df["event_num_int"] = pd.to_numeric(df.get("event_num", ""), errors="coerce").fillna(0).astype(int)
    return df[(df["GAME_ID"].isin(game_ids)) & (df["period_int"] > 0) & (df["event_num_int"] > 0)].copy()


def _build_raw_windows(raw_pbp: pd.DataFrame, event_rows: pd.DataFrame, window_events: int) -> pd.DataFrame:
    windows: list[pd.DataFrame] = []
    if raw_pbp.empty or event_rows.empty:
        return pd.DataFrame()
    for event in event_rows.itertuples(index=False):
        game_id = event.GAME_ID
        period = int(event.period_int)
        event_num = int(event.event_num_int)
        subset = raw_pbp[
            (raw_pbp["GAME_ID"] == game_id)
            & (raw_pbp["period"] == period)
            & (raw_pbp["event_num"].between(event_num - window_events, event_num + window_events))
        ].copy()
        if subset.empty:
            continue
        subset.insert(0, "focus_event_num", event_num)
        subset.insert(0, "focus_period", period)
        windows.append(subset)
    if not windows:
        return pd.DataFrame()
    return pd.concat(windows, ignore_index=True)


def _build_v3_windows(pbpv3: pd.DataFrame, event_rows: pd.DataFrame, window_events: int) -> pd.DataFrame:
    windows: list[pd.DataFrame] = []
    if pbpv3.empty or event_rows.empty or "actionNumber" not in pbpv3.columns:
        return pd.DataFrame()
    action_numbers = pd.to_numeric(pbpv3["actionNumber"], errors="coerce")
    periods = pd.to_numeric(pbpv3.get("period"), errors="coerce")
    df = pbpv3.assign(action_num_int=action_numbers, period_int=periods)
    for event in event_rows.itertuples(index=False):
        game_id = event.GAME_ID
        period = int(event.period_int)
        event_num = int(event.event_num_int)
        subset = df[
            (df["GAME_ID"] == game_id)
            & (df["period_int"] == period)
            & (df["action_num_int"].between(event_num - window_events, event_num + window_events))
        ].copy()
        if subset.empty:
            continue
        subset.insert(0, "focus_event_num", event_num)
        subset.insert(0, "focus_period", period)
        windows.append(subset)
    if not windows:
        return pd.DataFrame()
    return pd.concat(windows, ignore_index=True)


def _build_tpdev_windows(
    tpdev_pbp: pd.DataFrame,
    event_rows: pd.DataFrame,
    raw_pbp: pd.DataFrame,
    window_seconds: int,
) -> pd.DataFrame:
    windows: list[pd.DataFrame] = []
    if tpdev_pbp.empty or event_rows.empty or raw_pbp.empty:
        return pd.DataFrame()
    raw_lookup = (
        raw_pbp.dropna(subset=["clock_seconds_remaining"])
        .set_index(["GAME_ID", "period", "event_num"])["clock_seconds_remaining"]
        .to_dict()
    )
    for event in event_rows.itertuples(index=False):
        game_id = event.GAME_ID
        period = int(event.period_int)
        event_num = int(event.event_num_int)
        clock = raw_lookup.get((game_id, period, event_num))
        if clock is None:
            continue
        subset = tpdev_pbp[
            (tpdev_pbp["GAME_ID"] == game_id)
            & (tpdev_pbp["period"] == period)
            & ((tpdev_pbp["clock_seconds_remaining"] - float(clock)).abs() <= window_seconds)
        ].copy()
        if subset.empty:
            continue
        subset.insert(0, "focus_clock_seconds_remaining", clock)
        subset.insert(0, "focus_event_num", event_num)
        subset.insert(0, "focus_period", period)
        windows.append(subset)
    if not windows:
        return pd.DataFrame()
    return pd.concat(windows, ignore_index=True)


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _write_game_readme(game_dir: Path, row: pd.Series, counts: dict[str, int]) -> None:
    lines = [
        f"# {row['game_id']}",
        "",
        f"- season: {row.get('season', '')}",
        f"- release_gate_status: {row.get('release_gate_status', '')}",
        f"- execution_lane: {row.get('execution_lane', '')}",
        f"- release_reason_code: {row.get('release_reason_code', '')}",
        f"- research_open: {row.get('research_open', '')}",
        f"- actionable_event_rows: {row.get('n_actionable_event_rows', '')}",
        f"- max_abs_minute_diff: {row.get('max_abs_minute_diff', '')}",
        f"- pm_reference_delta_rows: {row.get('n_pm_reference_delta_rows', '')}",
        "",
        "## Files",
        "",
    ]
    for name, count in counts.items():
        lines.append(f"- `{name}`: {count} rows")
    notes = str(row.get("notes", "") or "").strip()
    if notes:
        lines.extend(["", "## Notes", "", notes])
    (game_dir / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_bundle(args: argparse.Namespace) -> dict:
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    games_dir = output_dir / "games"
    games_dir.mkdir(exist_ok=True)

    inventory = _read_csv(args.inventory_path.resolve())
    if inventory.empty:
        raise ValueError(f"No inventory rows found at {args.inventory_path}")
    inventory["game_id"] = inventory["game_id"].map(_normalize_game_id)
    game_ids = inventory["game_id"].tolist()
    game_id_set = set(game_ids)

    actionable = _read_csv(args.actionable_queue_path.resolve())
    events = _event_rows(actionable, game_id_set)

    raw_pbp = _load_raw_pbp_v2(args.raw_pbp_path.resolve(), game_ids)
    tpdev_pbp = _load_tpdev_pbp(args.tpdev_pbp_path.resolve(), game_ids)
    pbpv3 = _load_pbpv3(args.nba_raw_db_path.resolve(), game_ids)
    bbr_pbp, bbr_matches = _load_bbr_pbp(args.nba_raw_db_path.resolve(), args.bbr_db_path.resolve(), game_ids)

    raw_windows = _build_raw_windows(raw_pbp, events, args.window_events)
    v3_windows = _build_v3_windows(pbpv3, events, args.window_events)
    tpdev_windows = _build_tpdev_windows(tpdev_pbp, events, raw_pbp, args.window_seconds)

    _write_csv(inventory, output_dir / "raw_open_inventory.csv")
    _write_csv(events, output_dir / "actionable_event_rows.csv")
    _write_csv(raw_pbp, output_dir / "raw_pbp_v2_all.csv")
    _write_csv(pbpv3, output_dir / "pbpv3_all.csv")
    _write_csv(tpdev_pbp, output_dir / "tpdev_possessions_all.csv")
    _write_csv(bbr_matches, output_dir / "bbr_matches.csv")
    _write_csv(bbr_pbp, output_dir / "bbr_pbp_all.csv")
    _write_csv(raw_windows, output_dir / "raw_pbp_v2_focus_windows.csv")
    _write_csv(v3_windows, output_dir / "pbpv3_focus_windows.csv")
    _write_csv(tpdev_windows, output_dir / "tpdev_possession_focus_windows.csv")

    summary_rows = []
    for row in inventory.itertuples(index=False):
        game_id = row.game_id
        game_dir = games_dir / game_id
        game_dir.mkdir(parents=True, exist_ok=True)
        game_raw = raw_pbp[raw_pbp["GAME_ID"] == game_id].copy()
        game_v3 = pbpv3[pbpv3["GAME_ID"] == game_id].copy() if not pbpv3.empty else pd.DataFrame()
        game_tpdev = tpdev_pbp[tpdev_pbp["GAME_ID"] == game_id].copy() if not tpdev_pbp.empty else pd.DataFrame()
        game_bbr = bbr_pbp[bbr_pbp["GAME_ID"] == game_id].copy() if not bbr_pbp.empty else pd.DataFrame()
        game_events = events[events["GAME_ID"] == game_id].copy() if not events.empty else pd.DataFrame()
        game_raw_windows = raw_windows[raw_windows["GAME_ID"] == game_id].copy() if not raw_windows.empty else pd.DataFrame()
        game_v3_windows = v3_windows[v3_windows["GAME_ID"] == game_id].copy() if not v3_windows.empty else pd.DataFrame()
        game_tpdev_windows = tpdev_windows[tpdev_windows["GAME_ID"] == game_id].copy() if not tpdev_windows.empty else pd.DataFrame()

        files = {
            "raw_pbp_v2.csv": game_raw,
            "pbpv3.csv": game_v3,
            "tpdev_possessions.csv": game_tpdev,
            "bbr_pbp.csv": game_bbr,
            "actionable_event_rows.csv": game_events,
            "raw_pbp_v2_focus_windows.csv": game_raw_windows,
            "pbpv3_focus_windows.csv": game_v3_windows,
            "tpdev_possession_focus_windows.csv": game_tpdev_windows,
        }
        for filename, df in files.items():
            _write_csv(df, game_dir / filename)

        counts = {filename: len(df) for filename, df in files.items()}
        _write_game_readme(game_dir, pd.Series(row._asdict()), counts)
        summary_rows.append({"game_id": game_id, **pd.Series(row._asdict()).to_dict(), **{f"{k}_rows": v for k, v in counts.items()}})

    summary = pd.DataFrame(summary_rows)
    _write_csv(summary, output_dir / "bundle_summary.csv")

    status_counts = Counter(inventory["release_gate_status"])
    reason_counts = Counter(inventory["release_reason_code"])
    readme_lines = [
        "# Open Game PBP Bundle",
        "",
        f"- games: {len(game_ids)}",
        f"- raw_pbp_v2_all rows: {len(raw_pbp)}",
        f"- pbpv3_all rows: {len(pbpv3)}",
        f"- tpdev_possessions_all rows: {len(tpdev_pbp)}",
        f"- bbr_pbp_all rows: {len(bbr_pbp)}",
        f"- actionable event rows: {len(events)}",
        "",
        "## Release Gate Counts",
        "",
    ]
    for key, value in sorted(status_counts.items()):
        readme_lines.append(f"- {key}: {value}")
    readme_lines.extend(["", "## Reason Counts", ""])
    for key, value in sorted(reason_counts.items()):
        readme_lines.append(f"- {key}: {value}")
    readme_lines.extend(
        [
            "",
            "## Main Files",
            "",
            "- `bundle_summary.csv`",
            "- `raw_open_inventory.csv`",
            "- `actionable_event_rows.csv`",
            "- `raw_pbp_v2_all.csv`",
            "- `pbpv3_all.csv`",
            "- `tpdev_possessions_all.csv`",
            "- `bbr_pbp_all.csv`",
            "- `raw_pbp_v2_focus_windows.csv`",
            "- `pbpv3_focus_windows.csv`",
            "- `tpdev_possession_focus_windows.csv`",
            "- `games/<game_id>/...`",
        ]
    )
    (output_dir / "README.md").write_text("\n".join(readme_lines) + "\n", encoding="utf-8")

    return {
        "output_dir": str(output_dir),
        "game_count": len(game_ids),
        "raw_pbp_v2_rows": len(raw_pbp),
        "pbpv3_rows": len(pbpv3),
        "tpdev_possession_rows": len(tpdev_pbp),
        "bbr_pbp_rows": len(bbr_pbp),
        "actionable_event_rows": len(events),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build per-game PBP evidence files for the reviewed raw-open frontier.")
    parser.add_argument("--inventory-path", type=Path, default=DEFAULT_INVENTORY_PATH)
    parser.add_argument("--actionable-queue-path", type=Path, default=DEFAULT_ACTIONABLE_QUEUE_PATH)
    parser.add_argument("--raw-pbp-path", type=Path, default=DEFAULT_RAW_PBP_PATH)
    parser.add_argument("--tpdev-pbp-path", type=Path, default=DEFAULT_TPDEV_PBP_PATH)
    parser.add_argument("--nba-raw-db-path", type=Path, default=DEFAULT_NBA_RAW_DB_PATH)
    parser.add_argument("--bbr-db-path", type=Path, default=DEFAULT_BBR_DB_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--window-events", type=int, default=8)
    parser.add_argument("--window-seconds", type=int, default=45)
    return parser.parse_args()


def main() -> None:
    summary = build_bundle(parse_args())
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
