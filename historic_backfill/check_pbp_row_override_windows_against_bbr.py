from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from bbr_pbp_lookup import DEFAULT_BBR_DB_PATH, DEFAULT_NBA_RAW_DB_PATH, find_bbr_game_for_nba_game, load_bbr_play_by_play_rows


ROOT = Path(__file__).resolve().parent
DEFAULT_OVERRIDES_PATH = ROOT / "pbp_row_overrides.csv"
DEFAULT_PARQUET_PATH = ROOT / "playbyplayv2.parq"
DEFAULT_OUTPUT_DIR = ROOT / "pbp_row_override_bbr_window_audit_20260315_v1"


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _load_overrides(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str).fillna("")
    df["game_id"] = df["game_id"].map(_normalize_game_id)
    return df


def _load_raw_game_df(parquet_path: Path, game_id: str) -> pd.DataFrame:
    df = pd.read_parquet(
        parquet_path,
        filters=[("GAME_ID", "==", str(int(game_id)))],
        columns=[
            "GAME_ID",
            "EVENTNUM",
            "PERIOD",
            "PCTIMESTRING",
            "HOMEDESCRIPTION",
            "VISITORDESCRIPTION",
            "NEUTRALDESCRIPTION",
            "PLAYER1_NAME",
            "PLAYER2_NAME",
            "PLAYER3_NAME",
        ],
    )
    df["GAME_ID"] = df["GAME_ID"].astype(str).str.zfill(10)
    df["EVENTNUM_INT"] = pd.to_numeric(df["EVENTNUM"], errors="coerce").fillna(-1).astype(int)
    df["PERIOD_INT"] = pd.to_numeric(df["PERIOD"], errors="coerce").fillna(-1).astype(int)
    return df[df["GAME_ID"] == game_id].sort_values(["PERIOD_INT", "EVENTNUM_INT"]).reset_index(drop=True)


def _raw_description(row: pd.Series | dict[str, Any]) -> str:
    for col in ("VISITORDESCRIPTION", "HOMEDESCRIPTION", "NEUTRALDESCRIPTION"):
        value = str(row.get(col, "") or "").strip()
        if value:
            return value
    return ""


def _normalize_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()


def _extract_tokens(row: pd.Series | dict[str, Any], description: str) -> list[str]:
    tokens: list[str] = []
    for key in ("PLAYER1_NAME", "PLAYER2_NAME", "PLAYER3_NAME"):
        name = str(row.get(key, "") or "").strip()
        if not name:
            continue
        parts = [part for part in re.split(r"\s+", name) if part]
        if parts:
            tokens.append(parts[-1].lower())
            if len(parts) >= 2:
                tokens.append(f"{parts[0][0].lower()} {parts[-1].lower()}")

    desc = _normalize_text(description)
    for phrase in (
        "offensive rebound by team",
        "defensive rebound by team",
        "offensive rebound",
        "defensive rebound",
        "rebound by team",
        "misses free throw",
        "makes free throw",
        "misses 2 pt",
        "misses 3 pt",
        "makes 2 pt",
        "makes 3 pt",
        "personal foul",
        "shooting foul",
        "loose ball foul",
        "turnover",
        "jump ball",
        "timeout",
        "substitution",
    ):
        normalized_phrase = _normalize_text(phrase)
        if normalized_phrase in desc:
            tokens.append(normalized_phrase)
    return sorted(set(token for token in tokens if token))


def _bbr_play_text(row: dict[str, Any]) -> str:
    return str(row.get("away_play") or row.get("home_play") or "").strip()


def _first_hit_index(tokens: list[str], bbr_rows: list[dict[str, Any]]) -> int | None:
    if not tokens:
        return None
    for idx, row in enumerate(bbr_rows):
        normalized = _normalize_text(_bbr_play_text(row))
        if any(token in normalized for token in tokens):
            return idx
    return None


def _clock_sort_key(clock: str) -> tuple[int, int]:
    try:
        minutes, seconds = str(clock).replace(".0", "").split(":")
        return int(minutes), int(seconds)
    except ValueError:
        return (-1, -1)


def _classify_status(action: str, target_hit: int | None, anchor_hit: int | None, bbr_rows: list[dict[str, Any]]) -> str:
    if not bbr_rows:
        return "missing_bbr_clock"
    if action == "drop":
        return "bbr_omits_target_like_event" if target_hit is None else "bbr_keeps_target_like_event"
    if target_hit is None and anchor_hit is None:
        return "bbr_window_inconclusive"
    if target_hit is None or anchor_hit is None:
        return "bbr_partial_window"
    if action == "move_before":
        if target_hit < anchor_hit:
            return "bbr_supports_move_before"
        if target_hit > anchor_hit:
            return "bbr_supports_move_after_or_raw"
    if action == "move_after":
        if target_hit > anchor_hit:
            return "bbr_supports_move_after"
        if target_hit < anchor_hit:
            return "bbr_supports_move_before_or_raw"
    return "bbr_window_inconclusive"


def _stringify_rows(rows: pd.DataFrame | list[dict[str, Any]]) -> str:
    if isinstance(rows, pd.DataFrame):
        parts = []
        for _, row in rows.iterrows():
            parts.append(
                f"{int(row['EVENTNUM_INT']):>4} {row['PCTIMESTRING']:>5} {_raw_description(row)}"
            )
        return " || ".join(parts)
    parts = []
    for row in rows:
        parts.append(
            f"{int(row['event_index']):>4} {str(row.get('game_clock', '')):>7} {_bbr_play_text(row)}"
        )
    return " || ".join(parts)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare each row override's local raw PBP window against same-clock BBR PBP rows.")
    parser.add_argument("--overrides-path", type=Path, default=DEFAULT_OVERRIDES_PATH)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--nba-raw-db", type=Path, default=DEFAULT_NBA_RAW_DB_PATH)
    parser.add_argument("--bbr-db", type=Path, default=DEFAULT_BBR_DB_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    overrides = _load_overrides(args.overrides_path.resolve())
    raw_game_cache: dict[str, pd.DataFrame] = {}
    bbr_game_cache: dict[str, str | None] = {}
    bbr_clock_cache: dict[tuple[str, int, str], list[dict[str, Any]]] = {}

    audit_rows: list[dict[str, Any]] = []
    for override in overrides.to_dict(orient="records"):
        game_id = override["game_id"]
        raw_game = raw_game_cache.get(game_id)
        if raw_game is None:
            raw_game = _load_raw_game_df(args.parquet_path.resolve(), game_id)
            raw_game_cache[game_id] = raw_game

        target_event_num = int(override["event_num"])
        anchor_event_num = int(override["anchor_event_num"]) if override["anchor_event_num"] else None

        target_row = raw_game[raw_game["EVENTNUM_INT"] == target_event_num]
        anchor_row = raw_game[raw_game["EVENTNUM_INT"] == anchor_event_num] if anchor_event_num is not None else pd.DataFrame()

        period = None
        if not target_row.empty:
            period = int(target_row.iloc[0]["PERIOD_INT"])
        elif not anchor_row.empty:
            period = int(anchor_row.iloc[0]["PERIOD_INT"])

        target_clock = str(target_row.iloc[0]["PCTIMESTRING"]) if not target_row.empty else ""
        anchor_clock = str(anchor_row.iloc[0]["PCTIMESTRING"]) if not anchor_row.empty else ""
        candidate_clocks = sorted(set(clock for clock in [target_clock, anchor_clock] if clock), key=_clock_sort_key, reverse=True)

        low = target_event_num
        high = target_event_num
        if anchor_event_num is not None:
            low = min(low, anchor_event_num)
            high = max(high, anchor_event_num)
        raw_window = raw_game[
            raw_game["EVENTNUM_INT"].between(low - 2, high + 2)
            & ((period is None) | (raw_game["PERIOD_INT"] == period))
        ].copy()

        target_desc = _raw_description(target_row.iloc[0]) if not target_row.empty else ""
        anchor_desc = _raw_description(anchor_row.iloc[0]) if not anchor_row.empty else ""
        target_tokens = _extract_tokens(target_row.iloc[0], target_desc) if not target_row.empty else []
        anchor_tokens = _extract_tokens(anchor_row.iloc[0], anchor_desc) if not anchor_row.empty else []

        bbr_game_id = bbr_game_cache.get(game_id)
        if game_id not in bbr_game_cache:
            _, matches = find_bbr_game_for_nba_game(game_id, nba_raw_db_path=args.nba_raw_db.resolve(), bbr_db_path=args.bbr_db.resolve())
            bbr_game_id = matches[0].bbr_game_id if len(matches) == 1 else None
            bbr_game_cache[game_id] = bbr_game_id

        bbr_rows: list[dict[str, Any]] = []
        if bbr_game_id is not None and period is not None:
            for clock in candidate_clocks:
                cache_key = (bbr_game_id, period, clock)
                if cache_key not in bbr_clock_cache:
                    bbr_clock_cache[cache_key] = load_bbr_play_by_play_rows(
                        bbr_game_id,
                        bbr_db_path=args.bbr_db.resolve(),
                        period=period,
                        clock=clock,
                    )
                bbr_rows.extend(bbr_clock_cache[cache_key])

        target_hit = _first_hit_index(target_tokens, bbr_rows)
        anchor_hit = _first_hit_index(anchor_tokens, bbr_rows)
        status = _classify_status(override["action"], target_hit, anchor_hit, bbr_rows)

        audit_rows.append(
            {
                "game_id": game_id,
                "action": override["action"],
                "event_num": target_event_num,
                "anchor_event_num": anchor_event_num or "",
                "period": period or "",
                "target_clock": target_clock,
                "anchor_clock": anchor_clock,
                "target_description": target_desc,
                "anchor_description": anchor_desc,
                "target_tokens": "|".join(target_tokens),
                "anchor_tokens": "|".join(anchor_tokens),
                "bbr_game_id": bbr_game_id or "",
                "bbr_row_count": len(bbr_rows),
                "bbr_status": status,
                "bbr_target_hit_index": "" if target_hit is None else target_hit,
                "bbr_anchor_hit_index": "" if anchor_hit is None else anchor_hit,
                "raw_window": _stringify_rows(raw_window),
                "bbr_window": _stringify_rows(bbr_rows),
                "notes": override.get("notes", ""),
            }
        )

    report = pd.DataFrame(audit_rows).sort_values(
        ["bbr_status", "game_id", "event_num", "anchor_event_num"]
    ).reset_index(drop=True)
    report.to_csv(output_dir / "pbp_row_override_bbr_window_audit.csv", index=False)

    summary = {
        "rows": int(len(report)),
        "games": int(report["game_id"].nunique()),
        "bbr_status_counts": report["bbr_status"].value_counts(dropna=False).to_dict(),
        "missing_bbr_game_rows": int((report["bbr_game_id"] == "").sum()),
        "missing_bbr_clock_rows": int((report["bbr_status"] == "missing_bbr_clock").sum()),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
