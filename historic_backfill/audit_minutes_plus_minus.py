from __future__ import annotations

import argparse
import json
import sqlite3
import zlib
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

from boxscore_source_overrides import apply_boxscore_response_overrides


MINUTE_OUTLIER_THRESHOLD = 0.5


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def parse_official_minutes(value: Any) -> float:
    if value is None:
        return 0.0
    text = str(value).strip()
    if text == "" or text.upper() in {"DNP", "DND", "NWT"}:
        return 0.0
    if ":" in text:
        minutes, seconds = text.split(":", 1)
        try:
            return int(minutes) + (int(seconds) / 60.0)
        except ValueError:
            return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _load_raw_response(
    db_path: Path, game_id: str, endpoint: str
) -> Dict[str, Any] | None:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True, timeout=30)
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


def _decode_raw_response_blob(blob: Any) -> Dict[str, Any]:
    try:
        return json.loads(zlib.decompress(blob).decode())
    except (zlib.error, TypeError):
        if isinstance(blob, bytes):
            return json.loads(blob.decode())
        return json.loads(blob)


def _empty_official_boxscore_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "game_id",
            "player_id",
            "team_id",
            "player_name",
            "Minutes_official",
            "Plus_Minus_official",
        ]
    )


def _build_official_boxscore_df(game_id: str, raw: Dict[str, Any] | None) -> pd.DataFrame:
    raw = apply_boxscore_response_overrides(game_id, raw)
    if not raw:
        return _empty_official_boxscore_df()

    result_sets = raw.get("resultSets", [])
    if not result_sets:
        return _empty_official_boxscore_df()

    headers = result_sets[0].get("headers", [])
    rows = result_sets[0].get("rowSet", [])
    if not headers:
        return _empty_official_boxscore_df()

    official = pd.DataFrame(rows, columns=headers)
    if official.empty:
        return _empty_official_boxscore_df()

    official["PLAYER_ID"] = (
        pd.to_numeric(official["PLAYER_ID"], errors="coerce").fillna(0).astype(int)
    )
    official["TEAM_ID"] = (
        pd.to_numeric(official["TEAM_ID"], errors="coerce").fillna(0).astype(int)
    )
    official = official[official["PLAYER_ID"] > 0].copy()
    if official.empty:
        return pd.DataFrame()

    official["game_id"] = _normalize_game_id(game_id)
    official["player_id"] = official["PLAYER_ID"]
    official["team_id"] = official["TEAM_ID"]
    official["player_name"] = official.get("PLAYER_NAME", "").fillna("").astype(str)
    official["Minutes_official"] = official.get("MIN", "").apply(parse_official_minutes)
    official["Plus_Minus_official"] = (
        pd.to_numeric(official.get("PLUS_MINUS", 0), errors="coerce")
        .fillna(0.0)
        .astype(float)
    )
    return official[
        [
            "game_id",
            "player_id",
            "team_id",
            "player_name",
            "Minutes_official",
            "Plus_Minus_official",
        ]
    ].copy()


def load_official_boxscore_batch_df(
    db_path: Path,
    game_ids: Iterable[str | int],
    *,
    chunk_size: int = 500,
) -> pd.DataFrame:
    normalized_game_ids = sorted({_normalize_game_id(game_id) for game_id in game_ids})
    if not normalized_game_ids:
        return _empty_official_boxscore_df()

    frames: List[pd.DataFrame] = []
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True, timeout=30)
    try:
        for start in range(0, len(normalized_game_ids), chunk_size):
            chunk = normalized_game_ids[start : start + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            query = (
                "SELECT game_id, data FROM raw_responses "
                "WHERE endpoint='boxscore' AND team_id IS NULL "
                f"AND game_id IN ({placeholders})"
            )
            rows = conn.execute(query, chunk).fetchall()
            raw_by_game_id = {
                _normalize_game_id(game_id): _decode_raw_response_blob(blob)
                for game_id, blob in rows
            }
            for game_id in chunk:
                frames.append(_build_official_boxscore_df(game_id, raw_by_game_id.get(game_id)))
    finally:
        conn.close()

    if not frames:
        return _empty_official_boxscore_df()
    combined = pd.concat(frames, ignore_index=True)
    if combined.empty:
        return _empty_official_boxscore_df()
    return combined.sort_values(["game_id", "team_id", "player_id"]).reset_index(drop=True)


def load_official_boxscore_df(db_path: Path, game_id: str) -> pd.DataFrame:
    raw = _load_raw_response(db_path, game_id, "boxscore")
    return _build_official_boxscore_df(game_id, raw)


def _prepare_darko_df(darko_df: pd.DataFrame) -> pd.DataFrame:
    required = ["Game_SingleGame", "NbaDotComID", "Team_SingleGame", "FullName", "Minutes", "Plus_Minus"]
    missing = [column for column in required if column not in darko_df.columns]
    if missing:
        raise ValueError(f"darko dataframe is missing required columns: {missing}")

    prepared = darko_df.copy()
    prepared["game_id"] = prepared["Game_SingleGame"].apply(_normalize_game_id)
    prepared["player_id"] = (
        pd.to_numeric(prepared["NbaDotComID"], errors="coerce").fillna(0).astype(int)
    )
    prepared["team_id"] = (
        pd.to_numeric(prepared["Team_SingleGame"], errors="coerce").fillna(0).astype(int)
    )
    prepared["player_name"] = prepared["FullName"].fillna("").astype(str)
    prepared["Minutes_output"] = (
        pd.to_numeric(prepared["Minutes"], errors="coerce").fillna(0.0).astype(float)
    )
    prepared["Plus_Minus_output"] = (
        pd.to_numeric(prepared["Plus_Minus"], errors="coerce").fillna(0.0).astype(float)
    )
    prepared = prepared[prepared["player_id"] > 0].copy()
    return prepared[
        [
            "game_id",
            "player_id",
            "team_id",
            "player_name",
            "Minutes_output",
            "Plus_Minus_output",
        ]
    ].copy()


def build_minutes_plus_minus_audit(
    darko_df: pd.DataFrame,
    db_path: Path,
    minute_outlier_threshold: float = MINUTE_OUTLIER_THRESHOLD,
) -> pd.DataFrame:
    prepared = _prepare_darko_df(darko_df)
    if prepared.empty:
        return pd.DataFrame(
            columns=[
                "game_id",
                "team_id",
                "player_id",
                "player_name",
                "Minutes_output",
                "Minutes_official",
                "Minutes_diff",
                "Minutes_abs_diff",
                "Plus_Minus_output",
                "Plus_Minus_official",
                "Plus_Minus_diff",
                "has_minutes_mismatch",
                "has_plus_minus_mismatch",
                "is_minutes_outlier",
            ]
        )

    official = load_official_boxscore_batch_df(
        db_path,
        sorted(prepared["game_id"].unique()),
    )

    merged = prepared.merge(
        official,
        on=["game_id", "player_id", "team_id"],
        how="outer",
        suffixes=("_output", "_official"),
    )
    if merged.empty:
        return merged

    merged["player_name"] = (
        merged.get("player_name_output")
        .fillna(merged.get("player_name_official"))
        .fillna("")
        .astype(str)
    )
    merged["Minutes_output"] = pd.to_numeric(
        merged.get("Minutes_output", 0.0), errors="coerce"
    ).fillna(0.0)
    merged["Minutes_official"] = pd.to_numeric(
        merged.get("Minutes_official", 0.0), errors="coerce"
    ).fillna(0.0)
    merged["Plus_Minus_output"] = pd.to_numeric(
        merged.get("Plus_Minus_output", 0.0), errors="coerce"
    ).fillna(0.0)
    merged["Plus_Minus_official"] = pd.to_numeric(
        merged.get("Plus_Minus_official", 0.0), errors="coerce"
    ).fillna(0.0)
    merged["Minutes_diff"] = merged["Minutes_output"] - merged["Minutes_official"]
    merged["Minutes_abs_diff"] = merged["Minutes_diff"].abs()
    merged["Plus_Minus_diff"] = (
        merged["Plus_Minus_output"] - merged["Plus_Minus_official"]
    )
    merged["has_minutes_mismatch"] = merged["Minutes_abs_diff"] > (1.0 / 60.0)
    merged["has_plus_minus_mismatch"] = merged["Plus_Minus_diff"] != 0
    merged["is_minutes_outlier"] = merged["Minutes_abs_diff"] > minute_outlier_threshold

    return merged[
        [
            "game_id",
            "team_id",
            "player_id",
            "player_name",
            "Minutes_output",
            "Minutes_official",
            "Minutes_diff",
            "Minutes_abs_diff",
            "Plus_Minus_output",
            "Plus_Minus_official",
            "Plus_Minus_diff",
            "has_minutes_mismatch",
            "has_plus_minus_mismatch",
            "is_minutes_outlier",
        ]
    ].sort_values(["game_id", "team_id", "player_id"]).reset_index(drop=True)


def summarize_minutes_plus_minus_audit(audit_df: pd.DataFrame) -> Dict[str, Any]:
    if audit_df.empty:
        return {
            "rows": 0,
            "minutes_mismatches": 0,
            "minutes_outliers": 0,
            "plus_minus_mismatches": 0,
            "minutes_outlier_threshold": MINUTE_OUTLIER_THRESHOLD,
        }

    return {
        "rows": int(len(audit_df)),
        "minutes_mismatches": int(audit_df["has_minutes_mismatch"].sum()),
        "minutes_outliers": int(audit_df["is_minutes_outlier"].sum()),
        "plus_minus_mismatches": int(audit_df["has_plus_minus_mismatch"].sum()),
        "minutes_outlier_threshold": MINUTE_OUTLIER_THRESHOLD,
    }


def _load_darko_frames(paths: Iterable[Path]) -> pd.DataFrame:
    frames = [pd.read_parquet(path) for path in paths]
    if not frames:
        raise ValueError("No parquet files supplied")
    return pd.concat(frames, ignore_index=True)


def _discover_parquet_paths(input_path: Path) -> List[Path]:
    if input_path.is_file():
        return [input_path]
    if input_path.is_dir():
        return sorted(
            path
            for path in input_path.glob("darko_*.parquet")
            if path.name != "darko_1997_2020.parquet"
        )
    raise FileNotFoundError(input_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_path", type=Path)
    parser.add_argument("--db-path", type=Path, default=Path(__file__).with_name("nba_raw.db"))
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--minute-outlier-threshold", type=float, default=MINUTE_OUTLIER_THRESHOLD)
    args = parser.parse_args()

    parquet_paths = _discover_parquet_paths(args.input_path)
    darko_df = _load_darko_frames(parquet_paths)
    audit_df = build_minutes_plus_minus_audit(
        darko_df, db_path=args.db_path, minute_outlier_threshold=args.minute_outlier_threshold
    )
    summary = summarize_minutes_plus_minus_audit(audit_df)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    audit_df.to_csv(args.output_dir / "minutes_plus_minus_audit.csv", index=False)
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
