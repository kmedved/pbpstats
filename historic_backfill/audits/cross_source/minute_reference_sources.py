from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = ROOT / "data"
DEFAULT_TPDEV_BOX_PATH = (
    DATA_ROOT / "tpdev" / "tpdev_box.parq"
)
DEFAULT_TPDEV_BOX_NEW_PATH = (
    DATA_ROOT / "tpdev" / "tpdev_box_new.parq"
)
DEFAULT_TPDEV_BOX_CDN_PATH = (
    DATA_ROOT / "tpdev" / "tpdev_box_cdn.parq"
)
DEFAULT_TPDEV_PBP_PATH = (
    DATA_ROOT / "tpdev" / "full_pbp_new.parq"
)
DEFAULT_PBPSTATS_PLAYER_BOX_PATH = (
    DATA_ROOT / "pbpstats_player_box.parq"
)


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _game_ids_as_ints(game_ids: Iterable[str | int] | None) -> list[int]:
    if game_ids is None:
        return []
    return [int(_normalize_game_id(game_id)) for game_id in game_ids]


def _parse_pbpstats_minutes(value: object) -> float:
    text = str(value or "").strip()
    if text == "":
        return 0.0
    if ":" not in text:
        try:
            return float(text)
        except ValueError:
            return 0.0
    minutes_text, seconds_text = text.split(":", 1)
    try:
        return int(minutes_text) + (int(seconds_text) / 60.0)
    except ValueError:
        return 0.0


def load_tpdev_box_frame(
    path: Path,
    label: str,
    game_ids: Iterable[str | int] | None = None,
) -> pd.DataFrame:
    columns = [
        "game_id",
        "player_id",
        "team_id",
        f"Minutes_{label}",
        f"Plus_Minus_{label}",
    ]
    if not path.exists():
        return pd.DataFrame(columns=columns)

    filters = None
    int_ids = _game_ids_as_ints(game_ids)
    if int_ids:
        filters = [("Game_SingleGame", "in", int_ids)]

    df = pd.read_parquet(
        path,
        filters=filters,
        columns=[
            "Game_SingleGame",
            "Team_SingleGame",
            "NbaDotComID",
            "Minutes",
            "Plus_Minus",
        ],
    )
    if df.empty:
        return pd.DataFrame(columns=columns)

    df["game_id"] = df["Game_SingleGame"].apply(_normalize_game_id)
    df["player_id"] = pd.to_numeric(df["NbaDotComID"], errors="coerce").fillna(0).astype(int)
    df["team_id"] = pd.to_numeric(df["Team_SingleGame"], errors="coerce").fillna(0).astype(int)
    df[f"Minutes_{label}"] = pd.to_numeric(df["Minutes"], errors="coerce").fillna(0.0)
    df[f"Plus_Minus_{label}"] = pd.to_numeric(df["Plus_Minus"], errors="coerce").fillna(0.0)
    return df[columns].copy()


def load_pbpstats_player_box_frame(
    path: Path = DEFAULT_PBPSTATS_PLAYER_BOX_PATH,
    game_ids: Iterable[str | int] | None = None,
) -> pd.DataFrame:
    columns = [
        "game_id",
        "player_id",
        "team_id",
        "player_name_pbpstats_box",
        "Minutes_pbpstats_box",
        "pbpstats_box_seconds",
    ]
    if not path.exists():
        return pd.DataFrame(columns=columns)

    filters = None
    int_ids = _game_ids_as_ints(game_ids)
    if int_ids:
        filters = [("game_id", "in", int_ids)]

    df = pd.read_parquet(
        path,
        filters=filters,
        columns=["game_id", "nba_id", "tm_id", "Name", "Minutes"],
    )
    if df.empty:
        return pd.DataFrame(columns=columns)

    df["game_id"] = df["game_id"].apply(_normalize_game_id)
    df["player_id"] = pd.to_numeric(df["nba_id"], errors="coerce").fillna(0).astype(int)
    df["team_id"] = pd.to_numeric(df["tm_id"], errors="coerce").fillna(0).astype(int)
    df["player_name_pbpstats_box"] = df["Name"].fillna("").astype(str)
    df["Minutes_pbpstats_box"] = df["Minutes"].map(_parse_pbpstats_minutes)
    df["pbpstats_box_seconds"] = df["Minutes_pbpstats_box"] * 60.0
    return df[columns].copy()


def _period_length_seconds(period: int) -> int:
    return 720 if int(period) <= 4 else 300


def _accumulate_interval_filled_seconds(
    df: pd.DataFrame,
    team_id: int,
    lineup_cols: list[str],
) -> list[dict[str, object]]:
    totals: dict[int, float] = {}
    for quarter, quarter_df in df.groupby("Quarter"):
        quarter_df = quarter_df.sort_values("TimeRemainingStart", ascending=False).reset_index(drop=True)
        previous_end = _period_length_seconds(int(quarter))
        last_lineup: list[int] = []

        for row in quarter_df.itertuples(index=False):
            start = int(getattr(row, "TimeRemainingStart"))
            duration = max(0, int(getattr(row, "LengthInSeconds")))
            current_lineup = []
            for column in lineup_cols:
                player_id = pd.to_numeric(getattr(row, column), errors="coerce")
                if pd.isna(player_id):
                    continue
                player_int = int(player_id)
                if player_int <= 0 or player_int in current_lineup:
                    continue
                current_lineup.append(player_int)

            gap_seconds = max(0, previous_end - start)
            for player_id in current_lineup:
                totals[player_id] = totals.get(player_id, 0.0) + gap_seconds + duration

            previous_end = max(0, start - duration)
            last_lineup = current_lineup

        for player_id in last_lineup:
            totals[player_id] = totals.get(player_id, 0.0) + previous_end

    return [
        {
            "game_id": df["game_id"].iloc[0],
            "player_id": player_id,
            "team_id": team_id,
            "Minutes_tpdev_pbp": seconds / 60.0,
            "tpdev_pbp_seconds": seconds,
        }
        for player_id, seconds in sorted(totals.items())
    ]


def load_tpdev_pbp_minutes_frame(
    path: Path = DEFAULT_TPDEV_PBP_PATH,
    game_ids: Iterable[str | int] | None = None,
) -> pd.DataFrame:
    columns = [
        "game_id",
        "player_id",
        "team_id",
        "Minutes_tpdev_pbp",
        "tpdev_pbp_seconds",
    ]
    if not path.exists():
        return pd.DataFrame(columns=columns)

    filters = None
    int_ids = _game_ids_as_ints(game_ids)
    if int_ids:
        filters = [("game_id", "in", int_ids)]

    df = pd.read_parquet(
        path,
        filters=filters,
        columns=[
            "game_id",
            "Quarter",
            "TimeRemainingStart",
            "LengthInSeconds",
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
        ],
    )
    if df.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, object]] = []
    df["game_id"] = df["game_id"].apply(_normalize_game_id)
    for game_id, game_df in df.groupby("game_id", sort=True):
        home_team_id = pd.to_numeric(game_df["h_tm_id"], errors="coerce").dropna()
        away_team_id = pd.to_numeric(game_df["v_tm_id"], errors="coerce").dropna()
        if home_team_id.empty or away_team_id.empty:
            continue
        rows.extend(
            _accumulate_interval_filled_seconds(
                game_df,
                team_id=int(home_team_id.iloc[0]),
                lineup_cols=["h1", "h2", "h3", "h4", "h5"],
            )
        )
        rows.extend(
            _accumulate_interval_filled_seconds(
                game_df,
                team_id=int(away_team_id.iloc[0]),
                lineup_cols=["v1", "v2", "v3", "v4", "v5"],
            )
        )

    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns)
