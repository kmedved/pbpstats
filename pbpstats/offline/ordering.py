from typing import Callable, List, Dict
import numpy as np
import pandas as pd

FetchPbpV3Fn = Callable[[str], pd.DataFrame]


def create_raw_dicts_from_df(sorted_df: pd.DataFrame) -> List[dict]:
    """
    Convert a PBP DataFrame into a list of stats.nba-style event dicts.

    - NaNs -> None
    - numpy integer types -> plain Python ints

    This is the canonical bridge from pandas to pbpstats' enhanced event layer.
    """
    items: List[dict] = []
    records = sorted_df.to_dict("records")
    for row in records:
        clean_item: Dict[str, object] = {}
        for k, v in row.items():
            if pd.isna(v):
                clean_item[k] = None
            elif isinstance(v, (np.integer, np.int64)):
                clean_item[k] = int(v)
            else:
                clean_item[k] = v
        items.append(clean_item)
    return items


def dedupe_with_v3(
    game_df: pd.DataFrame,
    game_id: str,
    fetch_pbp_v3_fn: FetchPbpV3Fn | None = None,
) -> pd.DataFrame:
    """
    Filter play-by-play rows to ones present in playbyplayv3 (if available)
    and drop duplicates.

    v3 is treated as authoritative for which EVENTNUM values are "real".
    """
    df = game_df.copy()
    if fetch_pbp_v3_fn is None:
        return df.drop_duplicates(subset=["GAME_ID", "EVENTNUM"], keep="first")

    df_v3 = fetch_pbp_v3_fn(game_id)
    if df_v3.empty or "actionNumber" not in df_v3.columns:
        return df.drop_duplicates(subset=["GAME_ID", "EVENTNUM"], keep="first")

    df_v3 = df_v3.copy()
    df_v3["actionNumber"] = pd.to_numeric(df_v3["actionNumber"], errors="coerce")
    df_v3 = df_v3.dropna(subset=["actionNumber"])
    valid_nums = set(df_v3["actionNumber"].astype(int).tolist())

    df = df[df["EVENTNUM"].isin(valid_nums)].copy()
    df = df.drop_duplicates(subset=["GAME_ID", "EVENTNUM"], keep="first")
    return df


def patch_start_of_periods(
    game_df: pd.DataFrame,
    game_id: str,
    fetch_pbp_v3_fn: FetchPbpV3Fn | None = None,
) -> pd.DataFrame:
    """
    Ensure there is at least one StartOfPeriod (EVENTMSGTYPE == 12) row for
    each period present in the game.

    - For Period 1, synthesize a start-of-period row if missing.
    - For other periods, optionally use playbyplayv3 PERIOD/START markers.
    """
    df = game_df.copy()
    if "EVENTMSGTYPE" not in df.columns or "PERIOD" not in df.columns:
        return df

    # Existing start-of-period markers
    existing_periods = set(
        df.loc[df["EVENTMSGTYPE"] == 12, "PERIOD"].dropna().astype(int).tolist()
    )

    # Ensure Q1 start exists
    if 1 not in existing_periods and (df["PERIOD"] == 1).any():
        cols = list(df.columns)
        new_row: Dict[str, object] = {c: None for c in cols}

        if "GAME_ID" in cols:
            new_row["GAME_ID"] = game_id
        if "EVENTNUM" in cols:
            min_evnum_q1 = int(df.loc[df["PERIOD"] == 1, "EVENTNUM"].min())
            new_row["EVENTNUM"] = min_evnum_q1 - 1
        if "EVENTMSGTYPE" in cols:
            new_row["EVENTMSGTYPE"] = 12
        if "EVENTMSGACTIONTYPE" in cols:
            new_row["EVENTMSGACTIONTYPE"] = 0
        if "PERIOD" in cols:
            new_row["PERIOD"] = 1
        if "PCTIMESTRING" in cols:
            new_row["PCTIMESTRING"] = "12:00"

        for fld in [
            "PLAYER1_ID",
            "PLAYER1_TEAM_ID",
            "PLAYER2_ID",
            "PLAYER2_TEAM_ID",
            "PLAYER3_ID",
            "PLAYER3_TEAM_ID",
        ]:
            if fld in cols:
                new_row[fld] = 0
        if "VIDEO_AVAILABLE_FLAG" in cols:
            new_row["VIDEO_AVAILABLE_FLAG"] = 0

        df = pd.concat([pd.DataFrame([new_row]), df], ignore_index=True)
        if "EVENTNUM" in cols:
            df = df.sort_values(["PERIOD", "EVENTNUM"]).reset_index(drop=True)
        existing_periods.add(1)

    all_periods_in_game = set(df["PERIOD"].dropna().astype(int).unique())
    missing_periods = all_periods_in_game - existing_periods

    if not missing_periods or fetch_pbp_v3_fn is None:
        return df

    # Use v3 period/start markers if available
    df_v3 = fetch_pbp_v3_fn(game_id)
    if df_v3.empty or "actionType" not in df_v3.columns or "subType" not in df_v3.columns:
        return df

    mask = (
        df_v3["actionType"].astype(str).str.lower().eq("period")
        & df_v3["subType"].astype(str).str.lower().eq("start")
    )
    starts = df_v3.loc[mask]
    if starts.empty:
        return df

    cols = list(df.columns)
    new_rows = []

    for _, r in starts.iterrows():
        try:
            period = int(r.get("period", 0) or 0)
        except (TypeError, ValueError):
            continue
        if period <= 0 or period in existing_periods:
            continue

        action_num = r.get("actionNumber")
        try:
            eventnum = int(action_num)
        except (TypeError, ValueError):
            continue

        row: Dict[str, object] = {c: None for c in cols}
        if "GAME_ID" in cols:
            row["GAME_ID"] = game_id
        if "EVENTNUM" in cols:
            row["EVENTNUM"] = eventnum
        if "EVENTMSGTYPE" in cols:
            row["EVENTMSGTYPE"] = 12
        if "EVENTMSGACTIONTYPE" in cols:
            row["EVENTMSGACTIONTYPE"] = 0
        if "PERIOD" in cols:
            row["PERIOD"] = period
        if "PCTIMESTRING" in cols:
            row["PCTIMESTRING"] = "12:00"

        for fld in [
            "PLAYER1_ID",
            "PLAYER1_TEAM_ID",
            "PLAYER2_ID",
            "PLAYER2_TEAM_ID",
            "PLAYER3_ID",
            "PLAYER3_TEAM_ID",
        ]:
            if fld in cols:
                row[fld] = 0

        new_rows.append(row)
        existing_periods.add(period)

    if new_rows:
        df_new = pd.DataFrame(new_rows, columns=cols)
        df = pd.concat([df, df_new], ignore_index=True)

    return df


def reorder_with_v3(
    game_df: pd.DataFrame,
    game_id: str,
    fetch_pbp_v3_fn: FetchPbpV3Fn,
) -> pd.DataFrame:
    """
    Reorder pbp rows using playbyplayv3 actionId order.

    - Build actionNumber -> canonical index mapping from v3.
    - Sort events by that canonical index, then by EVENTNUM.
    """
    df_v3 = fetch_pbp_v3_fn(game_id)
    if df_v3.empty or "actionNumber" not in df_v3.columns or "actionId" not in df_v3.columns:
        raise RuntimeError(f"No v3 data for {game_id}")

    df_v3 = df_v3.copy()
    df_v3["actionNumber"] = pd.to_numeric(df_v3["actionNumber"], errors="coerce")
    df_v3 = df_v3.dropna(subset=["actionNumber"])
    df_v3["actionNumber"] = df_v3["actionNumber"].astype(int)
    df_v3 = df_v3.sort_values("actionId")

    order_map: Dict[int, int] = {}
    canonical_idx = 0
    for num in df_v3["actionNumber"]:
        if num not in order_map:
            order_map[num] = canonical_idx
            canonical_idx += 1

    result = game_df.copy()
    result["EVENTNUM"] = pd.to_numeric(result["EVENTNUM"], errors="coerce")
    result = result.dropna(subset=["EVENTNUM"])
    result["EVENTNUM"] = result["EVENTNUM"].astype(int)

    max_idx = len(order_map) + 1000
    result["__v3_order"] = result["EVENTNUM"].map(order_map).fillna(max_idx).astype(int)

    # Keep StartOfPeriod(1) at the very beginning if present
    if "EVENTMSGTYPE" in result.columns and "PERIOD" in result.columns:
        q1_start_mask = (result["EVENTMSGTYPE"] == 12) & (result["PERIOD"] == 1)
        result.loc[q1_start_mask, "__v3_order"] = -1

    result = result.sort_values(["__v3_order", "EVENTNUM"]).drop(columns="__v3_order")
    return result
