from typing import Callable, Dict, List

import numpy as np
import pandas as pd

FetchPbpV3Fn = Callable[[str], pd.DataFrame]


def _ensure_eventnum_int(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure EVENTNUM is an int column.

    - Coerce to numeric with errors='coerce'
    - Drop rows where EVENTNUM can't be parsed
    - Cast to int
    """
    result = df.copy()
    result["EVENTNUM"] = pd.to_numeric(result["EVENTNUM"], errors="coerce")
    result = result.dropna(subset=["EVENTNUM"])
    result["EVENTNUM"] = result["EVENTNUM"].astype(int)
    return result


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

    # Always normalize EVENTNUM to int before we do anything with it.
    df = _ensure_eventnum_int(df)

    if fetch_pbp_v3_fn is None:
        return df.drop_duplicates(subset=["GAME_ID", "EVENTNUM"], keep="first")

    df_v3 = fetch_pbp_v3_fn(game_id)
    if df_v3 is None or df_v3.empty or "actionNumber" not in df_v3.columns:
        return df.drop_duplicates(subset=["GAME_ID", "EVENTNUM"], keep="first")

    df_v3 = df_v3.copy()
    df_v3["actionNumber"] = pd.to_numeric(df_v3["actionNumber"], errors="coerce")
    df_v3 = df_v3.dropna(subset=["actionNumber"])
    valid_nums = set(df_v3["actionNumber"].astype(int).tolist())

    df = df[df["EVENTNUM"].isin(valid_nums)].copy()
    df = df.drop_duplicates(subset=["GAME_ID", "EVENTNUM"], keep="first")
    return df


def _build_start_of_period_row(
    cols: List[str],
    game_id: str,
    period: int,
    eventnum: int,
) -> Dict[str, object]:
    row: Dict[str, object] = {c: None for c in cols}

    if "GAME_ID" in cols:
        row["GAME_ID"] = game_id
    if "EVENTNUM" in cols:
        row["EVENTNUM"] = int(eventnum)
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

    if "VIDEO_AVAILABLE_FLAG" in cols:
        row["VIDEO_AVAILABLE_FLAG"] = 0

    return row


def _insert_row_before_period(
    df: pd.DataFrame,
    row: Dict[str, object],
    period: int,
) -> pd.DataFrame:
    period_mask = df["PERIOD"] == period
    if not period_mask.any():
        return pd.concat([df, pd.DataFrame([row], columns=df.columns)], ignore_index=True)

    insert_at = int(df.index[period_mask][0])
    return pd.concat(
        [
            df.iloc[:insert_at],
            pd.DataFrame([row], columns=df.columns),
            df.iloc[insert_at:],
        ],
        ignore_index=True,
    )


def patch_start_of_periods(
    game_df: pd.DataFrame,
    game_id: str,
    fetch_pbp_v3_fn: FetchPbpV3Fn | None = None,
) -> pd.DataFrame:
    """
    Ensure there is at least one StartOfPeriod (EVENTMSGTYPE == 12) row for
    each period present in the game.

    Preserve existing intra-period row order. Historical feeds often have valid
    chronology encoded in raw row order even when EVENTNUM is non-monotonic.

    - For Period 1, synthesize a start-of-period row if missing.
    - For other periods, optionally use playbyplayv3 PERIOD/START markers.
    """
    df = game_df.copy()
    if "EVENTMSGTYPE" not in df.columns or "PERIOD" not in df.columns:
        return df

    df = _ensure_eventnum_int(df)
    cols = list(df.columns)

    existing_periods = set(
        df.loc[df["EVENTMSGTYPE"] == 12, "PERIOD"].dropna().astype(int).tolist()
    )

    if 1 not in existing_periods and (df["PERIOD"] == 1).any():
        min_evnum_q1 = int(df.loc[df["PERIOD"] == 1, "EVENTNUM"].min())
        q1_row = _build_start_of_period_row(cols, game_id, 1, min_evnum_q1 - 1)
        df = pd.concat([pd.DataFrame([q1_row], columns=cols), df], ignore_index=True)
        existing_periods.add(1)

    all_periods_in_game = set(df["PERIOD"].dropna().astype(int).unique())
    missing_periods = all_periods_in_game - existing_periods
    if not missing_periods or fetch_pbp_v3_fn is None:
        return df.reset_index(drop=True)

    df_v3 = fetch_pbp_v3_fn(game_id)
    if (
        df_v3 is None
        or df_v3.empty
        or "actionType" not in df_v3.columns
        or "subType" not in df_v3.columns
    ):
        return df.reset_index(drop=True)

    mask = (
        df_v3["actionType"].astype(str).str.lower().eq("period")
        & df_v3["subType"].astype(str).str.lower().eq("start")
    )
    starts = df_v3.loc[mask]
    if starts.empty:
        return df.reset_index(drop=True)

    period_to_eventnum: Dict[int, int] = {}
    for _, row in starts.iterrows():
        try:
            period = int(row.get("period", 0) or 0)
        except (TypeError, ValueError):
            continue
        if period <= 0 or period in existing_periods or period in period_to_eventnum:
            continue

        try:
            period_to_eventnum[period] = int(row.get("actionNumber"))
        except (TypeError, ValueError):
            continue

    for period in sorted(period_to_eventnum):
        start_row = _build_start_of_period_row(
            cols,
            game_id,
            period,
            period_to_eventnum[period],
        )
        df = _insert_row_before_period(df, start_row, period)

    return df.reset_index(drop=True)


def reorder_with_v3(
    game_df: pd.DataFrame,
    game_id: str,
    fetch_pbp_v3_fn: FetchPbpV3Fn,
) -> pd.DataFrame:
    """
    Preserve the existing row order after v3-backed dedupe/period patching.

    Historical offline feeds often encode the useful chronology in raw row order,
    even when EVENTNUM/actionNumber disagree within same-clock clusters. Forcing a
    numeric v3 reorder can turn workable sequences such as:

      MISS -> REBOUND -> MADE TIP/PUTBACK
      MISS FT1 -> REBOUND -> SUBS -> FT2

    back into the broken event-number order and trigger thousands of fallback
    rebound deletions. The offline path now treats v3 as a source for dedupe and
    start-of-period markers, not as a chronology rewrite.
    """
    return _ensure_eventnum_int(game_df).reset_index(drop=True)
