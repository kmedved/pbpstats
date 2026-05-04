from __future__ import annotations

import re
import warnings
from typing import Callable, Dict, List

import numpy as np
import pandas as pd

import pbpstats
from pbpstats.game_id import normalize_game_id, uses_wnba_twenty_minute_halves
from pbpstats.offline.row_overrides import PBP_ROW_OVERRIDE_ACTION_COLUMN

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


def _ensure_period_int(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure PERIOD is an int column before period comparisons and insertions.
    """
    result = df.copy()
    result["PERIOD"] = pd.to_numeric(result["PERIOD"], errors="coerce")
    result = result.dropna(subset=["PERIOD"])
    result["PERIOD"] = result["PERIOD"].astype(int)
    return result


def _ensure_eventmsgtype_int(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure EVENTMSGTYPE is an int column before event-type comparisons.
    """
    result = df.copy()
    result["EVENTMSGTYPE"] = pd.to_numeric(result["EVENTMSGTYPE"], errors="coerce")
    result = result.dropna(subset=["EVENTMSGTYPE"])
    result["EVENTMSGTYPE"] = result["EVENTMSGTYPE"].astype(int)
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

    explicit_override_mask = pd.Series(False, index=df.index)
    if PBP_ROW_OVERRIDE_ACTION_COLUMN in df.columns:
        explicit_override_mask = (
            df[PBP_ROW_OVERRIDE_ACTION_COLUMN].fillna("").astype(str).str.strip() != ""
        )

    df = df[df["EVENTNUM"].isin(valid_nums) | explicit_override_mask].copy()
    df = df.drop_duplicates(subset=["GAME_ID", "EVENTNUM"], keep="first")
    return df


def _normalize_game_id_for_inference(
    game_id: object | None,
    league: str | None = None,
) -> str:
    return normalize_game_id(game_id, league=league)


def _infer_league_from_game_id(game_id: object | None) -> str | None:
    raw_game_id = _normalize_game_id_for_inference(game_id)
    if raw_game_id.startswith(pbpstats.NBA_GAME_ID_PREFIX):
        return pbpstats.NBA_STRING
    if raw_game_id.startswith(pbpstats.WNBA_GAME_ID_PREFIX):
        return pbpstats.WNBA_STRING
    if raw_game_id.startswith(pbpstats.G_LEAGUE_GAME_ID_PREFIX):
        return pbpstats.G_LEAGUE_STRING
    return None


def _infer_season_year_from_game_id(game_id: object | None) -> int | None:
    raw_game_id = _normalize_game_id_for_inference(game_id)
    if len(raw_game_id) < 5:
        return None
    try:
        suffix = int(raw_game_id[3:5])
    except ValueError:
        return None
    return 2000 + suffix if suffix < 90 else 1900 + suffix


def _uses_wnba_twenty_minute_halves(
    league: str | None,
    season_year: int | None,
) -> bool:
    return uses_wnba_twenty_minute_halves(league, season_year)


def _period_start_clock(
    period: int,
    league: str | None = None,
    season_year: int | None = None,
) -> str:
    period = int(period)
    if _uses_wnba_twenty_minute_halves(league, season_year):
        return "20:00" if period <= 2 else "5:00"
    if period > 4:
        return "5:00"
    if league == pbpstats.WNBA_STRING:
        return "10:00"
    return "12:00"


def _build_start_of_period_row(
    cols: List[str],
    game_id: str,
    period: int,
    eventnum: int,
    league: str | None = None,
    season_year: int | None = None,
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
        row["PCTIMESTRING"] = _period_start_clock(
            period,
            league=league,
            season_year=season_year,
        )

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
        return pd.concat(
            [df, pd.DataFrame([row], columns=df.columns)], ignore_index=True
        )

    insert_at = int(df.index[period_mask][0])
    return pd.concat(
        [
            df.iloc[:insert_at],
            pd.DataFrame([row], columns=df.columns),
            df.iloc[insert_at:],
        ],
        ignore_index=True,
    )


def _move_existing_period_start_before_initial_live_action(
    df: pd.DataFrame,
    league: str | None = None,
    season_year: int | None = None,
) -> pd.DataFrame:
    """
    Move an existing StartOfPeriod marker ahead of malformed live action at the
    exact period-start clock, without changing other intra-period ordering.

    Some feeds can place a real 12:00 live action before the StartOfPeriod row.
    Leaving that shape intact can double-count the period-opening interval:
    both the pre-start live action and the first post-start action point back to
    a 12:00 predecessor.  Technical/foul clusters are deliberately excluded
    because those exact-start cases can be scorer-convention boundary disputes.
    """
    required_cols = {"PERIOD", "EVENTMSGTYPE", "PCTIMESTRING"}
    if not required_cols.issubset(df.columns):
        return df.reset_index(drop=True)

    result = df.reset_index(drop=True)
    live_action_types = {1, 2, 4, 5, 10}

    while True:
        period_values = pd.to_numeric(result["PERIOD"], errors="coerce")
        event_types = pd.to_numeric(result["EVENTMSGTYPE"], errors="coerce")
        moved = False

        for period in sorted(period_values.dropna().astype(int).unique()):
            period_indices = result.index[period_values == period].tolist()
            if not period_indices:
                continue
            start_indices = [
                idx for idx in period_indices if event_types.loc[idx] == 12
            ]
            if not start_indices:
                continue

            first_period_idx = period_indices[0]
            first_start_idx = start_indices[0]
            if first_start_idx == first_period_idx:
                continue

            prior_period_indices = [
                idx for idx in period_indices if idx < first_start_idx
            ]
            start_clock = _period_start_clock(
                period,
                league=league,
                season_year=season_year,
            )
            prior_clocks = (
                result.loc[prior_period_indices, "PCTIMESTRING"].astype(str).str.strip()
            )
            if not prior_clocks.eq(start_clock).all():
                continue

            prior_types = event_types.loc[prior_period_indices].dropna().astype(int)
            if not prior_types.isin(live_action_types).any():
                continue

            order = result.index.tolist()
            order.remove(first_start_idx)
            insert_at = order.index(first_period_idx)
            order.insert(insert_at, first_start_idx)
            result = result.loc[order].reset_index(drop=True)
            moved = True
            break

        if not moved:
            return result


def patch_start_of_periods(
    game_df: pd.DataFrame,
    game_id: object,
    fetch_pbp_v3_fn: FetchPbpV3Fn | None = None,
    league: str | None = None,
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

    df = _ensure_eventmsgtype_int(_ensure_period_int(_ensure_eventnum_int(df)))
    resolved_game_id = _normalize_game_id_for_inference(game_id, league=league)
    resolved_league = league or _infer_league_from_game_id(resolved_game_id)
    resolved_season_year = _infer_season_year_from_game_id(resolved_game_id)
    if "GAME_ID" in df.columns:
        df["GAME_ID"] = resolved_game_id
    cols = list(df.columns)

    existing_periods = set(
        df.loc[df["EVENTMSGTYPE"] == 12, "PERIOD"].dropna().astype(int).tolist()
    )

    if 1 not in existing_periods and (df["PERIOD"] == 1).any():
        min_evnum_q1 = int(df.loc[df["PERIOD"] == 1, "EVENTNUM"].min())
        q1_row = _build_start_of_period_row(
            cols,
            resolved_game_id,
            1,
            min_evnum_q1 - 1,
            league=resolved_league,
            season_year=resolved_season_year,
        )
        df = pd.concat([pd.DataFrame([q1_row], columns=cols), df], ignore_index=True)
        existing_periods.add(1)

    all_periods_in_game = set(df["PERIOD"].dropna().astype(int).unique())
    missing_periods = all_periods_in_game - existing_periods
    if not missing_periods or fetch_pbp_v3_fn is None:
        return _move_existing_period_start_before_initial_live_action(
            df,
            league=resolved_league,
            season_year=resolved_season_year,
        )

    df_v3 = fetch_pbp_v3_fn(resolved_game_id)
    if (
        df_v3 is None
        or df_v3.empty
        or "actionType" not in df_v3.columns
        or "subType" not in df_v3.columns
    ):
        return _move_existing_period_start_before_initial_live_action(
            df,
            league=resolved_league,
            season_year=resolved_season_year,
        )

    mask = df_v3["actionType"].astype(str).str.lower().eq("period") & df_v3[
        "subType"
    ].astype(str).str.lower().eq("start")
    starts = df_v3.loc[mask]
    if starts.empty:
        return _move_existing_period_start_before_initial_live_action(
            df,
            league=resolved_league,
            season_year=resolved_season_year,
        )

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
            resolved_game_id,
            period,
            period_to_eventnum[period],
            league=resolved_league,
            season_year=resolved_season_year,
        )
        df = _insert_row_before_period(df, start_row, period)

    return _move_existing_period_start_before_initial_live_action(
        df,
        league=resolved_league,
        season_year=resolved_season_year,
    )


def enrich_clocks_with_v3(
    game_df: pd.DataFrame,
    game_id: str,
    fetch_pbp_v3_fn: FetchPbpV3Fn | None = None,
) -> pd.DataFrame:
    """
    Replace V2 PCTIMESTRING with V3 sub-second clock precision where available.

    The V2 playbyplayv2 PCTIMESTRING truncates game clock times to whole seconds
    (e.g. 40.5s → "0:40").  The V3 playbyplayv3 clock field preserves tenths
    (e.g. "PT00M40.50S").  Using the truncated V2 values causes per-player
    seconds to be systematically ~0.5s short at substitution boundaries, which
    accumulates to ~1s per-player discrepancies vs the official boxscore.

    This enrichment replaces V2 PCTIMESTRING with V3 sub-second values for
    events where V3 has fractional seconds.  Events where V3 has whole seconds
    (identical to V2) are left untouched.  Row order and all other columns are
    preserved.
    """
    if fetch_pbp_v3_fn is None:
        return game_df

    df_v3 = fetch_pbp_v3_fn(game_id)
    if df_v3 is None or df_v3.empty:
        return game_df
    if "clock" not in df_v3.columns or "actionNumber" not in df_v3.columns:
        return game_df

    # Build map: actionNumber -> enriched PCTIMESTRING (only for fractional seconds)
    v3_clock_map: Dict[int, str] = {}
    for _, row in df_v3.iterrows():
        anum = row.get("actionNumber")
        clock = row.get("clock", "")
        if anum is None or not isinstance(clock, str):
            continue
        m = re.match(r"PT(\d+)M([\d.]+)S", clock)
        if not m:
            continue
        minutes = int(m.group(1))
        seconds = float(m.group(2))
        if seconds == int(seconds):
            continue  # whole seconds — V2 already matches
        v3_clock_map[int(anum)] = f"{minutes}:{seconds:05.2f}"

    if not v3_clock_map:
        return game_df

    df = game_df.copy()
    df["EVENTNUM"] = pd.to_numeric(df["EVENTNUM"], errors="coerce")
    mask = df["EVENTNUM"].isin(v3_clock_map)
    if not mask.any():
        return game_df

    df.loc[mask, "PCTIMESTRING"] = df.loc[mask, "EVENTNUM"].map(v3_clock_map)
    return df


def preserve_order_after_v3_repairs(game_df: pd.DataFrame) -> pd.DataFrame:
    """
    Canonical offline ordering contract after v3-backed repairs.

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


def reorder_with_v3(
    game_df: pd.DataFrame,
    game_id: str,
    fetch_pbp_v3_fn: FetchPbpV3Fn,
) -> pd.DataFrame:
    """
    Backwards-compatible alias for preserve_order_after_v3_repairs().

    The legacy name and extra arguments suggested that the offline pipeline
    performed a v3-driven chronology rewrite here. It does not.

    Deprecated: use preserve_order_after_v3_repairs() instead. The active
    offline path preserves repaired raw row order and only normalizes
    EVENTNUM to integers.
    """
    _ignored_game_id = game_id
    _ignored_fetch_pbp_v3_fn = fetch_pbp_v3_fn
    del _ignored_game_id, _ignored_fetch_pbp_v3_fn
    warnings.warn(
        "reorder_with_v3() is deprecated and does not perform a v3-driven "
        "chronology rewrite; use preserve_order_after_v3_repairs() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return preserve_order_after_v3_repairs(game_df)
