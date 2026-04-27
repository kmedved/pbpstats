# %% [markdown]
# # Scrape Period Starters (v6)
# Single-call V6 resolver for periods 2+ using the `gamerotation` endpoint.
#
# Rule:
# 1. Fetch `gamerotation` once per game.
# 2. For each expected period boundary from local PBP, identify players whose
#    stint is active at the boundary:
#       IN_TIME_REAL <= period_start_tenths < OUT_TIME_REAL
# 3. Deduplicate by player id and require exactly 5 active players per team.
# 4. Save one resolved row per period, or unresolved / failure rows otherwise.

from __future__ import annotations

import random
import time
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

import pandas as pd
import requests
from tqdm.auto import tqdm

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# %% Config
DATA_DIR = Path(".")
RESOLVER_VERSION = "v6"
RESOLVED_PATH = DATA_DIR / f"period_starters_{RESOLVER_VERSION}.parquet"
UNRESOLVED_PATH = DATA_DIR / f"period_starters_unresolved_{RESOLVER_VERSION}.parquet"
FAILURES_PATH = DATA_DIR / f"period_starters_failures_{RESOLVER_VERSION}.parquet"
STINTS_PATH = DATA_DIR / f"gamerotation_stints_{RESOLVER_VERSION}.parquet"

MAX_WORKERS = 20
CHECKPOINT_INTERVAL = 100
FETCH_RETRIES = 4
FETCH_TIMEOUT_SECONDS = 45
RETRYABLE_STATUSES = {403, 429, 500, 502, 503, 504}
RESOLVER_MODE = "gamerotation_active_at_boundary"
PERIODIC_BACKOFF_EVERY_SECONDS = 15 * 60
PERIODIC_BACKOFF_DURATION_SECONDS = 60

# Set to a year (e.g. 2024 for 2024-25) to scrape one season, or None for all.
SEASON = None

# Proxy: None, "apify", or "dataimpulse"
PROXY = "dataimpulse"
URL = "https://stats.nba.com/stats/gamerotation"
API_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}

SCRAPE_START_MONOTONIC = None
SCRAPE_CLOCK_LOCK = Lock()


def get_proxies():
    if PROXY is None:
        return None
    if PROXY == "apify":
        proxy_groups = {"BUYPROXIES94952": 27}
        group = random.choice(list(proxy_groups.keys()))
        session_id = random.randint(0, proxy_groups[group] - 1)
        url = f"http://groups-{group},session-{session_id}:9rZjqKh9aXDRWkf6aA3pFRStN@proxy.apify.com:8000"
        return {"http": url, "https": url}
    if PROXY == "dataimpulse":
        session_id = f"nba_{random.randint(10000, 99999)}"
        user = f"a0feb795d77dbcf7861c_session-{session_id}"
        url = f"http://{user}:5fe46ff800ae77f1@gw.dataimpulse.com:823"
        return {"http": url, "https": url}
    raise ValueError(f"Unknown PROXY: {PROXY!r}. Use None, 'apify', or 'dataimpulse'")


def ensure_scrape_clock_started():
    global SCRAPE_START_MONOTONIC
    if SCRAPE_START_MONOTONIC is None:
        with SCRAPE_CLOCK_LOCK:
            if SCRAPE_START_MONOTONIC is None:
                SCRAPE_START_MONOTONIC = time.monotonic()
    return SCRAPE_START_MONOTONIC


def wait_for_periodic_backoff():
    start = ensure_scrape_clock_started()
    elapsed = time.monotonic() - start
    cycle_length = PERIODIC_BACKOFF_EVERY_SECONDS + PERIODIC_BACKOFF_DURATION_SECONDS
    cycle_position = elapsed % cycle_length
    if cycle_position >= PERIODIC_BACKOFF_EVERY_SECONDS:
        sleep_for = cycle_length - cycle_position
        time.sleep(sleep_for)


def period_start_tenths(period: int) -> int:
    if period == 1:
        return 0
    if period <= 4:
        return 7200 * (period - 1)
    return 4 * 7200 + 3000 * (period - 5)


def parse_result_set(result_set: dict) -> pd.DataFrame:
    return pd.DataFrame(result_set["rowSet"], columns=result_set["headers"])


def load_tricode_lookup():
    lookup = {}
    for name in ["period_starters_v5.parquet", "period_starters_v4.parquet"]:
        path = DATA_DIR / name
        if not path.exists():
            continue
        try:
            df = pd.read_parquet(
                path,
                columns=["game_id", "away_team_id", "away_tricode", "home_team_id", "home_tricode"],
            )
        except Exception:
            continue
        for row in df.itertuples(index=False):
            game_id = str(row.game_id).zfill(10)
            if pd.notna(row.away_team_id) and pd.notna(row.away_tricode):
                lookup.setdefault((game_id, int(row.away_team_id)), row.away_tricode)
            if pd.notna(row.home_team_id) and pd.notna(row.home_tricode):
                lookup.setdefault((game_id, int(row.home_team_id)), row.home_tricode)
    return lookup


def fetch_gamerotation(game_id: str):
    params = {"GameID": game_id, "LeagueID": "00"}
    last_error = None
    for attempt in range(FETCH_RETRIES):
        wait_for_periodic_backoff()
        try:
            resp = requests.get(
                URL,
                params=params,
                headers=API_HEADERS,
                proxies=get_proxies(),
                verify=False,
                timeout=FETCH_TIMEOUT_SECONDS,
            )
            if resp.status_code == 200:
                return resp.json(), None
            last_error = f"HTTP {resp.status_code}"
            if resp.status_code in RETRYABLE_STATUSES:
                time.sleep(1.5 * (attempt + 1))
                continue
            return None, last_error
        except requests.exceptions.RequestException as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            time.sleep(1.5 * (attempt + 1))
    return None, last_error


def normalize_gamerotation(game_id: str, data: dict):
    result_sets = data.get("resultSets", [])
    if len(result_sets) < 2:
        return None, "Expected at least 2 resultSets"

    away_df = parse_result_set(result_sets[0]).copy()
    home_df = parse_result_set(result_sets[1]).copy()
    away_df["_side"] = "away"
    home_df["_side"] = "home"
    all_stints = pd.concat([away_df, home_df], ignore_index=True)

    column_map = {
        "in_time_real": None,
        "out_time_real": None,
        "person_id": None,
        "team_id": None,
        "player_first": None,
        "player_last": None,
        "team_city": None,
        "team_name": None,
        "player_pts": None,
        "pt_diff": None,
        "usg_pct": None,
    }
    for column in all_stints.columns:
        upper = column.upper()
        if "IN_TIME_REAL" in upper:
            column_map["in_time_real"] = column
        elif "OUT_TIME_REAL" in upper:
            column_map["out_time_real"] = column
        elif "PERSON_ID" in upper:
            column_map["person_id"] = column
        elif "TEAM_ID" in upper:
            column_map["team_id"] = column
        elif "PLAYER_FIRST" in upper:
            column_map["player_first"] = column
        elif "PLAYER_LAST" in upper:
            column_map["player_last"] = column
        elif "TEAM_CITY" in upper:
            column_map["team_city"] = column
        elif "TEAM_NAME" in upper:
            column_map["team_name"] = column
        elif upper == "PLAYER_PTS":
            column_map["player_pts"] = column
        elif upper == "PT_DIFF":
            column_map["pt_diff"] = column
        elif upper == "USG_PCT":
            column_map["usg_pct"] = column

    required = ["in_time_real", "out_time_real", "person_id", "team_id"]
    missing = [key for key in required if column_map[key] is None]
    if missing:
        return None, f"Missing required columns: {missing}"

    all_stints[column_map["in_time_real"]] = pd.to_numeric(
        all_stints[column_map["in_time_real"]],
        errors="coerce",
    )
    all_stints[column_map["out_time_real"]] = pd.to_numeric(
        all_stints[column_map["out_time_real"]],
        errors="coerce",
    )
    all_stints[column_map["person_id"]] = pd.to_numeric(
        all_stints[column_map["person_id"]],
        errors="coerce",
    )
    all_stints[column_map["team_id"]] = pd.to_numeric(
        all_stints[column_map["team_id"]],
        errors="coerce",
    )
    if column_map["player_pts"] is not None:
        all_stints[column_map["player_pts"]] = pd.to_numeric(
            all_stints[column_map["player_pts"]],
            errors="coerce",
        )
    if column_map["pt_diff"] is not None:
        all_stints[column_map["pt_diff"]] = pd.to_numeric(
            all_stints[column_map["pt_diff"]],
            errors="coerce",
        )
    if column_map["usg_pct"] is not None:
        all_stints[column_map["usg_pct"]] = pd.to_numeric(
            all_stints[column_map["usg_pct"]],
            errors="coerce",
        )

    return (all_stints, column_map), None


def standardize_stints(all_stints, column_map):
    standardized = all_stints.copy()
    rename_map = {
        column_map["person_id"]: "PERSON_ID",
        column_map["team_id"]: "TEAM_ID",
        column_map["in_time_real"]: "IN_TIME_REAL",
        column_map["out_time_real"]: "OUT_TIME_REAL",
    }
    optional_renames = {
        "player_first": "PLAYER_FIRST",
        "player_last": "PLAYER_LAST",
        "team_city": "TEAM_CITY",
        "team_name": "TEAM_NAME",
        "player_pts": "PLAYER_PTS",
        "pt_diff": "PT_DIFF",
        "usg_pct": "USG_PCT",
    }
    for key, target in optional_renames.items():
        source = column_map.get(key)
        if source is not None:
            rename_map[source] = target
    standardized = standardized.rename(columns=rename_map)

    text_defaults = {
        "PLAYER_FIRST": "",
        "PLAYER_LAST": "",
        "TEAM_CITY": None,
        "TEAM_NAME": None,
    }
    numeric_defaults = {
        "PLAYER_PTS": None,
        "PT_DIFF": None,
        "USG_PCT": None,
    }
    for column, default in text_defaults.items():
        if column not in standardized.columns:
            standardized[column] = default
    for column, default in numeric_defaults.items():
        if column not in standardized.columns:
            standardized[column] = default

    return standardized


def sort_players(players):
    return sorted(players, key=lambda player: (player["name"], player["personId"]))


def build_player_record(game_id, row, column_map, tricode_lookup):
    person_id = row[column_map["person_id"]]
    team_id = row[column_map["team_id"]]
    first = ""
    last = ""
    if column_map["player_first"] is not None:
        first = str(row.get(column_map["player_first"], "") or "").strip()
    if column_map["player_last"] is not None:
        last = str(row.get(column_map["player_last"], "") or "").strip()
    name = f"{first} {last}".strip()
    team_city = None if column_map["team_city"] is None else row.get(column_map["team_city"])
    team_name = None if column_map["team_name"] is None else row.get(column_map["team_name"])
    return {
        "personId": int(person_id) if pd.notna(person_id) else None,
        "name": name,
        "team_id": int(team_id) if pd.notna(team_id) else None,
        "tricode": tricode_lookup.get((game_id, int(team_id))) if pd.notna(team_id) else None,
        "side": row["_side"],
        "team_city": team_city,
        "team_name": team_name,
        "in_time_real": None if pd.isna(row[column_map["in_time_real"]]) else int(row[column_map["in_time_real"]]),
        "out_time_real": None if pd.isna(row[column_map["out_time_real"]]) else int(row[column_map["out_time_real"]]),
    }


def resolve_period_from_stints(game_id, period, all_stints, column_map, tricode_lookup):
    pst = period_start_tenths(period)
    in_col = column_map["in_time_real"]
    out_col = column_map["out_time_real"]
    person_col = column_map["person_id"]

    active_mask = (all_stints[in_col] <= pst) & (pst < all_stints[out_col])
    active = all_stints[active_mask].copy()
    active = active.sort_values([in_col, out_col, person_col]).drop_duplicates(subset=[person_col], keep="last")

    away_rows = active[active["_side"] == "away"].copy()
    home_rows = active[active["_side"] == "home"].copy()

    away_players = [
        build_player_record(game_id, row, column_map, tricode_lookup)
        for _, row in away_rows.iterrows()
        if pd.notna(row[person_col])
    ]
    home_players = [
        build_player_record(game_id, row, column_map, tricode_lookup)
        for _, row in home_rows.iterrows()
        if pd.notna(row[person_col])
    ]
    away_players = sort_players(away_players)
    home_players = sort_players(home_players)

    return {
        "period": period,
        "period_start_tenths": pst,
        "active_rows": active,
        "away_players": away_players,
        "home_players": home_players,
        "away_active_count": len(away_players),
        "home_active_count": len(home_players),
        "exactly_5_per_team": len(away_players) == 5 and len(home_players) == 5,
        "total_stints_returned": len(all_stints),
        "active_total": len(active),
    }


def _build_resolved(game_id, resolved_period, now):
    away_players = resolved_period["away_players"]
    home_players = resolved_period["home_players"]
    away_team_id = away_players[0]["team_id"]
    home_team_id = home_players[0]["team_id"]
    away_tricode = away_players[0]["tricode"]
    home_tricode = home_players[0]["tricode"]
    away_team_city = away_players[0]["team_city"]
    away_team_name = away_players[0]["team_name"]
    home_team_city = home_players[0]["team_city"]
    home_team_name = home_players[0]["team_name"]
    pst = resolved_period["period_start_tenths"]
    return {
        "game_id": game_id,
        "period": resolved_period["period"],
        "away_team_id": away_team_id,
        "away_tricode": away_tricode,
        "home_team_id": home_team_id,
        "home_tricode": home_tricode,
        "away_player1": away_players[0]["personId"],
        "away_player2": away_players[1]["personId"],
        "away_player3": away_players[2]["personId"],
        "away_player4": away_players[3]["personId"],
        "away_player5": away_players[4]["personId"],
        "home_player1": home_players[0]["personId"],
        "home_player2": home_players[1]["personId"],
        "home_player3": home_players[2]["personId"],
        "home_player4": home_players[3]["personId"],
        "home_player5": home_players[4]["personId"],
        "start_range": pst,
        "end_range": None,
        "window_seconds": None,
        "requested_window_seconds": None,
        "first_event_elapsed": None,
        "first_nonzero_event_elapsed": None,
        "anchor_elapsed": None,
        "first_sub_elapsed": None,
        "window_capped_by_sub": None,
        "total_returned": resolved_period["active_total"],
        "away_gap": None,
        "home_gap": None,
        "min_gap": None,
        "gr_period_start_tenths": pst,
        "gr_total_stints_returned": resolved_period["total_stints_returned"],
        "gr_away_active_count": resolved_period["away_active_count"],
        "gr_home_active_count": resolved_period["home_active_count"],
        "gr_away_team_city": away_team_city,
        "gr_away_team_name": away_team_name,
        "gr_home_team_city": home_team_city,
        "gr_home_team_name": home_team_name,
        "resolver_mode": RESOLVER_MODE,
        "resolved": True,
        "scrape_ts": now,
    }


def _build_unresolved(game_id, resolved_period, reason, now):
    pst = resolved_period["period_start_tenths"]
    rows = []
    active = resolved_period["active_rows"]
    if len(active) == 0:
        rows.append(
            {
                "game_id": game_id,
                "period": resolved_period["period"],
                "personId": None,
                "name": None,
                "team_id": None,
                "tricode": None,
                "side": None,
                "in_time_real": None,
                "out_time_real": None,
                "active_at_boundary": False,
                "period_start_tenths": pst,
                "away_active_count": resolved_period["away_active_count"],
                "home_active_count": resolved_period["home_active_count"],
                "total_returned": resolved_period["active_total"],
                "total_stints_returned": resolved_period["total_stints_returned"],
                "reason": reason,
                "resolver_mode": RESOLVER_MODE,
                "scrape_ts": now,
            }
        )
        return rows

    for _, row in active.iterrows():
        player = {
            "game_id": game_id,
            "period": resolved_period["period"],
            "personId": int(row["PERSON_ID"]) if pd.notna(row["PERSON_ID"]) else None,
            "name": f"{str(row.get('PLAYER_FIRST', '') or '').strip()} {str(row.get('PLAYER_LAST', '') or '').strip()}".strip(),
            "team_id": int(row["TEAM_ID"]) if pd.notna(row["TEAM_ID"]) else None,
            "tricode": None,
            "side": row["_side"],
            "in_time_real": None if pd.isna(row["IN_TIME_REAL"]) else int(row["IN_TIME_REAL"]),
            "out_time_real": None if pd.isna(row["OUT_TIME_REAL"]) else int(row["OUT_TIME_REAL"]),
            "active_at_boundary": True,
            "period_start_tenths": pst,
            "away_active_count": resolved_period["away_active_count"],
            "home_active_count": resolved_period["home_active_count"],
            "total_returned": resolved_period["active_total"],
            "total_stints_returned": resolved_period["total_stints_returned"],
            "reason": reason,
            "resolver_mode": RESOLVER_MODE,
            "scrape_ts": now,
        }
        rows.append(player)
    return rows


def _build_stint_rows(game_id, all_stints, tricode_lookup, now):
    rows = []
    for _, row in all_stints.iterrows():
        team_id = int(row["TEAM_ID"]) if pd.notna(row["TEAM_ID"]) else None
        person_id = int(row["PERSON_ID"]) if pd.notna(row["PERSON_ID"]) else None
        first = str(row.get("PLAYER_FIRST", "") or "").strip()
        last = str(row.get("PLAYER_LAST", "") or "").strip()
        name = f"{first} {last}".strip()
        in_time_real = None if pd.isna(row["IN_TIME_REAL"]) else int(row["IN_TIME_REAL"])
        out_time_real = None if pd.isna(row["OUT_TIME_REAL"]) else int(row["OUT_TIME_REAL"])
        stint_tenths = None
        if in_time_real is not None and out_time_real is not None:
            stint_tenths = out_time_real - in_time_real
        rows.append(
            {
                "game_id": game_id,
                "side": row["_side"],
                "team_id": team_id,
                "tricode": tricode_lookup.get((game_id, team_id)) if team_id is not None else None,
                "team_city": row.get("TEAM_CITY"),
                "team_name": row.get("TEAM_NAME"),
                "personId": person_id,
                "player_first": first,
                "player_last": last,
                "name": name,
                "in_time_real": in_time_real,
                "out_time_real": out_time_real,
                "stint_tenths": stint_tenths,
                "player_pts": None if pd.isna(row.get("PLAYER_PTS")) else float(row.get("PLAYER_PTS")),
                "pt_diff": None if pd.isna(row.get("PT_DIFF")) else float(row.get("PT_DIFF")),
                "usg_pct": None if pd.isna(row.get("USG_PCT")) else float(row.get("USG_PCT")),
                "scrape_ts": now,
            }
        )
    return rows


def scrape_game(game_id, periods, tricode_lookup):
    now = datetime.now(timezone.utc).isoformat()
    data, err = fetch_gamerotation(game_id)
    if err is not None:
        failure_rows = [
            {
                "game_id": game_id,
                "period": period,
                "error": err,
                "scrape_ts": now,
            }
            for period in periods
        ]
        return [], [], failure_rows, []

    normalized, norm_err = normalize_gamerotation(game_id, data)
    if norm_err is not None:
        failure_rows = [
            {
                "game_id": game_id,
                "period": period,
                "error": norm_err,
                "scrape_ts": now,
            }
            for period in periods
        ]
        return [], [], failure_rows, []

    all_stints, column_map = normalized
    all_stints = standardize_stints(all_stints, column_map)
    column_map = {
        "person_id": "PERSON_ID",
        "team_id": "TEAM_ID",
        "in_time_real": "IN_TIME_REAL",
        "out_time_real": "OUT_TIME_REAL",
        "player_first": "PLAYER_FIRST",
        "player_last": "PLAYER_LAST",
        "team_city": "TEAM_CITY",
        "team_name": "TEAM_NAME",
    }

    resolved_rows = []
    unresolved_rows = []
    stint_rows = _build_stint_rows(game_id, all_stints, tricode_lookup, now)
    for period in periods:
        resolved_period = resolve_period_from_stints(game_id, period, all_stints, column_map, tricode_lookup)
        if resolved_period["exactly_5_per_team"]:
            resolved_rows.append(_build_resolved(game_id, resolved_period, now))
        else:
            unresolved_rows.extend(
                _build_unresolved(
                    game_id,
                    resolved_period,
                    reason=f"active_counts: away={resolved_period['away_active_count']}, home={resolved_period['home_active_count']}",
                    now=now,
                )
            )
    return resolved_rows, unresolved_rows, [], stint_rows


RESOLVED_COLS = [
    "game_id",
    "period",
    "away_team_id",
    "away_tricode",
    "home_team_id",
    "home_tricode",
    "away_player1",
    "away_player2",
    "away_player3",
    "away_player4",
    "away_player5",
    "home_player1",
    "home_player2",
    "home_player3",
    "home_player4",
    "home_player5",
    "start_range",
    "end_range",
    "window_seconds",
    "requested_window_seconds",
    "first_event_elapsed",
    "first_nonzero_event_elapsed",
    "anchor_elapsed",
    "first_sub_elapsed",
    "window_capped_by_sub",
    "total_returned",
    "away_gap",
    "home_gap",
    "min_gap",
    "gr_period_start_tenths",
    "gr_total_stints_returned",
    "gr_away_active_count",
    "gr_home_active_count",
    "gr_away_team_city",
    "gr_away_team_name",
    "gr_home_team_city",
    "gr_home_team_name",
    "resolver_mode",
    "resolved",
    "scrape_ts",
]
UNRESOLVED_COLS = [
    "game_id",
    "period",
    "personId",
    "name",
    "team_id",
    "tricode",
    "side",
    "in_time_real",
    "out_time_real",
    "active_at_boundary",
    "period_start_tenths",
    "away_active_count",
    "home_active_count",
    "total_returned",
    "total_stints_returned",
    "reason",
    "resolver_mode",
    "scrape_ts",
]
FAILURE_COLS = ["game_id", "period", "error", "scrape_ts"]
STINT_COLS = [
    "game_id",
    "side",
    "team_id",
    "tricode",
    "team_city",
    "team_name",
    "personId",
    "player_first",
    "player_last",
    "name",
    "in_time_real",
    "out_time_real",
    "stint_tenths",
    "player_pts",
    "pt_diff",
    "usg_pct",
    "scrape_ts",
]


def load_parquet_or_empty(path, cols):
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame(columns=cols)


def save_checkpoint(
    resolved_df,
    new_resolved,
    unresolved_df,
    new_unresolved,
    failures_df,
    new_failures,
    stints_df,
    new_stints,
):
    if new_resolved:
        resolved_df = pd.concat([resolved_df, pd.DataFrame(new_resolved)], ignore_index=True)
        resolved_df = resolved_df.drop_duplicates(subset=["game_id", "period"], keep="last")
        resolved_df.to_parquet(RESOLVED_PATH, index=False)
        resolved_keys = set(zip(resolved_df["game_id"], resolved_df["period"]))
        if len(unresolved_df):
            mask = [
                (game_id, period) not in resolved_keys
                for game_id, period in zip(unresolved_df["game_id"], unresolved_df["period"])
            ]
            unresolved_df = unresolved_df[mask].reset_index(drop=True)
            unresolved_df.to_parquet(UNRESOLVED_PATH, index=False)
        if len(failures_df):
            mask = [
                (game_id, period) not in resolved_keys
                for game_id, period in zip(failures_df["game_id"], failures_df["period"])
            ]
            failures_df = failures_df[mask].reset_index(drop=True)
            failures_df.to_parquet(FAILURES_PATH, index=False)
    if new_unresolved:
        new_unresolved_df = pd.DataFrame(new_unresolved)
        new_keys = set(zip(new_unresolved_df["game_id"], new_unresolved_df["period"]))
        if len(unresolved_df):
            mask = [
                (game_id, period) not in new_keys
                for game_id, period in zip(unresolved_df["game_id"], unresolved_df["period"])
            ]
            unresolved_df = unresolved_df[mask].reset_index(drop=True)
        unresolved_df = pd.concat([unresolved_df, new_unresolved_df], ignore_index=True)
        unresolved_df.to_parquet(UNRESOLVED_PATH, index=False)
    if new_failures:
        failures_df = pd.concat([failures_df, pd.DataFrame(new_failures)], ignore_index=True)
        failures_df = failures_df.drop_duplicates(subset=["game_id", "period"], keep="last")
        failures_df.to_parquet(FAILURES_PATH, index=False)
    if new_stints:
        stints_df = pd.concat([stints_df, pd.DataFrame(new_stints)], ignore_index=True)
        stints_df = stints_df.drop_duplicates(
            subset=["game_id", "side", "team_id", "personId", "in_time_real", "out_time_real"],
            keep="last",
        )
        stints_df.to_parquet(STINTS_PATH, index=False)
    return resolved_df, unresolved_df, failures_df, stints_df


def get_already_done():
    done = set()
    if RESOLVED_PATH.exists():
        done_df = pd.read_parquet(RESOLVED_PATH, columns=["game_id", "period"])
        done.update(zip(done_df["game_id"], done_df["period"]))
    return done


def build_jobs():
    pbp = pd.read_parquet("playbyplayv2.parq", columns=["GAME_ID", "PERIOD"])
    pbp["PERIOD"] = pd.to_numeric(pbp["PERIOD"], errors="coerce").astype("Int64")
    pbp = pbp[pbp["PERIOD"] >= 2].copy()

    if SEASON is not None:
        season_code = str(SEASON % 100).zfill(2)
        pbp["_gid"] = pbp["GAME_ID"].apply(lambda value: str(int(value)).zfill(10) if pd.notna(value) else "")
        pbp = pbp[pbp["_gid"].str[3:5] == season_code].drop(columns=["_gid"])
        print(f"Filtering to season {SEASON}-{SEASON + 1} (code={season_code})")

    period_keys = pbp[["GAME_ID", "PERIOD"]].drop_duplicates().copy()
    period_keys["game_id"] = period_keys["GAME_ID"].apply(lambda value: str(int(value)).zfill(10))

    jobs = (
        period_keys.groupby("game_id")["PERIOD"]
        .apply(lambda values: sorted(int(period) for period in values if pd.notna(period)))
        .reset_index(name="periods")
    )
    jobs["num_periods"] = jobs["periods"].map(len)

    def season_year(game_id):
        code = int(game_id[3:5])
        return code + 1900 if code >= 96 else code + 2000

    jobs["_sort_key"] = jobs["game_id"].map(season_year)
    jobs = jobs.sort_values(["_sort_key", "game_id"]).drop(columns=["_sort_key"]).reset_index(drop=True)
    return jobs


def main():
    global SCRAPE_START_MONOTONIC
    SCRAPE_START_MONOTONIC = time.monotonic()
    jobs = build_jobs()
    tricode_lookup = load_tricode_lookup()

    total_periods = int(jobs["num_periods"].sum()) if len(jobs) else 0
    print(f"Total games: {len(jobs)}")
    print(f"Total expected periods: {total_periods}")
    if len(jobs):
        print(f"\nPeriods-per-game stats:\n{jobs['num_periods'].describe()}")

    done = get_already_done()
    remaining = []
    for row in jobs.itertuples(index=False):
        pending_periods = [period for period in row.periods if (row.game_id, period) not in done]
        if pending_periods:
            remaining.append((row.game_id, pending_periods))
    remaining_periods = sum(len(periods) for _, periods in remaining)
    print(f"Already done: {len(done)}, remaining periods: {remaining_periods}, remaining games: {len(remaining)}")

    resolved_df = load_parquet_or_empty(RESOLVED_PATH, RESOLVED_COLS)
    unresolved_df = load_parquet_or_empty(UNRESOLVED_PATH, UNRESOLVED_COLS)
    failures_df = load_parquet_or_empty(FAILURES_PATH, FAILURE_COLS)
    stints_df = load_parquet_or_empty(STINTS_PATH, STINT_COLS)

    new_resolved = []
    new_unresolved = []
    new_failures = []
    new_stints = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(scrape_game, game_id, periods, tricode_lookup): (game_id, periods)
            for game_id, periods in remaining
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="Scraping"):
            game_id, periods = futures[future]
            now = datetime.now(timezone.utc).isoformat()
            try:
                resolved_rows, unresolved_rows, failure_rows, stint_rows = future.result()
            except Exception as exc:
                failure_rows = [
                    {
                        "game_id": game_id,
                        "period": period,
                        "error": str(exc),
                        "scrape_ts": now,
                    }
                    for period in periods
                ]
                resolved_rows = []
                unresolved_rows = []
                stint_rows = []

            if resolved_rows:
                new_resolved.extend(resolved_rows)
            if unresolved_rows:
                new_unresolved.extend(unresolved_rows)
            if failure_rows:
                new_failures.extend(failure_rows)
            if stint_rows:
                new_stints.extend(stint_rows)

            completed += 1
            if completed % CHECKPOINT_INTERVAL == 0:
                resolved_df, unresolved_df, failures_df, stints_df = save_checkpoint(
                    resolved_df,
                    new_resolved,
                    unresolved_df,
                    new_unresolved,
                    failures_df,
                    new_failures,
                    stints_df,
                    new_stints,
                )
                new_resolved, new_unresolved, new_failures, new_stints = [], [], [], []

    resolved_df, unresolved_df, failures_df, stints_df = save_checkpoint(
        resolved_df,
        new_resolved,
        unresolved_df,
        new_unresolved,
        failures_df,
        new_failures,
        stints_df,
        new_stints,
    )

    print(f"\n{'=' * 50}")
    print(f"Resolved:   {len(resolved_df)} periods")
    if UNRESOLVED_PATH.exists():
        unresolved = pd.read_parquet(UNRESOLVED_PATH)
        unresolved_periods = unresolved.drop_duplicates(["game_id", "period"]).shape[0]
        print(f"Unresolved: {unresolved_periods} periods ({len(unresolved)} player rows)")
    else:
        print("Unresolved: 0")
    print(f"Failures:   {len(failures_df)} periods")

    if len(resolved_df):
        print(f"\nResolver mode distribution:")
        print(resolved_df["resolver_mode"].value_counts().to_string())
        print(f"\nActive-row count stats:")
        print(resolved_df["total_returned"].describe())
        print(f"\nTotal-stints-returned stats:")
        print(resolved_df["gr_total_stints_returned"].describe())
    print(f"\nCached stints: {len(stints_df)} rows")


if __name__ == "__main__":
    main()
