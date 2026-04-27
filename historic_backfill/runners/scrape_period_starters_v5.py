# %% [markdown]
# # Scrape Period Starters (v5)
# Single-call V5 resolver for periods 2+.
#
# Rule:
# 1. Anchor on the first event with elapsed > 0 and EVENTMSGTYPE not in {8, 10, 12, 13}.
# 2. Request a window of max(anchor + 5, 20) seconds.
# 3. If a substitution exists, cap the window at first_sub - 1.
# 4. Make exactly one RT2 boxscore call for the period.
# 5. Take the top 5 players per team by returned seconds and require a minimum
#    per-team gap between players 5 and 6.

import math
import random
import re
import time
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from tqdm.auto import tqdm

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# %% Config
DATA_DIR = Path(".")
RESOLVER_VERSION = "v5"
RESOLVED_PATH = DATA_DIR / f"period_starters_{RESOLVER_VERSION}.parquet"
UNRESOLVED_PATH = DATA_DIR / f"period_starters_unresolved_{RESOLVER_VERSION}.parquet"
FAILURES_PATH = DATA_DIR / f"period_starters_failures_{RESOLVER_VERSION}.parquet"

MAX_WORKERS = 20
CHECKPOINT_INTERVAL = 100
MIN_GAP_SECONDS = 3
TARGET_BUFFER_SECONDS = 5
MIN_WINDOW_SECONDS = 20
SUB_MARGIN = 1
RESOLVER_MODE = "single_call_anchor_plus5_floor20_cap_sub"

FIRST_EVENT_EXCLUDED_TYPES = {8, 12, 13}
ANCHOR_EXCLUDED_TYPES = {8, 10, 12, 13}

# Set to a year (e.g. 2024 for 2024-25) to scrape one season, or None for all.
SEASON = None

# Proxy: None, "apify", or "dataimpulse"
PROXY = "dataimpulse"
URL = "https://stats.nba.com/stats/boxscoretraditionalv3"
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


def parse_minutes_to_seconds(mins_str):
    if not mins_str:
        return 0.0
    match = re.match(r"PT(\d+)M([\d.]+)S", mins_str)
    if match:
        return int(match.group(1)) * 60 + float(match.group(2))
    match = re.match(r"(\d+):([\d.]+)", mins_str)
    if match:
        return int(match.group(1)) * 60 + float(match.group(2))
    return 0.0


def clock_to_elapsed(pctimestring, period):
    try:
        minutes, seconds = str(pctimestring).split(":")
        remaining = int(minutes) * 60 + int(seconds)
    except (TypeError, ValueError):
        return None
    return (720 if int(period) <= 4 else 300) - remaining


def period_start_tenths(period):
    if period == 1:
        return 0
    if period <= 4:
        return 7200 * (period - 1)
    return 4 * 7200 + 3000 * (period - 5)


def seconds_to_tenths(seconds):
    return int(round(float(seconds) * 10))


def compute_requested_window(anchor_elapsed):
    return max(float(anchor_elapsed) + TARGET_BUFFER_SECONDS, float(MIN_WINDOW_SECONDS))


def compute_window(anchor_elapsed, first_sub_elapsed):
    if anchor_elapsed is None or pd.isna(anchor_elapsed):
        return None, None, False
    requested_window = compute_requested_window(anchor_elapsed)
    window_end = requested_window
    if first_sub_elapsed is not None and not pd.isna(first_sub_elapsed):
        window_end = min(window_end, float(first_sub_elapsed) - SUB_MARGIN)
    capped = window_end < requested_window
    return requested_window, window_end, capped


def fetch_window(game_id, start_range, end_range):
    params = {
        "GameID": game_id,
        "StartPeriod": 0,
        "EndPeriod": 0,
        "RangeType": 2,
        "StartRange": start_range,
        "EndRange": end_range,
    }
    try:
        resp = requests.get(
            URL,
            params=params,
            headers=API_HEADERS,
            proxies=get_proxies(),
            verify=False,
            timeout=30,
        )
    except requests.exceptions.RequestException as exc:
        return None, f"Request error: {type(exc).__name__}: {exc}"
    if resp.status_code != 200:
        return None, f"HTTP {resp.status_code}"
    try:
        data = resp.json()
    except Exception as exc:
        return None, f"JSON parse: {exc}"
    boxscore = data.get("boxScoreTraditional")
    if boxscore is None:
        return None, "No boxScoreTraditional key"
    players = []
    for team_key in ["awayTeam", "homeTeam"]:
        team = boxscore.get(team_key, {})
        team_id = team.get("teamId")
        tricode = team.get("teamTricode", "")
        for player in team.get("players", []):
            minutes = player.get("statistics", {}).get("minutes", "")
            players.append(
                {
                    "personId": player.get("personId"),
                    "name": f"{player.get('firstName', '')} {player.get('familyName', '')}".strip(),
                    "team_id": team_id,
                    "tricode": tricode,
                    "side": "away" if team_key == "awayTeam" else "home",
                    "minutes": minutes,
                    "seconds": parse_minutes_to_seconds(minutes),
                }
            )
    return players, None


def select_starters(players):
    away = sorted(
        [player for player in players if player["side"] == "away"],
        key=lambda player: player["seconds"],
        reverse=True,
    )
    home = sorted(
        [player for player in players if player["side"] == "home"],
        key=lambda player: player["seconds"],
        reverse=True,
    )
    if len(away) < 5 or len(home) < 5:
        return None, f"insufficient_players: away={len(away)}, home={len(home)}"
    away_gap = math.inf if len(away) == 5 else away[4]["seconds"] - away[5]["seconds"]
    home_gap = math.inf if len(home) == 5 else home[4]["seconds"] - home[5]["seconds"]
    if away_gap < MIN_GAP_SECONDS or home_gap < MIN_GAP_SECONDS:
        return None, f"weak_gap: away_gap={away_gap:.1f}, home_gap={home_gap:.1f}"
    return (away[:5], home[:5], away_gap, home_gap), None


def _build_resolved(
    game_id,
    period,
    result,
    start_range,
    window_seconds,
    requested_window_seconds,
    first_event_elapsed,
    first_nonzero_event_elapsed,
    anchor_elapsed,
    first_sub_elapsed,
    window_capped_by_sub,
    total_returned,
    now,
):
    away5, home5, away_gap, home_gap = result
    return {
        "game_id": game_id,
        "period": period,
        "away_team_id": away5[0]["team_id"],
        "away_tricode": away5[0]["tricode"],
        "home_team_id": home5[0]["team_id"],
        "home_tricode": home5[0]["tricode"],
        "away_player1": away5[0]["personId"],
        "away_player2": away5[1]["personId"],
        "away_player3": away5[2]["personId"],
        "away_player4": away5[3]["personId"],
        "away_player5": away5[4]["personId"],
        "home_player1": home5[0]["personId"],
        "home_player2": home5[1]["personId"],
        "home_player3": home5[2]["personId"],
        "home_player4": home5[3]["personId"],
        "home_player5": home5[4]["personId"],
        "start_range": start_range,
        "end_range": start_range + seconds_to_tenths(window_seconds),
        "window_seconds": window_seconds,
        "requested_window_seconds": requested_window_seconds,
        "first_event_elapsed": first_event_elapsed,
        "first_nonzero_event_elapsed": first_nonzero_event_elapsed,
        "anchor_elapsed": anchor_elapsed,
        "first_sub_elapsed": first_sub_elapsed,
        "window_capped_by_sub": window_capped_by_sub,
        "total_returned": total_returned,
        "away_gap": away_gap if away_gap != math.inf else None,
        "home_gap": home_gap if home_gap != math.inf else None,
        "min_gap": min(away_gap, home_gap) if min(away_gap, home_gap) != math.inf else None,
        "resolver_mode": RESOLVER_MODE,
        "resolved": True,
        "scrape_ts": now,
    }


def _build_unresolved(
    game_id,
    period,
    players,
    window_seconds,
    requested_window_seconds,
    first_event_elapsed,
    first_nonzero_event_elapsed,
    anchor_elapsed,
    first_sub_elapsed,
    window_capped_by_sub,
    reason,
    now,
):
    rows = []
    player_rows = players if players else [None]
    total_returned = len(players) if players else 0
    for player in player_rows:
        rows.append(
            {
                "game_id": game_id,
                "period": period,
                "personId": None if player is None else player["personId"],
                "name": None if player is None else player["name"],
                "team_id": None if player is None else player["team_id"],
                "tricode": None if player is None else player["tricode"],
                "side": None if player is None else player["side"],
                "minutes": None if player is None else player["minutes"],
                "seconds": None if player is None else player["seconds"],
                "window_seconds": window_seconds,
                "requested_window_seconds": requested_window_seconds,
                "first_event_elapsed": first_event_elapsed,
                "first_nonzero_event_elapsed": first_nonzero_event_elapsed,
                "anchor_elapsed": anchor_elapsed,
                "first_sub_elapsed": first_sub_elapsed,
                "window_capped_by_sub": window_capped_by_sub,
                "total_returned": total_returned,
                "reason": reason,
                "resolver_mode": RESOLVER_MODE,
                "scrape_ts": now,
            }
        )
    return rows


def scrape_period(
    game_id,
    period,
    first_event_elapsed,
    first_nonzero_event_elapsed,
    anchor_elapsed,
    first_sub_elapsed,
):
    now = datetime.now(timezone.utc).isoformat()
    requested_window_seconds, window_seconds, window_capped_by_sub = compute_window(
        anchor_elapsed,
        first_sub_elapsed,
    )
    if anchor_elapsed is None:
        return (
            None,
            _build_unresolved(
                game_id=game_id,
                period=period,
                players=None,
                window_seconds=None,
                requested_window_seconds=None,
                first_event_elapsed=first_event_elapsed,
                first_nonzero_event_elapsed=first_nonzero_event_elapsed,
                anchor_elapsed=None,
                first_sub_elapsed=first_sub_elapsed,
                window_capped_by_sub=False,
                reason="no_anchor_event",
                now=now,
            ),
            None,
        )
    if window_seconds is None or window_seconds <= 0:
        return (
            None,
            _build_unresolved(
                game_id=game_id,
                period=period,
                players=None,
                window_seconds=window_seconds,
                requested_window_seconds=requested_window_seconds,
                first_event_elapsed=first_event_elapsed,
                first_nonzero_event_elapsed=first_nonzero_event_elapsed,
                anchor_elapsed=anchor_elapsed,
                first_sub_elapsed=first_sub_elapsed,
                window_capped_by_sub=window_capped_by_sub,
                reason="window_nonpositive_after_sub_cap",
                now=now,
            ),
            None,
        )

    start_range = period_start_tenths(period)
    end_range = start_range + seconds_to_tenths(window_seconds)

    players = None
    err = None
    for attempt in range(3):
        players, err = fetch_window(game_id, start_range, end_range)
        if err is None:
            break
        time.sleep(1.0 * (attempt + 1))
    if err is not None:
        return None, None, f"requested_window={requested_window_seconds}s window={window_seconds}s: {err}"
    if not players:
        return None, None, "Empty player list from API"

    result, reason = select_starters(players)
    if result is None:
        return (
            None,
            _build_unresolved(
                game_id=game_id,
                period=period,
                players=players,
                window_seconds=window_seconds,
                requested_window_seconds=requested_window_seconds,
                first_event_elapsed=first_event_elapsed,
                first_nonzero_event_elapsed=first_nonzero_event_elapsed,
                anchor_elapsed=anchor_elapsed,
                first_sub_elapsed=first_sub_elapsed,
                window_capped_by_sub=window_capped_by_sub,
                reason=reason,
                now=now,
            ),
            None,
        )

    return (
        _build_resolved(
            game_id=game_id,
            period=period,
            result=result,
            start_range=start_range,
            window_seconds=window_seconds,
            requested_window_seconds=requested_window_seconds,
            first_event_elapsed=first_event_elapsed,
            first_nonzero_event_elapsed=first_nonzero_event_elapsed,
            anchor_elapsed=anchor_elapsed,
            first_sub_elapsed=first_sub_elapsed,
            window_capped_by_sub=window_capped_by_sub,
            total_returned=len(players),
            now=now,
        ),
        None,
        None,
    )


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
    "minutes",
    "seconds",
    "window_seconds",
    "requested_window_seconds",
    "first_event_elapsed",
    "first_nonzero_event_elapsed",
    "anchor_elapsed",
    "first_sub_elapsed",
    "window_capped_by_sub",
    "total_returned",
    "reason",
    "resolver_mode",
    "scrape_ts",
]
FAILURE_COLS = ["game_id", "period", "error", "scrape_ts"]


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
    return resolved_df, unresolved_df, failures_df


def get_already_done():
    done = set()
    if RESOLVED_PATH.exists():
        done_df = pd.read_parquet(RESOLVED_PATH, columns=["game_id", "period"])
        done.update(zip(done_df["game_id"], done_df["period"]))
    return done


def build_jobs():
    pbp = pd.read_parquet(
        "playbyplayv2.parq",
        columns=["GAME_ID", "PERIOD", "EVENTMSGTYPE", "PCTIMESTRING"],
    )
    pbp["PERIOD"] = pd.to_numeric(pbp["PERIOD"], errors="coerce").astype("Int64")
    pbp["EVENTMSGTYPE"] = pd.to_numeric(pbp["EVENTMSGTYPE"], errors="coerce").astype("Int64")

    if SEASON is not None:
        season_code = str(SEASON % 100).zfill(2)
        pbp["_gid"] = pbp["GAME_ID"].apply(lambda value: str(int(value)).zfill(10) if pd.notna(value) else "")
        pbp = pbp[pbp["_gid"].str[3:5] == season_code].drop(columns=["_gid"])
        print(f"Filtering to season {SEASON}-{SEASON + 1} (code={season_code})")

    pbp = pbp[pbp["PERIOD"] >= 2].copy()
    pbp["elapsed"] = [
        clock_to_elapsed(clock, period)
        for clock, period in zip(pbp["PCTIMESTRING"], pbp["PERIOD"])
    ]
    pbp = pbp.dropna(subset=["elapsed"])

    period_keys = pbp[["GAME_ID", "PERIOD"]].drop_duplicates().reset_index(drop=True)

    real_events = pbp[~pbp["EVENTMSGTYPE"].isin(FIRST_EVENT_EXCLUDED_TYPES)]
    first_event_df = (
        real_events.groupby(["GAME_ID", "PERIOD"])["elapsed"]
        .min()
        .reset_index()
        .rename(columns={"elapsed": "first_event_elapsed"})
    )

    nonzero_real_events = real_events[real_events["elapsed"] > 0]
    first_nonzero_df = (
        nonzero_real_events.groupby(["GAME_ID", "PERIOD"])["elapsed"]
        .min()
        .reset_index()
        .rename(columns={"elapsed": "first_nonzero_event_elapsed"})
    )

    anchor_events = pbp[(pbp["elapsed"] > 0) & (~pbp["EVENTMSGTYPE"].isin(ANCHOR_EXCLUDED_TYPES))]
    anchor_df = (
        anchor_events.groupby(["GAME_ID", "PERIOD"])["elapsed"]
        .min()
        .reset_index()
        .rename(columns={"elapsed": "anchor_elapsed"})
    )

    subs = pbp[pbp["EVENTMSGTYPE"] == 8]
    first_sub_df = (
        subs.groupby(["GAME_ID", "PERIOD"])["elapsed"]
        .min()
        .reset_index()
        .rename(columns={"elapsed": "first_sub_elapsed"})
    )

    job_df = (
        period_keys.merge(first_event_df, on=["GAME_ID", "PERIOD"], how="left")
        .merge(first_nonzero_df, on=["GAME_ID", "PERIOD"], how="left")
        .merge(anchor_df, on=["GAME_ID", "PERIOD"], how="left")
        .merge(first_sub_df, on=["GAME_ID", "PERIOD"], how="left")
    )

    all_jobs = []
    for _, row in job_df.iterrows():
        first_event_elapsed = row["first_event_elapsed"] if pd.notna(row["first_event_elapsed"]) else None
        first_nonzero_event_elapsed = (
            row["first_nonzero_event_elapsed"] if pd.notna(row["first_nonzero_event_elapsed"]) else None
        )
        anchor_elapsed = row["anchor_elapsed"] if pd.notna(row["anchor_elapsed"]) else None
        first_sub_elapsed = row["first_sub_elapsed"] if pd.notna(row["first_sub_elapsed"]) else None
        requested_window_seconds, window_seconds, window_capped_by_sub = compute_window(
            anchor_elapsed,
            first_sub_elapsed,
        )
        all_jobs.append(
            {
                "game_id": str(int(row["GAME_ID"])).zfill(10),
                "period": int(row["PERIOD"]),
                "first_event_elapsed": first_event_elapsed,
                "first_nonzero_event_elapsed": first_nonzero_event_elapsed,
                "anchor_elapsed": anchor_elapsed,
                "first_sub_elapsed": first_sub_elapsed,
                "requested_window_seconds": requested_window_seconds,
                "window_seconds": window_seconds,
                "window_capped_by_sub": window_capped_by_sub,
            }
        )

    jobs = pd.DataFrame(all_jobs).drop_duplicates(subset=["game_id", "period"]).reset_index(drop=True)

    def season_year(game_id):
        code = int(game_id[3:5])
        return code + 1900 if code >= 96 else code + 2000

    jobs["_sort_key"] = jobs["game_id"].map(season_year)
    jobs = jobs.sort_values(["_sort_key", "game_id", "period"]).drop(columns=["_sort_key"]).reset_index(drop=True)
    return jobs


def main():
    jobs = build_jobs()

    print(f"Total jobs: {len(jobs)}")
    if len(jobs):
        print(f"\nFirst-event elapsed stats:\n{jobs['first_event_elapsed'].describe()}")
        print(f"\nFirst-nonzero-event elapsed stats:\n{jobs['first_nonzero_event_elapsed'].describe()}")
        print(f"\nAnchor elapsed stats:\n{jobs['anchor_elapsed'].describe()}")
        print(f"\nFirst-sub elapsed stats:\n{jobs['first_sub_elapsed'].describe()}")
        print(f"\nRequested window stats:\n{jobs['requested_window_seconds'].describe()}")
        print(f"\nEffective window stats:\n{jobs['window_seconds'].describe()}")
        print(f"\nPeriods with first_event_elapsed == 0: {(jobs['first_event_elapsed'] == 0).sum()}")
        print(f"Periods with no anchor: {jobs['anchor_elapsed'].isna().sum()}")
        print(f"Periods with no subs: {jobs['first_sub_elapsed'].isna().sum()}")
        print(f"Periods capped by first sub: {jobs['window_capped_by_sub'].sum()}")

    done = get_already_done()
    remaining = [
        (
            row.game_id,
            row.period,
            row.first_event_elapsed,
            row.first_nonzero_event_elapsed,
            row.anchor_elapsed,
            row.first_sub_elapsed,
        )
        for row in jobs.itertuples(index=False)
        if (row.game_id, row.period) not in done
    ]
    print(f"Already done: {len(done)}, remaining: {len(remaining)}")

    resolved_df = load_parquet_or_empty(RESOLVED_PATH, RESOLVED_COLS)
    unresolved_df = load_parquet_or_empty(UNRESOLVED_PATH, UNRESOLVED_COLS)
    failures_df = load_parquet_or_empty(FAILURES_PATH, FAILURE_COLS)

    new_resolved = []
    new_unresolved = []
    new_failures = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                scrape_period,
                game_id,
                period,
                first_event_elapsed,
                first_nonzero_event_elapsed,
                anchor_elapsed,
                first_sub_elapsed,
            ): (game_id, period)
            for (
                game_id,
                period,
                first_event_elapsed,
                first_nonzero_event_elapsed,
                anchor_elapsed,
                first_sub_elapsed,
            ) in remaining
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="Scraping"):
            game_id, period = futures[future]
            now = datetime.now(timezone.utc).isoformat()
            try:
                row, unresolved_rows, err = future.result()
            except Exception as exc:
                new_failures.append(
                    {
                        "game_id": game_id,
                        "period": period,
                        "error": str(exc),
                        "scrape_ts": now,
                    }
                )
                completed += 1
                continue

            if row is not None:
                new_resolved.append(row)
            elif unresolved_rows:
                new_unresolved.extend(unresolved_rows)
            elif err:
                new_failures.append(
                    {
                        "game_id": game_id,
                        "period": period,
                        "error": err,
                        "scrape_ts": now,
                    }
                )

            completed += 1
            if completed % CHECKPOINT_INTERVAL == 0:
                resolved_df, unresolved_df, failures_df = save_checkpoint(
                    resolved_df,
                    new_resolved,
                    unresolved_df,
                    new_unresolved,
                    failures_df,
                    new_failures,
                )
                new_resolved, new_unresolved, new_failures = [], [], []

    resolved_df, unresolved_df, failures_df = save_checkpoint(
        resolved_df,
        new_resolved,
        unresolved_df,
        new_unresolved,
        failures_df,
        new_failures,
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
        print(f"\nPer-team gap stats (seconds between #5 and #6):")
        print(f"  Away gap:\n{resolved_df['away_gap'].describe()}")
        print(f"  Home gap:\n{resolved_df['home_gap'].describe()}")
        print(f"  Min gap:\n{resolved_df['min_gap'].describe()}")
        print(f"\nAnchor elapsed stats:\n{resolved_df['anchor_elapsed'].describe()}")
        print(f"\nWindow seconds stats:\n{resolved_df['window_seconds'].describe()}")
        print(f"\nRequested window seconds stats:\n{resolved_df['requested_window_seconds'].describe()}")
        print(f"\nCapped by first sub: {resolved_df['window_capped_by_sub'].sum()} / {len(resolved_df)}")


if __name__ == "__main__":
    main()
