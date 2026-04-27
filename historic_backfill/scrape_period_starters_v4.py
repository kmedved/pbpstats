# %% [markdown]
# # Scrape Period Starters (v4)
# PBP-informed V4 boxscore resolver. Uses first-event offset to set window,
# caps at first substitution, takes top 5 per team by minutes, validates
# per-team gap between 5th and 6th player. Periods 2+ only (Q1 from boxscore).
# Jobs are sorted chronologically so the scrape fills in season order.
# %% Config
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
DATA_DIR = Path(".")
RESOLVER_VERSION = "v4"
RESOLVED_PATH = DATA_DIR / f"period_starters_{RESOLVER_VERSION}.parquet"
UNRESOLVED_PATH = DATA_DIR / f"period_starters_unresolved_{RESOLVER_VERSION}.parquet"
FAILURES_PATH = DATA_DIR / f"period_starters_failures_{RESOLVER_VERSION}.parquet"
MAX_WORKERS = 20
CHECKPOINT_INTERVAL = 100
MIN_GAP_SECONDS = 3   # minimum per-team gap between #5 and #6
BUFFER_SECONDS = 5    # seconds past first event to extend window
SUB_MARGIN = 1        # seconds before first sub to stop window
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
    """Parse NBA API minutes string to total seconds."""
    if not mins_str:
        return 0.0
    m = re.match(r"PT(\d+)M([\d.]+)S", mins_str)
    if m:
        return int(m.group(1)) * 60 + float(m.group(2))
    m = re.match(r"(\d+):([\d.]+)", mins_str)
    if m:
        return int(m.group(1)) * 60 + float(m.group(2))
    return 0.0
def period_start_tenths(period):
    if period == 1:
        return 0
    if period <= 4:
        return 7200 * (period - 1)
    return 4 * 7200 + 3000 * (period - 5)
def fetch_window(game_id, start_range, end_range):
    """Single API probe. Returns (players_list, error_string_or_None)."""
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
            URL, params=params, headers=API_HEADERS,
            proxies=get_proxies(), verify=False, timeout=30,
        )
    except requests.exceptions.RequestException as e:
        return None, f"Request error: {type(e).__name__}: {e}"
    if resp.status_code != 200:
        return None, f"HTTP {resp.status_code}"
    try:
        data = resp.json()
    except Exception as e:
        return None, f"JSON parse: {e}"
    bst = data.get("boxScoreTraditional")
    if bst is None:
        return None, "No boxScoreTraditional key"
    players = []
    for team_key in ["awayTeam", "homeTeam"]:
        team = bst.get(team_key, {})
        team_id = team.get("teamId")
        tricode = team.get("teamTricode", "")
        for p in team.get("players", []):
            mins = p.get("statistics", {}).get("minutes", "")
            secs = parse_minutes_to_seconds(mins)
            players.append({
                "personId": p.get("personId"),
                "name": f"{p.get('firstName', '')} {p.get('familyName', '')}".strip(),
                "team_id": team_id,
                "tricode": tricode,
                "side": "away" if team_key == "awayTeam" else "home",
                "minutes": mins,
                "seconds": secs,
            })
    return players, None
def select_starters(players):
    """
    Top 5 per team by seconds. Returns (away5, home5, away_gap, home_gap) or (None, reason).
    """
    away = sorted([p for p in players if p["side"] == "away"],
                  key=lambda x: x["seconds"], reverse=True)
    home = sorted([p for p in players if p["side"] == "home"],
                  key=lambda x: x["seconds"], reverse=True)
    if len(away) < 5 or len(home) < 5:
        return None, f"insufficient_players: away={len(away)}, home={len(home)}"
    away_gap = math.inf if len(away) == 5 else away[4]["seconds"] - away[5]["seconds"]
    home_gap = math.inf if len(home) == 5 else home[4]["seconds"] - home[5]["seconds"]
    if away_gap < MIN_GAP_SECONDS or home_gap < MIN_GAP_SECONDS:
        return None, f"weak_gap: away_gap={away_gap:.1f}, home_gap={home_gap:.1f}"
    return (away[:5], home[:5], away_gap, home_gap), None
def scrape_period(game_id, period, first_event_elapsed, first_sub_elapsed,
                  first_nonzero_event_elapsed):
    """
    Scrape starters for one game-period using PBP-informed window.
    Window = period_start to min(first_event + buffer, first_sub - margin).
    Then top 5 per team by minutes with per-team gap validation.
    If first_event_elapsed == 0 and base window fails, retries with
    first_nonzero_event_elapsed as the anchor (first actual clock change).
    """
    tenths = period_start_tenths(period)
    now = datetime.now(timezone.utc).isoformat()
    # --- Base attempt ---
    window_end = first_event_elapsed + BUFFER_SECONDS
    if first_sub_elapsed is not None:
        window_end = min(window_end, first_sub_elapsed - SUB_MARGIN)
    # Need room past first real event
    if window_end <= first_event_elapsed:
        base_result = None
        base_reason = "first_sub_too_early"
        base_players = None
        base_window = window_end
    else:
        start_range = tenths
        end_range = tenths + int(window_end * 10)
        players = None
        err = None
        for attempt in range(3):
            players, err = fetch_window(game_id, start_range, end_range)
            if err is None:
                break
            time.sleep(1.0 * (attempt + 1))
        if err:
            return None, None, f"window={window_end}s: {err}"
        if not players:
            return None, None, "Empty player list from API"
        base_result, base_reason = select_starters(players)
        base_players = players
        base_window = window_end
    # If base resolved, return it
    if base_result is not None:
        return _build_resolved(game_id, period, base_result, tenths, base_window,
                               first_event_elapsed, first_sub_elapsed,
                               first_nonzero_event_elapsed,
                               len(base_players), "base_window", now)
    # --- Fallback: first clock change anchor ---
    # Only when first_event_elapsed == 0 and we have a nonzero anchor.
    # Use exactly first_nonzero_event_elapsed (no buffer) as the tightest window.
    fallback_reason = None
    if (first_event_elapsed == 0
            and first_nonzero_event_elapsed is not None
            and first_nonzero_event_elapsed > 0):
        alt_window_end = first_nonzero_event_elapsed
        if first_sub_elapsed is not None:
            alt_window_end = min(alt_window_end, first_sub_elapsed - SUB_MARGIN)
        if alt_window_end > 0:
            alt_start = tenths
            alt_end = tenths + int(alt_window_end * 10)
            alt_players = None
            alt_err = None
            for attempt in range(3):
                alt_players, alt_err = fetch_window(game_id, alt_start, alt_end)
                if alt_err is None:
                    break
                time.sleep(1.0 * (attempt + 1))
            if alt_err is not None:
                fallback_reason = f"transport_error: {alt_err}"
            elif not alt_players:
                fallback_reason = "empty_player_list"
            else:
                alt_result, alt_reason = select_starters(alt_players)
                if alt_result is not None:
                    return _build_resolved(game_id, period, alt_result, tenths, alt_window_end,
                                           first_event_elapsed, first_sub_elapsed,
                                           first_nonzero_event_elapsed,
                                           len(alt_players), "first_clock_change", now)
                fallback_reason = alt_reason
                # Keep base_players for the unresolved artifact — don't overwrite
    # Unresolved
    if base_players:
        unresolved_rows = _build_unresolved(game_id, period, base_players, base_window,
                                            first_event_elapsed, first_sub_elapsed,
                                            first_nonzero_event_elapsed, now,
                                            base_reason=base_reason,
                                            fallback_reason=fallback_reason)
    else:
        unresolved_rows = [{
            "game_id": game_id, "period": period,
            "personId": None, "name": None, "team_id": None, "tricode": None,
            "side": None, "minutes": None, "seconds": None,
            "window_seconds": base_window,
            "first_event_elapsed": first_event_elapsed,
            "first_nonzero_event_elapsed": first_nonzero_event_elapsed,
            "first_sub_elapsed": first_sub_elapsed,
            "total_returned": 0,
            "base_reason": base_reason, "fallback_reason": fallback_reason,
            "scrape_ts": now,
        }]
    return None, unresolved_rows, None
def _build_resolved(game_id, period, result, tenths, window_end,
                    first_event_elapsed, first_sub_elapsed,
                    first_nonzero_event_elapsed,
                    total_returned, resolver_mode, now):
    away5, home5, away_gap, home_gap = result
    row = {
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
        "start_range": tenths,
        "end_range": tenths + int(window_end * 10),
        "window_seconds": window_end,
        "first_event_elapsed": first_event_elapsed,
        "first_nonzero_event_elapsed": first_nonzero_event_elapsed,
        "first_sub_elapsed": first_sub_elapsed,
        "total_returned": total_returned,
        "away_gap": away_gap if away_gap != math.inf else None,
        "home_gap": home_gap if home_gap != math.inf else None,
        "min_gap": min(away_gap, home_gap) if min(away_gap, home_gap) != math.inf else None,
        "resolver_mode": resolver_mode,
        "resolved": True,
        "scrape_ts": now,
    }
    return row, None, None
def _build_unresolved(game_id, period, players, window_seconds,
                      first_event_elapsed, first_sub_elapsed,
                      first_nonzero_event_elapsed, now,
                      base_reason="weak_gap", fallback_reason=None):
    rows = []
    for p in players:
        rows.append({
            "game_id": game_id,
            "period": period,
            "personId": p["personId"],
            "name": p["name"],
            "team_id": p["team_id"],
            "tricode": p["tricode"],
            "side": p["side"],
            "minutes": p["minutes"],
            "seconds": p["seconds"],
            "window_seconds": window_seconds,
            "first_event_elapsed": first_event_elapsed,
            "first_nonzero_event_elapsed": first_nonzero_event_elapsed,
            "first_sub_elapsed": first_sub_elapsed,
            "total_returned": len(players),
            "base_reason": base_reason,
            "fallback_reason": fallback_reason,
            "scrape_ts": now,
        })
    return rows
RESOLVED_COLS = [
    "game_id", "period", "away_team_id", "away_tricode",
    "home_team_id", "home_tricode",
    "away_player1", "away_player2", "away_player3", "away_player4", "away_player5",
    "home_player1", "home_player2", "home_player3", "home_player4", "home_player5",
    "start_range", "end_range", "window_seconds",
    "first_event_elapsed", "first_nonzero_event_elapsed", "first_sub_elapsed",
    "total_returned", "away_gap", "home_gap", "min_gap",
    "resolver_mode", "resolved", "scrape_ts",
]
UNRESOLVED_COLS = [
    "game_id", "period", "personId", "name", "team_id", "tricode", "side",
    "minutes", "seconds", "window_seconds",
    "first_event_elapsed", "first_nonzero_event_elapsed", "first_sub_elapsed",
    "total_returned", "base_reason", "fallback_reason", "scrape_ts",
]
FAILURE_COLS = ["game_id", "period", "error", "scrape_ts"]
def load_parquet_or_empty(path, cols):
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame(columns=cols)
def save_checkpoint(resolved_df, new_resolved, unresolved_df, new_unresolved,
                    failures_df, new_failures):
    if new_resolved:
        resolved_df = pd.concat([resolved_df, pd.DataFrame(new_resolved)], ignore_index=True)
        resolved_df = resolved_df.drop_duplicates(subset=["game_id", "period"], keep="last")
        resolved_df.to_parquet(RESOLVED_PATH, index=False)
        resolved_keys = set(zip(resolved_df["game_id"], resolved_df["period"]))
        if len(unresolved_df):
            mask = [
                (g, p) not in resolved_keys
                for g, p in zip(unresolved_df["game_id"], unresolved_df["period"])
            ]
            unresolved_df = unresolved_df[mask].reset_index(drop=True)
            unresolved_df.to_parquet(UNRESOLVED_PATH, index=False)
        if len(failures_df):
            mask = [
                (g, p) not in resolved_keys
                for g, p in zip(failures_df["game_id"], failures_df["period"])
            ]
            failures_df = failures_df[mask].reset_index(drop=True)
            failures_df.to_parquet(FAILURES_PATH, index=False)
    if new_unresolved:
        new_unresolved_df = pd.DataFrame(new_unresolved)
        new_keys = set(zip(new_unresolved_df["game_id"], new_unresolved_df["period"]))
        if len(unresolved_df):
            mask = [
                (g, p) not in new_keys
                for g, p in zip(unresolved_df["game_id"], unresolved_df["period"])
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
    """Only resolved periods are permanently done."""
    done = set()
    if RESOLVED_PATH.exists():
        df = pd.read_parquet(RESOLVED_PATH, columns=["game_id", "period"])
        done.update(zip(df["game_id"], df["period"]))
    return done
# %% Step 1: Build job list with first-event and first-sub offsets from PBP
# Set to a year (e.g. 2024 for 2024-25) to scrape one season, or None for all
SEASON = None
pbp = pd.read_parquet("playbyplayv2.parq", columns=[
    "GAME_ID", "PERIOD", "EVENTMSGTYPE", "PCTIMESTRING",
])
# Normalize types
pbp["PERIOD"] = pd.to_numeric(pbp["PERIOD"], errors="coerce").astype("Int64")
pbp["EVENTMSGTYPE"] = pd.to_numeric(pbp["EVENTMSGTYPE"], errors="coerce").astype("Int64")
if SEASON is not None:
    season_code = str(SEASON % 100).zfill(2)
    pbp["_gid"] = pbp["GAME_ID"].apply(lambda x: str(int(x)).zfill(10) if pd.notna(x) else "")
    pbp = pbp[pbp["_gid"].str[3:5] == season_code].drop(columns=["_gid"])
    print(f"Filtering to season {SEASON}-{SEASON+1} (code={season_code})")
# Skip Q1 — starters come from boxscore START_POSITION
pbp = pbp[pbp["PERIOD"] >= 2]
def clock_to_elapsed(pctimestring, period):
    """Game clock string -> seconds elapsed in that period."""
    try:
        parts = str(pctimestring).split(":")
        remaining = int(parts[0]) * 60 + int(parts[1])
    except (ValueError, IndexError):
        return None
    period_length = 720 if period <= 4 else 300
    return period_length - remaining
pbp["elapsed"] = [
    clock_to_elapsed(t, p)
    for t, p in zip(pbp["PCTIMESTRING"], pbp["PERIOD"])
]
pbp = pbp.dropna(subset=["elapsed"])
# First real event (exclude StartOfPeriod=12, EndOfPeriod=13, Substitution=8)
real_events = pbp[~pbp["EVENTMSGTYPE"].isin([8, 12, 13])]
first_event_df = (
    real_events
    .groupby(["GAME_ID", "PERIOD"])["elapsed"]
    .min()
    .reset_index()
    .rename(columns={"elapsed": "first_event_elapsed"})
)
# First real event with elapsed > 0 (first actual clock change)
nonzero_events = real_events[real_events["elapsed"] > 0]
first_nonzero_df = (
    nonzero_events
    .groupby(["GAME_ID", "PERIOD"])["elapsed"]
    .min()
    .reset_index()
    .rename(columns={"elapsed": "first_nonzero_event_elapsed"})
)
# First substitution (EVENTMSGTYPE=8)
subs = pbp[pbp["EVENTMSGTYPE"] == 8]
first_sub_df = (
    subs
    .groupby(["GAME_ID", "PERIOD"])["elapsed"]
    .min()
    .reset_index()
    .rename(columns={"elapsed": "first_sub_elapsed"})
)
# Merge
job_df = (
    first_event_df
    .merge(first_nonzero_df, on=["GAME_ID", "PERIOD"], how="left")
    .merge(first_sub_df, on=["GAME_ID", "PERIOD"], how="left")
)
# Build job list
all_jobs = []
for _, row in job_df.iterrows():
    game_id = str(int(row["GAME_ID"])).zfill(10)
    first_sub = row["first_sub_elapsed"] if pd.notna(row["first_sub_elapsed"]) else None
    first_nonzero = row["first_nonzero_event_elapsed"] if pd.notna(row["first_nonzero_event_elapsed"]) else None
    all_jobs.append({
        "game_id": game_id,
        "period": int(row["PERIOD"]),
        "first_event_elapsed": row["first_event_elapsed"],
        "first_nonzero_event_elapsed": first_nonzero,
        "first_sub_elapsed": first_sub,
    })
jobs = pd.DataFrame(all_jobs).drop_duplicates(subset=["game_id", "period"]).reset_index(drop=True)

# Sort jobs chronologically (season codes wrap: 96-99 -> 00-24)
def _season_year(gid):
    code = int(gid[3:5])
    return code + 1900 if code >= 96 else code + 2000

jobs["_sort_key"] = jobs["game_id"].map(_season_year)
jobs = jobs.sort_values(["_sort_key", "game_id", "period"]).drop(columns=["_sort_key"]).reset_index(drop=True)

del pbp, real_events, nonzero_events, subs, first_event_df, first_nonzero_df, first_sub_df, job_df
print(f"Total jobs: {len(jobs)}")
print(f"\nFirst-event elapsed stats:\n{jobs['first_event_elapsed'].describe()}")
print(f"\nFirst-nonzero-event elapsed stats:\n{jobs['first_nonzero_event_elapsed'].describe()}")
print(f"\nFirst-sub elapsed stats:\n{jobs['first_sub_elapsed'].describe()}")
print(f"\nPeriods with first_event_elapsed == 0: {(jobs['first_event_elapsed'] == 0).sum()}")
print(f"Periods with no subs: {jobs['first_sub_elapsed'].isna().sum()}")
# %% Step 2: Scrape
done = get_already_done()
remaining = [
    (row.game_id, row.period, row.first_event_elapsed,
     row.first_sub_elapsed, row.first_nonzero_event_elapsed)
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
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    futures = {}
    for game_id, period, first_event_elapsed, first_sub_elapsed, first_nonzero in remaining:
        fut = ex.submit(scrape_period, game_id, period,
                        first_event_elapsed, first_sub_elapsed, first_nonzero)
        futures[fut] = (game_id, period)
    for fut in tqdm(as_completed(futures), total=len(futures), desc="Scraping"):
        game_id, period = futures[fut]
        now = datetime.now(timezone.utc).isoformat()
        try:
            row, unresolved_rows, err = fut.result()
        except Exception as e:
            new_failures.append({
                "game_id": game_id, "period": period,
                "error": str(e), "scrape_ts": now,
            })
            completed += 1
            continue
        if row is not None:
            new_resolved.append(row)
        elif unresolved_rows:
            new_unresolved.extend(unresolved_rows)
        elif err:
            new_failures.append({
                "game_id": game_id, "period": period,
                "error": err, "scrape_ts": now,
            })
        completed += 1
        if completed % CHECKPOINT_INTERVAL == 0:
            resolved_df, unresolved_df, failures_df = save_checkpoint(
                resolved_df, new_resolved, unresolved_df, new_unresolved,
                failures_df, new_failures,
            )
            new_resolved, new_unresolved, new_failures = [], [], []
# Final save
resolved_df, unresolved_df, failures_df = save_checkpoint(
    resolved_df, new_resolved, unresolved_df, new_unresolved,
    failures_df, new_failures,
)
# %% Step 3: Status
print(f"\n{'='*50}")
print(f"Resolved:   {len(resolved_df)} periods")
if UNRESOLVED_PATH.exists():
    ur = pd.read_parquet(UNRESOLVED_PATH)
    print(f"Unresolved: {ur.drop_duplicates(['game_id','period']).shape[0]} periods ({len(ur)} player rows)")
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
    print(f"\nFirst-event elapsed stats:\n{resolved_df['first_event_elapsed'].describe()}")
    print(f"\nWindow seconds stats:\n{resolved_df['window_seconds'].describe()}")
