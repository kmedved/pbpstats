import random, requests, urllib3, time, re, json, sys

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    session_id = f"nba_{random.randint(10000, 99999)}"
    user = f"a0feb795d77dbcf7861c_session-{session_id}"
    url = f"http://{user}:5fe46ff800ae77f1@gw.dataimpulse.com:823"
    return {"http": url, "https": url}

def parse_minutes(mins_str):
    if not mins_str:
        return 0.0
    m = re.match(r"PT(\d+)M([\d.]+)S", mins_str)
    if m:
        return int(m.group(1)) * 60 + float(m.group(2))
    return 0.0

def fetch(game_id, start_range, end_range):
    params = {
        "GameID": game_id,
        "StartPeriod": 0, "EndPeriod": 0,
        "RangeType": 2,
        "StartRange": start_range, "EndRange": end_range,
    }
    resp = requests.get(URL, params=params, headers=API_HEADERS,
                        proxies=get_proxies(), verify=False, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    bst = data["boxScoreTraditional"]
    results = []
    for team_key in ["awayTeam", "homeTeam"]:
        team = bst.get(team_key, {})
        tricode = team.get("teamTricode", "")
        for p in team.get("players", []):
            mins = p.get("statistics", {}).get("minutes", "")
            secs = parse_minutes(mins)
            name = f"{p.get('firstName', '')} {p.get('familyName', '')}".strip()
            results.append({"name": name, "team": tricode, "seconds": secs, "raw_minutes": mins})
    return results

def safe_fetch(game_id, start_range, end_range, label=""):
    """Fetch with retry and delay."""
    for attempt in range(3):
        try:
            result = fetch(game_id, start_range, end_range)
            return result
        except Exception as e:
            print(f"  [ERROR on attempt {attempt+1} for {label}]: {e}", flush=True)
            time.sleep(10)
    print(f"  [FAILED after 3 attempts for {label}]", flush=True)
    return None

def print_table(players, game_id, period_label, window_secs):
    """Print a formatted table of player minutes."""
    if players is None:
        print(f"\n{'='*80}")
        print(f"GAME {game_id} | {period_label} | Window: {window_secs}s | FETCH FAILED")
        print(f"{'='*80}")
        return

    # Get unique teams
    teams = sorted(set(p["team"] for p in players))

    print(f"\n{'='*90}")
    print(f"GAME {game_id} | {period_label} | Window: {window_secs}s")
    print(f"{'='*90}")

    for team in teams:
        team_players = [p for p in players if p["team"] == team]
        team_players.sort(key=lambda x: x["seconds"], reverse=True)

        print(f"\n  {team}:")
        print(f"  {'Rank':<5} {'Name':<25} {'Seconds':>10} {'Raw Minutes':<20} {'Top5?'}")
        print(f"  {'-'*70}")
        for i, p in enumerate(team_players):
            marker = " <-- TOP5" if i < 5 else ""
            print(f"  {i+1:<5} {p['name']:<25} {p['seconds']:>10.1f} {p['raw_minutes']:<20}{marker}")

    # Summary: count distinct second values per team
    for team in teams:
        team_players = [p for p in players if p["team"] == team]
        secs_values = [p["seconds"] for p in team_players if p["seconds"] > 0]
        distinct = len(set(secs_values))
        print(f"\n  {team}: {len(secs_values)} players with >0 secs, {distinct} distinct values")

    sys.stdout.flush()

# ============================================================
# PROBES
# ============================================================
all_results = {}
request_count = 0

# --- GAME 1: 0029700060 Period 3 (1997-98) ---
game1 = "0029700060"
p3_start = 14400
game1_windows = [12, 30, 60, 120, 180, 300, 720]

print("\n" + "#"*90)
print("# GAME 1: 0029700060 — 1997-98 Portland vs Houston, Period 3")
print("# Known: Portland starters should have Wallace (not O'Neal), Houston should have Drexler (not Bullard)")
print("#"*90, flush=True)

for w in game1_windows:
    end = p3_start + w * 10
    label = f"G1-P3-{w}s"
    print(f"\nFetching {label}: range [{p3_start}, {end}]...", flush=True)
    players = safe_fetch(game1, p3_start, end, label)
    print_table(players, game1, "Period 3", w)
    all_results[label] = players
    request_count += 1
    time.sleep(4)

# --- GAME 2: 0020401139 Period 5 OT1 (2004-05) ---
game2 = "0020401139"
p5_start = 28800
game2_windows = [12, 30, 60, 120, 180, 300]

print("\n" + "#"*90)
print("# GAME 2: 0020401139 — 2004-05 Spurs OT game, Period 5 (OT1)")
print("# Known: V3 correct = Bruce Bowen; strict PBP wrong = Brent Barry")
print("#"*90, flush=True)

for w in game2_windows:
    end = p5_start + w * 10
    label = f"G2-P5-{w}s"
    print(f"\nFetching {label}: range [{p5_start}, {end}]...", flush=True)
    players = safe_fetch(game2, p5_start, end, label)
    print_table(players, game2, "Period 5 (OT1)", w)
    all_results[label] = players
    request_count += 1
    time.sleep(4)

# --- GAME 3: 0020700319 Period 4 (2007-08) ---
game3 = "0020700319"
p4_start = 21600
game3_windows = [12, 30, 60, 120, 180, 300, 720]

print("\n" + "#"*90)
print("# GAME 3: 0020700319 — 2007-08 Knicks issue, Period 4")
print("# Known: V3 correct = Fred Jones; strict PBP wrong = Nate Robinson")
print("#"*90, flush=True)

for w in game3_windows:
    end = p4_start + w * 10
    label = f"G3-P4-{w}s"
    print(f"\nFetching {label}: range [{p4_start}, {end}]...", flush=True)
    players = safe_fetch(game3, p4_start, end, label)
    print_table(players, game3, "Period 4", w)
    all_results[label] = players
    request_count += 1
    time.sleep(4)

# --- GAME 4: Modern control 0022300001 Period 2 (2023-24) ---
game4 = "0022300001"
p2_start = 7200
game4_windows = [12, 60, 120, 300, 720]

print("\n" + "#"*90)
print("# GAME 4: 0022300001 — 2023-24 Modern control, Period 2")
print("#"*90, flush=True)

for w in game4_windows:
    end = p2_start + w * 10
    label = f"G4-P2-{w}s"
    print(f"\nFetching {label}: range [{p2_start}, {end}]...", flush=True)
    players = safe_fetch(game4, p2_start, end, label)
    print_table(players, game4, "Period 2", w)
    all_results[label] = players
    request_count += 1
    time.sleep(4)

# ============================================================
# CROSS-WINDOW ANALYSIS
# ============================================================
print("\n\n" + "#"*90)
print("# CROSS-WINDOW ANALYSIS")
print("#"*90)

def analyze_granularity(results_dict, prefix, windows):
    """Analyze how seconds values change across window sizes."""
    print(f"\n--- {prefix} ---")
    for w in windows:
        key = f"{prefix}-{w}s"
        players = results_dict.get(key)
        if players is None:
            print(f"  Window {w:>4}s: FAILED")
            continue
        nonzero = [p for p in players if p["seconds"] > 0]
        all_secs = [p["seconds"] for p in nonzero]
        distinct = len(set(all_secs))
        # Check for fractional seconds
        has_fractions = any(s != int(s) for s in all_secs)
        # Check if values are multiples of some base
        if all_secs:
            min_nonzero = min(s for s in all_secs if s > 0) if all_secs else 0
        else:
            min_nonzero = 0
        max_secs = max(all_secs) if all_secs else 0

        # Get top 5 per team
        teams = sorted(set(p["team"] for p in players))
        top5_summary = []
        for team in teams:
            tp = sorted([p for p in players if p["team"] == team], key=lambda x: x["seconds"], reverse=True)
            top5_names = [p["name"].split()[-1] for p in tp[:5]]
            top5_summary.append(f"{team}: {', '.join(top5_names)}")

        print(f"  Window {w:>4}s: {len(nonzero):>2} players>0, {distinct:>2} distinct vals, "
              f"fractions={'Y' if has_fractions else 'N'}, min={min_nonzero:.1f}, max={max_secs:.1f}")
        for s in top5_summary:
            print(f"    Top5 {s}")

analyze_granularity(all_results, "G1-P3", game1_windows)
analyze_granularity(all_results, "G2-P5", game2_windows)
analyze_granularity(all_results, "G3-P4", game3_windows)
analyze_granularity(all_results, "G4-P2", game4_windows)

# Check if top-5 per team is stable across windows
print("\n\n--- TOP-5 STABILITY CHECK ---")
def check_stability(results_dict, prefix, windows, team_filter=None):
    """Check if the top-5 per team changes across window sizes."""
    print(f"\n  {prefix}:")
    prev_top5 = {}
    for w in windows:
        key = f"{prefix}-{w}s"
        players = results_dict.get(key)
        if players is None:
            continue
        teams = sorted(set(p["team"] for p in players))
        for team in teams:
            tp = sorted([p for p in players if p["team"] == team], key=lambda x: x["seconds"], reverse=True)
            top5 = set(p["name"] for p in tp[:5])
            prev_key = f"{team}"
            if prev_key in prev_top5:
                added = top5 - prev_top5[prev_key]
                removed = prev_top5[prev_key] - top5
                if added or removed:
                    print(f"    Window {w}s {team}: CHANGED! +{added} -{removed}")
                else:
                    print(f"    Window {w}s {team}: same top-5")
            else:
                print(f"    Window {w}s {team}: {sorted(top5)}")
            prev_top5[prev_key] = top5

check_stability(all_results, "G1-P3", game1_windows)
check_stability(all_results, "G2-P5", game2_windows)
check_stability(all_results, "G3-P4", game3_windows)
check_stability(all_results, "G4-P2", game4_windows)

print(f"\n\nTotal requests made: {request_count}")
print("Done.", flush=True)
