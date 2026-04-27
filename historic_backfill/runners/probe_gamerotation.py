"""
Probe the gamerotation endpoint across 1997-2024 to assess viability
for extracting period starters from stint data.

Key questions:
1. Does it return data for 1996-97, 1999-00, 2000-01?
2. Do stints restart at each period boundary (fresh IN_TIME entries)?
3. Are there games where rotation data is incomplete (not exactly 5 per team)?
4. Does canary 0029700060 P3 give the correct starters?
"""
from __future__ import annotations

import json
import random
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "gamerotation_probe_results"

GAMEROTATION_URL = "https://stats.nba.com/stats/gamerotation"
API_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/139.0.0.0 Safari/537.36"
    ),
    "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}

SLEEP_BETWEEN = 2.0

# Canary games from explore_v3_stability.py
CANARY_GAMES = {
    "0029700060": "1998 POR-HOU false-positive canary (P3)",
    "0020000576": "2001 ORL-SAS OT ghost-plateau canary (P5)",
    "0020100162": "2001 NJN-UTA rescue OT canary (P5)",
    "0020401139": "2005 SAS-LAC helpful OT canary (P5)",
    "0020700319": "2008 NYK-SEA helpful Q4 canary (P4)",
}

# One mid-season game per season, 1996-97 through 2024-25
SEASON_SAMPLES = {
    "1996-97": "0029600595",
    "1997-98": "0029700595",
    "1998-99": "0029800363",
    "1999-00": "0029900595",
    "2000-01": "0020000595",
    "2001-02": "0020100595",
    "2002-03": "0020200595",
    "2003-04": "0020300595",
    "2004-05": "0020400616",
    "2005-06": "0020500616",
    "2006-07": "0020600616",
    "2007-08": "0020700616",
    "2008-09": "0020800616",
    "2009-10": "0020900616",
    "2010-11": "0021000616",
    "2011-12": "0021100496",
    "2012-13": "0021200615",
    "2013-14": "0021300616",
    "2014-15": "0021400616",
    "2015-16": "0021500616",
    "2016-17": "0021600616",
    "2017-18": "0021700616",
    "2018-19": "0021800616",
    "2019-20": "0021900529",
    "2020-21": "0022000541",
    "2021-22": "0022100616",
    "2022-23": "0022200616",
    "2023-24": "0022300616",
    "2024-25": "0022400616",
}


def get_proxies() -> dict[str, str]:
    session_id = f"nba_{random.randint(10000, 99999)}"
    user = f"a0feb795d77dbcf7861c_session-{session_id}"
    proxy_url = f"http://{user}:5fe46ff800ae77f1@gw.dataimpulse.com:823"
    return {"http": proxy_url, "https": proxy_url}


def period_start_tenths(period: int) -> int:
    if period == 1:
        return 0
    if period <= 4:
        return 7200 * (period - 1)
    return 4 * 7200 + 3000 * (period - 5)


def fetch_gamerotation(
    game_id: str, retries: int = 3
) -> dict[str, Any]:
    """Fetch gamerotation endpoint. Returns raw parsed JSON or error dict."""
    params = {"GameID": game_id, "LeagueID": "00"}
    last_error = None
    for attempt in range(retries):
        try:
            resp = requests.get(
                GAMEROTATION_URL,
                params=params,
                headers=API_HEADERS,
                proxies=get_proxies(),
                verify=False,
                timeout=45,
            )
            if resp.status_code == 200:
                return {
                    "status": 200,
                    "game_id": game_id,
                    "data": resp.json(),
                    "content_length": len(resp.content),
                }
            last_error = f"HTTP {resp.status_code}"
            if resp.status_code in (403, 429):
                time.sleep(max(SLEEP_BETWEEN * 2, 3.0) * (attempt + 1))
                continue
            # Other status codes — don't retry
            return {
                "status": resp.status_code,
                "game_id": game_id,
                "error": last_error,
                "body_preview": resp.text[:500],
            }
        except requests.exceptions.RequestException as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            time.sleep(max(SLEEP_BETWEEN * 2, 3.0) * (attempt + 1))
    return {"status": "failed", "game_id": game_id, "error": last_error}


def parse_result_set(result_set: dict) -> pd.DataFrame:
    """Convert a resultSets entry to a DataFrame."""
    headers = result_set["headers"]
    rows = result_set["rowSet"]
    return pd.DataFrame(rows, columns=headers)


def analyze_game(raw: dict[str, Any]) -> dict[str, Any]:
    """Analyze a successful gamerotation response for period-start stints."""
    data = raw["data"]
    game_id = raw["game_id"]
    result_sets = data.get("resultSets", [])

    if len(result_sets) < 2:
        return {
            "game_id": game_id,
            "error": f"Expected 2 resultSets, got {len(result_sets)}",
            "result_set_names": [rs.get("name") for rs in result_sets],
        }

    away_df = parse_result_set(result_sets[0])
    home_df = parse_result_set(result_sets[1])
    away_name = result_sets[0].get("name", "AwayTeam")
    home_name = result_sets[1].get("name", "HomeTeam")

    analysis: dict[str, Any] = {
        "game_id": game_id,
        "headers": list(away_df.columns),
        "away_team_name": away_name,
        "home_team_name": home_name,
        "away_rows": len(away_df),
        "home_rows": len(home_df),
        "total_stints": len(away_df) + len(home_df),
        "periods_found": [],
        "period_analysis": {},
    }

    # Combine for period analysis
    away_df["_side"] = "away"
    home_df["_side"] = "home"
    all_stints = pd.concat([away_df, home_df], ignore_index=True)

    # Detect column names — may vary
    in_time_col = None
    out_time_col = None
    person_id_col = None
    period_col = None
    team_id_col = None
    player_first_col = None
    player_last_col = None

    for col in all_stints.columns:
        cl = col.upper()
        if "IN_TIME_REAL" in cl:
            in_time_col = col
        elif "OUT_TIME_REAL" in cl:
            out_time_col = col
        elif "PERSON_ID" in cl:
            person_id_col = col
        elif col.upper() == "PERIOD":
            period_col = col
        elif "TEAM_ID" in cl:
            team_id_col = col
        elif "PLAYER_FIRST" in cl:
            player_first_col = col
        elif "PLAYER_LAST" in cl:
            player_last_col = col

    analysis["detected_columns"] = {
        "in_time": in_time_col,
        "out_time": out_time_col,
        "person_id": person_id_col,
        "period": period_col,
        "team_id": team_id_col,
        "player_first": player_first_col,
        "player_last": player_last_col,
    }

    if in_time_col is None or person_id_col is None:
        analysis["error"] = "Could not detect IN_TIME_REAL or PERSON_ID columns"
        return analysis

    # Coerce to numeric
    all_stints[in_time_col] = pd.to_numeric(all_stints[in_time_col], errors="coerce")
    if out_time_col:
        all_stints[out_time_col] = pd.to_numeric(all_stints[out_time_col], errors="coerce")

    # Discover periods present
    if period_col:
        periods = sorted(all_stints[period_col].dropna().unique())
    else:
        # Infer from IN_TIME_REAL ranges
        periods = []
        for p in range(1, 8):
            pst = period_start_tenths(p)
            if (all_stints[in_time_col] >= pst).any():
                periods.append(p)
    analysis["periods_found"] = [int(p) for p in periods]

    # For each period 2+, find stints starting at period_start_tenths
    for period in periods:
        p = int(period)
        if p < 2:
            continue
        pst = period_start_tenths(p)

        starters_at_boundary = all_stints[all_stints[in_time_col] == pst].copy()

        per_result: dict[str, Any] = {
            "period": p,
            "period_start_tenths": pst,
            "stints_at_boundary": len(starters_at_boundary),
        }

        # Group by side (away/home)
        for side in ["away", "home"]:
            side_starters = starters_at_boundary[starters_at_boundary["_side"] == side]
            player_ids = []
            player_names = []
            team_id = None
            for _, row in side_starters.iterrows():
                pid = row.get(person_id_col)
                if pd.notna(pid):
                    player_ids.append(int(pid))
                if player_first_col and player_last_col:
                    first = str(row.get(player_first_col, "")).strip()
                    last = str(row.get(player_last_col, "")).strip()
                    player_names.append(f"{first} {last}".strip())
                if team_id_col and pd.notna(row.get(team_id_col)):
                    team_id = int(row[team_id_col])

            per_result[f"{side}_count"] = len(player_ids)
            per_result[f"{side}_player_ids"] = sorted(player_ids)
            per_result[f"{side}_player_names"] = sorted(player_names)
            per_result[f"{side}_team_id"] = team_id

        per_result["total_at_boundary"] = (
            per_result.get("away_count", 0) + per_result.get("home_count", 0)
        )
        per_result["exactly_5_per_team"] = (
            per_result.get("away_count", 0) == 5
            and per_result.get("home_count", 0) == 5
        )

        # ACTIVE AT BOUNDARY: stints where IN_TIME_REAL <= pst < OUT_TIME_REAL
        # OR IN_TIME_REAL == pst (already counted above, but include for union)
        if out_time_col:
            active_mask = (
                (all_stints[in_time_col] <= pst)
                & (all_stints[out_time_col] > pst)
            ) | (all_stints[in_time_col] == pst)
            active_at_boundary = all_stints[active_mask].copy()

            for side in ["away", "home"]:
                side_active = active_at_boundary[active_at_boundary["_side"] == side]
                # Deduplicate by person_id (a player could match both conditions)
                side_active = side_active.drop_duplicates(subset=[person_id_col])
                a_ids = sorted(int(pid) for pid in side_active[person_id_col].dropna())
                a_names = []
                if player_first_col and player_last_col:
                    for _, row in side_active.iterrows():
                        first = str(row.get(player_first_col, "")).strip()
                        last = str(row.get(player_last_col, "")).strip()
                        a_names.append(f"{first} {last}".strip())
                    a_names = sorted(a_names)
                per_result[f"{side}_active_count"] = len(a_ids)
                per_result[f"{side}_active_ids"] = a_ids
                per_result[f"{side}_active_names"] = a_names

            per_result["active_total"] = (
                per_result.get("away_active_count", 0)
                + per_result.get("home_active_count", 0)
            )
            per_result["active_exactly_5_per_team"] = (
                per_result.get("away_active_count", 0) == 5
                and per_result.get("home_active_count", 0) == 5
            )

        analysis["period_analysis"][str(p)] = per_result

    return analysis


def cross_validate_v5(
    analysis: dict[str, Any], v5: pd.DataFrame
) -> dict[str, Any]:
    """Compare gamerotation period starters against period_starters_v5."""
    game_id = analysis["game_id"]
    v5_game = v5[(v5["game_id"] == game_id) & v5["resolved"]]
    if len(v5_game) == 0:
        return {"game_id": game_id, "v5_periods": 0, "matches": {}}

    matches: dict[str, Any] = {}
    for period_str, per_data in analysis.get("period_analysis", {}).items():
        period = int(period_str)
        v5_row = v5_game[v5_game["period"] == period]
        if len(v5_row) == 0:
            matches[period_str] = {"v5_exists": False}
            continue

        v5_row = v5_row.iloc[0]
        v5_away = set(
            int(v5_row[f"away_player{i}"]) for i in range(1, 6)
        )
        v5_home = set(
            int(v5_row[f"home_player{i}"]) for i in range(1, 6)
        )

        # Compare both: boundary-start IDs and active-at-boundary IDs
        gr_away_start = set(per_data.get("away_player_ids", []))
        gr_home_start = set(per_data.get("home_player_ids", []))
        gr_away_active = set(per_data.get("away_active_ids", []))
        gr_home_active = set(per_data.get("home_active_ids", []))

        match_info: dict[str, Any] = {
            "v5_exists": True,
            # boundary-start comparison
            "start_away_match": gr_away_start == v5_away,
            "start_home_match": gr_home_start == v5_home,
            "start_full_match": gr_away_start == v5_away and gr_home_start == v5_home,
            # active-at-boundary comparison
            "active_away_match": gr_away_active == v5_away,
            "active_home_match": gr_home_active == v5_home,
            "active_full_match": gr_away_active == v5_away and gr_home_active == v5_home,
            "away_active_count": len(gr_away_active),
            "home_active_count": len(gr_home_active),
        }
        if not match_info["active_full_match"]:
            if not match_info["active_away_match"]:
                match_info["away_active_only_in_gr"] = sorted(gr_away_active - v5_away)
                match_info["away_active_only_in_v5"] = sorted(v5_away - gr_away_active)
            if not match_info["active_home_match"]:
                match_info["home_active_only_in_gr"] = sorted(gr_home_active - v5_home)
                match_info["home_active_only_in_v5"] = sorted(v5_home - gr_home_active)
        matches[period_str] = match_info

    return {
        "game_id": game_id,
        "v5_periods": len(v5_game),
        "matches": matches,
    }


def print_report(results: list[dict[str, Any]]) -> None:
    """Print console summary."""
    print("\n" + "=" * 90)
    print("GAMEROTATION PROBE RESULTS")
    print("=" * 90)

    # Summary table
    print(f"\n{'Game ID':<12} {'Label':<45} {'Status':>6} {'Stints':>6} {'Periods':>8}")
    print("-" * 90)
    for r in results:
        game_id = r["game_id"]
        label = r.get("label", "")[:44]
        status = r.get("fetch_status", "?")
        stints = r.get("analysis", {}).get("total_stints", "-")
        periods = r.get("analysis", {}).get("periods_found", [])
        period_str = ",".join(str(p) for p in periods) if periods else "-"
        print(f"{game_id:<12} {label:<45} {status:>6} {stints:>6} {period_str:>8}")

    # Period starter accuracy — two approaches
    print("\n\nPERIOD STARTER EXTRACTION")
    print("-" * 90)
    total_periods = 0
    start_exact_5 = 0
    active_exact_5 = 0
    active_not_5: list[str] = []

    for r in results:
        analysis = r.get("analysis", {})
        for period_str, per_data in analysis.get("period_analysis", {}).items():
            total_periods += 1
            if per_data.get("exactly_5_per_team"):
                start_exact_5 += 1
            if per_data.get("active_exactly_5_per_team"):
                active_exact_5 += 1
            else:
                away_s = per_data.get("away_count", "?")
                home_s = per_data.get("home_count", "?")
                away_a = per_data.get("away_active_count", "?")
                home_a = per_data.get("home_active_count", "?")
                active_not_5.append(
                    f"  {r['game_id']} P{period_str}: "
                    f"start_at={away_s}+{home_s}  active={away_a}+{home_a}"
                )

    print(f"Total period boundaries checked: {total_periods}")
    print(f"Method 1 - IN_TIME == boundary:  {start_exact_5}/{total_periods} "
          f"({start_exact_5*100/max(total_periods,1):.1f}%)")
    print(f"Method 2 - active at boundary:   {active_exact_5}/{total_periods} "
          f"({active_exact_5*100/max(total_periods,1):.1f}%)")
    if active_not_5:
        print(f"\nActive NOT exactly 5 ({len(active_not_5)}):")
        for d in active_not_5[:20]:
            print(d)
        if len(active_not_5) > 20:
            print(f"  ... and {len(active_not_5) - 20} more")

    # V5 cross-validation
    print("\n\nV5 CROSS-VALIDATION (active-at-boundary vs V5)")
    print("-" * 90)
    total_v5 = 0
    active_full_match = 0
    mismatches: list[str] = []

    for r in results:
        xv = r.get("cross_validation", {})
        for period_str, match_info in xv.get("matches", {}).items():
            if not match_info.get("v5_exists"):
                continue
            total_v5 += 1
            if match_info.get("active_full_match"):
                active_full_match += 1
            else:
                away_m = "ok" if match_info.get("active_away_match") else "DIFF"
                home_m = "ok" if match_info.get("active_home_match") else "DIFF"
                ac = f"{match_info.get('away_active_count','?')}+{match_info.get('home_active_count','?')}"
                mismatches.append(
                    f"  {r['game_id']} P{period_str}: away={away_m}, home={home_m}  (counts={ac})"
                )
                if not match_info.get("active_away_match"):
                    mismatches.append(
                        f"    away GR-only: {match_info.get('away_active_only_in_gr')}, "
                        f"V5-only: {match_info.get('away_active_only_in_v5')}"
                    )
                if not match_info.get("active_home_match"):
                    mismatches.append(
                        f"    home GR-only: {match_info.get('home_active_only_in_gr')}, "
                        f"V5-only: {match_info.get('home_active_only_in_v5')}"
                    )

    print(f"Periods compared against V5: {total_v5}")
    print(f"Active full match: {active_full_match} ({active_full_match*100/max(total_v5,1):.1f}%)")
    if mismatches:
        mismatch_periods = len([m for m in mismatches if not m.startswith("    ")])
        print(f"Mismatches ({mismatch_periods} periods):")
        for m in mismatches[:40]:
            print(m)
        if len(mismatches) > 40:
            print(f"  ... and more")

    # Canary deep dive
    print("\n\nCANARY GAME DEEP DIVE")
    print("-" * 90)
    for r in results:
        if r["game_id"] not in CANARY_GAMES:
            continue
        analysis = r.get("analysis", {})
        print(f"\n  {r['game_id']} — {r.get('label', '')}")
        print(f"  Status: {r.get('fetch_status')}, Stints: {analysis.get('total_stints', '?')}")
        for period_str, per_data in analysis.get("period_analysis", {}).items():
            print(f"  Period {period_str}:")
            print(f"    Stints starting at boundary ({per_data.get('away_count', '?')}+"
                  f"{per_data.get('home_count', '?')}):")
            print(f"      Away: {per_data.get('away_player_names', [])}")
            print(f"      Home: {per_data.get('home_player_names', [])}")
            print(f"    Active at boundary ({per_data.get('away_active_count', '?')}+"
                  f"{per_data.get('home_active_count', '?')}):")
            print(f"      Away: {per_data.get('away_active_names', [])}")
            print(f"      Home: {per_data.get('home_active_names', [])}")
            print(f"    Exactly 5 per team (active): "
                  f"{per_data.get('active_exactly_5_per_team', '?')}")

    # Season coverage
    print("\n\nSEASON COVERAGE")
    print("-" * 90)
    for r in results:
        if r["game_id"] in CANARY_GAMES:
            continue
        season = r.get("label", "")
        status = r.get("fetch_status", "?")
        stints = r.get("analysis", {}).get("total_stints", "-")
        periods = r.get("analysis", {}).get("periods_found", [])
        max_period = max(periods) if periods else 0
        per_analyses = r.get("analysis", {}).get("period_analysis", {})
        all_exact = all(
            pa.get("active_exactly_5_per_team", False)
            for pa in per_analyses.values()
        )
        exact_flag = "ALL-5" if all_exact and per_analyses else ("GAPS" if per_analyses else "-")
        print(f"  {season:<12} {r['game_id']}  status={status}  "
              f"stints={stints}  periods=1..{max_period}  starter_quality={exact_flag}")

    sys.stdout.flush()


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load V5 for cross-validation
    v5_path = ROOT / "period_starters_v5.parquet"
    v5 = pd.read_parquet(v5_path) if v5_path.exists() else pd.DataFrame()

    # Build ordered game list: canaries first, then season samples
    games: list[tuple[str, str]] = []
    for gid, label in CANARY_GAMES.items():
        games.append((gid, label))
    for season, gid in sorted(SEASON_SAMPLES.items()):
        if gid not in CANARY_GAMES:
            games.append((gid, f"Season sample {season}"))

    print(f"Probing {len(games)} games via gamerotation endpoint (proxy + {SLEEP_BETWEEN}s sleep)")
    print(f"V5 cross-validation: {'available' if len(v5) > 0 else 'NOT available'}")
    sys.stdout.flush()

    results: list[dict[str, Any]] = []

    for i, (game_id, label) in enumerate(games):
        print(f"\n[{i+1}/{len(games)}] {game_id} — {label} ...", end="", flush=True)

        raw = fetch_gamerotation(game_id)
        status = raw.get("status", "failed")
        print(f" {status}", end="", flush=True)

        entry: dict[str, Any] = {
            "game_id": game_id,
            "label": label,
            "fetch_status": status,
        }

        if status == 200:
            # Save raw response structure info (not full data to keep JSON manageable)
            data = raw["data"]
            entry["content_length"] = raw.get("content_length")
            entry["result_set_names"] = [rs.get("name") for rs in data.get("resultSets", [])]
            entry["result_set_row_counts"] = [
                len(rs.get("rowSet", [])) for rs in data.get("resultSets", [])
            ]

            analysis = analyze_game(raw)
            entry["analysis"] = analysis

            # Cross-validate against V5
            if len(v5) > 0:
                entry["cross_validation"] = cross_validate_v5(analysis, v5)

            n_periods = len(analysis.get("period_analysis", {}))
            active_exact = sum(
                1 for pa in analysis.get("period_analysis", {}).values()
                if pa.get("active_exactly_5_per_team")
            )
            print(f"  stints={analysis.get('total_stints')}  "
                  f"periods={n_periods}  active_exact_5={active_exact}/{n_periods}", flush=True)
        else:
            entry["error"] = raw.get("error", str(status))
            if "body_preview" in raw:
                entry["body_preview"] = raw["body_preview"]
            print(f"  ERROR: {entry['error'][:80]}", flush=True)

        results.append(entry)
        time.sleep(SLEEP_BETWEEN)

    # Write summary JSON
    summary_path = OUTPUT_DIR / "summary.json"
    summary_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nWrote {summary_path}")

    # Print console report
    print_report(results)

    print(f"\nDone. {len(results)} games probed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
