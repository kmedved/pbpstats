from __future__ import annotations

import argparse
import json
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd
import requests
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parent
DEFAULT_PBP_PATH = ROOT / "playbyplayv2.parq"
DEFAULT_OUTPUT_DIR = ROOT / "v3_canary_research_20260317_v4"

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
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/139.0.0.0 Safari/537.36"
    ),
    "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}


DEFAULT_CASES: List[dict[str, Any]] = [
    {
        "game_id": "0029700060",
        "period": 3,
        "label": "1998 POR-HOU false-positive canary",
        "disputed": {
            "POR": {"expected_in": "Rasheed Wallace", "expected_out": "Jermaine O'Neal"},
            "HOU": {"expected_in": "Clyde Drexler", "expected_out": "Matt Bullard"},
        },
    },
    {
        "game_id": "0020401139",
        "period": 5,
        "label": "2005 SAS-LAC helpful OT canary",
        "disputed": {
            "SAS": {"expected_in": "Bruce Bowen", "expected_out": "Brent Barry"},
        },
    },
    {
        "game_id": "0020700319",
        "period": 4,
        "label": "2008 NYK-SEA helpful Q4 canary",
        "disputed": {
            "NYK": {"expected_in": "Fred Jones", "expected_out": "Nate Robinson"},
        },
    },
    {
        "game_id": "0020100162",
        "period": 5,
        "label": "2001 NJN-UTA rescue OT canary",
        "disputed": {
            "NJN": {"expected_in": "Richard Jefferson", "expected_out": "Lucious Harris"},
            "UTA": {"expected_in": "Quincy Lewis", "expected_out": "John Starks"},
        },
    },
    {
        "game_id": "0020400932",
        "period": 3,
        "label": "2005 DET-ATL rescue Q3 canary",
        "disputed": {
            "DET": {"expected_in": "Ben Wallace", "expected_out": "Antonio McDyess"},
        },
    },
    {
        "game_id": "0020000576",
        "period": 5,
        "label": "2001 ORL-SAS OT ghost-plateau canary",
        "disputed": {
            "ORL": {"expected_in": "Andrew DeClercq", "expected_out": "Pat Garrity"},
            "SAS": {"expected_in": "Steve Kerr", "expected_out": "Danny Ferry"},
        },
    },
    {
        "game_id": "0029700438",
        "period": 2,
        "label": "1998 SEA-PHI missing-sub canary",
        "inference_focus": {
            "team": "SEA",
            "player_in": "Detlef Schrempf",
        },
    },
]

DEFAULT_BINARY_SEARCHES: List[dict[str, Any]] = [
    {
        "label": "Detlef Schrempf entry localization",
        "game_id": "0029700438",
        "period": 2,
        "player_id": 96,
        "player_name": "Detlef Schrempf",
        "team_side": "homeTeam",
        "lo_tenths": 100,
        "hi_tenths": 5200,
        "resolution": 50,
    }
]

DEFAULT_MISSING_SUB_CASES: List[dict[str, Any]] = [
    {
        "label": "Detlef Schrempf missing-sub inference",
        "game_id": "0029700438",
        "period": 2,
        "team": "SEA",
        "team_side": "homeTeam",
        "player_in_id": 96,
        "player_in_name": "Detlef Schrempf",
        "anchor_window": 20.0,
        "binary_search_label": "Detlef Schrempf entry localization",
    }
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Research-only scanner for V3 period starter stability. "
            "Queries a small ladder of windows, scores stability, and can "
            "optionally binary-search the first non-zero appearance of a player."
        )
    )
    parser.add_argument("--pbp-path", type=Path, default=DEFAULT_PBP_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--offsets",
        default="0",
        help="Comma-separated StartRange offsets in tenths. Default: 0",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.5,
        help="Delay between successful live requests. Default: 1.5",
    )
    parser.add_argument(
        "--include-pre-sub",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include first_sub - 1 as an extra ladder window when available.",
    )
    parser.add_argument(
        "--run-binary-search",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run the default disputed-player binary-search examples.",
    )
    return parser.parse_args()


def get_proxies() -> dict[str, str]:
    session_id = f"nba_{random.randint(10000, 99999)}"
    user = f"a0feb795d77dbcf7861c_session-{session_id}"
    proxy_url = f"http://{user}:5fe46ff800ae77f1@gw.dataimpulse.com:823"
    return {"http": proxy_url, "https": proxy_url}


def parse_seconds(value: str | None) -> float:
    if not value:
        return 0.0
    match = re.match(r"PT(\d+)M([\d.]+)S", value)
    if match:
        return int(match.group(1)) * 60 + float(match.group(2))
    match = re.match(r"(\d+):([\d.]+)", value)
    if match:
        return int(match.group(1)) * 60 + float(match.group(2))
    return 0.0


def period_start_tenths(period: int) -> int:
    if period == 1:
        return 0
    if period <= 4:
        return 7200 * (period - 1)
    return 4 * 7200 + 3000 * (period - 5)


def clock_to_elapsed(clock: str, period: int) -> float | None:
    try:
        minutes, seconds = str(clock).split(":")
        remaining = int(minutes) * 60 + int(seconds)
    except ValueError:
        return None
    period_length = 720 if period <= 4 else 300
    return float(period_length - remaining)


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def load_pbp_context(
    pbp_path: Path,
    cases: Iterable[dict[str, Any]],
) -> tuple[dict[tuple[str, int], dict[str, Any]], dict[tuple[str, int], pd.DataFrame]]:
    cols = [
        "GAME_ID",
        "PERIOD",
        "EVENTNUM",
        "EVENTMSGTYPE",
        "PCTIMESTRING",
        "HOMEDESCRIPTION",
        "VISITORDESCRIPTION",
        "PLAYER1_ID",
        "PLAYER1_NAME",
        "PLAYER2_ID",
        "PLAYER2_NAME",
        "PLAYER3_ID",
        "PLAYER3_NAME",
        "PLAYER1_TEAM_ABBREVIATION",
        "PLAYER2_TEAM_ABBREVIATION",
        "PLAYER3_TEAM_ABBREVIATION",
    ]
    pbp = pd.read_parquet(pbp_path, columns=cols)
    pbp["GAME_ID"] = pbp["GAME_ID"].map(_normalize_game_id)
    pbp["PERIOD"] = pd.to_numeric(pbp["PERIOD"], errors="coerce")
    pbp["EVENTMSGTYPE"] = pd.to_numeric(pbp["EVENTMSGTYPE"], errors="coerce")

    context: dict[tuple[str, int], dict[str, Any]] = {}
    period_rows: dict[tuple[str, int], pd.DataFrame] = {}
    for case in cases:
        game_id = case["game_id"]
        period = int(case["period"])
        sub = pbp[(pbp["GAME_ID"] == game_id) & (pbp["PERIOD"] == period)].copy()
        sub["elapsed"] = [clock_to_elapsed(t, period) for t in sub["PCTIMESTRING"]]
        sub = sub.sort_values(["elapsed", "EVENTNUM"])
        period_rows[(game_id, period)] = sub.reset_index(drop=True)

        real = sub[~sub["EVENTMSGTYPE"].isin([8, 12, 13])]
        first_event = float(real["elapsed"].min()) if len(real) else None
        subs = sub[sub["EVENTMSGTYPE"] == 8]
        first_sub = float(subs["elapsed"].min()) if len(subs) else None

        if first_sub is None:
            before_first_sub = sub[sub["elapsed"].notna()]
        else:
            before_first_sub = sub[(sub["elapsed"].notna()) & (sub["elapsed"] <= first_sub)]

        seen_by_team: dict[str, list[dict[str, Any]]] = {}
        for _, row in before_first_sub.iterrows():
            for name_col, id_col, team_col in [
                ("PLAYER1_NAME", "PLAYER1_ID", "PLAYER1_TEAM_ABBREVIATION"),
                ("PLAYER2_NAME", "PLAYER2_ID", "PLAYER2_TEAM_ABBREVIATION"),
                ("PLAYER3_NAME", "PLAYER3_ID", "PLAYER3_TEAM_ABBREVIATION"),
            ]:
                name = row.get(name_col)
                player_id = row.get(id_col)
                team = row.get(team_col)
                if pd.notna(name) and str(name).strip() and pd.notna(team) and str(team).strip():
                    seen_by_team.setdefault(str(team).strip(), []).append(
                        {
                            "personId": int(player_id) if pd.notna(player_id) else None,
                            "name": str(name).strip(),
                            "elapsed": row["elapsed"],
                        }
                    )

        deduped_seen: dict[str, list[dict[str, Any]]] = {}
        for team, rows in seen_by_team.items():
            ordered = []
            seen_keys = set()
            for row in sorted(rows, key=lambda item: (item["elapsed"], item["name"])):
                key = (row["personId"], row["name"])
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                ordered.append(row)
            deduped_seen[team] = ordered

        context[(game_id, period)] = {
            "first_event_elapsed": first_event,
            "first_sub_elapsed": first_sub,
            "period_start_tenths": period_start_tenths(period),
            "pbp_seen_before_first_sub": deduped_seen,
        }
    return context, period_rows


def build_ladder_windows(first_sub_elapsed: float | None, include_pre_sub: bool) -> list[float]:
    windows: list[float] = [10.0, 20.0, 30.0]
    anchor = 60.0 if first_sub_elapsed is None else min(60.0, first_sub_elapsed - 1.0)
    if anchor > 0:
        windows.append(anchor)
    if include_pre_sub and first_sub_elapsed is not None:
        pre_sub = first_sub_elapsed - 1.0
        if pre_sub > 0:
            windows.append(pre_sub)
    deduped: list[float] = []
    for window in windows:
        rounded = round(window, 1)
        if rounded > 0 and rounded not in deduped:
            deduped.append(rounded)
    return deduped


def fetch_window(
    game_id: str,
    period: int,
    window_seconds: float,
    start_offset: int = 0,
    retries: int = 3,
    sleep_seconds: float = 1.5,
) -> dict[str, Any]:
    start_range = period_start_tenths(period) + start_offset
    end_range = start_range + int(window_seconds * 10)
    params = {
        "GameID": game_id,
        "StartPeriod": 0,
        "EndPeriod": 0,
        "RangeType": 2,
        "StartRange": start_range,
        "EndRange": end_range,
    }

    error_text = None
    for attempt in range(retries):
        try:
            resp = requests.get(
                URL,
                params=params,
                headers=API_HEADERS,
                proxies=get_proxies(),
                verify=False,
                timeout=45,
            )
            if resp.status_code == 200:
                data = resp.json()["boxScoreTraditional"]
                rows = []
                for team_key in ["awayTeam", "homeTeam"]:
                    team = data.get(team_key, {})
                    tricode = team.get("teamTricode", "")
                    for player in team.get("players", []):
                        minutes = player.get("statistics", {}).get("minutes", "")
                        rows.append(
                            {
                                "team": tricode,
                                "team_side": team_key,
                                "personId": player.get("personId"),
                                "name": (
                                    f"{player.get('firstName', '')} "
                                    f"{player.get('familyName', '')}"
                                ).strip(),
                                "seconds": parse_seconds(minutes),
                                "raw_minutes": minutes,
                            }
                        )
                return {
                    "status": 200,
                    "game_id": game_id,
                    "period": period,
                    "window_seconds": window_seconds,
                    "start_offset": start_offset,
                    "start_range": start_range,
                    "end_range": end_range,
                    "players": rows,
                }
            error_text = f"HTTP {resp.status_code}"
            if resp.status_code in (403, 429):
                time.sleep(max(sleep_seconds * 2, 3.0) * (attempt + 1))
                continue
            break
        except requests.exceptions.RequestException as exc:
            error_text = f"{type(exc).__name__}: {exc}"
            time.sleep(max(sleep_seconds * 2, 3.0) * (attempt + 1))
    return {
        "status": error_text or "failed",
        "game_id": game_id,
        "period": period,
        "window_seconds": window_seconds,
        "start_offset": start_offset,
        "start_range": start_range,
        "end_range": end_range,
        "players": None,
    }


def summarize_players(players: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for team in sorted({row["team"] for row in players}):
        team_players = sorted(
            [row for row in players if row["team"] == team],
            key=lambda item: (-item["seconds"], item["name"]),
        )
        gap = None
        if len(team_players) > 5:
            gap = team_players[4]["seconds"] - team_players[5]["seconds"]
        summary[team] = {
            "top6": [
                {
                    "personId": row["personId"],
                    "name": row["name"],
                    "seconds": row["seconds"],
                }
                for row in team_players[:6]
            ],
            "nonzero_count": sum(1 for row in team_players if row["seconds"] > 0),
            "gap_5_6": gap,
        }
    return summary


def top5_sets(entry: dict[str, Any]) -> dict[str, tuple[str, ...]]:
    return {
        team: tuple(sorted(player["name"] for player in info["top6"][:5]))
        for team, info in entry["summary"].items()
    }


def _find_anchor_pairs(entries_by_window: dict[float, dict[str, Any]]) -> list[dict[str, Any]]:
    available = sorted(entries_by_window)
    if not available:
        return []

    pairs: list[dict[str, Any]] = []

    def _same(a: float, b: float) -> bool:
        return top5_sets(entries_by_window[a]) == top5_sets(entries_by_window[b])

    if 30.0 in entries_by_window:
        later = [window for window in available if window > 30.0]
        for window in later:
            if _same(30.0, window):
                pairs.append({"earliest_window": 30.0, "confirm_window": window, "rule": "30_to_later"})
                break

    if 20.0 in entries_by_window and 30.0 in entries_by_window and _same(20.0, 30.0):
        pairs.append({"earliest_window": 20.0, "confirm_window": 30.0, "rule": "20_to_30"})

    for idx, window in enumerate(available[:-1]):
        current = top5_sets(entries_by_window[window])
        if any(top5_sets(entries_by_window[later]) == current for later in available[idx + 1 :]):
            pairs.append(
                {
                    "earliest_window": window,
                    "confirm_window": next(
                        later
                        for later in available[idx + 1 :]
                        if top5_sets(entries_by_window[later]) == current
                    ),
                    "rule": "earliest_repeat",
                }
            )
            break
    return pairs


def evaluate_pbp_support(selected_entry: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    support: dict[str, Any] = {}
    seen_before_first_sub = context["pbp_seen_before_first_sub"]
    for team, info in selected_entry["summary"].items():
        seen_rows = seen_before_first_sub.get(team, [])
        seen_names = {row["name"] for row in seen_rows}
        top5_names = {player["name"] for player in info["top6"][:5]}
        support[team] = {
            "seen_before_first_sub_names": sorted(seen_names),
            "seen_before_first_sub_count": len(seen_names),
            "top5_unseen_in_pbp": sorted(top5_names - seen_names),
            "pbp_seen_but_omitted_from_top5": sorted(seen_names - top5_names),
        }
    return support


def _player_event_records(
    period_rows: pd.DataFrame,
    player_name: str,
    elapsed_threshold: float,
    end_elapsed: float | None = None,
) -> list[dict[str, Any]]:
    mask = (
        period_rows["elapsed"].notna()
        & (period_rows["elapsed"] >= elapsed_threshold)
        & (
            period_rows["PLAYER1_NAME"].eq(player_name)
            | period_rows["PLAYER2_NAME"].eq(player_name)
            | period_rows["PLAYER3_NAME"].eq(player_name)
        )
    )
    if end_elapsed is not None:
        mask = mask & (period_rows["elapsed"] <= end_elapsed)
    rows = []
    for _, row in period_rows.loc[mask].sort_values(["elapsed", "EVENTNUM"]).iterrows():
        matched_role = None
        if row["PLAYER1_NAME"] == player_name:
            matched_role = "player1"
        elif row["PLAYER2_NAME"] == player_name:
            matched_role = "player2"
        elif row["PLAYER3_NAME"] == player_name:
            matched_role = "player3"
        event_type = int(row["EVENTMSGTYPE"]) if pd.notna(row["EVENTMSGTYPE"]) else None
        counts_as_on_floor = not (event_type == 8 and matched_role == "player2")
        rows.append(
            {
                "elapsed": row["elapsed"],
                "clock": row["PCTIMESTRING"],
                "eventnum": int(row["EVENTNUM"]) if pd.notna(row["EVENTNUM"]) else None,
                "event_type": event_type,
                "matched_role": matched_role,
                "counts_as_on_floor": counts_as_on_floor,
                "home_description": row["HOMEDESCRIPTION"] if pd.notna(row["HOMEDESCRIPTION"]) else None,
                "visitor_description": (
                    row["VISITORDESCRIPTION"] if pd.notna(row["VISITORDESCRIPTION"]) else None
                ),
            }
        )
    return rows


def _row_team_abbreviation(row: pd.Series) -> str | None:
    for col in [
        "PLAYER1_TEAM_ABBREVIATION",
        "PLAYER2_TEAM_ABBREVIATION",
        "PLAYER3_TEAM_ABBREVIATION",
    ]:
        value = row.get(col)
        if pd.notna(value) and str(value).strip():
            return str(value).strip()
    return None


def _first_team_sub_after(
    period_rows: pd.DataFrame,
    team: str,
    elapsed_threshold: float,
) -> float | None:
    sub_mask = (
        period_rows["EVENTMSGTYPE"].eq(8)
        & period_rows["elapsed"].notna()
        & (period_rows["elapsed"] >= elapsed_threshold)
    )
    subs = period_rows.loc[sub_mask].copy()
    if len(subs) == 0:
        return None
    subs["row_team"] = [_row_team_abbreviation(row) for _, row in subs.iterrows()]
    team_subs = subs[subs["row_team"] == team]
    if len(team_subs) == 0:
        return None
    return float(team_subs["elapsed"].min())


def evaluate_disputed_future_actions(
    case: dict[str, Any],
    selected_window: float | None,
    period_rows: pd.DataFrame,
) -> dict[str, Any]:
    if selected_window is None or not case.get("disputed"):
        return {}

    support: dict[str, Any] = {}
    for team, dispute in case["disputed"].items():
        expected_in = dispute["expected_in"]
        expected_out = dispute["expected_out"]
        support_end = _first_team_sub_after(period_rows, team, selected_window)
        in_events = _player_event_records(period_rows, expected_in, selected_window, end_elapsed=support_end)
        out_events = _player_event_records(period_rows, expected_out, selected_window, end_elapsed=support_end)
        in_on_floor = [event for event in in_events if event["counts_as_on_floor"]]
        out_on_floor = [event for event in out_events if event["counts_as_on_floor"]]

        if in_on_floor and not out_on_floor:
            verdict = "strong_support_expected_in"
        elif out_on_floor and not in_on_floor:
            verdict = "contradicts_expected_in"
        elif in_on_floor or out_on_floor:
            verdict = "mixed_or_ambiguous"
        else:
            verdict = "no_future_action_signal"

        support[team] = {
            "expected_in": expected_in,
            "expected_out": expected_out,
            "support_end_elapsed": support_end,
            "in_event_count": len(in_on_floor),
            "out_event_count": len(out_on_floor),
            "in_events": in_events[:5],
            "out_events": out_events[:5],
            "verdict": verdict,
        }
    return support


def combine_case_confidence(
    stability: dict[str, Any],
    future_action_support: dict[str, Any],
) -> dict[str, Any]:
    support_verdicts = [info["verdict"] for info in future_action_support.values()]
    has_contradiction = any(verdict == "contradicts_expected_in" for verdict in support_verdicts)
    has_strong_support = any(verdict == "strong_support_expected_in" for verdict in support_verdicts)
    has_mixed = any(verdict == "mixed_or_ambiguous" for verdict in support_verdicts)

    reasons = [f"stability={stability['verdict']}"]
    if has_contradiction:
        reasons.append("future_actions=contradiction")
    elif has_strong_support:
        reasons.append("future_actions=strong_support")
    elif has_mixed:
        reasons.append("future_actions=mixed")
    elif support_verdicts:
        reasons.append("future_actions=no_signal")

    if stability["verdict"] == "no_stable_signal":
        verdict = "low"
    elif has_contradiction:
        verdict = "low"
    elif stability["verdict"] == "stable_candidate" and has_strong_support:
        verdict = "high"
    elif stability["verdict"] == "stable_candidate":
        verdict = "medium"
    elif has_strong_support:
        verdict = "medium"
    else:
        verdict = "low"

    return {"verdict": verdict, "reasons": reasons}


def score_stability_rule(entries: list[dict[str, Any]], context: dict[str, Any]) -> dict[str, Any]:
    successful = [entry for entry in entries if entry.get("summary")]
    entries_by_window = {entry["window_seconds"]: entry for entry in successful if entry["start_offset"] == 0}
    anchor_pairs = _find_anchor_pairs(entries_by_window)
    selected_pair = None

    for pair in anchor_pairs:
        if pair["rule"] == "20_to_30":
            selected_pair = pair
            break
    if selected_pair is None:
        for pair in anchor_pairs:
            if pair["rule"] == "30_to_later":
                selected_pair = pair
                break
    if selected_pair is None:
        for pair in anchor_pairs:
            if pair["rule"] == "earliest_repeat":
                selected_pair = pair
                break

    ten_precedes_first_event = (
        context["first_event_elapsed"] is not None and 10.0 < context["first_event_elapsed"]
    )
    early_flip = False
    meaningful_early_flip = False
    if selected_pair is not None and 10.0 in entries_by_window:
        early_flip = top5_sets(entries_by_window[10.0]) != top5_sets(entries_by_window[selected_pair["earliest_window"]])
        meaningful_early_flip = early_flip and not ten_precedes_first_event

    result: dict[str, Any] = {
        "available_windows": sorted(entries_by_window),
        "anchor_pairs": anchor_pairs,
        "selected_pair": selected_pair,
        "ten_precedes_first_event": ten_precedes_first_event,
        "early_flip_against_selected": early_flip,
        "meaningful_early_flip": meaningful_early_flip,
    }

    if selected_pair is None:
        result["verdict"] = "no_stable_signal"
        return result

    selected_entry = entries_by_window[selected_pair["earliest_window"]]
    result["selected_window"] = selected_pair["earliest_window"]
    result["selected_summary"] = selected_entry["summary"]
    result["pbp_support"] = evaluate_pbp_support(selected_entry, context)
    result["verdict"] = "stable_but_needs_confirmation" if meaningful_early_flip else "stable_candidate"
    return result


def fetch_team_minutes(
    game_id: str,
    period: int,
    end_elapsed_tenths: int,
    team_side: str,
    sleep_seconds: float,
    retries: int = 3,
) -> dict[int, dict[str, Any]] | None:
    start = period_start_tenths(period)
    end = start + end_elapsed_tenths
    for attempt in range(retries):
        try:
            resp = requests.get(
                URL,
                params={
                    "GameID": game_id,
                    "StartPeriod": 0,
                    "EndPeriod": 0,
                    "RangeType": 2,
                    "StartRange": start,
                    "EndRange": end,
                },
                headers=API_HEADERS,
                proxies=get_proxies(),
                verify=False,
                timeout=30,
            )
            if resp.status_code == 200:
                team = resp.json()["boxScoreTraditional"][team_side]
                return {
                    player["personId"]: {
                        "name": (
                            f"{player.get('firstName', '')} "
                            f"{player.get('familyName', '')}"
                        ).strip(),
                        "secs": parse_seconds(player.get("statistics", {}).get("minutes", "")),
                    }
                    for player in team.get("players", [])
                }
        except requests.exceptions.RequestException:
            pass
        time.sleep(max(sleep_seconds, 0.8) * (attempt + 1))
    return None


def binary_search_entry(search: dict[str, Any], sleep_seconds: float) -> dict[str, Any]:
    game_id = search["game_id"]
    period = int(search["period"])
    player_id = int(search["player_id"])
    team_side = search["team_side"]
    lo_tenths = int(search["lo_tenths"])
    hi_tenths = int(search["hi_tenths"])
    resolution = int(search.get("resolution", 100))

    snap_lo = fetch_team_minutes(game_id, period, lo_tenths, team_side, sleep_seconds=sleep_seconds)
    time.sleep(max(sleep_seconds, 0.8))
    if snap_lo and snap_lo.get(player_id, {}).get("secs", 0) > 0:
        return {"search": search, "entry_tenths": lo_tenths, "notes": ["already_present_at_lo"]}

    snap_hi = fetch_team_minutes(game_id, period, hi_tenths, team_side, sleep_seconds=sleep_seconds)
    time.sleep(max(sleep_seconds, 0.8))
    if snap_hi is None or snap_hi.get(player_id, {}).get("secs", 0) == 0:
        return {"search": search, "entry_tenths": None, "notes": ["absent_at_hi"]}

    notes: list[str] = []
    pre_entry = snap_lo
    probes = 0
    while (hi_tenths - lo_tenths) > resolution:
        mid = (lo_tenths + hi_tenths) // 2
        mid = (mid // resolution) * resolution
        if mid <= lo_tenths:
            mid = lo_tenths + resolution

        snap_mid = fetch_team_minutes(game_id, period, mid, team_side, sleep_seconds=sleep_seconds)
        time.sleep(max(sleep_seconds, 0.8))
        probes += 1

        if snap_mid is None:
            notes.append(f"probe_failed_at_{mid}")
            continue

        if snap_mid.get(player_id, {}).get("secs", 0) > 0:
            hi_tenths = mid
            snap_hi = snap_mid
        else:
            lo_tenths = mid
            pre_entry = snap_mid

    outgoing_candidates: list[dict[str, Any]] = []
    if pre_entry and snap_hi:
        for pid, before in pre_entry.items():
            if pid == player_id:
                continue
            after = snap_hi.get(pid, {"name": before["name"], "secs": 0.0})
            if before["secs"] > 0 and after["secs"] == before["secs"]:
                outgoing_candidates.append(
                    {
                        "personId": pid,
                        "name": before["name"],
                        "secs_before": before["secs"],
                        "secs_at_entry": after["secs"],
                    }
                )
    outgoing_candidates.sort(key=lambda row: (-row["secs_before"], row["name"]))

    return {
        "search": search,
        "entry_tenths": hi_tenths,
        "entry_seconds": hi_tenths / 10.0,
        "player_at_entry": snap_hi.get(player_id) if snap_hi else None,
        "outgoing_candidates": outgoing_candidates,
        "snapshot_before": pre_entry,
        "snapshot_at_entry": snap_hi,
        "notes": notes + [f"probes={probes}"],
    }


def _team_top5_from_case(case_result: dict[str, Any], team: str, preferred_window: float) -> dict[str, Any] | None:
    snapshots = sorted(
        [
            snap
            for snap in case_result["snapshots"]
            if snap.get("summary") and snap["start_offset"] == 0 and team in snap["summary"]
        ],
        key=lambda item: item["window_seconds"],
    )
    exact = next((snap for snap in snapshots if snap["window_seconds"] == preferred_window), None)
    if exact is not None:
        return exact
    earlier = [snap for snap in snapshots if snap["window_seconds"] <= preferred_window]
    if earlier:
        return earlier[-1]
    return snapshots[0] if snapshots else None


def _apply_logged_subs(
    starting_lineup: list[str],
    period_rows: pd.DataFrame,
    team: str,
    up_to_elapsed: float,
) -> tuple[list[str], list[dict[str, Any]]]:
    lineup = list(starting_lineup)
    trace: list[dict[str, Any]] = []
    sub_mask = (
        period_rows["EVENTMSGTYPE"].eq(8)
        & period_rows["elapsed"].notna()
        & (period_rows["elapsed"] < up_to_elapsed)
    )
    subs = period_rows.loc[sub_mask].sort_values(["elapsed", "EVENTNUM"])
    for _, row in subs.iterrows():
        row_team = None
        for col in [
            "PLAYER1_TEAM_ABBREVIATION",
            "PLAYER2_TEAM_ABBREVIATION",
            "PLAYER3_TEAM_ABBREVIATION",
        ]:
            value = row.get(col)
            if pd.notna(value) and str(value).strip():
                row_team = str(value).strip()
                break
        if row_team != team:
            continue

        outgoing = str(row["PLAYER1_NAME"]).strip() if pd.notna(row["PLAYER1_NAME"]) else None
        incoming = str(row["PLAYER2_NAME"]).strip() if pd.notna(row["PLAYER2_NAME"]) else None
        before = list(lineup)
        if outgoing and outgoing in lineup:
            lineup[lineup.index(outgoing)] = incoming
        elif incoming and incoming not in lineup and len(lineup) < 5:
            lineup.append(incoming)
        trace.append(
            {
                "clock": row["PCTIMESTRING"],
                "elapsed": row["elapsed"],
                "eventnum": int(row["EVENTNUM"]) if pd.notna(row["EVENTNUM"]) else None,
                "outgoing": outgoing,
                "incoming": incoming,
                "lineup_before": before,
                "lineup_after": list(lineup),
            }
        )
    return lineup, trace


def _future_action_summary(
    period_rows: pd.DataFrame,
    players: list[str],
    elapsed_threshold: float,
) -> list[dict[str, Any]]:
    rows = []
    for name in players:
        events = _player_event_records(period_rows, name, elapsed_threshold)
        rows.append(
            {
                "name": name,
                "event_count": len(events),
                "first_event": events[0] if events else None,
                "last_event": events[-1] if events else None,
            }
        )
    return sorted(rows, key=lambda item: (item["event_count"], item["name"]))


def infer_missing_sub_case(
    inference_case: dict[str, Any],
    case_result: dict[str, Any],
    period_rows: pd.DataFrame,
    binary_search_result: dict[str, Any] | None,
) -> dict[str, Any]:
    team = inference_case["team"]
    player_in = inference_case["player_in_name"]
    anchor_window = float(inference_case.get("anchor_window", 20.0))

    anchor_snapshot = _team_top5_from_case(case_result, team, anchor_window)
    if anchor_snapshot is None:
        return {
            "label": inference_case["label"],
            "verdict": "insufficient_data",
            "reason": "no_anchor_snapshot",
        }

    starting_lineup = [
        player["name"] for player in anchor_snapshot["summary"][team]["top6"][:5]
    ]
    entry_seconds = None if binary_search_result is None else binary_search_result.get("entry_seconds")
    if entry_seconds is None:
        return {
            "label": inference_case["label"],
            "verdict": "insufficient_data",
            "reason": "no_entry_time",
            "starting_lineup": starting_lineup,
        }

    logged_lineup, sub_trace = _apply_logged_subs(starting_lineup, period_rows, team, entry_seconds)
    current_official_lineup = [name for name in logged_lineup if name != player_in]
    future_actions = _future_action_summary(
        period_rows,
        current_official_lineup + [player_in],
        entry_seconds,
    )

    lineup_actions = [row for row in future_actions if row["name"] != player_in]
    best_candidates = [row for row in lineup_actions if row["event_count"] == lineup_actions[0]["event_count"]]

    if len(best_candidates) == 1 and best_candidates[0]["event_count"] == 0:
        verdict = "strong_best_fit"
    elif len(best_candidates) == 1:
        verdict = "best_fit_but_not_unique_absence"
    else:
        verdict = "ambiguous"

    return {
        "label": inference_case["label"],
        "game_id": inference_case["game_id"],
        "period": inference_case["period"],
        "team": team,
        "player_in": player_in,
        "entry_seconds": entry_seconds,
        "anchor_window": anchor_snapshot["window_seconds"],
        "starting_lineup": starting_lineup,
        "logged_lineup_before_missing_entry": current_official_lineup,
        "logged_sub_trace": sub_trace,
        "future_actions": future_actions,
        "best_outgoing_candidates": best_candidates,
        "verdict": verdict,
    }


def run_scan(args: argparse.Namespace) -> dict[str, Any]:
    offsets = [int(value) for value in args.offsets.split(",") if value.strip()]
    cases = DEFAULT_CASES
    context, period_rows = load_pbp_context(args.pbp_path, cases)

    request_count = 0
    case_results: list[dict[str, Any]] = []

    for case in cases:
        game_id = case["game_id"]
        period = int(case["period"])
        case_context = context[(game_id, period)]
        windows = build_ladder_windows(case_context["first_sub_elapsed"], include_pre_sub=args.include_pre_sub)

        snapshots: list[dict[str, Any]] = []
        for start_offset in offsets:
            for window in windows:
                snapshot = fetch_window(
                    game_id,
                    period,
                    window,
                    start_offset=start_offset,
                    sleep_seconds=args.sleep_seconds,
                )
                request_count += 1
                if snapshot.get("players"):
                    snapshot["summary"] = summarize_players(snapshot["players"])
                snapshots.append(snapshot)
                time.sleep(args.sleep_seconds)

        stability = score_stability_rule(snapshots, case_context)
        future_action_support = evaluate_disputed_future_actions(
            case,
            stability.get("selected_window"),
            period_rows[(game_id, period)],
        )
        combined_assessment = combine_case_confidence(stability, future_action_support)
        case_results.append(
            {
                "case": case,
                "context": case_context,
                "windows": windows,
                "offsets": offsets,
                "snapshots": snapshots,
                "stability": stability,
                "future_action_support": future_action_support,
                "combined_assessment": combined_assessment,
            }
        )

    binary_results = []
    if args.run_binary_search:
        for search in DEFAULT_BINARY_SEARCHES:
            binary_results.append(binary_search_entry(search, sleep_seconds=args.sleep_seconds))

    binary_by_label = {
        row["search"]["label"]: row
        for row in binary_results
        if row.get("search", {}).get("label")
    }
    case_by_key = {
        (row["case"]["game_id"], int(row["case"]["period"])): row
        for row in case_results
    }
    missing_sub_inference = []
    for inference_case in DEFAULT_MISSING_SUB_CASES:
        key = (inference_case["game_id"], int(inference_case["period"]))
        missing_sub_inference.append(
            infer_missing_sub_case(
                inference_case,
                case_by_key[key],
                period_rows[key],
                binary_by_label.get(inference_case["binary_search_label"]),
            )
        )

    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "request_count": request_count,
        "cases": case_results,
        "binary_searches": binary_results,
        "missing_sub_inference": missing_sub_inference,
    }


def print_summary(result: dict[str, Any]) -> None:
    print(f"Requests: {result['request_count']}")
    for case_result in result["cases"]:
        case = case_result["case"]
        stability = case_result["stability"]
        print(f"\n{case['game_id']} P{case['period']} {case['label']}")
        print(f"  verdict: {stability['verdict']}")
        print(f"  selected_pair: {stability.get('selected_pair')}")
        print(f"  early_flip: {stability.get('early_flip_against_selected')}")
        print(f"  combined: {case_result['combined_assessment']['verdict']}")
    for search in result["binary_searches"]:
        label = search["search"]["label"]
        print(f"\nBinary search: {label}")
        print(f"  entry_seconds: {search.get('entry_seconds')}")
        print(f"  notes: {search.get('notes')}")
    for inference in result.get("missing_sub_inference", []):
        print(f"\nMissing-sub inference: {inference['label']}")
        print(f"  verdict: {inference.get('verdict')}")
        print(f"  best_outgoing_candidates: {inference.get('best_outgoing_candidates')}")


def write_markdown_report(result: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# V3 Canary Research Report",
        "",
        f"Generated: `{result['generated_at']}`",
        "",
        "## Starter Canary Summary",
        "",
        "| Game | Label | V3 Stability | Future-Action Support | Combined |",
        "| --- | --- | --- | --- | --- |",
    ]

    for case_result in result["cases"]:
        case = case_result["case"]
        if not case.get("disputed"):
            continue
        future_verdicts = ", ".join(
            f"{team}:{info['verdict']}"
            for team, info in sorted(case_result["future_action_support"].items())
        ) or "n/a"
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{case['game_id']} P{case['period']}`",
                    case["label"],
                    case_result["stability"]["verdict"],
                    future_verdicts,
                    case_result["combined_assessment"]["verdict"],
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Missing-Sub Inference",
            "",
        ]
    )
    for inference in result.get("missing_sub_inference", []):
        lines.append(f"### {inference['label']}")
        lines.append("")
        lines.append(f"- Verdict: `{inference.get('verdict')}`")
        lines.append(f"- Entry seconds: `{inference.get('entry_seconds')}`")
        lines.append(f"- Anchor window: `{inference.get('anchor_window')}`")
        lines.append(
            "- Starting lineup from early stable V3: "
            + ", ".join(f"`{name}`" for name in inference.get("starting_lineup", []))
        )
        lines.append(
            "- Logged lineup before missing entry: "
            + ", ".join(
                f"`{name}`" for name in inference.get("logged_lineup_before_missing_entry", [])
            )
        )
        candidates = inference.get("best_outgoing_candidates", [])
        if candidates:
            lines.append(
                "- Best outgoing candidates: "
                + ", ".join(
                    f"`{row['name']}` ({row['event_count']} future actions)"
                    for row in candidates
                )
            )
        future_actions = inference.get("future_actions", [])
        if future_actions:
            lines.append("- Future actions after entry:")
            for row in future_actions:
                first_clock = row["first_event"]["clock"] if row["first_event"] else None
                lines.append(
                    f"  - `{row['name']}`: {row['event_count']} events"
                    + (f", first at `{first_clock}`" if first_clock else "")
                )
        lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    result = run_scan(args)
    output_path = args.output_dir / "summary.json"
    report_path = args.output_dir / "report.md"
    output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    write_markdown_report(result, report_path)

    print_summary(result)
    print(f"\nWrote {output_path}")
    print(f"Wrote {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
