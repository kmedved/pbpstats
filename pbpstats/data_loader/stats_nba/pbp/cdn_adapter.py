# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, Mapping, Optional, Set, Tuple

# ISO8601 duration: PTmmMss(.ff)S
_CLOCK = re.compile(r"^PT(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$")
_log = logging.getLogger(__name__)
_seen_unknown: Set[Tuple[str, str, str]] = set()
_DEFAULT_JSON_PATH = os.path.join(os.path.dirname(__file__), "cdn_maps.json")
_MAP_GROUPS = ("FT_MAP", "SHOT_MAP", "TOV_MAP", "FOUL_MAP", "VIOL_MAP")


def _warn_once(key: Tuple[str, str, str]) -> None:
    if key not in _seen_unknown:
        _seen_unknown.add(key)
        _log.warning("Unmapped PBP subtype: %s", key)


def _canon(value: Optional[str]) -> str:
    """
    Canonicalize subtype/descriptor strings so that common presentation
    variants (spaces, hyphens, underscores, case) map to the same key.
    Examples:
      "Double Dribble" -> "doubledribble"
      "defensive-goaltending" -> "defensivegoaltending"
    """
    if not value:
        return ""
    return value.lower().replace(" ", "").replace("-", "").replace("_", "")


def iso_to_pctimestring(iso: Optional[str]) -> str:
    """PT11M38.00S -> '11:38'; PT0M00.50S -> '0:00.5'; fallback '0:00'"""
    if not iso:
        return "0:00"
    match = _CLOCK.match(iso)
    if not match:
        return "0:00"
    mins = int(match.group(1) or 0)
    secs = float(match.group(2) or 0.0)
    pct = f"{mins}:{secs:05.2f}"
    pct = pct.rstrip("0").rstrip(".")
    if ":" not in pct:
        pct = f"{mins}:00"
    return pct


def map_eventmsgtype(action: Dict[str, Any]) -> Optional[int]:
    t = (action.get("actionType") or "").lower()
    shot_result = (action.get("shotResult") or "").lower()
    if t in ("2pt", "3pt"):
        return 1 if shot_result == "made" else 2
    # Map period/game early so we can avoid warn-once noise for these meta actions.
    if t in ("period", "game"):
        st = (action.get("subType") or "").lower()
        if st == "start":
            return 12
        if st == "end":
            return 13
        return None
    if t == "heave":
        return 1 if shot_result == "made" else 2
    mapping = {
        "freethrow": 3,
        "rebound": 4,
        "turnover": 5,
        "foul": 6,
        "violation": 7,
        "substitution": 8,
        "timeout": 9,
        "jumpball": 10,
        "instantreplay": 18,
        "stoppage": 20,
    }
    if t in mapping:
        return mapping[t]
    # Do not warn for types we handle elsewhere or intentionally filter upstream
    # (period/game handled above; others filtered in web loader). Unknown types
    # still warn once with empty subtype/desc context.
    try:
        _warn_once((t, "", ""))
    except NameError:
        pass  # _warn_once exists in this module; safe guard for import order
    return None


FT_MAP = {"1of1": 12, "1of2": 10, "2of2": 11, "1of3": 13, "2of3": 14, "3of3": 15}
SHOT_MAP = {
    "jumpshot": 1,
    "bankshot": 1,
    "fadeaway": 1,
    "pullup": 1,
    "stepback": 1,
    "layup": 2,
    "fingerroll": 2,
    "drivinglayup": 2,
    "runninglayup": 2,
    "dunk": 3,
    "hook": 4,
    "tipin": 5,
    "tip": 5,
}
TOV_MAP = {
    # Core turnover types
    "badpass": 1,
    "lostball": 2,
    "doubledribble": 3,
    "traveling": 5,
    "shotclock": 9,
    "backcourt": 10,
    "eightsecond": 11,
    "fivesecond": 12,
    "3secondviolation": 13,
    "outofbounds": 15,
    "stepoutofbounds": 15,
    "offensivefoul": 18,
    "palming": 24,
    "carry": 24,
    "carrying": 24,
    "inbound": 29,
    # Offensive BI/GT frequently scored as turnover in feeds
    "offensivegoaltending": 7,
    "offensivebasketinterference": 7,
}
FOUL_MAP = {
    # Personal family
    "shooting": 1,
    "personal": 2,
    "blocking": 2,
    "looseball": 3,
    "offensive": 4,
    "charge": 6,
    # Technicals / flagrants
    "technical": 11,
    "doubletechnical": 14,
    "flagranttype1": 12,
    "flagranttype2": 13,
    # Special situations
    "awayfromplay": 17,
    "clearpath": 20,
    "defensive3second": 22,
    "illegaldefense": 22,
    "take": 30,
    "transitiontake": 30,
    "team": 0,
}
VIOL_MAP = {
    "kickedball": 1,
    "defensivegoaltending": 2,
    "delayofgame": 3,
    "lane": 4,
    "doublelane": 5,
    "jumpballviolation": 6,
}

_BASE_MAPS = {
    "FT_MAP": dict(FT_MAP),
    "SHOT_MAP": dict(SHOT_MAP),
    "TOV_MAP": dict(TOV_MAP),
    "FOUL_MAP": dict(FOUL_MAP),
    "VIOL_MAP": dict(VIOL_MAP),
}


def _merge_group(dst: Dict[str, int], src: Mapping[str, Any]) -> None:
    for key, value in src.items():
        if isinstance(value, int):
            dst[_canon(key)] = value
        else:
            _log.warning("Ignoring non-integer map value for key %r: %r", key, value)


def _load_json_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def _apply_json_maps(
    runtime_maps: Dict[str, Dict[str, int]], maps_blob: Mapping[str, Any]
) -> None:
    for group in _MAP_GROUPS:
        blob = maps_blob.get(group)
        if isinstance(blob, dict):
            _merge_group(runtime_maps[group], blob)
        elif blob is not None:
            _log.warning("Ignoring non-dict mapping for %s in JSON file", group)


def _runtime_maps_reset() -> Dict[str, Dict[str, int]]:
    return {group: dict(_BASE_MAPS[group]) for group in _MAP_GROUPS}


def reload_cdn_maps(paths: Optional[str] = None) -> None:
    """
    Reload runtime mapping tables from packaged JSON + optional overlays.
    Overlays are read from colon/pathsep-separated files passed in explicitly
    or via the `PBPSTATS_CDN_MAPS` environment variable.
    """
    global FT_MAP, SHOT_MAP, TOV_MAP, FOUL_MAP, VIOL_MAP
    runtime = _runtime_maps_reset()
    try:
        if os.path.isfile(_DEFAULT_JSON_PATH):
            _apply_json_maps(runtime, _load_json_file(_DEFAULT_JSON_PATH))
    except Exception as exc:
        _log.warning("Failed loading packaged cdn_maps.json: %s", exc)
    raw_paths = paths if paths is not None else os.getenv("PBPSTATS_CDN_MAPS")
    if raw_paths:
        for path in raw_paths.split(os.pathsep):
            candidate = path.strip()
            if not candidate:
                continue
            try:
                _apply_json_maps(runtime, _load_json_file(candidate))
                _log.info("Applied CDN map overlay: %s", candidate)
            except FileNotFoundError:
                _log.warning("CDN map overlay not found: %s", candidate)
            except Exception as exc:
                _log.warning("Error applying CDN map overlay %s: %s", candidate, exc)
    FT_MAP = runtime["FT_MAP"]
    SHOT_MAP = runtime["SHOT_MAP"]
    TOV_MAP = runtime["TOV_MAP"]
    FOUL_MAP = runtime["FOUL_MAP"]
    VIOL_MAP = runtime["VIOL_MAP"]


reload_cdn_maps()


def map_eventmsgactiontype(
    action: Dict[str, Any], evt_type: Optional[int]
) -> Optional[int]:
    t = _canon(action.get("actionType"))
    st = _canon(action.get("subType"))
    desc = _canon(action.get("descriptor"))

    if evt_type in (1, 2):
        result = SHOT_MAP.get(st) or SHOT_MAP.get(desc)
        if result is not None:
            return result
    elif evt_type == 3:
        result = FT_MAP.get(st) or (
            11 if "technical" in desc else 12 if "flagrant" in desc else None
        )
        if result is not None:
            return result
    elif evt_type == 5:
        result = TOV_MAP.get(st) or TOV_MAP.get(desc)
        if result is not None:
            return result
    elif evt_type in (4, 8, 9, 10):
        # Rebounds, substitutions, timeouts, jump balls do not require subcodes.
        return 0
    elif evt_type == 6:
        result = FOUL_MAP.get(st) or FOUL_MAP.get(desc)
        if result is not None:
            return result
    elif evt_type == 7:
        result = VIOL_MAP.get(st) or VIOL_MAP.get(desc)
        if result is not None:
            return result
    if t in ("period", "game"):
        # Start/end already mapped to EVENTMSGTYPE 12/13; subtype defaults to 0.
        return 0
    if evt_type is not None:
        _warn_once((t, st, desc))
        return 0
    _warn_once((t, st, desc))
    return None


def cdn_to_stats_row(action: Dict[str, Any], game_id: str) -> Dict[str, Any]:
    """
    Convert a CDN liveData action into a Stats v2-style row dict expected by StatsNba items.
    """
    evt_type = map_eventmsgtype(action)
    t = (action.get("actionType") or "").lower()
    st = (action.get("subType") or "").lower()
    if evt_type is None and t in ("period", "game"):
        if st == "start":
            evt_type = 12
        elif st == "end":
            evt_type = 13

    evtmsgactiontype = map_eventmsgactiontype(action, evt_type)

    row: Dict[str, Any] = {
        "GAME_ID": game_id,
        "EVENTNUM": action.get("actionNumber") or action.get("orderNumber"),
        "PERIOD": action.get("period"),
        "PCTIMESTRING": iso_to_pctimestring(action.get("clock")),
        "WCTIMESTRING": action.get("timeActual") or None,
        "EVENTMSGTYPE": evt_type or 0,
        "EVENTMSGACTIONTYPE": evtmsgactiontype or 0,
        "NEUTRALDESCRIPTION": action.get("description") or "",
    }

    score_home = action.get("scoreHome")
    score_away = action.get("scoreAway")
    if score_home is not None and score_away is not None:
        row["SCORE"] = f"{score_home}-{score_away}"
        try:
            diff = int(score_home) - int(score_away)
        except (TypeError, ValueError):
            diff = None
        if diff is not None:
            row["SCOREMARGIN"] = "TIE" if diff == 0 else str(diff)

    if action.get("teamId") is not None:
        row["PLAYER1_TEAM_ID"] = action["teamId"]
    if action.get("personId") is not None:
        row["PLAYER1_ID"] = action["personId"]

    if t in ("2pt", "3pt"):
        if action.get("assistPersonId") is not None:
            row["PLAYER2_ID"] = action["assistPersonId"]
        if action.get("blockPersonId") is not None:
            row["PLAYER3_ID"] = action["blockPersonId"]
    elif t == "turnover":
        if action.get("stealPersonId") is not None:
            row["PLAYER2_ID"] = action["stealPersonId"]
    elif t == "foul":
        if action.get("foulDrawnPersonId") is not None:
            row["PLAYER2_ID"] = action["foulDrawnPersonId"]
    elif t == "jumpball":
        # Capture winner/loser/recovered players to mirror stats v2 fields
        if action.get("jumpBallWonPersonId") is not None:
            row["PLAYER1_ID"] = action["jumpBallWonPersonId"]
        if action.get("jumpBallLostPersonId") is not None:
            row["PLAYER2_ID"] = action["jumpBallLostPersonId"]
        recovered = action.get("jumpBallRecoverdPersonId")
        if recovered is None:
            recovered = action.get("jumpBallRecoveredPersonId")
        if recovered is not None:
            row["PLAYER3_ID"] = recovered
    elif t == "substitution":
        # PLAYER1_ID -> outgoing, PLAYER2_ID -> incoming to match stats v2 convention
        out_pid = action.get("subOutPersonId")
        in_pid = action.get("subInPersonId")
        if out_pid is None and st == "out":
            out_pid = action.get("personId")
        if in_pid is None and st == "in":
            in_pid = action.get("personId")
        if out_pid is not None:
            row["PLAYER1_ID"] = out_pid
        if in_pid is not None:
            row["PLAYER2_ID"] = in_pid

    extras = (
        "x",
        "y",
        "xLegacy",
        "yLegacy",
        "shotDistance",
        "area",
        "areaDetail",
        "isTargetScoreLastPeriod",
        "timeActual",
        "descriptor",
        "qualifiers",
        "personIdsFilter",
        "possession",
        "periodType",
    )
    for key in extras:
        if action.get(key) is not None:
            row[key] = action[key]
    return row
