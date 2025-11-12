# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import re
from typing import Any, Dict, Optional, Set, Tuple

# ISO8601 duration: PTmmMss(.ff)S
_CLOCK = re.compile(r"^PT(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$")
_log = logging.getLogger(__name__)
_seen_unknown: Set[Tuple[str, str, str]] = set()


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
    }
    if t in mapping:
        return mapping[t]
    # Unknown event type; warn once with empty subtype/desc context
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
    elif evt_type == 6:
        result = FOUL_MAP.get(st) or FOUL_MAP.get(desc)
        if result is not None:
            return result
    elif evt_type == 7:
        result = VIOL_MAP.get(st) or VIOL_MAP.get(desc)
        if result is not None:
            return result
    if t in ("period", "game"):
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
        if action.get("jumpBallRecoverdPersonId") is not None:
            row["PLAYER3_ID"] = action["jumpBallRecoverdPersonId"]

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
    )
    for key in extras:
        if action.get(key) is not None:
            row[key] = action[key]
    return row
