# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from typing import Any, Dict, Optional

# ISO8601 duration: PTmmMss(.ff)S
_CLOCK = re.compile(r"^PT(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$")


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
        "heave": 2,
    }
    return mapping.get(t)


FT_MAP = {"1of1": 12, "1of2": 10, "2of2": 11, "1of3": 13, "2of3": 14, "3of3": 15}
SHOT_MAP = {"jumpshot": 1, "layup": 2, "dunk": 3, "hook": 4, "tipin": 5}
TOV_MAP = {
    "badpass": 1,
    "lostball": 2,
    "traveling": 5,
    "shotclock": 9,
    "3-second-violation": 13,
    "outofbounds": 15,
    "offensivefoul": 18,
    "palming": 24,
}
FOUL_MAP = {
    "shooting": 1,
    "looseball": 3,
    "offensive": 4,
    "charge": 6,
    "technical": 11,
    "flagrant-type-1": 12,
    "flagrant-type-2": 13,
    "away-from-play": 17,
    "defensive3second": 22,
    "take": 30,
}


def map_eventmsgactiontype(
    action: Dict[str, Any], evt_type: Optional[int]
) -> Optional[int]:
    t = (action.get("actionType") or "").lower()
    st = (action.get("subType") or "").lower()
    desc = (action.get("descriptor") or "").lower()

    if evt_type in (1, 2):
        return SHOT_MAP.get(st)
    if evt_type == 3:
        return FT_MAP.get(st) or (
            11 if "technical" in desc else 12 if "flagrant" in desc else None
        )
    if evt_type == 5:
        return TOV_MAP.get(st)
    if evt_type == 6:
        return FOUL_MAP.get(st) or FOUL_MAP.get(desc)
    if t == "period":
        if st == "start":
            return 12
        if st == "end":
            return 13
    if t == "game":
        if st == "start":
            return 12
        if st == "end":
            return 13
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

    row: Dict[str, Any] = {
        "GAME_ID": game_id,
        "EVENTNUM": action.get("actionNumber") or action.get("orderNumber"),
        "PERIOD": action.get("period"),
        "PCTIMESTRING": iso_to_pctimestring(action.get("clock")),
        "EVENTMSGTYPE": evt_type,
        "EVENTMSGACTIONTYPE": map_eventmsgactiontype(action, evt_type),
        "NEUTRALDESCRIPTION": action.get("description"),
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
