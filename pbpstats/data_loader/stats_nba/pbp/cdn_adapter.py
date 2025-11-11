# -*- coding: utf-8 -*-
"""
CDN to Stats v2 adapter for play-by-play data.

Converts CDN liveData actions into stats.nba.com playbyplayv2-compatible rows.
"""
from __future__ import annotations
from typing import Any, Dict, Optional
import re

# ISO8601 duration: PTmmMss(.ff)S
_CLOCK = re.compile(r"^PT(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$")


def iso_to_pctimestring(iso: Optional[str]) -> str:
    """
    Convert ISO8601 duration to v2 PCTIMESTRING format.

    Examples:
        PT11M38.00S -> '11:38'
        PT0M00.50S -> '0:00.5'
        PT12M00S -> '12:00'
        None -> '0:00'

    :param str iso: ISO8601 duration string (e.g., 'PT11M38.00S')
    :returns: v2-style clock string (e.g., '11:38')
    """
    if not iso:
        return "0:00"

    m = _CLOCK.match(iso)
    if not m:
        return "0:00"

    mins = int(m.group(1) or 0)
    secs = float(m.group(2) or 0.0)

    # Format with leading zero for seconds, include decimals if non-zero
    if secs == int(secs):
        # No decimal part
        return f"{mins}:{int(secs):02d}"
    else:
        # Has decimal part - show it
        s = f"{mins}:{secs:05.2f}"
        s = s.rstrip("0").rstrip(".")  # Remove trailing zeros and dot if needed
        return s


# Free throw subtype mapping (CDN subType -> v2 EVENTMSGACTIONTYPE)
FT_MAP = {
    "1of1": 10,
    "1of2": 10,
    "2of2": 11,
    "1of3": 13,
    "2of3": 14,
    "3of3": 15,
    # Technical/flagrant are handled via descriptor
}

# Shot type mapping (CDN subType -> v2 EVENTMSGACTIONTYPE)
SHOT_MAP = {
    "jumpshot": 1,
    "jump shot": 1,
    "layup": 5,
    "dunk": 7,
    "hook": 3,
    "hookshot": 3,
    "hook shot": 3,
    "tipin": 2,
    "tip": 2,
    "alleyoop": 52,
    "alley oop": 52,
    "floatingjumpshot": 1,
    "pullupjumpshot": 1,
    "pullup jump shot": 1,
    "turnaroundjumpshot": 1,
    "turnaround jump shot": 1,
    "fingerroll": 8,
    "finger roll": 8,
    "runninglayup": 5,
    "running layup": 5,
    "drivinglayup": 5,
    "driving layup": 5,
    "drivingdunk": 7,
    "driving dunk": 7,
    "stepbackjumpshot": 1,
    "step back jump shot": 1,
    "fadeawayjumpshot": 1,
    "fadeaway jump shot": 1,
}

# Turnover subtype mapping
TOV_MAP = {
    "badpass": 1,
    "bad pass": 1,
    "lostball": 2,
    "lost ball": 2,
    "traveling": 4,
    "travel": 4,
    "doubledribble": 3,
    "double dribble": 3,
    "shotclock": 11,
    "shot clock": 11,
    "3-second-violation": 7,
    "3secondviolation": 7,
    "3 second violation": 7,
    "outofbounds": 40,
    "out of bounds": 40,
    "offensivefoul": 37,
    "offensive foul": 37,
    "palming": 10,
    "backcourt": 9,
    "back court": 9,
    "offensive goaltending": 8,
    "discontinue dribble": 45,
    "illegal screen": 46,
}

# Foul subtype/descriptor mapping
FOUL_MAP = {
    "shooting": 1,
    "looseball": 5,
    "loose ball": 5,
    "offensive": 2,
    "charge": 3,
    "personal": 1,
    "technical": 11,
    "flagrant-type-1": 14,
    "flagrant1": 14,
    "flagrant type 1": 14,
    "flagrant-type-2": 15,
    "flagrant2": 15,
    "flagrant type 2": 15,
    "away-from-play": 6,
    "awayfromplay": 6,
    "away from play": 6,
    "defensive3second": 9,
    "defensive 3 second": 9,
    "take": 28,
    "take foul": 28,
    "clear path": 29,
    "clearpath": 29,
    "double technical": 16,
    "double personal": 25,
    "double": 25,
}


def map_eventmsgtype(action: Dict[str, Any]) -> Optional[int]:
    """
    Map CDN actionType to v2 EVENTMSGTYPE.

    :param dict action: CDN action dict
    :returns: v2 event message type code (1-18 or None)
    """
    t = (action.get("actionType") or "").lower().strip()
    shot_result = (action.get("shotResult") or "").lower().strip()

    if t in ("2pt", "3pt"):
        return 1 if shot_result == "made" else 2

    return {
        "freethrow": 3,
        "free throw": 3,
        "rebound": 4,
        "turnover": 5,
        "foul": 6,
        "violation": 7,
        "substitution": 8,
        "timeout": 9,
        "jumpball": 10,
        "jump ball": 10,
        "ejection": 11,
        "instantreplay": 18,
        "instant replay": 18,
        "stoppage": None,  # Not a v2 event
        "period": None,  # Handled via subtype below
        "game": None,  # Handled via subtype below
    }.get(t)


def map_eventmsgactiontype(
    action: Dict[str, Any], evt_type: Optional[int]
) -> Optional[int]:
    """
    Map CDN subType/descriptor to v2 EVENTMSGACTIONTYPE.

    :param dict action: CDN action dict
    :param int evt_type: v2 EVENTMSGTYPE from map_eventmsgtype
    :returns: v2 event message action type code (or None)
    """
    t = (action.get("actionType") or "").lower().strip()
    st = (action.get("subType") or "").lower().strip()
    desc = (action.get("descriptor") or "").lower().strip()

    if evt_type in (1, 2):  # FG make/miss
        # Try subtype first, then descriptor
        result = SHOT_MAP.get(st) or SHOT_MAP.get(desc)
        return result

    if evt_type == 3:  # FT
        # Prefer subtype (1of2/2of2/etc.), fall back to descriptor (technical/flagrant)
        result = FT_MAP.get(st)
        if result is not None:
            return result
        # Check descriptor for special FT types
        if "technical" in desc:
            return 16  # Technical FT
        if "flagrant" in desc:
            return 17  # Flagrant FT
        return None

    if evt_type == 5:  # Turnover
        result = TOV_MAP.get(st) or TOV_MAP.get(desc)
        return result

    if evt_type == 6:  # Foul
        result = FOUL_MAP.get(st) or FOUL_MAP.get(desc)
        return result

    # Period/game start/end
    if t in ("period", "game"):
        if st == "start":
            return 12
        elif st == "end":
            return 13
        return None

    return None
