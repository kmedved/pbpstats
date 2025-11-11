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


def cdn_to_stats_row(action: Dict[str, Any], game_id: str) -> Dict[str, Any]:
    """
    Convert a CDN liveData action into a Stats v2-style row dict expected by StatsNba items.

    :param dict action: CDN action dictionary
    :param str game_id: 10-digit NBA game ID
    :returns: Dict with v2-compatible fields (GAME_ID, EVENTNUM, EVENTMSGTYPE, etc.)
    """
    evt_type = map_eventmsgtype(action)

    # Special handling for period/game start/end
    t = (action.get("actionType") or "").lower().strip()
    st = (action.get("subType") or "").lower().strip()
    desc = (action.get("descriptor") or "").lower().strip()
    if evt_type is None and t in ("period", "game"):
        evt_type = 12 if st == "start" else 13 if st == "end" else None

    # Build base row
    row: Dict[str, Any] = {
        "GAME_ID": game_id,
        "EVENTNUM": action.get("actionNumber") or action.get("orderNumber"),
        "PERIOD": action.get("period"),
        "PCTIMESTRING": iso_to_pctimestring(action.get("clock")),
        "EVENTMSGTYPE": evt_type,
        "EVENTMSGACTIONTYPE": map_eventmsgactiontype(action, evt_type),
    }

    # v2 descriptions are split by team; we supply neutral and let the item build unified description
    row["HOMEDESCRIPTION"] = None
    row["NEUTRALDESCRIPTION"] = action.get("description") or ""
    row["VISITORDESCRIPTION"] = None

    # Scores (v2 kept a single SCORE string). Use home-away order here.
    sh = action.get("scoreHome")
    sa = action.get("scoreAway")
    if sh is not None and sa is not None:
        row["SCORE"] = f"{sh}-{sa}"
        diff = int(sh) - int(sa)
        row["SCOREMARGIN"] = "TIE" if diff == 0 else str(diff)
    else:
        row["SCORE"] = None
        row["SCOREMARGIN"] = None

    # WCTIMESTRING - v2 has this but CDN doesn't provide it; leave None
    row["WCTIMESTRING"] = None

    # Primary actor & team
    if action.get("teamId") is not None:
        row["PLAYER1_TEAM_ID"] = action["teamId"]
    else:
        row["PLAYER1_TEAM_ID"] = None

    if action.get("personId") is not None:
        row["PLAYER1_ID"] = action["personId"]
    else:
        row["PLAYER1_ID"] = None

    # PERSON*TYPE fields - v2 has these (1-7 codes), CDN doesn't have direct mapping
    # Leave as 0 for now (can be enhanced later if needed)
    row["PERSON1TYPE"] = 0
    row["PERSON2TYPE"] = 0
    row["PERSON3TYPE"] = 0

    # PLAYER1 name/team fields - v2 has these but CDN doesn't provide
    # Leave as None (existing code may derive from roster data)
    row["PLAYER1_NAME"] = None
    row["PLAYER1_TEAM_CITY"] = None
    row["PLAYER1_TEAM_NICKNAME"] = None
    row["PLAYER1_TEAM_ABBREVIATION"] = action.get("teamTricode")

    # Initialize PLAYER2/3 fields
    row["PLAYER2_ID"] = None
    row["PLAYER2_NAME"] = None
    row["PLAYER2_TEAM_ID"] = None
    row["PLAYER2_TEAM_CITY"] = None
    row["PLAYER2_TEAM_NICKNAME"] = None
    row["PLAYER2_TEAM_ABBREVIATION"] = None

    row["PLAYER3_ID"] = None
    row["PLAYER3_NAME"] = None
    row["PLAYER3_TEAM_ID"] = None
    row["PLAYER3_TEAM_CITY"] = None
    row["PLAYER3_TEAM_NICKNAME"] = None
    row["PLAYER3_TEAM_ABBREVIATION"] = None

    # Secondary actors by context
    if t in ("2pt", "3pt"):
        # Shots: PLAYER2 = assist, PLAYER3 = block
        if action.get("assistPersonId") is not None:
            row["PLAYER2_ID"] = action["assistPersonId"]
            row["PLAYER2_TEAM_ID"] = action.get("teamId")  # Same team as shooter

        if action.get("blockPersonId") is not None:
            row["PLAYER3_ID"] = action["blockPersonId"]
            # Block is by opponent, don't set team here (may need opponent team ID)

    elif t == "turnover":
        # Turnover: PLAYER2 = steal
        if action.get("stealPersonId") is not None:
            row["PLAYER2_ID"] = action["stealPersonId"]

    elif t == "foul":
        # Foul: PLAYER2 = foul drawn
        if action.get("foulDrawnPersonId") is not None:
            row["PLAYER2_ID"] = action["foulDrawnPersonId"]

    elif t in ("jumpball", "jump ball"):
        # Jump ball: store recovered as PLAYER3 to match v2 convention
        if action.get("jumpBallWonPersonId") is not None:
            row["PLAYER2_ID"] = action["jumpBallWonPersonId"]
        if action.get("jumpBallRecoverdPersonId") is not None:
            row["PLAYER3_ID"] = action["jumpBallRecoverdPersonId"]

    # VIDEO_AVAILABLE_FLAG - v2 has this, default to 0
    row["VIDEO_AVAILABLE_FLAG"] = 0

    # Non-breaking extras for future analytics (pass through CDN-specific fields)
    for k in (
        "x",
        "y",
        "xLegacy",
        "yLegacy",
        "shotDistance",
        "area",
        "areaDetail",
        "isTargetScoreLastPeriod",
        "timeActual",
        "orderNumber",
        "qualifiers",
        "descriptor",
        "subType",
        "edited",
        "shotActionNumber",
    ):
        if action.get(k) is not None:
            row[k] = action[k]

    # Add helper boolean flags for easier downstream processing
    # These supplement EVENTMSGACTIONTYPE when subtype codes are missing
    _add_helper_flags(row, action, evt_type, t, st, desc)

    return row


def _add_helper_flags(
    row: Dict[str, Any],
    action: Dict[str, Any],
    evt_type: Optional[int],
    action_type: str,
    subtype: str,
    descriptor: str,
):
    """
    Add boolean helper flags to assist possession logic and analytics.

    These flags bridge gaps where EVENTMSGACTIONTYPE may be None but we can
    infer the event type from CDN fields.
    """
    # Foul flags
    if evt_type == 6:  # Foul
        row["is_shooting_foul"] = subtype == "shooting" or "shooting" in descriptor
        row["is_offensive_foul"] = (
            subtype == "offensive"
            or subtype == "charge"
            or "offensive" in descriptor
            or "charge" in descriptor
        )
        row["is_loose_ball_foul"] = (
            subtype == "looseball"
            or subtype == "loose ball"
            or "loose ball" in descriptor
        )
        row["is_charge"] = subtype == "charge" or "charge" in descriptor
        row["is_technical"] = subtype == "technical" or "technical" in descriptor
        row["is_flagrant"] = "flagrant" in subtype or "flagrant" in descriptor
        row["is_away_from_play_foul"] = (
            "away-from-play" in subtype
            or "awayfromplay" in subtype
            or "away from play" in descriptor
        )
        row["is_defensive_3_seconds"] = (
            "defensive3second" in subtype or "defensive 3 second" in descriptor
        )
        row["is_transition_take_foul"] = "take" in subtype or "take foul" in descriptor

    # Free throw flags
    if evt_type == 3:  # FT
        row["is_end_ft"] = subtype in ("1of1", "2of2", "3of3")
        # Determine num_ft_for_trip from subtype
        if "1of1" in subtype:
            row["num_ft_for_trip"] = 1
        elif "2of2" in subtype or "1of2" in subtype:
            row["num_ft_for_trip"] = 2
        elif "3of3" in subtype or "2of3" in subtype or "1of3" in subtype:
            row["num_ft_for_trip"] = 3
        else:
            row["num_ft_for_trip"] = None

        row["is_technical_ft"] = "technical" in descriptor
        row["is_flagrant_ft"] = "flagrant" in descriptor

    # Target score flag (from CDN isTargetScoreLastPeriod)
    if action.get("isTargetScoreLastPeriod") is not None:
        row["is_target_score_last_period"] = action["isTargetScoreLastPeriod"]
