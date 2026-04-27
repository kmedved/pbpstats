from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd


_PLAYER_SLOTS = (1, 2, 3)
_NAME_SUFFIXES = frozenset({"JR", "SR", "II", "III", "IV", "V"})


@dataclass(frozen=True)
class _RosterPlayer:
    player_id: int
    player_name: str
    last_name: str
    pattern: re.Pattern[str]


def _normalize_text(value: object) -> str:
    text = "" if value is None else str(value)
    return re.sub(r"[^A-Z0-9]+", " ", text.upper()).strip()


def _blank(value: object) -> bool:
    return _normalize_text(value) == ""


def _coerce_int(value: object) -> int | None:
    try:
        value = int(value)
    except (TypeError, ValueError):
        return None
    return value


def _typed_player_id_value(series: pd.Series, player_id: int) -> object:
    if isinstance(series.dtype, pd.StringDtype):
        return str(player_id)
    return player_id


def _typed_team_id_value(series: pd.Series, team_id: int) -> object:
    if isinstance(series.dtype, pd.StringDtype):
        return str(team_id)
    return team_id


def _extract_last_name(full_name: object) -> str:
    parts = [part for part in _normalize_text(full_name).split() if part]
    while parts and parts[-1] in _NAME_SUFFIXES:
        parts.pop()
    return parts[-1] if parts else ""


def _build_roster_by_team(official_boxscore: pd.DataFrame) -> dict[int, list[_RosterPlayer]]:
    required_cols = {"PLAYER_ID", "TEAM_ID", "PLAYER_NAME"}
    if official_boxscore.empty or not required_cols.issubset(official_boxscore.columns):
        return {}

    roster_by_team: dict[int, list[_RosterPlayer]] = {}
    for row in official_boxscore.itertuples(index=False):
        player_id = _coerce_int(getattr(row, "PLAYER_ID", None))
        team_id = _coerce_int(getattr(row, "TEAM_ID", None))
        if not player_id or not team_id:
            continue

        last_name = _extract_last_name(getattr(row, "PLAYER_NAME", ""))
        if not last_name:
            continue

        roster_by_team.setdefault(team_id, []).append(
            _RosterPlayer(
                player_id=player_id,
                player_name=str(getattr(row, "PLAYER_NAME", "")),
                last_name=last_name,
                pattern=re.compile(rf"(?<![A-Z0-9]){re.escape(last_name)}(?![A-Z0-9])"),
            )
        )
    return roster_by_team


def _build_team_by_player_id(official_boxscore: pd.DataFrame) -> dict[int, int]:
    required_cols = {"PLAYER_ID", "TEAM_ID"}
    if official_boxscore.empty or not required_cols.issubset(official_boxscore.columns):
        return {}

    team_by_player_id: dict[int, int] = {}
    for row in official_boxscore.itertuples(index=False):
        player_id = _coerce_int(getattr(row, "PLAYER_ID", None))
        team_id = _coerce_int(getattr(row, "TEAM_ID", None))
        if not player_id or not team_id:
            continue
        team_by_player_id[player_id] = team_id
    return team_by_player_id


def _build_single_missing_roster_aliases(
    game_df: pd.DataFrame,
    roster_by_team: dict[int, list[_RosterPlayer]],
) -> dict[int, dict[int, _RosterPlayer]]:
    if game_df.empty or not roster_by_team:
        return {}

    official_ids_by_team = {
        team_id: {player.player_id for player in players}
        for team_id, players in roster_by_team.items()
    }
    seen_valid_ids_by_team: dict[int, set[int]] = {
        team_id: set() for team_id in roster_by_team
    }
    off_roster_ids_by_team: dict[int, set[int]] = {}

    for _, row in game_df.iterrows():
        for slot in _PLAYER_SLOTS:
            id_col = f"PLAYER{slot}_ID"
            team_col = f"PLAYER{slot}_TEAM_ID"
            if id_col not in game_df.columns or team_col not in game_df.columns:
                continue

            player_id = _coerce_int(row.get(id_col))
            team_id = _coerce_int(row.get(team_col))
            if not player_id or not team_id or player_id >= 1610000000:
                continue

            team_roster_ids = official_ids_by_team.get(team_id)
            if not team_roster_ids:
                continue

            if player_id in team_roster_ids:
                seen_valid_ids_by_team.setdefault(team_id, set()).add(player_id)
            else:
                off_roster_ids_by_team.setdefault(team_id, set()).add(player_id)

    aliases: dict[int, dict[int, _RosterPlayer]] = {}
    for team_id, players in roster_by_team.items():
        missing_players = [
            player
            for player in players
            if player.player_id not in seen_valid_ids_by_team.get(team_id, set())
        ]
        off_roster_ids = off_roster_ids_by_team.get(team_id, set())
        if len(missing_players) != 1 or len(off_roster_ids) != 1:
            continue

        aliases[team_id] = {next(iter(off_roster_ids)): missing_players[0]}

    return aliases


def _row_description_text(row: pd.Series) -> str:
    parts = []
    for column in ("HOMEDESCRIPTION", "VISITORDESCRIPTION", "NEUTRALDESCRIPTION"):
        if column not in row.index:
            continue
        value = row[column]
        if not _blank(value):
            parts.append(str(value))
    return _normalize_text(" ".join(parts))


def _row_description_parts(row: pd.Series) -> list[str]:
    parts = []
    for column in ("HOMEDESCRIPTION", "VISITORDESCRIPTION", "NEUTRALDESCRIPTION"):
        if column not in row.index:
            continue
        value = row[column]
        if not _blank(value):
            parts.append(_normalize_text(value))
    return parts


def _extract_slot_last_name(row: pd.Series, slot: int) -> str | None:
    event_type = _coerce_int(row.get("EVENTMSGTYPE"))
    if event_type is None:
        return None

    parts = _row_description_parts(row)
    if not parts:
        return None

    combined = " ".join(parts)

    if event_type == 8:
        match = re.search(r"\bSUB ([A-Z0-9]+) FOR ([A-Z0-9]+)\b", combined)
        if match:
            if slot == 1:
                return match.group(2)
            if slot == 2:
                return match.group(1)
        return None

    if event_type == 5:
        if slot == 1:
            for part in parts:
                match = re.match(r"([A-Z0-9]+)\b.*\bTURNOVER\b", part)
                if match:
                    return match.group(1)
        if slot == 2:
            for part in parts:
                match = re.match(r"([A-Z0-9]+)\s+STEAL\b", part)
                if match:
                    return match.group(1)
        return None

    if event_type == 2:
        if slot == 1:
            match = re.search(r"\bMISS ([A-Z0-9]+)\b", combined)
            if match:
                return match.group(1)
        if slot == 3:
            match = re.search(r"\b([A-Z0-9]+)\s+BLOCK\b", combined)
            if match:
                return match.group(1)
        return None

    if event_type == 10:
        if slot == 1:
            match = re.search(r"\bJUMP BALL ([A-Z0-9]+)\b", combined)
            if match:
                return match.group(1)
        if slot == 2:
            match = re.search(r"\bVS ([A-Z0-9]+)\b", combined)
            if match:
                return match.group(1)
        if slot == 3:
            match = re.search(r"\bTIP TO ([A-Z0-9]+)\b", combined)
            if match:
                return match.group(1)
        return None

    return None


def normalize_single_game_player_ids(
    game_df: pd.DataFrame,
    official_boxscore: pd.DataFrame,
) -> pd.DataFrame:
    """
    Repair malformed historical player ids and missing team ids using the official game roster.

    The shufinskiy-backed historical feed occasionally stores a blank player name
    with an id that does not appear in the official boxscore roster, even though
    the play description clearly names the player. For those rows, replace the
    bogus id only when the description text uniquely identifies one roster player
    on the same team after excluding already-known valid players from the row.

    Some rebound rows also carry a valid roster player id but omit PLAYER*_TEAM_ID.
    For those rows, restore the missing team id directly from the official roster
    before any id-alias logic runs.
    """
    if game_df.empty:
        return game_df.copy()

    roster_by_team = _build_roster_by_team(official_boxscore)
    team_by_player_id = _build_team_by_player_id(official_boxscore)
    if not roster_by_team:
        return game_df.copy()

    roster_player_ids = {
        roster_player.player_id
        for players in roster_by_team.values()
        for roster_player in players
    }
    single_missing_aliases = _build_single_missing_roster_aliases(
        game_df, roster_by_team
    )

    normalized = game_df.copy()
    for idx, row in normalized.iterrows():
        description_text = _row_description_text(row)

        known_player_ids = set()
        for slot in _PLAYER_SLOTS:
            id_col = f"PLAYER{slot}_ID"
            if id_col not in normalized.columns:
                continue
            player_id = _coerce_int(row[id_col])
            if player_id in roster_player_ids:
                known_player_ids.add(player_id)

        for slot in _PLAYER_SLOTS:
            id_col = f"PLAYER{slot}_ID"
            name_col = f"PLAYER{slot}_NAME"
            team_col = f"PLAYER{slot}_TEAM_ID"
            if id_col not in normalized.columns or team_col not in normalized.columns:
                continue

            player_id = _coerce_int(row[id_col])
            if not player_id:
                continue

            team_id = _coerce_int(row[team_col])
            roster_team_id = team_by_player_id.get(player_id)
            if not team_id and roster_team_id:
                normalized.at[idx, team_col] = _typed_team_id_value(
                    normalized[team_col], roster_team_id
                )
                team_id = roster_team_id

            if player_id in roster_player_ids:
                continue

            if not team_id:
                continue

            alias_player = single_missing_aliases.get(team_id, {}).get(player_id)
            if alias_player is not None and alias_player.player_id not in known_player_ids:
                normalized.at[idx, id_col] = _typed_player_id_value(
                    normalized[id_col], alias_player.player_id
                )
                if name_col in normalized.columns:
                    normalized.at[idx, name_col] = alias_player.player_name
                known_player_ids.add(alias_player.player_id)
                continue

            if name_col in normalized.columns and not _blank(row[name_col]):
                continue
            if not description_text:
                continue

            players = roster_by_team.get(team_id, [])
            extracted_last_name = _extract_slot_last_name(row, slot)
            if extracted_last_name:
                candidates = [
                    roster_player
                    for roster_player in players
                    if roster_player.player_id not in known_player_ids
                    and roster_player.last_name == extracted_last_name
                ]
            else:
                candidates = [
                    roster_player
                    for roster_player in players
                    if roster_player.player_id not in known_player_ids
                    and roster_player.pattern.search(description_text)
                ]
            unique_candidate_ids = {candidate.player_id for candidate in candidates}
            if len(unique_candidate_ids) != 1:
                continue

            replacement = candidates[0]
            normalized.at[idx, id_col] = _typed_player_id_value(normalized[id_col], replacement.player_id)
            if name_col in normalized.columns:
                normalized.at[idx, name_col] = replacement.player_name
            known_player_ids.add(replacement.player_id)

    return normalized
