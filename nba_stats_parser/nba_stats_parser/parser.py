from __future__ import annotations

from typing import Dict, List

from . import models

EVENT_TYPE_MAP = {
    1: models.FieldGoal,
    2: models.FieldGoal,
    3: models.FreeThrow,
    4: models.Rebound,
    5: models.Turnover,
    6: models.Foul,
    7: models.Violation,
    8: models.Substitution,
    9: models.Timeout,
    10: models.JumpBall,
    11: models.Ejection,
    12: models.StartOfPeriod,
    13: models.EndOfPeriod,
    18: models.Replay,
}


def _build_possessions(events: List[models.Event]) -> List[models.Possession]:
    possessions = []
    current = []
    for event in events:
        current.append(event)
        if event.is_possession_ending_event:
            possessions.append(models.Possession(current))
            current = []
    if current:
        possessions.append(models.Possession(current))
    for i, poss in enumerate(possessions):
        if i > 0:
            poss.previous_possession = possessions[i - 1]
            possessions[i - 1].next_possession = poss
    return possessions


def parse_game_data(
    game_id: str,
    raw_pbp: Dict,
    raw_boxscore: Dict,
    raw_shot_charts: Dict,
    overrides_dir: str | None = None,
) -> List[models.Possession]:
    rows = raw_pbp.get("PlayByPlay", [])
    events: List[models.Event] = []
    for order, row in enumerate(rows):
        cls = EVENT_TYPE_MAP.get(row.get("EVENTMSGTYPE"), models.Event)
        evt = cls(row, order)
        events.append(evt)
    for i, evt in enumerate(events):
        if i > 0:
            evt.previous_event = events[i - 1]
            events[i - 1].next_event = evt

    # populate FT is_made cache before rebounds query it
    for evt in events:
        if isinstance(evt, models.FreeThrow):
            _ = evt.is_made
    # starters from boxscore
    starters_by_team: Dict[int, List[int]] = {}
    for row in raw_boxscore.get("PlayerStats", []):
        if row.get("START_POSITION"):
            starters_by_team.setdefault(row["TEAM_ID"], []).append(row["PLAYER_ID"])
    current_players = {tid: list(players) for tid, players in starters_by_team.items()}
    for evt in events:
        if isinstance(evt, models.StartOfPeriod):
            evt._current_players = {
                tid: list(players) for tid, players in current_players.items()
            }
        elif isinstance(evt, models.Substitution):
            current_players[evt.team_id] = [
                evt.incoming_player_id if p == evt.outgoing_player_id else p
                for p in current_players.get(evt.team_id, [])
            ]
            evt._current_players = {
                tid: list(players) for tid, players in current_players.items()
            }
        else:
            evt._current_players = {
                tid: list(players) for tid, players in current_players.items()
            }
    return _build_possessions(events)
