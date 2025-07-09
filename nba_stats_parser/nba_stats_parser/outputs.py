from __future__ import annotations

from typing import Dict, List

import pandas as pd

from . import models


def generate_boxscore(raw_boxscore_data: Dict) -> Dict[str, pd.DataFrame]:
    player_stats = pd.DataFrame(raw_boxscore_data.get("PlayerStats", []))
    team_stats = pd.DataFrame(raw_boxscore_data.get("TeamStats", []))
    return {"players": player_stats, "teams": team_stats}


def generate_rapm_data(possessions: List[models.Possession]) -> pd.DataFrame:
    rapm_rows = []
    for poss in possessions:
        offense_team = poss.offense_team_id
        start_score = poss.start_score_margin
        end_score = poss.events[-1].score_margin
        points_scored = end_score - start_score
        offensive_players = poss.events[0].current_players[offense_team]
        team_ids = list(poss.events[0].current_players.keys())
        defense_team = team_ids[0] if offense_team == team_ids[1] else team_ids[1]
        defensive_players = poss.events[0].current_players[defense_team]
        rapm_rows.append(
            {
                "offense_team_id": offense_team,
                "defense_team_id": defense_team,
                "points": points_scored,
                "offensive_player_ids": offensive_players,
                "defensive_player_ids": defensive_players,
            }
        )
    return pd.DataFrame(rapm_rows)
