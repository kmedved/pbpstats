from collections import Counter

import pbpstats
import pytest
from pbpstats.client import Client
from pbpstats.resources.enhanced_pbp.field_goal import FieldGoal
from pbpstats.resources.enhanced_pbp.free_throw import FreeThrow
from pbpstats.resources.enhanced_pbp.rebound import Rebound


@pytest.fixture(scope="module")
def game():
    settings = {
        "dir": "tests/data",
        "EnhancedPbp": {"source": "file", "data_provider": "stats_nba"},
        "Possessions": {"source": "file", "data_provider": "stats_nba"},
    }
    client = Client(settings)
    return client.Game("0021600270")


def _collect_on_court_totals(player_stats, player_id):
    totals = {
        pbpstats.TEAM_FGA_STRING: 0,
        pbpstats.TEAM_FGM_STRING: 0,
        pbpstats.TEAM_3PA_STRING: 0,
        pbpstats.TEAM_3PM_STRING: 0,
        pbpstats.TEAM_FTA_STRING: 0,
        pbpstats.TEAM_FTM_STRING: 0,
    }
    for stat in player_stats:
        if stat.get("player_id") != player_id:
            continue
        if stat["stat_key"] in totals:
            totals[stat["stat_key"]] += stat["stat_value"]
    return totals


def _collect_personal_counts(events, player_id):
    counts = {"fga": 0, "fg3a": 0, "fta": 0}
    for event in events:
        if isinstance(event, FieldGoal) and event.player1_id == player_id:
            counts["fga"] += 1
            if event.shot_value == 3:
                counts["fg3a"] += 1
        if isinstance(event, FreeThrow) and event.player1_id == player_id:
            counts["fta"] += 1
    return counts


def _collect_team_box_counts(events):
    counts = {
        1610612760: {"fga": 0, "fg3a": 0, "fta": 0},
        1610612764: {"fga": 0, "fg3a": 0, "fta": 0},
    }
    for event in events:
        if isinstance(event, FieldGoal):
            counts[event.team_id]["fga"] += 1
            if event.shot_value == 3:
                counts[event.team_id]["fg3a"] += 1
        if isinstance(event, FreeThrow):
            counts[event.team_id]["fta"] += 1
    return counts


def _lookup_team_stat(team_stats, team_id, stat_key):
    for stat in team_stats:
        if stat.get("team_id") == team_id and stat.get("stat_key") == stat_key:
            return stat["stat_value"]
    raise AssertionError(f"Missing {stat_key} for team {team_id}")


def _collect_rebound_fga_expected_stats(events):
    rebound_fga_keys = {
        pbpstats.ON_FLOOR_OFFENSIVE_REBOUND_FGA_STRING,
        pbpstats.ON_FLOOR_DEFENSIVE_REBOUND_FGA_STRING,
    }
    expected_team = Counter()
    expected_player = Counter()
    for event in events:
        if not isinstance(event, Rebound):
            continue
        if not event.is_real_rebound:
            continue
        if not isinstance(event.missed_shot, FieldGoal):
            continue

        shooting_team_id = event.missed_shot.team_id
        current_players = event.current_players
        defending_team_id = next(
            team_id for team_id in current_players if team_id != shooting_team_id
        )
        expected_team[
            (shooting_team_id, pbpstats.ON_FLOOR_OFFENSIVE_REBOUND_FGA_STRING)
        ] += 1
        expected_team[
            (defending_team_id, pbpstats.ON_FLOOR_DEFENSIVE_REBOUND_FGA_STRING)
        ] += 1
        for player_id in current_players[shooting_team_id]:
            expected_player[
                (
                    player_id,
                    shooting_team_id,
                    pbpstats.ON_FLOOR_OFFENSIVE_REBOUND_FGA_STRING,
                )
            ] += 1
        for player_id in current_players[defending_team_id]:
            expected_player[
                (
                    player_id,
                    defending_team_id,
                    pbpstats.ON_FLOOR_DEFENSIVE_REBOUND_FGA_STRING,
                )
            ] += 1
    return rebound_fga_keys, expected_team, expected_player


def _collect_actual_player_rebound_fga_stats(player_stats, rebound_fga_keys):
    actual_player = Counter()
    for stat in player_stats:
        if stat["stat_key"] not in rebound_fga_keys:
            continue
        actual_player[(stat["player_id"], stat["team_id"], stat["stat_key"])] += stat[
            "stat_value"
        ]
    return actual_player


def test_team_on_court_constants_present():
    for const_name in [
        "TEAM_FGA_STRING",
        "TEAM_FGM_STRING",
        "TEAM_3PA_STRING",
        "TEAM_3PM_STRING",
        "TEAM_FTA_STRING",
        "TEAM_FTM_STRING",
        "ON_FLOOR_OFFENSIVE_REBOUND_FGA_STRING",
        "ON_FLOOR_DEFENSIVE_REBOUND_FGA_STRING",
    ]:
        assert hasattr(pbpstats, const_name)


def test_player_team_on_court_stats_cover_personal_offense(game):
    player_ids = [201566, 202693]
    on_court_stats = {
        pid: _collect_on_court_totals(game.possessions.player_stats, pid)
        for pid in player_ids
    }
    personal_counts = {
        pid: _collect_personal_counts(game.enhanced_pbp.items, pid)
        for pid in player_ids
    }

    for pid in player_ids:
        assert (
            on_court_stats[pid][pbpstats.TEAM_FGA_STRING] >= personal_counts[pid]["fga"]
        )
        assert (
            on_court_stats[pid][pbpstats.TEAM_3PA_STRING]
            >= personal_counts[pid]["fg3a"]
        )
        assert (
            on_court_stats[pid][pbpstats.TEAM_FTA_STRING] >= personal_counts[pid]["fta"]
        )


def test_team_on_court_totals_match_boxscore_counts(game):
    box_counts = _collect_team_box_counts(game.enhanced_pbp.items)
    for team_id, expected_counts in box_counts.items():
        assert _lookup_team_stat(
            game.possessions.team_stats, team_id, pbpstats.TEAM_FGA_STRING
        ) == pytest.approx(expected_counts["fga"])
        assert _lookup_team_stat(
            game.possessions.team_stats, team_id, pbpstats.TEAM_3PA_STRING
        ) == pytest.approx(expected_counts["fg3a"])
        assert _lookup_team_stat(
            game.possessions.team_stats, team_id, pbpstats.TEAM_FTA_STRING
        ) == pytest.approx(expected_counts["fta"])


def test_rebound_fga_team_totals_match_real_missed_fg_opportunities(game):
    rebound_fga_keys, expected_team, _expected_player = (
        _collect_rebound_fga_expected_stats(game.enhanced_pbp.items)
    )
    actual_team = {
        (stat["team_id"], stat["stat_key"]): stat["stat_value"]
        for stat in game.possessions.team_stats
        if stat["stat_key"] in rebound_fga_keys
    }

    assert set(actual_team) == set(expected_team)
    for key, expected_value in expected_team.items():
        assert actual_team[key] == pytest.approx(expected_value)


def test_rebound_fga_player_totals_match_on_floor_context(game):
    rebound_fga_keys, _expected_team, expected_player = (
        _collect_rebound_fga_expected_stats(game.enhanced_pbp.items)
    )
    actual_player = _collect_actual_player_rebound_fga_stats(
        game.possessions.player_stats, rebound_fga_keys
    )

    assert actual_player == expected_player
