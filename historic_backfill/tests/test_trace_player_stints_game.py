from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trace_player_stints_game import (
    _build_player_stints,
    _classify_largest_discrepancy_cause,
)


class StatsStartOfPeriod:
    def __init__(self, period, clock, current_players):
        self.game_id = "0029700001"
        self.period = period
        self.clock = clock
        self.current_players = current_players
        self.previous_event = None
        self.next_event = None


class StatsFieldGoal:
    def __init__(self, period, clock, current_players):
        self.game_id = "0029700001"
        self.period = period
        self.clock = clock
        self.current_players = current_players
        self.previous_event = None
        self.next_event = None


class StatsSubstitution:
    def __init__(self, period, clock, current_players):
        self.game_id = "0029700001"
        self.period = period
        self.clock = clock
        self.current_players = current_players
        self.previous_event = None
        self.next_event = None


class StatsEndOfPeriod:
    def __init__(self, period, clock, current_players):
        self.game_id = "0029700001"
        self.period = period
        self.clock = clock
        self.current_players = current_players
        self.previous_event = None
        self.next_event = None


def _link_events(events):
    for previous, current in zip(events, events[1:]):
        current.previous_event = previous
        previous.next_event = current
    return events


def test_build_player_stints_tracks_normal_substitution():
    start = StatsStartOfPeriod(1, "12:00", {1: [1, 2, 3, 4, 5], 2: [6, 7, 8, 9, 10]})
    mid = StatsFieldGoal(1, "11:00", {1: [1, 2, 3, 4, 5], 2: [6, 7, 8, 9, 10]})
    sub = StatsSubstitution(1, "10:00", {1: [1, 2, 3, 4, 11], 2: [6, 7, 8, 9, 10]})
    end = StatsEndOfPeriod(1, "0:00", {1: [1, 2, 3, 4, 11], 2: [6, 7, 8, 9, 10]})
    events = _link_events([start, mid, sub, end])

    stints = _build_player_stints(events, {5: "Out", 11: "In"})

    outgoing = stints[(stints["team_id"] == 1) & (stints["player_id"] == 5)].iloc[0]
    incoming = stints[(stints["team_id"] == 1) & (stints["player_id"] == 11)].iloc[0]

    assert outgoing["duration_seconds"] == 120.0
    assert outgoing["start_reason"] == "start_of_period"
    assert outgoing["end_reason"] == "substitution_out"
    assert incoming["duration_seconds"] == 600.0
    assert incoming["start_reason"] == "substitution_in"
    assert incoming["end_reason"] == "end_of_period"


def test_build_player_stints_splits_period_boundary_carryover():
    q1_start = StatsStartOfPeriod(1, "12:00", {1: [1, 2, 3, 4, 5], 2: [6, 7, 8, 9, 10]})
    q1_end = StatsEndOfPeriod(1, "0:00", {1: [1, 2, 3, 4, 5], 2: [6, 7, 8, 9, 10]})
    q2_start = StatsStartOfPeriod(2, "12:00", {1: [1, 2, 3, 4, 5], 2: [6, 7, 8, 9, 10]})
    q2_end = StatsEndOfPeriod(2, "0:00", {1: [1, 2, 3, 4, 5], 2: [6, 7, 8, 9, 10]})
    events = _link_events([q1_start, q1_end, q2_start, q2_end])

    stints = _build_player_stints(events, {1: "Player 1"})
    player_stints = stints[(stints["team_id"] == 1) & (stints["player_id"] == 1)]

    assert len(player_stints) == 2
    assert player_stints["duration_seconds"].tolist() == [720.0, 720.0]
    assert player_stints["start_reason"].tolist() == ["start_of_period", "start_of_period"]


def test_build_player_stints_handles_same_clock_substitution_case():
    start = StatsStartOfPeriod(1, "12:00", {1: [1, 2, 3, 4, 5], 2: [6, 7, 8, 9, 10]})
    score = StatsFieldGoal(1, "5:00", {1: [1, 2, 3, 4, 5], 2: [6, 7, 8, 9, 10]})
    sub = StatsSubstitution(1, "5:00", {1: [1, 2, 3, 4, 11], 2: [6, 7, 8, 9, 10]})
    follow = StatsFieldGoal(1, "4:30", {1: [1, 2, 3, 4, 11], 2: [6, 7, 8, 9, 10]})
    end = StatsEndOfPeriod(1, "0:00", {1: [1, 2, 3, 4, 11], 2: [6, 7, 8, 9, 10]})
    events = _link_events([start, score, sub, follow, end])

    stints = _build_player_stints(events, {5: "Out", 11: "In"})
    outgoing = stints[(stints["team_id"] == 1) & (stints["player_id"] == 5)].iloc[0]
    incoming = stints[(stints["team_id"] == 1) & (stints["player_id"] == 11)].iloc[0]

    assert outgoing["duration_seconds"] == 420.0
    assert incoming["duration_seconds"] == 300.0
    assert len(stints[(stints["team_id"] == 1) & (stints["player_id"] == 11)]) == 1


def test_classify_largest_discrepancy_cause_handles_blank_tpdev_case():
    row = pd.Series(
        {
            "team_id": 1,
            "player_id": 101,
            "output_seconds": 1140.0,
            "consensus_seconds": 1200.0,
            "largest_discrepancy_seconds": 60.0,
            "output_matches_official": False,
            "output_matches_tpdev": False,
            "output_matches_bbr": False,
            "official_matches_tpdev": False,
            "official_matches_bbr": True,
        }
    )

    cause = _classify_largest_discrepancy_cause(
        row,
        missing_starter_players=set(),
        extra_starter_players=set(),
        same_clock_substitution_scoring_events=0,
    )

    assert cause == "missing sub-in"
