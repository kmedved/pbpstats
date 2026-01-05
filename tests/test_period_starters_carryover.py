import os
import sys

sys.path.insert(0, os.path.abspath("."))

from pbpstats.resources.enhanced_pbp.start_of_period import StartOfPeriod

TEAM_A = 100
TEAM_B = 200


class DummyPrevEnd:
    period = 1
    current_players = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }


class DummyStart(StartOfPeriod):
    period = 2

    def get_period_starters(self, file_directory):
        return {}


def test_fill_missing_starters_from_previous_period_end_fills_subset():
    start = DummyStart()
    start.previous_period_end_event = DummyPrevEnd()
    starters_by_team = {TEAM_A: [1, 2, 3, 4]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result[TEAM_A] == [1, 2, 3, 4, 5]


def test_fill_missing_starters_from_previous_period_end_skips_non_subset():
    start = DummyStart()
    start.previous_period_end_event = DummyPrevEnd()
    starters_by_team = {TEAM_A: [1, 2, 3, 99]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result[TEAM_A] == [1, 2, 3, 99]


def test_fill_missing_starters_skips_when_team_not_present():
    start = DummyStart()
    start.previous_period_end_event = DummyPrevEnd()
    starters_by_team = {}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result == {}


def test_fill_missing_starters_partial_fill_multiple():
    start = DummyStart()
    start.previous_period_end_event = DummyPrevEnd()
    starters_by_team = {TEAM_A: [1, 2, 3]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert len(result[TEAM_A]) == 5
    assert set(result[TEAM_A]) == {1, 2, 3, 4, 5}


def test_fill_missing_starters_noop_when_already_5():
    start = DummyStart()
    start.previous_period_end_event = DummyPrevEnd()
    starters_by_team = {TEAM_A: [1, 2, 3, 4, 5]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result[TEAM_A] == [1, 2, 3, 4, 5]


def test_fill_missing_starters_noop_when_prev_period_mismatch():
    start = DummyStart()
    prev = DummyPrevEnd()
    prev.period = 99
    start.previous_period_end_event = prev
    starters_by_team = {TEAM_A: [1, 2, 3, 4]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result[TEAM_A] == [1, 2, 3, 4]
