import os
import sys
import json

sys.path.insert(0, os.path.abspath("."))

from pbpstats.resources.enhanced_pbp import EndOfPeriod, Substitution
from pbpstats.resources.enhanced_pbp.start_of_period import (
    InvalidNumberOfStartersException,
    StartOfPeriod,
)
from pbpstats.resources.enhanced_pbp.stats_nba.start_of_period import StatsStartOfPeriod

TEAM_A = 100
TEAM_B = 200


class DummyStart(StartOfPeriod):
    game_id = "0020000001"
    period = 2
    seconds_remaining = 720.0
    next_event = None
    player1_id = 0
    team_id = None

    def get_period_starters(self, file_directory):
        return {}


class DummyEnd(EndOfPeriod):
    next_event = None


class DummyEvent:
    def __init__(
        self,
        *,
        player1_id=0,
        player2_id=0,
        player3_id=0,
        team_id=None,
        player1_team_id=None,
        period=2,
        seconds_remaining=720.0,
        next_event=None,
    ):
        self.player1_id = player1_id
        self.player2_id = player2_id
        self.player3_id = player3_id
        self.team_id = team_id
        self.player1_team_id = player1_team_id
        self.period = period
        self.seconds_remaining = seconds_remaining
        self.next_event = next_event


class DummySubstitution(Substitution):
    def __init__(
        self,
        *,
        team_id,
        incoming_player_id,
        outgoing_player_id,
        seconds_remaining,
        period=2,
        next_event=None,
    ):
        self.team_id = team_id
        self.player1_id = outgoing_player_id
        self._incoming_player_id = incoming_player_id
        self._outgoing_player_id = outgoing_player_id
        self.seconds_remaining = seconds_remaining
        self.period = period
        self.next_event = next_event

    @property
    def incoming_player_id(self):
        return self._incoming_player_id

    @property
    def outgoing_player_id(self):
        return self._outgoing_player_id


class DummyPeriodBoxscoreLoader:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def load_data(self, game_id, period, mode):
        self.calls.append((game_id, period, mode))
        return self.responses.get(mode)


def _make_v3_team(team_id, player_ids):
    return {
        "teamId": team_id,
        "players": [
            {
                "personId": player_id,
                "statistics": {"minutes": "0:00"},
            }
            for player_id in player_ids
        ],
    }


def _make_v3_response(away_team_id, away_players, home_team_id, home_players):
    return {
        "boxScoreTraditional": {
            "awayTeam": _make_v3_team(away_team_id, away_players),
            "homeTeam": _make_v3_team(home_team_id, home_players),
        }
    }


class DummyStatsStart(StatsStartOfPeriod):
    def __init__(self):
        pass


def _link_events(*events):
    for index, event in enumerate(events[:-1]):
        event.next_event = events[index + 1]
    return events[0]


def test_fill_missing_starters_from_previous_period_end_fills_subset():
    start = DummyStart()
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start.previous_period_end_period = 1
    starters_by_team = {TEAM_A: [1, 2, 3, 4]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result[TEAM_A] == [1, 2, 3, 4, 5]


def test_fill_missing_starters_from_previous_period_end_skips_non_subset():
    start = DummyStart()
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start.previous_period_end_period = 1
    starters_by_team = {TEAM_A: [1, 2, 3, 99]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result[TEAM_A] == [1, 2, 3, 99]


def test_fill_missing_starters_relaxes_non_subset_when_only_one_viable_carryover_remains():
    end = DummyEnd()
    later_sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=4,
        outgoing_player_id=8,
        seconds_remaining=600.0,
        next_event=end,
    )
    start = DummyStart()
    start.next_event = later_sub
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start.previous_period_end_period = 1
    starters_by_team = {TEAM_A: [1, 2, 3, 99]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result[TEAM_A] == [1, 2, 3, 99, 5]


def test_fill_missing_starters_skips_when_team_not_present():
    start = DummyStart()
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start.previous_period_end_period = 1
    starters_by_team = {}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result == {}


def test_fill_missing_starters_partial_fill_multiple():
    start = DummyStart()
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start.previous_period_end_period = 1
    starters_by_team = {TEAM_A: [1, 2, 3]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert len(result[TEAM_A]) == 5
    assert set(result[TEAM_A]) == {1, 2, 3, 4, 5}


def test_fill_missing_starters_noop_when_already_5():
    start = DummyStart()
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start.previous_period_end_period = 1
    starters_by_team = {TEAM_A: [1, 2, 3, 4, 5]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result[TEAM_A] == [1, 2, 3, 4, 5]


def test_fill_missing_starters_noop_when_prev_period_mismatch():
    start = DummyStart()
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start.previous_period_end_period = 99
    starters_by_team = {TEAM_A: [1, 2, 3, 4]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result[TEAM_A] == [1, 2, 3, 4]


def test_get_players_who_started_period_skips_team_ids_in_related_player_slots():
    end = DummyEnd()
    event = DummyEvent(
        player1_id=1,
        player2_id=1610612748,
        player3_id=1610612755,
        team_id=TEAM_A,
        next_event=end,
    )
    start = DummyStart()
    start.game_id = "0020100810"
    start.player1_id = 0
    start.team_id = None
    start.next_event = event
    start.previous_period_end_lineups = {TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]}

    starters, _, first_seen, subbed_in = start._get_players_who_started_period_with_team_map()

    assert starters == [1]
    assert subbed_in == []
    assert 1610612748 not in first_seen
    assert 1610612755 not in first_seen


def test_trim_excess_starters_prefers_pre_sub_players():
    end = DummyEnd()
    late_sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=9,
        outgoing_player_id=5,
        seconds_remaining=600.0,
        next_event=end,
    )
    start = DummyStart()
    start.game_id = "0020100810"
    start.player1_id = 0
    start.team_id = None
    start.seconds_remaining = 720.0
    start.next_event = late_sub
    start.previous_period_end_lineups = {TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]}

    starters_by_team = {TEAM_A: [1, 2, 3, 4, 5, 9]}
    player_first_seen_order = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 9: 6}

    trimmed = start._trim_excess_starters(
        starters_by_team, player_first_seen_order, {TEAM_A, TEAM_B}
    )

    assert trimmed[TEAM_A] == [1, 2, 3, 4, 5]


def test_outgoing_period_sub_records_first_seen_for_trim_logic():
    end = DummyEnd()
    late_sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=9,
        outgoing_player_id=5,
        seconds_remaining=600.0,
        next_event=end,
    )
    start = DummyStart()
    start.next_event = late_sub
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }

    starters, _, first_seen, subbed_in = start._get_players_who_started_period_with_team_map()

    assert 5 in starters
    assert 9 in subbed_in
    assert first_seen[5] == 2


def test_misordered_sub_in_demotes_player_from_starters():
    end = DummyEnd()
    sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=9,
        outgoing_player_id=5,
        seconds_remaining=406.0,
        next_event=end,
    )
    late_action = DummyEvent(
        player1_id=9,
        team_id=TEAM_A,
        seconds_remaining=403.0,
        next_event=sub,
    )
    start = DummyStart()
    start.next_event = late_action
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }

    starters, _, _, subbed_in = start._get_players_who_started_period_with_team_map()

    assert 9 not in starters
    assert 9 in subbed_in


def test_same_clock_sub_in_demotes_player_from_starters():
    end = DummyEnd()
    sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=9,
        outgoing_player_id=5,
        seconds_remaining=406.0,
        next_event=end,
    )
    same_clock_action = DummyEvent(
        player1_id=9,
        team_id=TEAM_A,
        seconds_remaining=406.0,
        next_event=sub,
    )
    start = DummyStart()
    start.next_event = same_clock_action
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }

    starters, _, _, subbed_in = start._get_players_who_started_period_with_team_map()

    assert 9 not in starters
    assert 9 in subbed_in


def test_period_starter_overrides_replace_existing_five_player_set(tmp_path):
    start = DummyStart()
    start.game_id = "0021900156"
    start.period = 1
    override_dir = tmp_path / "overrides"
    override_dir.mkdir(parents=True)
    (override_dir / "period_starters_overrides.json").write_text(
        json.dumps(
            {
                21900156: {
                    1: {
                        TEAM_A: [11, 12, 13, 14, 15],
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    starters_by_team = {TEAM_A: [1, 2, 3, 4, 5]}

    result = start._apply_period_starter_overrides(starters_by_team, str(tmp_path))

    assert result[TEAM_A] == [11, 12, 13, 14, 15]


def test_period_boxscore_rt2_exact_ten_returns_direct_starters():
    start = DummyStart()
    start.period_boxscore_source_loader = DummyPeriodBoxscoreLoader(
        {
            "rt2_start_window": _make_v3_response(
                TEAM_A, [1, 2, 3, 4, 5], TEAM_B, [11, 12, 13, 14, 15]
            )
        }
    )

    starters = start._get_starters_from_boxscore_request()

    assert starters == {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }


def test_period_boxscore_rt2_non_exact_uses_substitution_narrowing():
    end = DummyEnd()
    sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=6,
        outgoing_player_id=5,
        seconds_remaining=700.0,
        next_event=end,
    )
    start = DummyStart()
    start.next_event = sub
    start.first_period_event = sub
    start.period_boxscore_source_loader = DummyPeriodBoxscoreLoader(
        {
            "rt2_start_window": _make_v3_response(
                TEAM_A, [1, 2, 3, 4, 5, 6], TEAM_B, [11, 12, 13, 14, 15]
            )
        }
    )

    starters = start._get_starters_from_boxscore_request()

    assert starters == {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }


def test_period_boxscore_rt1_participants_used_when_rt2_unresolved():
    end = DummyEnd()
    sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=6,
        outgoing_player_id=5,
        seconds_remaining=700.0,
        next_event=end,
    )
    start = DummyStart()
    start.next_event = sub
    start.first_period_event = sub
    loader = DummyPeriodBoxscoreLoader(
        {
            "rt2_start_window": _make_v3_response(
                TEAM_A, [1, 2, 3, 4, 5], TEAM_B, []
            ),
            "rt1_period_participants": _make_v3_response(
                TEAM_A, [1, 2, 3, 4, 5, 6], TEAM_B, [11, 12, 13, 14, 15]
            ),
        }
    )
    start.period_boxscore_source_loader = loader

    starters = start._get_starters_from_boxscore_request()

    assert starters == {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    assert loader.calls == [
        ("0020000001", 2, "rt2_start_window"),
        ("0020000001", 2, "rt1_period_participants"),
    ]


def test_period_boxscore_narrowing_uses_earliest_same_clock_sub_event():
    end = DummyEnd()
    later_sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=5,
        outgoing_player_id=6,
        seconds_remaining=700.0,
        next_event=end,
    )
    first_sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=6,
        outgoing_player_id=5,
        seconds_remaining=700.0,
        next_event=later_sub,
    )
    start = DummyStart()
    start.next_event = first_sub
    start.first_period_event = first_sub

    substitution_lookup = start._get_period_substitution_order_lookup()

    assert start._classify_period_boxscore_candidate(TEAM_A, 5, substitution_lookup) is True
    assert start._classify_period_boxscore_candidate(TEAM_A, 6, substitution_lookup) is False


def test_stats_start_of_period_uses_period_boxscore_before_best_effort():
    start = DummyStatsStart()
    call_log = []

    def strict(file_directory, ignore_missing_starters=False):
        call_log.append(("strict", ignore_missing_starters))
        if ignore_missing_starters:
            return {"best_effort": True}
        raise InvalidNumberOfStartersException("strict failure sentinel")

    start._get_period_starters_from_period_events = strict
    start._get_period_starters_from_boxscore_loader = lambda: None
    start._get_starters_from_boxscore_request = lambda: {"v3_fallback": True}

    starters = start.get_period_starters()

    assert starters == {"v3_fallback": True}
    assert call_log == [("strict", False)]


def test_stats_start_of_period_prefers_v3_over_strict_when_loader_available():
    start = DummyStatsStart()
    start.period = 5
    start.period_boxscore_source_loader = object()
    call_log = []

    def strict(file_directory, ignore_missing_starters=False):
        call_log.append(("strict", ignore_missing_starters))
        return {TEAM_A: [1, 2, 3, 4, 6], TEAM_B: [11, 12, 13, 14, 15]}

    start._get_period_starters_from_period_events = strict
    start._get_starters_from_boxscore_request = lambda: {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start._get_period_starters_from_boxscore_loader = lambda: None

    starters = start.get_period_starters()

    assert starters == {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    assert call_log == [("strict", False)]
