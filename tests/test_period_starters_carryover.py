import os
import sys
import json

sys.path.insert(0, os.path.abspath("."))

from pbpstats.resources.enhanced_pbp import EndOfPeriod, Substitution
from pbpstats.resources.enhanced_pbp.start_of_period import StartOfPeriod

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
