import os
import sys
import json

sys.path.insert(0, os.path.abspath("."))

from pbpstats.resources.enhanced_pbp import (
    Ejection,
    EndOfPeriod,
    Foul,
    FreeThrow,
    Substitution,
)
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


class DummyFoul(Foul):
    def __init__(
        self,
        *,
        player1_id,
        team_id,
        seconds_remaining,
        period=2,
        next_event=None,
        is_technical=False,
        is_double_technical=False,
        is_flagrant1=False,
        is_flagrant2=False,
        clock="12:00",
    ):
        self.player1_id = player1_id
        self.team_id = team_id
        self.seconds_remaining = seconds_remaining
        self.period = period
        self.next_event = next_event
        self._is_technical = is_technical
        self._is_double_technical = is_double_technical
        self._is_flagrant1 = is_flagrant1
        self._is_flagrant2 = is_flagrant2
        self.clock = clock

    @property
    def number_of_fta_for_foul(self):
        return 0

    @property
    def is_personal_foul(self):
        return False

    @property
    def is_shooting_foul(self):
        return False

    @property
    def is_loose_ball_foul(self):
        return False

    @property
    def is_offensive_foul(self):
        return False

    @property
    def is_inbound_foul(self):
        return False

    @property
    def is_away_from_play_foul(self):
        return False

    @property
    def is_clear_path_foul(self):
        return False

    @property
    def is_double_foul(self):
        return False

    @property
    def is_technical(self):
        return self._is_technical

    @property
    def is_flagrant1(self):
        return self._is_flagrant1

    @property
    def is_flagrant2(self):
        return self._is_flagrant2

    @property
    def is_double_technical(self):
        return self._is_double_technical

    @property
    def is_defensive_3_seconds(self):
        return False

    @property
    def is_delay_of_game(self):
        return False

    @property
    def is_charge(self):
        return False

    @property
    def is_personal_block_foul(self):
        return False

    @property
    def is_personal_take_foul(self):
        return False

    @property
    def is_shooting_block_foul(self):
        return False

    @property
    def is_transition_take_foul(self):
        return False


class DummyFreeThrow(FreeThrow):
    def __init__(
        self,
        *,
        player1_id,
        team_id,
        seconds_remaining,
        period=2,
        next_event=None,
        is_technical_ft=True,
        is_flagrant_ft=False,
        ft_number=1,
        trip_size=1,
        clock="12:00",
    ):
        self.player1_id = player1_id
        self.team_id = team_id
        self.seconds_remaining = seconds_remaining
        self.period = period
        self.next_event = next_event
        self._is_technical_ft = is_technical_ft
        self._is_flagrant_ft = is_flagrant_ft
        self._ft_number = ft_number
        self._trip_size = trip_size
        self.clock = clock
        self.description = ""

    @property
    def is_made(self):
        return True

    @property
    def is_ft_1_of_1(self):
        return self._ft_number == 1 and self._trip_size == 1

    @property
    def is_ft_1_of_2(self):
        return self._ft_number == 1 and self._trip_size == 2

    @property
    def is_ft_2_of_2(self):
        return self._ft_number == 2 and self._trip_size == 2

    @property
    def is_ft_1_of_3(self):
        return self._ft_number == 1 and self._trip_size == 3

    @property
    def is_ft_2_of_3(self):
        return self._ft_number == 2 and self._trip_size == 3

    @property
    def is_ft_3_of_3(self):
        return self._ft_number == 3 and self._trip_size == 3

    @property
    def is_technical_ft(self):
        return self._is_technical_ft

    @property
    def is_flagrant_ft(self):
        return self._is_flagrant_ft

    @property
    def is_ft_1pt(self):
        return False

    @property
    def is_ft_2pt(self):
        return False

    @property
    def is_ft_3pt(self):
        return False


class DummyEjection(Ejection):
    def __init__(
        self,
        *,
        player1_id,
        team_id,
        seconds_remaining,
        period=2,
        next_event=None,
        clock="12:00",
    ):
        self.player1_id = player1_id
        self.team_id = team_id
        self.seconds_remaining = seconds_remaining
        self.period = period
        self.next_event = next_event
        self.clock = clock


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


def test_fill_missing_starters_keeps_outgoing_player_for_period_start_technical_ft_cluster():
    end = DummyEnd()
    sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=9,
        outgoing_player_id=5,
        seconds_remaining=720.0,
        next_event=end,
    )
    tech_ft = DummyFreeThrow(
        player1_id=5,
        team_id=TEAM_A,
        seconds_remaining=720.0,
        next_event=sub,
    )
    tech_foul = DummyFoul(
        player1_id=11,
        team_id=TEAM_B,
        seconds_remaining=720.0,
        next_event=tech_ft,
        is_technical=True,
    )
    start = DummyStart()
    start.next_event = tech_foul
    start.first_period_event = tech_foul
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start.previous_period_end_period = 1
    starters_by_team = {TEAM_A: [1, 2, 3, 4]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result[TEAM_A] == [1, 2, 3, 4, 5]


def test_fill_missing_starters_keeps_outgoing_player_for_period_start_flagrant_cluster():
    end = DummyEnd()
    opponent_ft = DummyFreeThrow(
        player1_id=12,
        team_id=TEAM_B,
        seconds_remaining=720.0,
        next_event=end,
        is_technical_ft=False,
    )
    sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=9,
        outgoing_player_id=5,
        seconds_remaining=720.0,
        next_event=opponent_ft,
    )
    ejection = DummyEjection(
        player1_id=5,
        team_id=TEAM_A,
        seconds_remaining=720.0,
        next_event=sub,
    )
    flagrant = DummyFoul(
        player1_id=5,
        team_id=TEAM_A,
        seconds_remaining=720.0,
        next_event=ejection,
        is_flagrant2=True,
    )
    start = DummyStart()
    start.next_event = flagrant
    start.first_period_event = flagrant
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start.previous_period_end_period = 1
    starters_by_team = {TEAM_A: [1, 2, 3, 4]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result[TEAM_A] == [1, 2, 3, 4, 5]


def test_fill_missing_starters_still_treats_plain_period_start_sub_as_pre_cluster():
    end = DummyEnd()
    sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=9,
        outgoing_player_id=5,
        seconds_remaining=720.0,
        next_event=end,
    )
    start = DummyStart()
    start.next_event = sub
    start.first_period_event = sub
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start.previous_period_end_period = 1
    starters_by_team = {TEAM_A: [1, 2, 3, 4]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result[TEAM_A] == [1, 2, 3, 4]


def test_fill_missing_starters_keeps_outgoing_player_for_pre_marker_same_clock_technical_ft_cluster():
    end = DummyEnd()
    tech_ft = DummyFreeThrow(
        player1_id=5,
        team_id=TEAM_A,
        seconds_remaining=720.0,
        next_event=end,
    )
    tech_foul = DummyFoul(
        player1_id=11,
        team_id=TEAM_B,
        seconds_remaining=720.0,
        next_event=tech_ft,
        is_technical=True,
    )
    start_marker = DummyEvent(
        seconds_remaining=720.0,
        period=2,
        next_event=tech_foul,
    )
    sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=9,
        outgoing_player_id=5,
        seconds_remaining=720.0,
        next_event=start_marker,
    )
    start = DummyStart()
    start.next_event = sub
    start.first_period_event = sub
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start.previous_period_end_period = 1
    starters_by_team = {TEAM_A: [1, 2, 3, 4]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result[TEAM_A] == [1, 2, 3, 4, 5]


def test_fill_missing_starters_keeps_outgoing_player_for_pre_marker_same_clock_flagrant_ft_cluster():
    end = DummyEnd()
    opponent_ft = DummyFreeThrow(
        player1_id=12,
        team_id=TEAM_B,
        seconds_remaining=720.0,
        next_event=end,
        is_technical_ft=False,
    )
    ejection = DummyEjection(
        player1_id=5,
        team_id=TEAM_A,
        seconds_remaining=720.0,
        next_event=opponent_ft,
    )
    flagrant = DummyFoul(
        player1_id=5,
        team_id=TEAM_A,
        seconds_remaining=720.0,
        next_event=ejection,
        is_flagrant2=True,
    )
    start_marker = DummyEvent(
        seconds_remaining=720.0,
        period=2,
        next_event=flagrant,
    )
    sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=9,
        outgoing_player_id=5,
        seconds_remaining=720.0,
        next_event=start_marker,
    )
    start = DummyStart()
    start.next_event = sub
    start.first_period_event = sub
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start.previous_period_end_period = 1
    starters_by_team = {TEAM_A: [1, 2, 3, 4]}

    result = start._fill_missing_starters_from_previous_period_end(starters_by_team)

    assert result[TEAM_A] == [1, 2, 3, 4, 5]


def test_fill_missing_starters_keeps_outgoing_player_for_pre_marker_same_clock_technical_ft_cluster():
    end = DummyEnd()
    tech_ft = DummyFreeThrow(
        player1_id=5,
        team_id=TEAM_A,
        seconds_remaining=720.0,
        next_event=end,
        is_technical_ft=True,
    )
    tech_foul = DummyFoul(
        player1_id=11,
        team_id=TEAM_B,
        seconds_remaining=720.0,
        next_event=tech_ft,
        is_technical=True,
    )
    pre_marker_sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=9,
        outgoing_player_id=5,
        seconds_remaining=720.0,
        next_event=tech_foul,
    )
    start = DummyStart()
    start.next_event = tech_foul
    start.first_period_event = pre_marker_sub
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start.previous_period_end_period = 1

    result = start._fill_missing_starters_from_previous_period_end(
        {
            TEAM_A: [1, 2, 3, 4],
            TEAM_B: [11, 12, 13, 14, 15],
        }
    )

    assert result[TEAM_A] == [1, 2, 3, 4, 5]


def test_fill_missing_starters_keeps_outgoing_player_for_pre_marker_same_clock_flagrant_ft_cluster():
    end = DummyEnd()
    second_flagrant_ft = DummyFreeThrow(
        player1_id=5,
        team_id=TEAM_A,
        seconds_remaining=720.0,
        next_event=end,
        is_technical_ft=False,
        is_flagrant_ft=True,
        ft_number=2,
        trip_size=2,
    )
    first_flagrant_ft = DummyFreeThrow(
        player1_id=5,
        team_id=TEAM_A,
        seconds_remaining=720.0,
        next_event=second_flagrant_ft,
        is_technical_ft=False,
        is_flagrant_ft=True,
        ft_number=1,
        trip_size=2,
    )
    support_ruling = DummyEjection(
        player1_id=41,
        team_id=TEAM_B,
        seconds_remaining=720.0,
        next_event=first_flagrant_ft,
    )
    pre_marker_sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=9,
        outgoing_player_id=5,
        seconds_remaining=720.0,
        next_event=support_ruling,
    )
    start = DummyStart()
    start.next_event = support_ruling
    start.first_period_event = pre_marker_sub
    start.previous_period_end_lineups = {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start.previous_period_end_period = 1

    result = start._fill_missing_starters_from_previous_period_end(
        {
            TEAM_A: [1, 2, 3, 4],
            TEAM_B: [11, 12, 13, 14, 15],
        }
    )

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


def test_stats_start_of_period_uses_strict_pbp_when_it_succeeds():
    """When strict PBP finds 10 starters, use them — don't override with V3."""
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

    # Strict PBP result is used, V3 is NOT consulted
    assert starters == {
        TEAM_A: [1, 2, 3, 4, 6],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    assert call_log == [("strict", False)]


def test_stats_start_of_period_prefers_strict_over_exact_v6_for_opening_technical_cluster():
    end = DummyEnd()
    tech_ft = DummyFreeThrow(
        player1_id=5,
        team_id=TEAM_A,
        seconds_remaining=720.0,
        next_event=end,
    )
    tech_foul = DummyFoul(
        player1_id=11,
        team_id=TEAM_B,
        seconds_remaining=720.0,
        next_event=tech_ft,
        is_technical=True,
    )
    pre_marker_sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=9,
        outgoing_player_id=5,
        seconds_remaining=720.0,
        next_event=tech_foul,
    )
    start = DummyStatsStart()
    start.game_id = "0020000001"
    start.period = 2
    start.next_event = tech_foul
    start.first_period_event = pre_marker_sub
    start._has_period_starter_override = lambda file_directory: False
    start._get_period_starters_from_period_events = lambda file_directory, ignore_missing_starters=False: {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start._get_exact_local_period_boxscore_starters = lambda: (
        {
            TEAM_A: [1, 2, 3, 4, 9],
            TEAM_B: [11, 12, 13, 14, 15],
        },
        "v6",
    )

    starters = start.get_period_starters()

    assert starters == {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }


def test_stats_start_of_period_still_prefers_exact_v6_without_supported_cluster():
    end = DummyEnd()
    plain_sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=9,
        outgoing_player_id=5,
        seconds_remaining=720.0,
        next_event=end,
    )
    start = DummyStatsStart()
    start.game_id = "0020000001"
    start.period = 2
    start.next_event = plain_sub
    start.first_period_event = plain_sub
    start._has_period_starter_override = lambda file_directory: False
    start._get_period_starters_from_period_events = lambda file_directory, ignore_missing_starters=False: {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    start._get_exact_local_period_boxscore_starters = lambda: (
        {
            TEAM_A: [1, 2, 3, 4, 9],
            TEAM_B: [11, 12, 13, 14, 15],
        },
        "v6",
    )

    starters = start.get_period_starters()

    assert starters == {
        TEAM_A: [1, 2, 3, 4, 9],
        TEAM_B: [11, 12, 13, 14, 15],
    }


def test_stats_start_of_period_prefers_strict_over_exact_v6_for_real_opening_cluster_canaries():
    scenarios = [
        {
            "label": "0021200444_P4_PHX_brown_over_morris",
            "game_id": "0021200444",
            "period": 4,
            "strict": {
                1610612756: [101162, 2449, 2742, 200769, 200782],
                1610612742: [708, 1880, 200755, 201580, 201954],
            },
            "exact_v6": {
                1610612756: [101162, 2449, 2742, 200782, 202693],
                1610612742: [708, 1880, 200755, 201580, 201954],
            },
            "first_period_event_factory": lambda: _link_events(
                DummyEvent(seconds_remaining=720.0, period=4),
                DummyFoul(
                    player1_id=708,
                    team_id=1610612742,
                    seconds_remaining=720.0,
                    period=4,
                    is_technical=True,
                ),
                DummyFreeThrow(
                    player1_id=200769,
                    team_id=1610612756,
                    seconds_remaining=720.0,
                    period=4,
                ),
                DummySubstitution(
                    team_id=1610612756,
                    incoming_player_id=202693,
                    outgoing_player_id=200769,
                    seconds_remaining=720.0,
                    period=4,
                ),
            ),
            "next_event_selector": lambda first: first,
        },
        {
            "label": "0021300594_P3_IND_west_over_scola",
            "game_id": "0021300594",
            "period": 3,
            "strict": {
                1610612754: [1718, 2449, 2561, 2590, 2733],
                1610612746: [101114, 201935, 202332, 203085, 203143],
            },
            "exact_v6": {
                1610612754: [1718, 2449, 2590, 2733, 2564],
                1610612746: [101114, 201935, 202332, 203085, 203143],
            },
            "first_period_event_factory": lambda: _link_events(
                DummyEvent(seconds_remaining=720.0, period=3),
                DummyFoul(
                    player1_id=2561,
                    team_id=1610612754,
                    seconds_remaining=720.0,
                    period=3,
                    is_flagrant2=True,
                ),
                DummyEjection(
                    player1_id=2561,
                    team_id=1610612754,
                    seconds_remaining=720.0,
                    period=3,
                ),
                DummySubstitution(
                    team_id=1610612754,
                    incoming_player_id=2564,
                    outgoing_player_id=2561,
                    seconds_remaining=720.0,
                    period=3,
                ),
                DummyFreeThrow(
                    player1_id=203085,
                    team_id=1610612746,
                    seconds_remaining=720.0,
                    period=3,
                    is_technical_ft=False,
                    is_flagrant_ft=True,
                    ft_number=1,
                    trip_size=2,
                ),
                DummyFreeThrow(
                    player1_id=203085,
                    team_id=1610612746,
                    seconds_remaining=720.0,
                    period=3,
                    is_technical_ft=False,
                    is_flagrant_ft=True,
                    ft_number=2,
                    trip_size=2,
                ),
            ),
            "next_event_selector": lambda first: first,
        },
        {
            "label": "0021400336_P2_CLE_james_over_irving",
            "game_id": "0021400336",
            "period": 2,
            "strict": {
                1610612739: [2544, 2592, 2738, 201567, 202684],
                1610612764: [101162, 201575, 202322, 202397, 203078],
            },
            "exact_v6": {
                1610612739: [202681, 2592, 2738, 201567, 202684],
                1610612764: [101162, 201575, 202322, 202397, 203078],
            },
            "first_period_event_factory": lambda: _link_events(
                DummyEvent(seconds_remaining=720.0, period=2),
                DummyFoul(
                    player1_id=203078,
                    team_id=1610612764,
                    seconds_remaining=720.0,
                    period=2,
                    is_technical=True,
                ),
                DummyFoul(
                    player1_id=2544,
                    team_id=1610612739,
                    seconds_remaining=720.0,
                    period=2,
                    is_technical=True,
                ),
                DummyFreeThrow(
                    player1_id=2544,
                    team_id=1610612739,
                    seconds_remaining=720.0,
                    period=2,
                ),
                DummyFreeThrow(
                    player1_id=2544,
                    team_id=1610612739,
                    seconds_remaining=720.0,
                    period=2,
                ),
                DummySubstitution(
                    team_id=1610612739,
                    incoming_player_id=202681,
                    outgoing_player_id=2544,
                    seconds_remaining=720.0,
                    period=2,
                ),
            ),
            "next_event_selector": lambda first: first,
        },
        {
            "label": "0021800748_P3_LAC_williams_over_sga",
            "game_id": "0021800748",
            "period": 3,
            "strict": {
                1610612746: [101162, 201976, 202340, 202699, 1626179],
                1610612745: [101145, 200782, 201935, 202331, 203991],
            },
            "exact_v6": {
                1610612746: [202340, 101162, 201976, 1628983, 202699],
                1610612745: [101145, 200782, 201935, 202331, 203991],
            },
            "first_period_event_factory": lambda: _link_events(
                DummyEvent(seconds_remaining=720.0, period=3),
                DummyFoul(
                    player1_id=101145,
                    team_id=1610612745,
                    seconds_remaining=720.0,
                    period=3,
                    is_technical=True,
                ),
                DummyFreeThrow(
                    player1_id=1626179,
                    team_id=1610612746,
                    seconds_remaining=720.0,
                    period=3,
                ),
                DummySubstitution(
                    team_id=1610612746,
                    incoming_player_id=1628983,
                    outgoing_player_id=1626179,
                    seconds_remaining=720.0,
                    period=3,
                ),
            ),
            "next_event_selector": lambda first: first,
        },
    ]

    for scenario in scenarios:
        first_period_event = scenario["first_period_event_factory"]()
        start = DummyStatsStart()
        start.game_id = scenario["game_id"]
        start.period = scenario["period"]
        start.first_period_event = first_period_event
        start.next_event = scenario["next_event_selector"](first_period_event)
        start._has_period_starter_override = lambda file_directory: False
        start._get_period_starters_from_period_events = (
            lambda file_directory, ignore_missing_starters=False, strict=scenario["strict"]: strict
        )
        start._get_exact_local_period_boxscore_starters = (
            lambda exact_v6=scenario["exact_v6"]: (exact_v6, "v6")
        )

        starters = start.get_period_starters()

        assert starters == scenario["strict"], scenario["label"]


def test_stats_start_of_period_uses_period_boxscore_when_strict_result_is_impossible():
    end = DummyEnd()
    later_sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=6,
        outgoing_player_id=5,
        seconds_remaining=600.0,
        next_event=end,
    )
    start = DummyStatsStart()
    start.period = 2
    start.next_event = later_sub
    start.first_period_event = later_sub
    call_log = []

    def strict(file_directory, ignore_missing_starters=False):
        call_log.append(("strict", ignore_missing_starters))
        if ignore_missing_starters:
            return {"best_effort": True}
        return {TEAM_A: [1, 2, 3, 4, 6], TEAM_B: [11, 12, 13, 14, 15]}

    start._get_period_starters_from_period_events = strict
    start._get_period_starters_from_boxscore_loader = lambda: None
    start._get_starters_from_boxscore_request = lambda: {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }

    starters = start.get_period_starters()

    assert starters == {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }
    assert call_log == [("strict", False)]


def test_stats_start_of_period_uses_best_effort_when_strict_result_is_impossible_and_v3_fails():
    end = DummyEnd()
    later_sub = DummySubstitution(
        team_id=TEAM_A,
        incoming_player_id=6,
        outgoing_player_id=5,
        seconds_remaining=600.0,
        next_event=end,
    )
    start = DummyStatsStart()
    start.period = 2
    start.next_event = later_sub
    start.first_period_event = later_sub
    call_log = []

    def strict(file_directory, ignore_missing_starters=False):
        call_log.append(("strict", ignore_missing_starters))
        if ignore_missing_starters:
            return {"best_effort": True}
        return {TEAM_A: [1, 2, 3, 4, 6], TEAM_B: [11, 12, 13, 14, 15]}

    start._get_period_starters_from_period_events = strict
    start._get_period_starters_from_boxscore_loader = lambda: None

    def v3_fallback():
        raise InvalidNumberOfStartersException("fallback unavailable")

    start._get_starters_from_boxscore_request = v3_fallback

    starters = start.get_period_starters()

    assert starters == {"best_effort": True}
    assert call_log == [("strict", False), ("strict", True)]
