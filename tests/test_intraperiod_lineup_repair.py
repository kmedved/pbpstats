import os
import sys
import importlib.util

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_TESTS_DIR, ".."))

sys.path.insert(0, _REPO_ROOT)

from pbpstats.resources.enhanced_pbp.enhanced_pbp_item import EnhancedPbpItem
from pbpstats.resources.enhanced_pbp.intraperiod_lineup_repair import (
    build_generated_lineup_override_lookup,
    build_intraperiod_missing_sub_candidates,
)
from pbpstats.resources.enhanced_pbp.start_of_period import StartOfPeriod
from pbpstats.resources.enhanced_pbp.substitution import Substitution

_LOADER_PATH = os.path.join(
    _REPO_ROOT,
    "pbpstats",
    "data_loader",
    "nba_enhanced_pbp_loader.py",
)
_LOADER_SPEC = importlib.util.spec_from_file_location(
    "nba_enhanced_pbp_loader_test",
    _LOADER_PATH,
)
_LOADER_MODULE = importlib.util.module_from_spec(_LOADER_SPEC)
assert _LOADER_SPEC.loader is not None
_LOADER_SPEC.loader.exec_module(_LOADER_MODULE)
NbaEnhancedPbpLoader = _LOADER_MODULE.NbaEnhancedPbpLoader

TEAM_A = 100
TEAM_B = 200


def _seconds_remaining(clock: str) -> float:
    minutes_text, seconds_text = clock.split(":", 1)
    return int(minutes_text) * 60 + float(seconds_text)


class SeedEvent:
    def __init__(self, current_players):
        self._players = {
            team_id: [player_id for player_id in player_ids]
            for team_id, player_ids in current_players.items()
        }

    @property
    def current_players(self):
        return {
            team_id: [player_id for player_id in player_ids]
            for team_id, player_ids in self._players.items()
        }

    @property
    def _raw_current_players(self):
        return self.current_players


class DummyEnhancedEvent(EnhancedPbpItem):
    def __init__(
        self,
        *,
        event_num,
        period,
        previous_event,
        team_id=TEAM_A,
        clock="5:00",
        player1_id=0,
        player2_id=0,
        player3_id=0,
        description="",
        order=None,
    ):
        self.game_id = "0029800075"
        self.event_num = event_num
        self.period = period
        self.previous_event = previous_event
        self.next_event = None
        self.team_id = team_id
        self.clock = clock
        self.order = event_num if order is None else order
        self.player1_id = player1_id
        self.player2_id = player2_id
        self.player3_id = player3_id
        self.description = description

    @property
    def is_possession_ending_event(self):
        return False

    @property
    def event_stats(self):
        return []

    def get_offense_team_id(self):
        return self.team_id

    @property
    def seconds_remaining(self):
        return _seconds_remaining(self.clock)


class StatsFieldGoal(DummyEnhancedEvent):
    pass


class StatsTimeout(DummyEnhancedEvent):
    pass


class DummySubstitution(Substitution, DummyEnhancedEvent):
    def __init__(
        self,
        *,
        event_num,
        period,
        previous_event,
        team_id,
        outgoing_player_id,
        incoming_player_id,
        clock="5:00",
    ):
        DummyEnhancedEvent.__init__(
            self,
            event_num=event_num,
            period=period,
            previous_event=previous_event,
            team_id=team_id,
            clock=clock,
            player1_id=outgoing_player_id,
            player2_id=incoming_player_id,
        )
        self._incoming_player_id = incoming_player_id
        self._outgoing_player_id = outgoing_player_id

    @property
    def incoming_player_id(self):
        return self._incoming_player_id

    @property
    def outgoing_player_id(self):
        return self._outgoing_player_id


class DummyStartOfPeriod(StartOfPeriod):
    def __init__(self, *, period_starters, period=1):
        self.game_id = "0029800075"
        self.period = period
        self.clock = "12:00"
        self.player1_id = 0
        self.player2_id = 0
        self.player3_id = 0
        self.team_id = None
        self.description = "Start"
        self.order = 0
        self.period_starters = {
            team_id: [player_id for player_id in player_ids]
            for team_id, player_ids in period_starters.items()
        }
        self.previous_event = None
        self.next_event = None

    @property
    def seconds_remaining(self):
        return 720.0

    @property
    def is_possession_ending_event(self):
        return False

    @property
    def event_stats(self):
        return []

    def get_offense_team_id(self):
        return TEAM_A

    def get_period_starters(self, file_directory):
        return self.period_starters

    def get_team_starting_with_ball(self):
        return TEAM_A


class DummyLoader(NbaEnhancedPbpLoader):
    def __init__(self, *, items):
        self.game_id = "0029800075"
        self.league = "nba"
        self.file_directory = None
        self.items = items


def _link_events(events):
    for index, event in enumerate(events):
        event.previous_event = events[index - 1] if index > 0 else None
        event.next_event = events[index + 1] if index + 1 < len(events) else None
    return events


def _build_reentry_case():
    start = DummyStartOfPeriod(
        period_starters={TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]}
    )
    timeout = StatsTimeout(
        event_num=5,
        period=1,
        previous_event=start,
        team_id=TEAM_A,
        clock="5:21",
        description="Timeout",
    )
    first_credit = StatsFieldGoal(
        event_num=10,
        period=1,
        previous_event=timeout,
        team_id=TEAM_A,
        clock="5:20",
        player1_id=6,
        description="Player 6 score",
    )
    second_credit = StatsFieldGoal(
        event_num=12,
        period=1,
        previous_event=first_credit,
        team_id=TEAM_A,
        clock="5:15",
        player1_id=6,
        description="Player 6 score again",
    )
    later_reentry = DummySubstitution(
        event_num=20,
        period=1,
        previous_event=second_credit,
        team_id=TEAM_A,
        outgoing_player_id=2,
        incoming_player_id=1,
        clock="5:00",
    )
    return _link_events([start, timeout, first_credit, second_credit, later_reentry])


def _build_ambiguous_case():
    start = DummyStartOfPeriod(
        period_starters={TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]}
    )
    timeout = StatsTimeout(
        event_num=5,
        period=1,
        previous_event=start,
        team_id=TEAM_A,
        clock="5:21",
        description="Timeout",
    )
    first_credit = StatsFieldGoal(
        event_num=10,
        period=1,
        previous_event=timeout,
        team_id=TEAM_A,
        clock="5:20",
        player1_id=6,
        description="Player 6 score",
    )
    second_credit = StatsFieldGoal(
        event_num=12,
        period=1,
        previous_event=first_credit,
        team_id=TEAM_A,
        clock="5:15",
        player1_id=6,
        description="Player 6 score again",
    )
    return _link_events([start, timeout, first_credit, second_credit])


def _build_same_clock_case():
    start = DummyStartOfPeriod(
        period_starters={TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]}
    )
    substitution = DummySubstitution(
        event_num=10,
        period=1,
        previous_event=start,
        team_id=TEAM_A,
        outgoing_player_id=1,
        incoming_player_id=6,
        clock="5:00",
    )
    same_clock_credit = StatsFieldGoal(
        event_num=11,
        period=1,
        previous_event=substitution,
        team_id=TEAM_A,
        clock="5:00",
        player1_id=1,
        description="Player 1 score at same clock",
    )
    return _link_events([start, substitution, same_clock_credit])


def _build_broken_sub_context_case():
    start = DummyStartOfPeriod(
        period_starters={TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]}
    )
    bad_sub = DummySubstitution(
        event_num=10,
        period=1,
        previous_event=start,
        team_id=TEAM_A,
        outgoing_player_id=99,
        incoming_player_id=6,
        clock="5:00",
    )
    later_credit = StatsFieldGoal(
        event_num=12,
        period=1,
        previous_event=bad_sub,
        team_id=TEAM_A,
        clock="4:50",
        player1_id=6,
        description="Player 6 score",
    )
    later_sub_out = DummySubstitution(
        event_num=20,
        period=1,
        previous_event=later_credit,
        team_id=TEAM_A,
        outgoing_player_id=6,
        incoming_player_id=7,
        clock="4:00",
    )
    return _link_events([start, bad_sub, later_credit, later_sub_out])


def _build_future_signal_case():
    start = DummyStartOfPeriod(
        period_starters={TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]}
    )
    timeout = StatsTimeout(
        event_num=5,
        period=1,
        previous_event=start,
        team_id=TEAM_A,
        clock="5:21",
        description="Timeout",
    )
    first_credit = StatsFieldGoal(
        event_num=10,
        period=1,
        previous_event=timeout,
        team_id=TEAM_A,
        clock="5:20",
        player1_id=6,
        description="Player 6 score",
    )
    player_two_credit = StatsFieldGoal(
        event_num=11,
        period=1,
        previous_event=first_credit,
        team_id=TEAM_A,
        clock="5:19",
        player1_id=2,
        description="Player 2 score",
    )
    sub_three = DummySubstitution(
        event_num=20,
        period=1,
        previous_event=player_two_credit,
        team_id=TEAM_A,
        outgoing_player_id=3,
        incoming_player_id=7,
        clock="5:00",
    )
    sub_four = DummySubstitution(
        event_num=21,
        period=1,
        previous_event=sub_three,
        team_id=TEAM_A,
        outgoing_player_id=4,
        incoming_player_id=8,
        clock="4:40",
    )
    sub_five = DummySubstitution(
        event_num=22,
        period=1,
        previous_event=sub_four,
        team_id=TEAM_A,
        outgoing_player_id=5,
        incoming_player_id=9,
        clock="4:20",
    )
    return _link_events([start, timeout, first_credit, player_two_credit, sub_three, sub_four, sub_five])


def _build_propagation_case():
    start = DummyStartOfPeriod(
        period_starters={TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]}
    )
    timeout = StatsTimeout(
        event_num=5,
        period=1,
        previous_event=start,
        team_id=TEAM_A,
        clock="5:21",
        description="Timeout",
    )
    first_credit = StatsFieldGoal(
        event_num=10,
        period=1,
        previous_event=timeout,
        team_id=TEAM_A,
        clock="5:20",
        player1_id=6,
        description="Player 6 score",
    )
    unrelated_sub = DummySubstitution(
        event_num=20,
        period=1,
        previous_event=first_credit,
        team_id=TEAM_A,
        outgoing_player_id=4,
        incoming_player_id=7,
        clock="5:00",
    )
    later_credit = StatsFieldGoal(
        event_num=21,
        period=1,
        previous_event=unrelated_sub,
        team_id=TEAM_A,
        clock="4:40",
        player1_id=6,
        description="Player 6 score again",
    )
    explicit_reentry = DummySubstitution(
        event_num=30,
        period=1,
        previous_event=later_credit,
        team_id=TEAM_A,
        outgoing_player_id=8,
        incoming_player_id=1,
        clock="3:30",
    )
    return _link_events(
        [start, timeout, first_credit, unrelated_sub, later_credit, explicit_reentry]
    )


def _build_prior_swap_support_case():
    start = DummyStartOfPeriod(
        period_starters={TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]}
    )
    earlier_sub = DummySubstitution(
        event_num=4,
        period=1,
        previous_event=start,
        team_id=TEAM_A,
        outgoing_player_id=1,
        incoming_player_id=6,
        clock="9:55",
    )
    timeout = StatsTimeout(
        event_num=5,
        period=1,
        previous_event=earlier_sub,
        team_id=TEAM_A,
        clock="5:21",
        description="Timeout",
    )
    first_credit = StatsFieldGoal(
        event_num=10,
        period=1,
        previous_event=timeout,
        team_id=TEAM_A,
        clock="5:20",
        player1_id=1,
        description="Player 1 score after missing re-entry",
    )
    later_credit = StatsFieldGoal(
        event_num=20,
        period=1,
        previous_event=first_credit,
        team_id=TEAM_A,
        clock="4:40",
        player1_id=1,
        description="Player 1 score again",
    )
    return _link_events([start, earlier_sub, timeout, first_credit, later_credit])


def _build_multi_deadball_reentry_case():
    start = DummyStartOfPeriod(
        period_starters={TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]}
    )
    early_timeout = StatsTimeout(
        event_num=4,
        period=1,
        previous_event=start,
        team_id=TEAM_A,
        clock="6:00",
        description="Earlier timeout",
    )
    timeout = StatsTimeout(
        event_num=5,
        period=1,
        previous_event=early_timeout,
        team_id=TEAM_A,
        clock="5:21",
        description="Latest timeout",
    )
    first_credit = StatsFieldGoal(
        event_num=10,
        period=1,
        previous_event=timeout,
        team_id=TEAM_A,
        clock="5:20",
        player1_id=6,
        description="Player 6 score",
    )
    second_credit = StatsFieldGoal(
        event_num=12,
        period=1,
        previous_event=first_credit,
        team_id=TEAM_A,
        clock="5:15",
        player1_id=6,
        description="Player 6 score again",
    )
    later_reentry = DummySubstitution(
        event_num=20,
        period=1,
        previous_event=second_credit,
        team_id=TEAM_A,
        outgoing_player_id=2,
        incoming_player_id=1,
        clock="5:00",
    )
    return _link_events([start, early_timeout, timeout, first_credit, second_credit, later_reentry])


def _build_after_window_reentry_case():
    start = DummyStartOfPeriod(
        period_starters={TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]}
    )
    earlier_sub = DummySubstitution(
        event_num=4,
        period=1,
        previous_event=start,
        team_id=TEAM_A,
        outgoing_player_id=1,
        incoming_player_id=6,
        clock="9:55",
    )
    timeout = StatsTimeout(
        event_num=5,
        period=1,
        previous_event=earlier_sub,
        team_id=TEAM_A,
        clock="5:34",
        description="Timeout",
    )
    same_clock_credit = StatsFieldGoal(
        event_num=6,
        period=1,
        previous_event=timeout,
        team_id=TEAM_A,
        clock="5:34",
        player1_id=6,
        description="Player 6 score before missing re-entry",
    )
    first_credit = StatsFieldGoal(
        event_num=10,
        period=1,
        previous_event=same_clock_credit,
        team_id=TEAM_A,
        clock="5:20",
        player1_id=1,
        description="Player 1 score after cluster",
    )
    second_credit = StatsFieldGoal(
        event_num=12,
        period=1,
        previous_event=first_credit,
        team_id=TEAM_A,
        clock="5:15",
        player1_id=1,
        description="Player 1 score again",
    )
    return _link_events([start, earlier_sub, timeout, same_clock_credit, first_credit, second_credit])


def test_intraperiod_repair_prefers_player_with_later_explicit_reentry():
    events = _build_reentry_case()

    candidates = build_intraperiod_missing_sub_candidates(events, game_id="0029800075")

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["player_in_id"] == 6
    assert candidate["player_out_id"] == 1
    assert candidate["deadball_event_num"] == 5
    assert candidate["promotion_decision"] == "auto_apply"
    assert candidate["auto_apply"] is True
    assert candidate["later_explicit_reentry_support"] == 1

    lookup, generated_candidates = build_generated_lineup_override_lookup(
        events, game_id="0029800075"
    )
    assert generated_candidates[0]["player_out_id"] == 1
    assert lookup[2][TEAM_A] == [6, 2, 3, 4, 5]
    assert lookup[3][TEAM_A] == [6, 2, 3, 4, 5]


def test_intraperiod_repair_rejects_ambiguous_one_for_one_cases():
    events = _build_ambiguous_case()

    candidates = build_intraperiod_missing_sub_candidates(events, game_id="0029800075")

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["auto_apply"] is False
    assert candidate["promotion_decision"] == "insufficient_local_context"


def test_same_clock_cluster_sets_same_clock_consistency_signal():
    events = _build_same_clock_case()

    candidates = build_intraperiod_missing_sub_candidates(events, game_id="0029800075")

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["same_clock_cluster_consistency"] == 1
    assert candidate["deadball_event_num"] == 10


def test_future_local_signals_break_one_for_one_tie():
    events = _build_future_signal_case()

    candidates = build_intraperiod_missing_sub_candidates(events, game_id="0029800075")

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["player_out_id"] == 1
    assert candidate["auto_apply"] is False
    assert candidate["promotion_decision"] == "insufficient_local_context"
    assert candidate["later_explicit_sub_out_penalty"] == 0


def test_broken_substitution_context_blocks_auto_apply():
    events = _build_broken_sub_context_case()

    candidates = build_intraperiod_missing_sub_candidates(events, game_id="0029800075")

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["incoming_missing_current_support"] == 1
    assert candidate["broken_substitution_context"] == 1
    assert candidate["auto_apply"] is False
    assert candidate["promotion_decision"] == "broken_substitution_context"


def test_generated_override_propagates_swap_through_unrelated_substitutions():
    events = _build_propagation_case()

    lookup, candidates = build_generated_lineup_override_lookup(
        events, game_id="0029800075"
    )

    auto_candidates = [candidate for candidate in candidates if candidate["auto_apply"]]
    assert len(auto_candidates) == 1
    candidate = auto_candidates[0]
    assert candidate["player_out_id"] == 1
    assert candidate["deadball_apply_position"] == "after_window_end"
    assert candidate["override_start_event_num"] == 10
    assert candidate["override_end_event_num"] == 21
    assert lookup[2][TEAM_A] == [6, 2, 3, 4, 5]
    assert lookup[3][TEAM_A] == [6, 2, 3, 7, 5]
    assert lookup[4][TEAM_A] == [6, 2, 3, 7, 5]


def test_prior_complementary_swap_support_allows_missing_reentry_repair():
    events = _build_prior_swap_support_case()

    candidates = build_intraperiod_missing_sub_candidates(events, game_id="0029800075")

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["player_in_id"] == 1
    assert candidate["player_out_id"] == 6
    assert candidate["prior_complementary_swap_support"] == 1
    assert candidate["period_repeat_contradiction_support"] == 1
    assert candidate["prior_repeat_swap_support"] == 1
    assert candidate["auto_apply"] is True
    assert candidate["promotion_decision"] == "auto_apply"


def test_multiple_deadball_windows_emit_choice_diagnostics():
    events = _build_multi_deadball_reentry_case()

    candidates = build_intraperiod_missing_sub_candidates(events, game_id="0029800075")

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["deadball_event_num"] == 5
    assert candidate["deadball_window_start_event_num"] == 5
    assert candidate["deadball_window_end_event_num"] == 5
    assert candidate["deadball_choice_kind"] == "latest_winning"
    assert candidate["best_vs_runner_up_confidence_gap"] is not None
    assert candidate["forward_simulation_contradiction_delta"] == (
        candidate["contradictions_removed"] - candidate["new_contradictions_introduced"]
    )


def test_same_clock_window_can_apply_after_cluster_end():
    events = _build_after_window_reentry_case()

    candidates = build_intraperiod_missing_sub_candidates(events, game_id="0029800075")

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["player_in_id"] == 1
    assert candidate["player_out_id"] == 6
    assert candidate["deadball_event_num"] == 5
    assert candidate["deadball_apply_position"] == "after_window_end"
    assert candidate["auto_apply"] is True
    assert candidate["promotion_decision"] == "auto_apply"

    lookup, generated_candidates = build_generated_lineup_override_lookup(
        events, game_id="0029800075"
    )
    assert generated_candidates[0]["deadball_apply_position"] == "after_window_end"
    assert 2 not in lookup
    assert 3 not in lookup
    assert lookup[4][TEAM_A] == [1, 2, 3, 4, 5]
    assert lookup[5][TEAM_A] == [1, 2, 3, 4, 5]


def test_explicit_lineup_window_override_beats_generated_override():
    seed = SeedEvent({TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]})
    event = StatsFieldGoal(
        event_num=10,
        period=1,
        previous_event=seed,
        team_id=TEAM_A,
        clock="5:00",
        player1_id=6,
    )
    event.lineup_override_by_team = {TEAM_A: [91, 92, 93, 94, 95]}
    loader = DummyLoader(items=[event])

    loader._merge_generated_lineup_override_lookup(
        {
            0: {
                TEAM_A: [6, 2, 3, 4, 5],
                TEAM_B: [21, 22, 23, 24, 25],
            }
        }
    )

    assert event.lineup_override_by_team[TEAM_A] == [91, 92, 93, 94, 95]
    assert event.lineup_override_by_team[TEAM_B] == [21, 22, 23, 24, 25]


def test_generated_intraperiod_repair_is_blocked_when_manual_window_exists():
    events = _build_reentry_case()
    loader = DummyLoader(items=events)
    loader.lineup_window_overrides = {
        "0029800075": [
            {
                "period": 1,
                "team_id": TEAM_A,
                "start_event_num": 10,
                "end_event_num": 12,
                "lineup_player_ids": [6, 2, 3, 4, 5],
            }
        ]
    }

    lookup = loader._build_generated_intraperiod_lineup_override_lookup()

    assert lookup == {}
    assert loader.generated_intraperiod_lineup_repair_candidates[0]["promotion_decision"] == "blocked_by_explicit_override"
    assert loader.generated_intraperiod_lineup_repair_candidates[0]["auto_apply"] is False
