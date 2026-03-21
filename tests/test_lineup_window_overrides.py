import json
import os
import sys
import importlib.util

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_TESTS_DIR, ".."))

sys.path.insert(0, _REPO_ROOT)

from pbpstats.resources.enhanced_pbp.enhanced_pbp_item import EnhancedPbpItem
from pbpstats.resources.enhanced_pbp.substitution import Substitution
from pbpstats.resources.enhanced_pbp.start_of_period import StartOfPeriod

_LOADER_PATH = os.path.join(
    _REPO_ROOT,
    "pbpstats",
    "data_loader",
    "nba_enhanced_pbp_loader.py",
)
_LOADER_SPEC = importlib.util.spec_from_file_location(
    "nba_enhanced_pbp_loader_lineup_window_test",
    _LOADER_PATH,
)
_LOADER_MODULE = importlib.util.module_from_spec(_LOADER_SPEC)
assert _LOADER_SPEC.loader is not None
_LOADER_SPEC.loader.exec_module(_LOADER_MODULE)
NbaEnhancedPbpLoader = _LOADER_MODULE.NbaEnhancedPbpLoader

TEAM_A = 100
TEAM_B = 200


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
    def __init__(self, *, event_num, period, previous_event, team_id=TEAM_A, order=None):
        self.game_id = "0021900156"
        self.event_num = event_num
        self.period = period
        self.previous_event = previous_event
        self.next_event = None
        self.team_id = team_id
        self.clock = "1:00"
        self.order = event_num if order is None else order

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
        return 60.0


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
    ):
        DummyEnhancedEvent.__init__(
            self,
            event_num=event_num,
            period=period,
            previous_event=previous_event,
            team_id=team_id,
        )
        self.player1_id = outgoing_player_id
        self.player2_id = incoming_player_id
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
        self.game_id = "0021900156"
        self.period = period
        self.seconds_remaining = 720.0
        self.period_starters = {
            team_id: [player_id for player_id in player_ids]
            for team_id, player_ids in period_starters.items()
        }
        self.player1_id = 0
        self.team_id = None
        self.next_event = None

    def get_period_starters(self, file_directory):
        return self.period_starters


class DummyLoader(NbaEnhancedPbpLoader):
    def __init__(self, *, items, file_directory=None):
        self.game_id = "0021900156"
        self.league = "nba"
        self.file_directory = file_directory
        self.items = items


def test_lineup_window_override_lookup_uses_linked_event_order_and_int_game_id(tmp_path):
    override_dir = tmp_path / "overrides"
    override_dir.mkdir(parents=True)
    (override_dir / "lineup_window_overrides.json").write_text(
        json.dumps(
            {
                21900156: [
                    {
                        "period": 1,
                        "team_id": TEAM_A,
                        "start_event_num": 50,
                        "end_event_num": 30,
                        "lineup_player_ids": [9, 8, 7, 6, 5],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    seed = SeedEvent({TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]})
    items = [
        DummyEnhancedEvent(event_num=10, period=1, previous_event=seed),
        DummyEnhancedEvent(event_num=20, period=1, previous_event=seed),
        DummyEnhancedEvent(event_num=50, period=1, previous_event=seed),
        DummyEnhancedEvent(event_num=30, period=1, previous_event=seed),
        DummyEnhancedEvent(event_num=60, period=1, previous_event=seed),
    ]
    loader = DummyLoader(items=items, file_directory=str(tmp_path))

    loader.lineup_window_overrides = loader._load_lineup_window_overrides()
    lookup = loader._build_lineup_window_override_lookup()

    assert set(lookup.keys()) == {2, 3}
    assert lookup[2][TEAM_A] == [9, 8, 7, 6, 5]
    assert lookup[3][TEAM_A] == [9, 8, 7, 6, 5]


def test_lineup_window_override_lookup_merges_multiple_windows(tmp_path):
    override_dir = tmp_path / "overrides"
    override_dir.mkdir(parents=True)
    (override_dir / "lineup_window_overrides.json").write_text(
        json.dumps(
            {
                "0021900156": [
                    {
                        "period": 1,
                        "team_id": TEAM_A,
                        "start_event_num": 10,
                        "end_event_num": 20,
                        "lineup_player_ids": [1, 2, 3, 4, 5],
                    },
                    {
                        "period": 1,
                        "team_id": TEAM_B,
                        "start_event_num": 20,
                        "end_event_num": 30,
                        "lineup_player_ids": [11, 12, 13, 14, 15],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    seed = SeedEvent({TEAM_A: [91, 92, 93, 94, 95], TEAM_B: [81, 82, 83, 84, 85]})
    items = [
        DummyEnhancedEvent(event_num=10, period=1, previous_event=seed),
        DummyEnhancedEvent(event_num=20, period=1, previous_event=seed),
        DummyEnhancedEvent(event_num=30, period=1, previous_event=seed),
    ]
    loader = DummyLoader(items=items, file_directory=str(tmp_path))

    loader.lineup_window_overrides = loader._load_lineup_window_overrides()
    lookup = loader._build_lineup_window_override_lookup()

    assert lookup[0][TEAM_A] == [1, 2, 3, 4, 5]
    assert lookup[1][TEAM_A] == [1, 2, 3, 4, 5]
    assert lookup[1][TEAM_B] == [11, 12, 13, 14, 15]
    assert lookup[2][TEAM_B] == [11, 12, 13, 14, 15]


def test_lineup_window_override_lookup_ignores_malformed_lineups(tmp_path):
    override_dir = tmp_path / "overrides"
    override_dir.mkdir(parents=True)
    (override_dir / "lineup_window_overrides.json").write_text(
        json.dumps(
            {
                "0021900156": [
                    {
                        "period": 1,
                        "team_id": TEAM_A,
                        "start_event_num": 10,
                        "end_event_num": 20,
                        "lineup_player_ids": [1, 2, 3, 4],
                    },
                    {
                        "period": 1,
                        "team_id": TEAM_B,
                        "start_event_num": 10,
                        "end_event_num": 20,
                        "lineup_player_ids": [11, 11, 12, 13, 14],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    seed = SeedEvent({TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]})
    items = [
        DummyEnhancedEvent(event_num=10, period=1, previous_event=seed),
        DummyEnhancedEvent(event_num=20, period=1, previous_event=seed),
    ]
    loader = DummyLoader(items=items, file_directory=str(tmp_path))

    loader.lineup_window_overrides = loader._load_lineup_window_overrides()
    lookup = loader._build_lineup_window_override_lookup()

    assert lookup == {}


def test_non_sub_event_override_only_affects_specified_team():
    seed = SeedEvent({TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]})
    event = DummyEnhancedEvent(event_num=10, period=1, previous_event=seed)
    event.lineup_override_by_team = {TEAM_A: [6, 7, 8, 9, 10]}

    current_players = event.current_players

    assert current_players[TEAM_A] == [6, 7, 8, 9, 10]
    assert current_players[TEAM_B] == [11, 12, 13, 14, 15]


def test_non_sub_event_inherits_raw_lineup_from_start_of_period():
    start = DummyStartOfPeriod(
        period_starters={TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]}
    )
    event = DummyEnhancedEvent(event_num=10, period=1, previous_event=start)

    assert event.current_players == {
        TEAM_A: [1, 2, 3, 4, 5],
        TEAM_B: [11, 12, 13, 14, 15],
    }


def test_non_sub_event_override_does_not_leak_to_following_event():
    seed = SeedEvent({TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]})
    overridden = DummyEnhancedEvent(event_num=10, period=1, previous_event=seed)
    overridden.lineup_override_by_team = {TEAM_A: [6, 7, 8, 9, 10]}
    following = DummyEnhancedEvent(event_num=11, period=1, previous_event=overridden)

    assert overridden.current_players[TEAM_A] == [6, 7, 8, 9, 10]
    assert following.current_players[TEAM_A] == [1, 2, 3, 4, 5]


def test_substitution_override_is_bounded_and_preserves_raw_following_lineup():
    seed = SeedEvent({TEAM_A: [1, 2, 3, 4, 5], TEAM_B: [11, 12, 13, 14, 15]})
    substitution = DummySubstitution(
        event_num=20,
        period=1,
        previous_event=seed,
        team_id=TEAM_A,
        outgoing_player_id=1,
        incoming_player_id=6,
    )
    substitution.lineup_override_by_team = {TEAM_A: [7, 8, 9, 10, 11]}
    following = DummyEnhancedEvent(event_num=21, period=1, previous_event=substitution)

    assert substitution.current_players[TEAM_A] == [7, 8, 9, 10, 11]
    assert following.current_players[TEAM_A] == [6, 2, 3, 4, 5]


def test_substitution_with_missing_team_context_is_treated_as_no_op():
    seed = SeedEvent({TEAM_B: [11, 12, 13, 14, 15]})
    substitution = DummySubstitution(
        event_num=20,
        period=1,
        previous_event=seed,
        team_id=TEAM_A,
        outgoing_player_id=1,
        incoming_player_id=6,
    )

    current_players = substitution.current_players

    assert TEAM_A not in current_players
    assert current_players[TEAM_B] == [11, 12, 13, 14, 15]
