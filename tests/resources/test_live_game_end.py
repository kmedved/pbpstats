import pytest

from pbpstats.resources.enhanced_pbp.live.enhanced_pbp_factory import (
    LiveEnhancedPbpFactory,
)
from pbpstats.resources.enhanced_pbp.live.game_end import LiveGameEnd


class DummyEvent:
    def __init__(self, offense_team_id):
        self.offense_team_id = offense_team_id

    def get_offense_team_id(self):
        return self.offense_team_id


def build_game_end_event(**overrides):
    item = {
        "actionType": "game",
        "subType": "end",
        "clock": "PT00:00",
        "period": 4,
        "possession": 0,
        "orderNumber": 1,
    }
    item.update(overrides)
    return LiveGameEnd(item, "0020000001")


def test_game_end_uses_provided_offense_team_id():
    previous_event = DummyEvent(999)
    game_end_event = build_game_end_event(possession=123)
    game_end_event.previous_event = previous_event

    assert game_end_event.get_offense_team_id() == 123


def test_game_end_inherits_offense_team_id_from_previous_event_when_missing():
    previous_event = DummyEvent(987)
    game_end_event = build_game_end_event(possession=0)
    game_end_event.previous_event = previous_event

    assert game_end_event.get_offense_team_id() == 987


def test_live_factory_registers_game_end_event():
    factory = LiveEnhancedPbpFactory()

    assert factory.get_event_class("game", "end") is LiveGameEnd
