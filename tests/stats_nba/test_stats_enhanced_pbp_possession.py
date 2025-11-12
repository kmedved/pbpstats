# -*- coding: utf-8 -*-
from pbpstats.resources.enhanced_pbp.stats_nba.enhanced_pbp_item import (
    StatsEnhancedPbpItem,
)


class _DummyStatsEvent(StatsEnhancedPbpItem):
    event_type = 0

    def __init__(self, event):
        super().__init__(event, 0)

    @property
    def event_stats(self):
        return []


def _base_event(**overrides):
    event = {
        "GAME_ID": "0020000001",
        "EVENTNUM": 1,
        "PCTIMESTRING": "12:00",
        "PERIOD": 1,
        "EVENTMSGACTIONTYPE": 1,
        "EVENTMSGTYPE": 5,
        "PLAYER1_TEAM_ID": 1610612737,
        "PLAYER1_ID": 123,
        "NEUTRALDESCRIPTION": "Test event",
    }
    event.update(overrides)
    return event


def test_possession_index_exposed_and_preferred_for_offense_hint():
    possession_team_id = 1610612744
    event = _base_event(possession=possession_team_id)
    item = _DummyStatsEvent(event)

    assert item.possession_index == possession_team_id
    assert item.possession_team_id == possession_team_id
    assert item.get_offense_team_id() == possession_team_id
