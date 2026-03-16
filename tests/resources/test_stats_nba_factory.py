from pbpstats.resources.enhanced_pbp.stats_nba.enhanced_pbp_factory import (
    StatsNbaEnhancedPbpFactory,
)
from pbpstats.resources.enhanced_pbp.stats_nba.replay import StatsReplay


def test_stats_factory_handles_numeric_string_event_types() -> None:
    factory = StatsNbaEnhancedPbpFactory()

    assert factory.get_event_class("18") is StatsReplay
