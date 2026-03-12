"""
The ``Possessions`` class has some basic properties for aggregating possession stats
"""
from itertools import groupby
from operator import itemgetter
import logging

from pbpstats import KEYS_OFF_BY_FACTOR_OF_5_WHEN_AGGREGATING_FOR_TEAM_AND_LINEUPS
from pbpstats.resources.base import Base

logger = logging.getLogger(__name__)


class Possessions(Base):
    """
    Class for possession items

    :param list items: list of
        :obj:`~pbpstats.resources.possessions.possession.Possession` items,
        typically from a possession data loader
    """

    def __init__(self, items):
        self.items = items

    @property
    def data(self):
        """
        returns possessions dict
        """
        return self.__dict__

    def _aggregate_event_stats(self, *args):
        """
        Aggregates event stats across possessions.

        For well-formed games, this behaves exactly as before. For older /
        malformed games, if event.event_stats raises an exception for a
        specific event, that event is skipped instead of crashing the
        entire aggregation.
        """
        stats = []
        for item in self.items:
            for event in item.events:
                try:
                    ev_stats = event.event_stats
                except Exception as e:
                    # do no harm: ignore events whose stats can't be computed
                    logger.warning(
                        "Skipping stats for event %r (game_id=%s) in Possessions "
                        "aggregation due to error in event_stats: %s",
                        event,
                        getattr(event, "game_id", "unknown"),
                        e,
                    )
                    continue
                if not ev_stats:
                    continue
                stats.extend(ev_stats)

        if not stats:
            return []

        grouper = itemgetter(*args)
        results = []
        for key, grp in groupby(sorted(stats, key=grouper), grouper):
            temp_dict = dict(zip([*args], key))
            value = sum(item["stat_value"] for item in grp)
            if (
                temp_dict["stat_key"]
                in KEYS_OFF_BY_FACTOR_OF_5_WHEN_AGGREGATING_FOR_TEAM_AND_LINEUPS
                and "player_id" not in args
            ):
                # since stat keys are summed up from player stats
                # team and lineup stats will need some stats to be divided by 5
                value = value / 5
            temp_dict["stat_value"] = (
                value if isinstance(value, int) else round(value, 1)
            )
            results.append(temp_dict)
        return results

    @property
    def team_stats(self):
        """
        returns list of dicts with aggregated stats by team
        """
        return self._aggregate_event_stats("team_id", "stat_key")

    @property
    def opponent_stats(self):
        """
        returns list of dicts with aggregated stats by opponent
        """
        return self._aggregate_event_stats("opponent_team_id", "stat_key")

    @property
    def player_stats(self):
        """
        returns list of dicts with aggregated stats by player
        """
        return self._aggregate_event_stats("player_id", "team_id", "stat_key")

    @property
    def lineup_stats(self):
        """
        returns list of dicts with aggregated stats by lineup
        """
        return self._aggregate_event_stats("lineup_id", "team_id", "stat_key")

    @property
    def lineup_opponent_stats(self):
        """
        returns list of dicts with aggregated stats by lineup opponent
        """
        return self._aggregate_event_stats(
            "opponent_lineup_id", "opponent_team_id", "stat_key"
        )
