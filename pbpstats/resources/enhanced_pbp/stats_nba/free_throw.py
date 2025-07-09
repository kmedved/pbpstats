from functools import cached_property

from pbpstats.resources.enhanced_pbp import FreeThrow
from pbpstats.resources.enhanced_pbp.stats_nba.enhanced_pbp_item import (
    StatsEnhancedPbpItem,
)


class StatsFreeThrow(FreeThrow, StatsEnhancedPbpItem):
    """
    Class for free throw events
    """

    event_type = 3

    def __init__(self, *args):
        super().__init__(*args)

    @cached_property
    def is_made(self):
        """Return ``True`` if the free throw was made.

        Decision tree (v2):
            1. If the description explicitly contains ``"MISS"`` → ``False``.
            2. If the description explicitly lists points (``" PTS)"``) → ``True``.
            3. Otherwise the text is ambiguous.  If this ambiguous FT is the last
               in its trip (``is_end_ft`` or ``is_ft_1_of_1`` or ``is_technical_ft``)
               and the very next event is a rebound by the opposing team,
               infer a miss → ``False``.
            4. In all other cases assume it was made.
        """

        if "MISS " in self.description:
            return False

        if " PTS)" in self.description:
            return True

        if (
            (self.is_end_ft or self.is_ft_1_of_1 or self.is_technical_ft)
            and self.next_event is not None
            and getattr(self.next_event, "event_type", None) == 4
            and getattr(self.next_event, "team_id", 0) != 0
            and getattr(self, "team_id", 0) != 0
            and self.next_event.team_id != self.team_id
        ):
            return False

        return True

    @property
    def was_ambiguous_raw(self) -> bool:
        """Return ``True`` if the play-by-play text omitted both ``"MISS"`` and
        explicit points, meaning the result is ambiguous before inference."""

        return "MISS" not in self.description and " PTS)" not in self.description

    def get_offense_team_id(self):
        """
        returns team id that took the shot
        """
        return self.team_id

    @property
    def is_ft_1_of_1(self):
        # action_type 20 is flagrant 1 of 1
        return self.event_action_type == 10 or self.event_action_type == 20

    @property
    def is_ft_1_of_2(self):
        return self.event_action_type == 11

    @property
    def is_ft_2_of_2(self):
        return self.event_action_type == 12

    @property
    def is_ft_1_of_3(self):
        # action_type 20 is flagrant 1 of 3
        return self.event_action_type == 13 or self.event_action_type == 27

    @property
    def is_ft_2_of_3(self):
        return self.event_action_type == 14

    @property
    def is_ft_3_of_3(self):
        return self.event_action_type == 15

    @property
    def is_technical_ft(self):
        return " Technical" in self.description

    @property
    def is_flagrant_ft(self):
        return " Flagrant" in self.description

    @property
    def is_ft_1pt(self):
        """
        returns True if free throw is a 1 point free throw, False otherwise
        Only used in g-league, starting in 2019-20 season
        """
        return self.event_action_type == 30 or self.event_action_type == 35

    @property
    def is_ft_2pt(self):
        """
        returns True if free throw is a 2 point free throw, False otherwise
        Only used in g-league, starting in 2019-20 season
        """
        return self.event_action_type == 31 or self.event_action_type == 36

    @property
    def is_ft_3pt(self):
        """
        returns True if free throw is a 3 point free throw, False otherwise
        Only used in g-league, starting in 2019-20 season
        """
        return self.event_action_type == 32 or self.event_action_type == 37
