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

    @property
    def is_made(self):
        """
        returns True if shot was made, False otherwise
        """
        # Explicit "MISS" always means missed
        if "MISS " in self.description:
            return False

        # Explicit points in description often means made (e.g., "(2 PTS)")
        if " PTS)" in self.description:
            return True
        
        # If it's a final FT of a trip (or 1 of 1, or technical)
        # AND its description is ambiguous (no "MISS", no explicit " PTS)")
        # AND the next immediate event is any rebound,
        # then infer it was a miss.
        # This relies on self.next_event being populated.
        if (self.is_end_ft or self.is_ft_1_of_1 or self.is_technical_ft) and \
        self.next_event is not None and \
        hasattr(self.next_event, 'event_type') and self.next_event.event_type == 4:
            return False

        # Default: if not explicitly "MISS" and no other strong indicator of miss, assume made.
        # This maintains original behavior for FTs that explicitly state points or are not followed by opponent rebound.
        return True

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
