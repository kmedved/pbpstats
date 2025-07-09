from __future__ import annotations

import abc
from collections import defaultdict
from typing import Dict, List, Optional

import pbpstats

KEY_ATTR_MAPPER = {
    "GAME_ID": "game_id",
    "EVENTNUM": "event_num",
    "PCTIMESTRING": "clock",
    "PERIOD": "period",
    "EVENTMSGACTIONTYPE": "event_action_type",
    "EVENTMSGTYPE": "event_type",
    "PLAYER1_ID": "player1_id",
    "PLAYER1_TEAM_ID": "team_id",
    "PLAYER2_ID": "player2_id",
    "PLAYER3_ID": "player3_id",
    "VIDEO_AVAILABLE_FLAG": "video_available",
}


class Event(metaclass=abc.ABCMeta):
    """Base class for parsed play-by-play events."""

    def __init__(self, event: Dict, order: int):
        for key, value in KEY_ATTR_MAPPER.items():
            if event.get(key) is not None:
                setattr(self, value, event.get(key))

        if (
            event.get("HOMEDESCRIPTION") is not None
            and event.get("VISITORDESCRIPTION") is not None
        ):
            self.description = (
                f"{event.get('HOMEDESCRIPTION')}: {event.get('VISITORDESCRIPTION')}"
            )
        elif event.get("HOMEDESCRIPTION") is not None:
            self.description = f"{event.get('HOMEDESCRIPTION')}"
        elif event.get("VISITORDESCRIPTION") is not None:
            self.description = f"{event.get('VISITORDESCRIPTION')}"
        elif event.get("NEUTRALDESCRIPTION") is not None:
            self.description = f"{event.get('NEUTRALDESCRIPTION')}"
        else:
            self.description = ""

        if (
            event.get("PLAYER1_TEAM_ID") is None
            and event.get("PLAYER1_ID") is not None
            and event.get("EVENTMSGTYPE") != 18
        ):
            # need to set team id in these cases where player id is team id
            # EVENTMSGTYPE 18 is replay event - it is ignored because it has no team id
            self.team_id = event.get("PLAYER1_ID", 0)
            self.player1_id = 0

        if self.event_type == 10:
            # jump ball PLAYER3_TEAM_ID is player who ball gets tipped to
            self.player2_id = event["PLAYER3_ID"]
            self.player3_id = event["PLAYER2_ID"]
            if event["PLAYER3_TEAM_ID"] is not None:
                self.team_id = event["PLAYER3_TEAM_ID"]
            else:
                # when jump ball is tipped out of bounds, winning team is PLAYER3_ID
                self.team_id = event["PLAYER3_ID"]
                if hasattr(self, "player2_id"):
                    delattr(self, "player2_id")
        elif self.event_type in [5, 6]:
            # steals need to change PLAYER2_ID to player3_id - this is player who turned ball over
            # fouls need to change PLAYER2_ID to player3_id - this is player who drew foul
            if hasattr(self, "player2_id"):
                delattr(self, "player2_id")
            if event.get("PLAYER2_ID") is not None:
                self.player3_id = event["PLAYER2_ID"]

        if hasattr(self, "player2_id") and self.player2_id == 0:
            delattr(self, "player2_id")
        if hasattr(self, "player3_id") and self.player3_id == 0:
            delattr(self, "player3_id")

        self.order = order
        self.previous_event: Optional[Event] = None
        self.next_event: Optional[Event] = None
        self.player_game_fouls = defaultdict(int)
        self.possession_changing_override = False
        self.non_possession_changing_override = False
        self.score = defaultdict(int)

    @property
    def data(self) -> Dict:
        return self.__dict__

    @property
    def seconds_remaining(self) -> float:
        split = self.clock.split(":")
        return float(split[0]) * 60 + float(split[1])

    def __repr__(self):
        return (
            f"<{type(self).__name__} GameId: {self.game_id}, Description: {self.description},"
            f" Time: {self.clock}, EventNum: {self.event_num}>"
        )

    def get_all_events_at_current_time(self) -> List["Event"]:
        events = [self]
        event = self
        while event is not None and self.seconds_remaining == event.seconds_remaining:
            if event != self:
                events.append(event)
            event = event.previous_event
        event = self
        while event is not None and self.seconds_remaining == event.seconds_remaining:
            if event != self:
                events.append(event)
            event = event.next_event
        return sorted(events, key=lambda k: k.order)

    @property
    def current_players(self) -> Dict[int, List[int]]:
        if hasattr(self, "_current_players"):
            return self._current_players
        return self.previous_event.current_players

    @property
    def score_margin(self) -> int:
        if self.previous_event is None:
            score = self.score
        else:
            score = self.previous_event.score
        offense_team_id = self.get_offense_team_id()
        offense_points = score[offense_team_id]
        defense_points = 0
        for team_id, points in score.items():
            if team_id != offense_team_id:
                defense_points = points
        return offense_points - defense_points

    @property
    def lineup_ids(self) -> Dict[int, str]:
        lineup_ids = {}
        for team_id, team_players in self.current_players.items():
            players = [str(player_id) for player_id in team_players]
            sorted_player_ids = sorted(players)
            lineup_id = "-".join(sorted_player_ids)
            lineup_ids[team_id] = lineup_id
        return lineup_ids

    @property
    def seconds_since_previous_event(self) -> float:
        if self.previous_event is None:
            return 0
        if self.seconds_remaining == 720:
            return 0
        if self.seconds_remaining == 300 and self.period > 4:
            return 0
        return self.previous_event.seconds_remaining - self.seconds_remaining

    def is_second_chance_event(self) -> bool:
        event = self.previous_event
        if isinstance(event, Rebound) and event.is_real_rebound and event.oreb:
            return True
        while not (event is None or event.is_possession_ending_event):
            if isinstance(event, Rebound) and event.is_real_rebound and event.oreb:
                return True
            event = event.previous_event
        return False

    def is_penalty_event(self) -> bool:
        return False

    @property
    def count_as_possession(self) -> bool:
        if self.is_possession_ending_event:
            if self.seconds_remaining > 2:
                return True
            prev_event = self.previous_event
            while prev_event is not None and not prev_event.is_possession_ending_event:
                prev_event = prev_event.previous_event
            if prev_event is None or prev_event.seconds_remaining > 2:
                return True
            next_event = prev_event.next_event
            while next_event is not None:
                if isinstance(next_event, FreeThrow) or (
                    isinstance(next_event, FieldGoal) and next_event.is_made
                ):
                    return True
                next_event = next_event.next_event
        return False

    def _get_seconds_played_stats_items(self) -> List[Dict]:
        return []

    def _get_possessions_played_stats_items(self) -> List[Dict]:
        return []

    @property
    def base_stats(self) -> List[Dict]:
        return (
            self._get_seconds_played_stats_items()
            + self._get_possessions_played_stats_items()
        )

    @abc.abstractproperty
    def is_possession_ending_event(self) -> bool:
        pass

    @abc.abstractproperty
    def event_stats(self) -> List[Dict]:
        pass

    @abc.abstractmethod
    def get_offense_team_id(self) -> int:
        pass


class FieldGoal(Event):
    event_type = [1, 2]

    def __init__(self, event: Dict, order: int):
        super().__init__(event, order)

    @property
    def is_made(self) -> bool:
        return self.event_type == 1 or getattr(self, "made", False)

    @property
    def shot_value(self) -> int:
        return 3 if "3PT" in self.description else 2

    def get_offense_team_id(self) -> int:
        return self.team_id

    @property
    def is_possession_ending_event(self) -> bool:
        return self.is_made

    @property
    def event_stats(self) -> List[Dict]:
        return []


class FreeThrow(Event):
    event_type = 3

    def __init__(self, event: Dict, order: int):
        super().__init__(event, order)

    @property
    def is_made(self) -> bool:
        return "MISS" not in self.description

    def get_offense_team_id(self) -> int:
        return self.team_id

    @property
    def is_possession_ending_event(self) -> bool:
        return self.is_made and "of 1" in self.description

    @property
    def event_stats(self) -> List[Dict]:
        return []


class Rebound(Event):
    event_type = 4

    def __init__(self, event: Dict, order: int):
        super().__init__(event, order)

    @property
    def is_real_rebound(self) -> bool:
        return True

    @property
    def oreb(self) -> bool:
        if hasattr(self, "missed_shot"):
            return self.team_id == self.missed_shot.team_id
        return False

    def get_offense_team_id(self) -> int:
        return self.team_id

    @property
    def is_possession_ending_event(self) -> bool:
        return not self.oreb

    @property
    def event_stats(self) -> List[Dict]:
        return []


class Turnover(Event):
    event_type = 5

    def __init__(self, event: Dict, order: int):
        super().__init__(event, order)

    @property
    def is_no_turnover(self) -> bool:
        return self.event_action_type == 0

    def get_offense_team_id(self) -> int:
        if self.is_no_turnover:
            return self.previous_event.get_offense_team_id()
        return self.team_id

    @property
    def is_possession_ending_event(self) -> bool:
        return not self.is_no_turnover

    @property
    def event_stats(self) -> List[Dict]:
        return []


class Foul(Event):
    event_type = 6

    def __init__(self, event: Dict, order: int):
        super().__init__(event, order)

    @property
    def number_of_fta_for_foul(self) -> Optional[int]:
        return None

    @property
    def is_personal_foul(self) -> bool:
        return self.event_action_type == 1

    def get_offense_team_id(self) -> int:
        return self.team_id

    @property
    def is_possession_ending_event(self) -> bool:
        return False

    @property
    def event_stats(self) -> List[Dict]:
        return []


class Substitution(Event):
    event_type = 8

    def __init__(self, event: Dict, order: int):
        super().__init__(event, order)

    @property
    def outgoing_player_id(self) -> int:
        return getattr(self, "player1_id", 0)

    @property
    def incoming_player_id(self) -> int:
        return getattr(self, "player2_id", 0)

    @property
    def current_players(self) -> Dict[int, List[int]]:
        players = self.previous_event.current_players.copy()
        team_players = players.get(self.team_id, [])
        players[self.team_id] = [
            self.incoming_player_id if p == self.outgoing_player_id else p
            for p in team_players
        ]
        return players

    def get_offense_team_id(self) -> int:
        return self.previous_event.get_offense_team_id()

    @property
    def is_possession_ending_event(self) -> bool:
        return False

    @property
    def event_stats(self) -> List[Dict]:
        return []


class JumpBall(Event):
    event_type = 10

    def __init__(self, event: Dict, order: int):
        super().__init__(event, order)

    def get_offense_team_id(self) -> int:
        return self.team_id

    @property
    def is_possession_ending_event(self) -> bool:
        return True

    @property
    def event_stats(self) -> List[Dict]:
        return []


class StartOfPeriod(Event):
    event_type = 12

    def __init__(self, event: Dict, order: int):
        super().__init__(event, order)
        self.period_starters: Dict[int, List[int]] = {}

    def get_offense_team_id(self) -> int:
        return self.team_starting_with_ball

    @property
    def team_starting_with_ball(self) -> int:
        return list(self.period_starters.keys())[0] if self.period_starters else 0

    @property
    def current_players(self) -> Dict[int, List[int]]:
        return self.period_starters

    @property
    def is_possession_ending_event(self) -> bool:
        return False

    @property
    def event_stats(self) -> List[Dict]:
        return []


class EndOfPeriod(Event):
    event_type = 13

    def __init__(self, event: Dict, order: int):
        super().__init__(event, order)

    def get_offense_team_id(self) -> int:
        return self.previous_event.get_offense_team_id()

    @property
    def is_possession_ending_event(self) -> bool:
        return True

    @property
    def event_stats(self) -> List[Dict]:
        return []


class Ejection(Event):
    event_type = 11

    def __init__(self, event: Dict, order: int):
        super().__init__(event, order)

    def get_offense_team_id(self) -> int:
        return self.previous_event.get_offense_team_id()

    @property
    def is_possession_ending_event(self) -> bool:
        return False

    @property
    def event_stats(self) -> List[Dict]:
        return []


class Timeout(Event):
    event_type = 9

    def __init__(self, event: Dict, order: int):
        super().__init__(event, order)

    def get_offense_team_id(self) -> int:
        return self.previous_event.get_offense_team_id()

    @property
    def is_possession_ending_event(self) -> bool:
        return False

    @property
    def event_stats(self) -> List[Dict]:
        return []


class Replay(Event):
    event_type = 18

    def __init__(self, event: Dict, order: int):
        super().__init__(event, order)

    def get_offense_team_id(self) -> int:
        return self.previous_event.get_offense_team_id()

    @property
    def is_possession_ending_event(self) -> bool:
        return False

    @property
    def event_stats(self) -> List[Dict]:
        return []


class Violation(Event):
    event_type = 7

    def __init__(self, event: Dict, order: int):
        super().__init__(event, order)

    def get_offense_team_id(self) -> int:
        return self.team_id

    @property
    def is_possession_ending_event(self) -> bool:
        return True

    @property
    def event_stats(self) -> List[Dict]:
        return []


"""
The ``Possession`` class has some basic properties for handling possession data
"""
from itertools import groupby
from operator import itemgetter

import pbpstats


class Possession(object):
    """
    Class for possession

    :param list events: list of
        :obj:`~pbpstats.resources.enhanced_pbp.enhanced_pbp_item.EnhancedPbpItem` items for possession,
        typically from a possession data loader
    """

    def __init__(self, events):
        self.game_id = events[0].game_id
        self.period = events[0].period
        self.events = events

    def __repr__(self):
        return (
            f"<{type(self).__name__} GameId: {self.game_id}, Period: {self.period}, "
            f"Number: {self.number}, StartTime: {self.start_time}, EndTime: {self.end_time}, "
            f"OffenseTeamId: {self.offense_team_id}>"
        )

    @property
    def data(self):
        """
        returns dict possession data
        """
        return self.__dict__

    @property
    def start_time(self):
        """
        returns the time remaining (MM:SS) in the period when the possession started
        """
        if not hasattr(self, "previous_possession") or self.previous_possession is None:
            return self.events[0].clock
        return self.previous_possession.events[-1].clock

    @property
    def end_time(self):
        """
        returns the time remaining (MM:SS) in the period when the possession ended
        """
        return self.events[-1].clock

    @property
    def start_score_margin(self):
        """
        returns the score margin from the perspective of the team on offense when the possession started
        """
        if not hasattr(self, "previous_possession") or self.previous_possession is None:
            score = self.events[0].score
        else:
            score = self.previous_possession.events[-1].score
        offense_team_id = self.offense_team_id
        offense_points = score[offense_team_id]
        defense_points = 0
        for team_id, points in score.items():
            if team_id != offense_team_id:
                defense_points = points
        return offense_points - defense_points

    def get_team_ids(self):
        """
        returns a list with the team ids of both teams playing
        """
        team_ids = list(
            set(
                [
                    event.team_id
                    for event in self.events
                    if hasattr(event, "team_id") and event.team_id != 0
                ]
            )
        )
        prev_poss = self.previous_possession
        while len(team_ids) != 2 and prev_poss is not None:
            team_ids += [
                event.team_id for event in prev_poss.events if event.team_id != 0
            ]
            team_ids = list(set(team_ids))
            prev_poss = prev_poss.previous_possession
        next_poss = self.next_possession
        while len(team_ids) != 2 and next_poss is not None:
            team_ids += [
                event.team_id for event in next_poss.events if event.team_id != 0
            ]
            team_ids = list(set(team_ids))
            next_poss = next_poss.next_possession
        return team_ids

    @property
    def offense_team_id(self):
        """
        returns team id for team on offense on possession
        """
        if len(self.events) == 1 and isinstance(self.events[0], JumpBall):
            # if possession only has one event and it is a jump ball, need to check
            # how previous possession ended to see which team actually started with the ball
            # because team id on jump ball is team that won the jump ball
            prev_event = self.previous_possession_ending_event
            if isinstance(prev_event, Turnover) and not prev_event.is_no_turnover:
                team_ids = self.get_team_ids()
                return (
                    team_ids[0]
                    if team_ids[1] == prev_event.get_offense_team_id()
                    else team_ids[1]
                )
            if isinstance(prev_event, Rebound) and prev_event.is_real_rebound:
                if not prev_event.oreb:
                    team_ids = self.get_team_ids()
                    return (
                        team_ids[0]
                        if team_ids[1] == prev_event.get_offense_team_id()
                        else team_ids[1]
                    )
                return prev_event.get_offense_team_id()
            if isinstance(prev_event, (FieldGoal, FreeThrow)):
                if prev_event.is_made:
                    team_ids = self.get_team_ids()
                    return (
                        team_ids[0]
                        if team_ids[1] == prev_event.get_offense_team_id()
                        else team_ids[1]
                    )
                return prev_event.get_offense_team_id()
        return self.events[0].get_offense_team_id()

    @property
    def possession_has_timeout(self):
        """
        returns True if there was a timeout called on the current possession, False otherwise
        """
        for i, event in enumerate(self.events):
            if isinstance(event, Timeout) and event.clock != self.end_time:
                # timeout is not at possession end time
                if not (
                    event.next_event is not None
                    and (
                        isinstance(event.next_event, FreeThrow)
                        and not event.next_event.is_technical_ft
                    )
                    and event.clock == event.next_event.clock
                ):
                    # check to make sure timeout is not between/before FTs
                    return True
            elif isinstance(event, Timeout) and event.clock == self.end_time:
                timeout_time = event.clock
                after_timeout_index = i + 1
                # call time out and turn ball over at same time as timeout following time out
                for possession_event in self.events[after_timeout_index:]:
                    if (
                        isinstance(possession_event, Turnover)
                        and not possession_event.is_no_turnover
                        and possession_event.clock == timeout_time
                    ):
                        return True
        return False

    @property
    def previous_possession_has_timeout(self):
        """
        returns True if there was a timeout called at same time as possession ended, False otherwise
        """
        if self.previous_possession is not None:
            for event in self.previous_possession.events:
                if isinstance(event, Timeout) and event.clock == self.start_time:
                    if not (
                        event.next_event is not None
                        and isinstance(event.next_event, FreeThrow)
                        and event.clock == event.next_event.clock
                    ):
                        # check to make sure timeout is not beween FTs
                        return True
        return False

    @property
    def previous_possession_ending_event(self):
        """
        returns previous possession ending event - ignoring subs
        """
        previous_event_index = -1
        while isinstance(
            self.previous_possession.events[previous_event_index], Substitution
        ) and len(self.previous_possession.events) > abs(previous_event_index):
            previous_event_index -= 1
        return self.previous_possession.events[previous_event_index]

    @property
    def possession_start_type(self):
        """
        returns possession start type string
        """
        if self.number == 1:
            return pbpstats.OFF_DEADBALL_STRING
        if self.possession_has_timeout or self.previous_possession_has_timeout:
            return pbpstats.OFF_TIMEOUT_STRING
        previous_possession_ending_event = self.previous_possession_ending_event
        if (
            isinstance(previous_possession_ending_event, (FieldGoal, FreeThrow))
            and previous_possession_ending_event.is_made
        ):
            shot_type = previous_possession_ending_event.shot_type
            return f"Off{shot_type}{pbpstats.MAKE_STRING}"
        if isinstance(previous_possession_ending_event, Turnover):
            if previous_possession_ending_event.is_steal:
                return pbpstats.OFF_LIVE_BALL_TURNOVER_STRING
            return pbpstats.OFF_DEADBALL_STRING
        if isinstance(previous_possession_ending_event, Rebound):
            if previous_possession_ending_event.player1_id == 0:
                # team rebound
                return pbpstats.OFF_DEADBALL_STRING
            missed_shot = previous_possession_ending_event.missed_shot
            shot_type = missed_shot.shot_type
            if hasattr(missed_shot, "is_blocked") and missed_shot.is_blocked:
                return f"Off{shot_type}{pbpstats.BLOCK_STRING}"
            return f"Off{shot_type}{pbpstats.MISS_STRING}"

        if isinstance(previous_possession_ending_event, JumpBall):
            # jump balls tipped out of bounds have no player2_id and should be off deadball
            if not hasattr(previous_possession_ending_event, "player2_id"):
                return pbpstats.OFF_LIVE_BALL_TURNOVER_STRING
            else:
                return pbpstats.OFF_DEADBALL_STRING
        return pbpstats.OFF_DEADBALL_STRING

    @property
    def previous_possession_end_shooter_player_id(self):
        """
        returns player id of player who took shot (make or miss) that ended previous possession.
        returns 0 if previous possession did not end with made field goal or live ball rebound
        """
        if self.previous_possession is not None and not (
            self.possession_has_timeout or self.previous_possession_has_timeout
        ):
            previous_possession_ending_event = self.previous_possession_ending_event
            if (
                isinstance(previous_possession_ending_event, FieldGoal)
                and previous_possession_ending_event.is_made
            ):
                return previous_possession_ending_event.player1_id
            if isinstance(previous_possession_ending_event, Rebound):
                if previous_possession_ending_event.player1_id != 0:
                    missed_shot = previous_possession_ending_event.missed_shot
                    return missed_shot.player1_id
        return 0

    @property
    def previous_possession_end_rebound_player_id(self):
        """
        returns player id of player who got rebound that ended previous possession.
        returns 0 if previous possession did not end with a live ball rebound
        """
        if self.previous_possession is not None and not (
            self.possession_has_timeout or self.previous_possession_has_timeout
        ):
            previous_possession_ending_event = self.previous_possession_ending_event
            if isinstance(previous_possession_ending_event, Rebound):
                if previous_possession_ending_event.player1_id != 0:
                    return previous_possession_ending_event.player1_id
        return 0

    @property
    def previous_possession_end_turnover_player_id(self):
        """
        returns player id of player who turned ball over that ended previous possession.
        returns 0 if previous possession did not end with a live ball turnover
        """
        if self.previous_possession is not None and not (
            self.possession_has_timeout or self.previous_possession_has_timeout
        ):
            previous_possession_ending_event = self.previous_possession_ending_event
            if isinstance(previous_possession_ending_event, Turnover):
                if previous_possession_ending_event.is_steal:
                    return previous_possession_ending_event.player1_id
        return 0

    @property
    def previous_possession_end_steal_player_id(self):
        """
        returns player id of player who got steal that ended previous possession.
        returns 0 if previous possession did not end with a live ball turnover
        """
        if self.previous_possession is not None and not (
            self.possession_has_timeout or self.previous_possession_has_timeout
        ):
            previous_possession_ending_event = self.previous_possession_ending_event
            if isinstance(previous_possession_ending_event, Turnover):
                if previous_possession_ending_event.is_steal:
                    return previous_possession_ending_event.player3_id
        return 0

    @property
    def possession_stats(self):
        """
        returns list of dicts with aggregate stats for possession
        """
        grouper = itemgetter(
            "player_id",
            "team_id",
            "opponent_team_id",
            "lineup_id",
            "opponent_lineup_id",
            "stat_key",
        )
        results = []
        event_stats = [
            event_stat for event in self.events for event_stat in event.event_stats
        ]
        for key, group in groupby(sorted(event_stats, key=grouper), grouper):
            temp_dict = dict(
                zip(
                    [
                        "player_id",
                        "team_id",
                        "opponent_team_id",
                        "lineup_id",
                        "opponent_lineup_id",
                        "stat_key",
                    ],
                    key,
                )
            )
            temp_dict["stat_value"] = sum(item["stat_value"] for item in group)
            results.append(temp_dict)

        return results
