"""
``StatsNbaEnhancedPbpLoader`` loads pbp data for a game and
creates :obj:`~pbpstats.resources.enhanced_pbp.enhanced_pbp_item.EnhancedPbpItem` objects
for each event

Enhanced data for each event includes current players on floor, score, fouls to give and number of fouls committed by each player,
plus additional data depending on event type

The following code will load pbp data for game id "0021900001" from a file located in a subdirectory of the /data directory

.. code-block:: python

    from pbpstats.data_loader import StatsNbaEnhancedPbpFileLoader, StatsNbaEnhancedPbpLoader

    source_loader = StatsNbaEnhancedPbpFileLoader("/data")
    pbp_loader = StatsNbaEnhancedPbpLoader("0021900001", source_loader)
    print(pbp_loader.items[0].data)  # prints dict with the first event of the game
"""
import json
import os
import logging

from pbpstats.data_loader.data_nba.pbp.loader import DataNbaPbpLoader
from pbpstats.data_loader.data_nba.pbp.web import DataNbaPbpWebLoader
from pbpstats.data_loader.nba_enhanced_pbp_loader import NbaEnhancedPbpLoader
from pbpstats.data_loader.stats_nba.pbp.loader import StatsNbaPbpLoader
from pbpstats.data_loader.stats_nba.pbp.web import StatsNbaPbpV3WebLoader
from pbpstats.data_loader.stats_nba.shots.loader import StatsNbaShotsLoader
from pbpstats.resources.enhanced_pbp import FieldGoal
from pbpstats.resources.enhanced_pbp.rebound import EventOrderError
from pbpstats.resources.enhanced_pbp.stats_nba.enhanced_pbp_factory import (
    StatsNbaEnhancedPbpFactory,
)

logger = logging.getLogger(__name__)


class StatsNbaEnhancedPbpLoader(StatsNbaPbpLoader, NbaEnhancedPbpLoader):
    """
    Loads stats.nba.com source enhanced pbp data for game.
    Events are stored in items attribute as :obj:`~pbpstats.resources.enhanced_pbp.enhanced_pbp_item.EnhancedPbpItem` objects

    :param str game_id: NBA Stats Game Id
    :param source_loader: :obj:`~pbpstats.data_loader.stats_nba.enhanced_pbp.file.StatsNbaEnhancedPbpFileLoader` or :obj:`~pbpstats.data_loader.stats_nba.enhanced_pbp.file.StatsNbaEnhancedPbpWebLoader` object
    :raises: :obj:`~pbpstats.resources.enhanced_pbp.start_of_period.InvalidNumberOfStartersException`:
        If all 5 players that start the period for a team can't be determined.
        You can add the correct period starters to overrides/missing_period_starters.json in your data directory to fix this.
    :raises: :obj:`~pbpstats.resources.enhanced_pbp.rebound.EventOrderError`:
        If rebound event is not immediately following a missed shot event.
        You can manually edit the event order in the pbp file stored on disk to fix this.
    """

    data_provider = "stats_nba"
    resource = "EnhancedPbp"
    parent_object = "Game"

    def __init__(self, game_id, source_loader):
        self.shots_source_loader = source_loader.shots_source_loader
        self.v3_source_loader = getattr(source_loader, "v3_source_loader", None)
        self.boxscore_source_loader = getattr(
            source_loader, "boxscore_source_loader", None
        )
        super().__init__(game_id, source_loader)

    def _make_pbp_items(self):
        self._fix_order_when_technical_foul_before_period_start()
        self.factory = StatsNbaEnhancedPbpFactory()
        self.items = [
            self.factory.get_event_class(item["EVENTMSGTYPE"])(item, i)
            for i, item in enumerate(self.data)
        ]

        if getattr(self, "boxscore_source_loader", None) is not None:
            from pbpstats.resources.enhanced_pbp import StartOfPeriod

            for ev in self.items:
                if isinstance(ev, StartOfPeriod):
                    ev.boxscore_source_loader = self.boxscore_source_loader

        self._add_extra_attrs_to_all_events()
        self._check_rebound_event_order(6)
        self._add_shot_x_y_coords()

    def _add_shot_x_y_coords(self):
        shots_loader = StatsNbaShotsLoader(self.game_id, self.shots_source_loader)
        shots_event_num_map = {
            item.game_event_id: {"loc_x": item.loc_x, "loc_y": item.loc_y}
            for item in shots_loader.items
        }
        for item in self.items:
            if (
                isinstance(item, FieldGoal)
                and item.event_num in shots_event_num_map.keys()
            ):
                item.locX = shots_event_num_map[item.event_num]["loc_x"]
                item.locY = shots_event_num_map[item.event_num]["loc_y"]

    def _check_rebound_event_order(self, max_retries):
        """
        checks rebound events to make sure they are ordered correctly
        """
        attempts = 0

        # First, try known pattern-based fixes up to max_retries times.
        while attempts <= max_retries:
            try:
                for event in self.items:
                    if hasattr(event, "missed_shot"):
                        # Accessing missed_shot will raise EventOrderError if order is bad
                        event.missed_shot
                # If we got here, all rebound -> missed_shot links are valid
                return
            except EventOrderError as e:
                self._fix_common_event_order_error(e)
                # rebuild items after modifying underlying source_data
                self.items = [
                    self.factory.get_event_class(item["EVENTMSGTYPE"])(item, i)
                    for i, item in enumerate(self.data)
                ]
                self._add_extra_attrs_to_all_events()
                attempts += 1

        # If common fixes didn't fully resolve problems, try v3 ordering.
        try:
            self._use_stats_nba_v3_event_order()
            self.items = [
                self.factory.get_event_class(item["EVENTMSGTYPE"])(item, i)
                for i, item in enumerate(self.data)
            ]
            self._add_extra_attrs_to_all_events()

            # Re-check after v3 ordering.
            for event in self.items:
                if hasattr(event, "missed_shot"):
                    event.missed_shot
            return
        except EventOrderError:
            # v3 ordering still leads to errors; fall through to data.nba.com
            pass
        except Exception:
            # v3 unavailable or malformed; fall through
            pass

    def _fix_order_when_technical_foul_before_period_start(self):
        """
        When someone gets a technical foul between periods the technical foul and free throw are
        between the end of period event and the start of period event.
        The causes an error when parsing possessions. Move events to after start of period event
        """
        headers = self.source_data["resultSets"][0]["headers"]
        rows = self.source_data["resultSets"][0]["rowSet"]
        event_msg_type_index = headers.index("EVENTMSGTYPE")
        event_msg_type_action_index = headers.index("EVENTMSGACTIONTYPE")
        period_index = headers.index("PERIOD")
        period_events_without_period_start_event = {}
        period_start_events = {}
        start_period_event_msg_type = 12
        technical_foul_event_msg_type = 6
        technical_foul_event_msg_action_types = [11, 12, 13, 16, 18, 19, 25, 30]
        new_order_of_events = []
        reorder_events = False
        period_start_events_found = []

        # Check if there is a technical foul event before a period start event
        for row in rows:
            period = row[period_index]
            if row[event_msg_type_index] == start_period_event_msg_type:
                period_start_events_found.append(period)
            if (
                row[event_msg_type_index] == technical_foul_event_msg_type
                and row[event_msg_type_action_index]
                in technical_foul_event_msg_action_types
            ):
                if period not in period_start_events_found:
                    reorder_events = True

        # If there isn't, do nothing
        if not reorder_events:
            return

        # If there is rearrange event order so that period start is always the first event appearing for the period
        for row in rows:
            period = row[period_index]
            if row[event_msg_type_index] == start_period_event_msg_type:
                period_start_events[period] = row
            elif period not in period_events_without_period_start_event.keys():
                period_events_without_period_start_event[period] = [row]
            else:
                period_events_without_period_start_event[period].append(row)

        for period in range(1, 11):
            if period in period_start_events.keys():
                new_order_of_events.append(period_start_events[period])
            if period in period_events_without_period_start_event.keys():
                for event in period_events_without_period_start_event[period]:
                    new_order_of_events.append(event)

        self.source_data["resultSets"][0]["rowSet"] = new_order_of_events
        self._save_data_to_file()

    def _fix_common_event_order_error(self, exception):
        """
        fixs common cases where events are out of order
        current cases are:
        - subs/timeouts between free throws being between missed second FT and rebound
        - end of period replay events being between missed shot and rebound
        - rebound and shot in reverse order
        - shot, rebound, rebound - first rebound needs to me moved to before shot
        - shot, rebound, rebound - second rebound needs to be moved ahead of shot and first rebound
        """
        event_num = int(str(exception).split("EventNum: ")[-1].split(">")[0])
        headers = self.source_data["resultSets"][0]["headers"]
        rows = self.source_data["resultSets"][0]["rowSet"]
        event_num_index = headers.index("EVENTNUM")
        event_type_index = headers.index("EVENTMSGTYPE")
        # Additional indices for rebound fix patterns
        try:
            period_index = headers.index("PERIOD")
        except ValueError:
            period_index = None

        try:
            pctimestring_index = headers.index("PCTIMESTRING")
        except ValueError:
            pctimestring_index = None

        try:
            player1_id_index = headers.index("PLAYER1_ID")
        except ValueError:
            player1_id_index = None

        handled = False
        issue_event_index = None
        for i, row in enumerate(rows):
            if row[event_num_index] == int(event_num):
                issue_event_index = i

        if issue_event_index is None:
            # Fall back to saving without changes if the event wasn't found
            self._save_data_to_file()
            return

        if rows[issue_event_index][event_type_index] in [8, 9]:
            # these are subs/timeouts that are in between missed ft and rebound
            # move all sub/timeout events between ft and rebound to before ft
            row_index = issue_event_index
            while rows[row_index][event_type_index] in [8, 9]:
                row_index -= 1

            # rows[row_index] should be moved to right before rows[issue_event_index]
            new_rows = []
            row_to_move_event_num = rows[row_index][event_num_index]
            for row in rows:
                if row[event_num_index] == row_to_move_event_num:
                    row_to_move = row
                elif row[event_num_index] == int(event_num):
                    new_rows.append(row)
                    new_rows.append(row_to_move)
                else:
                    new_rows.append(row)

            self.source_data["resultSets"][0]["rowSet"] = new_rows
            handled = True
        elif (
            rows[issue_event_index][event_type_index] == 18
            and rows[issue_event_index + 1][event_type_index] == 4
        ):
            # these are replays that are in between missed shot and rebound
            # move replay event after rebound
            replay_event = rows[issue_event_index]
            rebound_event = rows[issue_event_index + 1]

            self.source_data["resultSets"][0]["rowSet"][
                issue_event_index + 1
            ] = replay_event
            self.source_data["resultSets"][0]["rowSet"][
                issue_event_index
            ] = rebound_event
            handled = True
        elif (
            rows[issue_event_index + 1][event_type_index] == 4
            and rows[issue_event_index + 1][event_num_index] == int(event_num) - 1
        ):
            # rebound and shot need to be flipped
            rebound_event = rows[issue_event_index + 1]
            shot_event = rows[issue_event_index]
            self.source_data["resultSets"][0]["rowSet"][
                issue_event_index
            ] = rebound_event
            self.source_data["resultSets"][0]["rowSet"][
                issue_event_index + 1
            ] = shot_event
            handled = True
        elif (
            rows[issue_event_index + 1][event_type_index] == 4
            and rows[issue_event_index + 1][event_num_index] == int(event_num) + 2
            and rows[issue_event_index - 1][event_type_index] == 2
            and rows[issue_event_index - 1][event_num_index] == int(event_num) + 1
        ):
            # shot, rebound, rebound - first rebound need to me moved to before shot
            rebound_event = rows[issue_event_index]
            shot_event = rows[issue_event_index - 1]
            self.source_data["resultSets"][0]["rowSet"][issue_event_index] = shot_event
            self.source_data["resultSets"][0]["rowSet"][
                issue_event_index - 1
            ] = rebound_event
            handled = True
        elif (
            rows[issue_event_index + 1][event_type_index] == 4
            and rows[issue_event_index + 1][event_num_index] == int(event_num) - 2
            and rows[issue_event_index - 1][event_type_index] == 2
            and rows[issue_event_index - 1][event_num_index] == int(event_num) - 1
        ):
            # shot, rebound, rebound - second rebound needs to be moved ahead of shot and first rebound
            first_rebound = rows[issue_event_index + 1]
            second_rebound = rows[issue_event_index]
            shot_event = rows[issue_event_index - 1]
            self.source_data["resultSets"][0]["rowSet"][
                issue_event_index + 1
            ] = second_rebound
            self.source_data["resultSets"][0]["rowSet"][issue_event_index] = shot_event
            self.source_data["resultSets"][0]["rowSet"][
                issue_event_index - 1
            ] = first_rebound
            handled = True

        if not handled:
            # === NEW PATTERN: duplicate adjacent rebounds - delete team rebound ===
            if (
                issue_event_index is not None
                and issue_event_index + 1 < len(rows)
                and rows[issue_event_index][event_type_index] == 4
                and rows[issue_event_index + 1][event_type_index] == 4
                and player1_id_index is not None
            ):
                first = rows[issue_event_index]
                second = rows[issue_event_index + 1]
                first_pid = first[player1_id_index] or 0
                second_pid = second[player1_id_index] or 0

                # Treat PLAYER1_ID >= 1610000000 or 0 as "team"/placeholder rebound
                first_is_team = first_pid == 0 or first_pid >= 1610000000
                second_is_team = second_pid == 0 or second_pid >= 1610000000

                # Prefer to delete a team rebound if paired with a player rebound
                if first_is_team and not second_is_team:
                    del rows[issue_event_index]
                elif second_is_team and not first_is_team:
                    del rows[issue_event_index + 1]
                else:
                    # If both look like players or both look like teams, fall back
                    # to deleting the later one to keep earlier ordering stable.
                    del rows[issue_event_index + 1]

                self.source_data["resultSets"][0]["rowSet"] = rows
                self._save_data_to_file()
                return

            # === NEW PATTERN: rebound at same clock as later shot/FT - move rebound ===
            if (
                issue_event_index is not None
                and rows[issue_event_index][event_type_index] == 4
                and period_index is not None
                and pctimestring_index is not None
            ):
                period = rows[issue_event_index][period_index]
                clock = rows[issue_event_index][pctimestring_index]

                # Scan a few events ahead in same period and same clock
                j = issue_event_index + 1
                max_j = min(issue_event_index + 5, len(rows))
                target_index = None

                while j < max_j:
                    row_j = rows[j]
                    # Stop if period changes
                    if row_j[period_index] != period:
                        break
                    # Stop if clock changes
                    if row_j[pctimestring_index] != clock:
                        break
                    # If we find a MISS (2) or FT (3) at the same time, that's our target
                    if row_j[event_type_index] in (2, 3):
                        target_index = j
                        break
                    j += 1

                if target_index is not None:
                    # Move the rebound to immediately after the shot/FT
                    rebound_row = rows.pop(issue_event_index)
                    # Note: if target_index > issue_event_index, indices shift down by 1
                    insert_at = target_index
                    if target_index > issue_event_index:
                        insert_at = target_index - 1
                    rows.insert(insert_at + 1, rebound_row)

                    self.source_data["resultSets"][0]["rowSet"] = rows
                    self._save_data_to_file()
                    return

        self._save_data_to_file()

    def _use_stats_nba_v3_event_order(self):
        """
        Reorders all stats.nba.com pbp events to match playbyplayv3 actionId order.

        This method:
          - Fetches playbyplayv3 for the game (if available),
          - Builds an actionNumber -> canonical order index mapping based on
            actionId ordering,
          - Sorts the existing stats.nba.com rowSet using that mapping,
          - Does *not* insert or delete any events.
        If v3 data is unavailable or malformed, this method is a no-op.
        """
        try:
            if getattr(self, "v3_source_loader", None) is not None:
                v3_data = self.v3_source_loader.load_data(self.game_id)
            else:
                v3_loader = StatsNbaPbpV3WebLoader(self.file_directory)
                v3_data = v3_loader.load_data(self.game_id)
        except Exception:
            # v3 request failed (or local loader errored); nothing to do
            return

        actions = v3_data.get("game", {}).get("actions", [])
        if not actions:
            return

        # Filter actions that have the fields we need, then sort by actionId
        try:
            filtered = [
                a for a in actions
                if "actionNumber" in a and "actionId" in a
            ]
            filtered.sort(key=lambda a: a["actionId"])
        except Exception:
            return

        # Build mapping: actionNumber -> order index
        order_map = {}
        idx = 0
        for a in filtered:
            try:
                num = int(a["actionNumber"])
            except (TypeError, ValueError):
                continue
            if num not in order_map:
                order_map[num] = idx
                idx += 1

        if not order_map:
            return

        headers = self.source_data["resultSets"][0]["headers"]
        rows = self.source_data["resultSets"][0]["rowSet"]
        try:
            event_num_idx = headers.index("EVENTNUM")
        except ValueError:
            return

        default_order = len(order_map) + 1000

        def sort_key(row):
            evnum = row[event_num_idx]
            try:
                evnum_int = int(evnum)
            except (TypeError, ValueError):
                evnum_int = evnum
            base = order_map.get(evnum_int, default_order)
            return (base, evnum_int)

        rows_sorted = sorted(rows, key=sort_key)
        self.source_data["resultSets"][0]["rowSet"] = rows_sorted
        self._save_data_to_file()

    def _use_data_nba_event_order(self):
        """
        Reorders all events to be the same order as data.nba.com pbp.

        This is now used as a secondary fallback after trying the
        playbyplayv3-based repair, so behavior for existing users
        remains unchanged if data.nba.com is available.
        """
        # Order event numbers of events in data.nba.com pbp
        data_nba_pbp = DataNbaPbpLoader(self.game_id, DataNbaPbpWebLoader())
        data_nba_event_num_order = [item.evt for item in data_nba_pbp.items]

        headers = self.source_data["resultSets"][0]["headers"]
        rows = self.source_data["resultSets"][0]["rowSet"]
        event_num_index = headers.index("EVENTNUM")

        # reorder stats.nba.com events to be in same order as data.nba.com events
        new_event_order = []
        for event_num in data_nba_event_num_order:
            for row in rows:
                if row[event_num_index] == event_num:
                    new_event_order.append(row)
        self.source_data["resultSets"][0]["rowSet"] = new_event_order
        self._save_data_to_file()

    def _save_data_to_file(self):
        if self.file_directory is not None and os.path.isdir(self.file_directory):
            file_path = f"{self.file_directory}/pbp/stats_{self.game_id}.json"
            with open(file_path, "w") as outfile:
                json.dump(self.source_data, outfile)
