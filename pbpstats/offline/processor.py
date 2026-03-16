from typing import Callable, List, Dict, Optional
import pandas as pd

from pbpstats.data_loader.nba_enhanced_pbp_loader import NbaEnhancedPbpLoader
from pbpstats.data_loader.nba_possession_loader import NbaPossessionLoader
from pbpstats.resources.enhanced_pbp import Rebound
from pbpstats.resources.enhanced_pbp.rebound import EventOrderError as ReboundEventOrderError
from pbpstats.resources.enhanced_pbp.stats_nba.enhanced_pbp_factory import (
    StatsNbaEnhancedPbpFactory,
)
from pbpstats.resources.possessions.possession import Possession
from pbpstats.resources.possessions.possessions import Possessions

from pbpstats.offline.ordering import (
    create_raw_dicts_from_df,
    dedupe_with_v3,
    patch_start_of_periods,
    reorder_with_v3,
    _ensure_eventnum_int,
)

FetchPbpV3Fn = Callable[[str], pd.DataFrame]

# If True, the fallback in _fix_event_order will never auto-delete PLAYER rebounds.
# It will only auto-delete TEAM/zero rebounds and will re-raise when only player
# rebounds are candidates.
REBOUND_STRICT_MODE: bool = True
MAX_REBOUND_REPAIR_RETRIES: int = 100


def set_rebound_strict_mode(strict: bool = True) -> None:
    """
    Toggle strict mode for rebound event-order repair.

    strict=True: only TEAM/0 rebounds may be auto-deleted.
    strict=False: PLAYER rebounds may also be auto-deleted as a last resort.
    """
    global REBOUND_STRICT_MODE
    REBOUND_STRICT_MODE = strict


class PbpProcessor(NbaEnhancedPbpLoader, NbaPossessionLoader):
    """
    Offline processor that:

      - Takes raw stats.nba-style event dicts,
      - Builds enhanced events (lineups, score, fouls, shot clock, etc.),
      - Repairs rebound event ordering where necessary,
      - Splits events into Possession objects.

    This wraps the core pbpstats logic (NbaEnhancedPbpLoader + NbaPossessionLoader)
    with a robust event-order repair loop designed for parquet/offline workflows.
    """

    def __init__(
        self,
        game_id: str,
        raw_data_dicts: List[dict],
        rebound_deletions_list: Optional[List[Dict]] = None,
        boxscore_source_loader=None,
        file_directory: Optional[str] = None,
    ):
        self.game_id = str(game_id).zfill(10)
        self.league = "nba"
        self.file_directory = file_directory
        self.data = raw_data_dicts
        self.factory = StatsNbaEnhancedPbpFactory()
        self.boxscore_source_loader = boxscore_source_loader
        # Per-processor log of any fallback rebound deletions
        self._rebound_deletions_list = rebound_deletions_list

        self._process_with_retries(max_retries=MAX_REBOUND_REPAIR_RETRIES)

    def _build_items_from_data(self) -> None:
        self.items = [
            self.factory.get_event_class(item["EVENTMSGTYPE"])(item, i)
            for i, item in enumerate(self.data)
        ]

        if self.boxscore_source_loader is not None:
            from pbpstats.resources.enhanced_pbp import StartOfPeriod

            for item in self.items:
                if isinstance(item, StartOfPeriod):
                    item.boxscore_source_loader = self.boxscore_source_loader

    def _repair_silent_ft_rebound_windows(self) -> None:
        """
        Repair narrow historical missed-FT clusters that do not raise
        ReboundEventOrderError but still pair a real player rebound to the wrong
        missed free throw, causing it to be treated as a placeholder.
        """
        rows = self.data

        def et(idx: int) -> Optional[int]:
            if 0 <= idx < len(rows):
                return rows[idx].get("EVENTMSGTYPE")
            return None

        def text_value(idx: int, key: str) -> str:
            if 0 <= idx < len(rows):
                return str(rows[idx].get(key) or "")
            return ""

        def int_value(value) -> Optional[int]:
            if value is None or pd.isna(value):
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        def team_id(idx: int) -> Optional[int]:
            if 0 <= idx < len(rows):
                return int_value(rows[idx].get("PLAYER1_TEAM_ID"))
            return None

        def effective_team_id(idx: int) -> Optional[int]:
            team = team_id(idx)
            if team is not None and team > 0:
                return team
            if 0 <= idx < len(rows):
                player = int_value(rows[idx].get("PLAYER1_ID"))
                if player is not None and player >= 1610000000:
                    return player
            return None

        def is_team_rebound(idx: int) -> bool:
            if et(idx) != 4 or not (0 <= idx < len(rows)):
                return False
            player = int_value(rows[idx].get("PLAYER1_ID"))
            return player is None or player == 0 or player >= 1610000000

        def is_missed_ft(idx: int) -> bool:
            if et(idx) != 3:
                return False
            desc = " ".join(
                [
                    text_value(idx, "HOMEDESCRIPTION"),
                    text_value(idx, "VISITORDESCRIPTION"),
                    text_value(idx, "NEUTRALDESCRIPTION"),
                ]
            ).lower()
            return "miss" in desc

        def clock_seconds(idx: int) -> Optional[int]:
            if not (0 <= idx < len(rows)):
                return None
            clock = rows[idx].get("PCTIMESTRING")
            if not clock:
                return None
            try:
                minutes, seconds = str(clock).split(":")
                return int(minutes) * 60 + int(seconds)
            except (TypeError, ValueError):
                return None

        changed = True
        while changed:
            changed = False

            # Reversed and-one / 1-of-1 block:
            #   REBOUND_B -> MISS_FT_A -> FOUL_B -> MADE_A
            # should be:
            #   MADE_A -> FOUL_B -> MISS_FT_A -> REBOUND_B
            for idx in range(len(rows) - 3):
                if not (
                    et(idx) == 4
                    and et(idx + 1) == 3
                    and et(idx + 2) == 6
                    and et(idx + 3) == 1
                ):
                    continue
                if is_team_rebound(idx) or not is_missed_ft(idx + 1):
                    continue
                period = rows[idx].get("PERIOD")
                clock = rows[idx].get("PCTIMESTRING")
                if any(
                    rows[scan_idx].get("PERIOD") != period
                    or rows[scan_idx].get("PCTIMESTRING") != clock
                    for scan_idx in range(idx, idx + 4)
                ):
                    continue
                rebound_team = effective_team_id(idx)
                ft_team = effective_team_id(idx + 1)
                foul_team = effective_team_id(idx + 2)
                made_team = effective_team_id(idx + 3)
                if (
                    rebound_team is None
                    or ft_team is None
                    or foul_team is None
                    or made_team is None
                    or rebound_team != foul_team
                    or ft_team != made_team
                    or rebound_team == ft_team
                ):
                    continue
                rows[idx : idx + 4] = [
                    rows[idx + 3],
                    rows[idx + 2],
                    rows[idx + 1],
                    rows[idx],
                ]
                changed = True
                break

            if changed:
                continue

            # Reversed two-shot FT block with a real player rebound stranded after
            # the shooting foul but before the missed last FT has been placed:
            #   MISS_FT2_A -> TEAM_REBOUND_A -> MISS_FT1_A -> FOUL_B -> REBOUND_B
            # should be:
            #   FOUL_B -> MISS_FT1_A -> TEAM_REBOUND_A -> MISS_FT2_A -> REBOUND_B
            for idx in range(len(rows) - 4):
                if not (
                    et(idx) == 3
                    and et(idx + 1) == 4
                    and et(idx + 2) == 3
                    and et(idx + 3) == 6
                    and et(idx + 4) == 4
                ):
                    continue
                if (
                    not is_missed_ft(idx)
                    or not is_missed_ft(idx + 2)
                    or not is_team_rebound(idx + 1)
                    or is_team_rebound(idx + 4)
                ):
                    continue
                period = rows[idx].get("PERIOD")
                if any(rows[scan_idx].get("PERIOD") != period for scan_idx in range(idx, idx + 5)):
                    continue
                ft_clock = clock_seconds(idx)
                if (
                    ft_clock is None
                    or clock_seconds(idx + 1) != ft_clock
                    or clock_seconds(idx + 2) != ft_clock
                    or clock_seconds(idx + 3) != ft_clock
                ):
                    continue
                rebound_clock = clock_seconds(idx + 4)
                if rebound_clock is None or not (0 <= ft_clock - rebound_clock <= 1):
                    continue
                ft_team = effective_team_id(idx)
                team_rebound_team = effective_team_id(idx + 1)
                earlier_ft_team = effective_team_id(idx + 2)
                foul_team = effective_team_id(idx + 3)
                rebound_team = effective_team_id(idx + 4)
                if (
                    ft_team is None
                    or team_rebound_team is None
                    or earlier_ft_team is None
                    or foul_team is None
                    or rebound_team is None
                    or ft_team != team_rebound_team
                    or ft_team != earlier_ft_team
                    or foul_team != rebound_team
                    or foul_team == ft_team
                ):
                    continue
                if rows[idx].get("PLAYER1_ID") != rows[idx + 2].get("PLAYER1_ID"):
                    continue
                later_ft_action = int_value(rows[idx].get("EVENTMSGACTIONTYPE"))
                earlier_ft_action = int_value(rows[idx + 2].get("EVENTMSGACTIONTYPE"))
                if (
                    later_ft_action is None
                    or earlier_ft_action is None
                    or later_ft_action <= earlier_ft_action
                ):
                    continue
                rows[idx : idx + 5] = [
                    rows[idx + 3],
                    rows[idx + 2],
                    rows[idx + 1],
                    rows[idx],
                    rows[idx + 4],
                ]
                changed = True
                break

        self.data = rows

    def _process_with_retries(self, max_retries: int) -> None:
        attempts = 0
        while attempts <= max_retries:
            self._repair_silent_ft_rebound_windows()
            try:
                # Build enhanced events
                self._build_items_from_data()
                self._add_extra_attrs_to_all_events()

                # Force evaluation of Rebound.missed_shot so EventOrderError
                # surfaces here instead of downstream. Using the abstract
                # Rebound base class keeps this check stable if factories
                # swap concrete rebound implementations.
                for event in self.items:
                    if isinstance(event, Rebound):
                        _ = event.missed_shot

                # Split into possessions
                self.events = self.items
                events_by_possession = self._split_events_by_possession()
                self.possessions = [Possession(events) for events in events_by_possession]

                # Treat possessions as "items" for NbaPossessionLoader logic
                self.items = self.possessions
                self._add_extra_attrs_to_all_possessions()
                return

            except ReboundEventOrderError as e:
                if attempts == max_retries:
                    raise ReboundEventOrderError(
                        f"Game {self.game_id}: unable to fix rebound event order "
                        f"after {max_retries} attempts. Last error: {e}"
                    )
                self._fix_event_order(e)
                attempts += 1

    def _fix_event_order(self, exception: ReboundEventOrderError) -> None:
        """
        Fix event ordering issues that cause ReboundEventOrderError.

        This is a direct port of the custom repair logic you had in your
        script, including pattern-based fixes and a conservative fallback
        that may delete orphan TEAM rebounds when necessary.
        """
        rows = self.data
        previous_event_num = getattr(exception, "previous_event_num", None)
        rebound_event_num = getattr(exception, "rebound_event_num", None)

        def _find_index(event_num: Optional[int]) -> Optional[int]:
            if event_num is None:
                return None
            for i, row in enumerate(rows):
                if row.get("EVENTNUM") == event_num:
                    return i
            return None

        issue_event_index = _find_index(previous_event_num)
        rebound_event_index = _find_index(rebound_event_num)

        if issue_event_index is None:
            msg = str(exception)
            try:
                legacy_event_num = int(msg.split("EventNum: ")[-1].split(">")[0])
            except Exception:
                raise exception
            issue_event_index = _find_index(legacy_event_num)

        if issue_event_index is None and rebound_event_index is not None and rebound_event_index > 0:
            issue_event_index = rebound_event_index - 1

        if rebound_event_index is None and issue_event_index is not None:
            candidate_index = issue_event_index + 1
            if candidate_index < len(rows) and rows[candidate_index].get("EVENTMSGTYPE") == 4:
                rebound_event_index = candidate_index

        if issue_event_index is None:
            raise exception

        def et(idx: int) -> Optional[int]:
            if 0 <= idx < len(rows):
                return rows[idx].get("EVENTMSGTYPE")
            return None

        def en(idx: int) -> Optional[int]:
            if 0 <= idx < len(rows):
                return rows[idx].get("EVENTNUM")
            return None

        context_event_num = en(issue_event_index)

        def text_value(idx: int, key: str) -> str:
            if 0 <= idx < len(rows):
                return str(rows[idx].get(key) or "")
            return ""

        def team_id(idx: int) -> Optional[int]:
            if 0 <= idx < len(rows):
                return rows[idx].get("PLAYER1_TEAM_ID")
            return None

        def effective_team_id(idx: int) -> Optional[int]:
            team = team_id(idx)
            if team is not None and not pd.isna(team):
                team = int(team)
                if team > 0:
                    return team
            if 0 <= idx < len(rows):
                player = rows[idx].get("PLAYER1_ID")
                if player is not None and not pd.isna(player):
                    player = int(player)
                    if player >= 1610000000:
                        return player
            return None

        def is_team_rebound(idx: int) -> bool:
            if et(idx) != 4 or not (0 <= idx < len(rows)):
                return False
            player = rows[idx].get("PLAYER1_ID")
            if player is None or pd.isna(player):
                return True
            player = int(player)
            return player == 0 or player >= 1610000000

        rebound_period = None
        if rebound_event_index is not None and 0 <= rebound_event_index < len(rows):
            rebound_period = rows[rebound_event_index].get("PERIOD")

        def is_missed_shot_or_ft(idx: int) -> bool:
            if not (0 <= idx < len(rows)):
                return False
            msg_type = et(idx)
            if msg_type == 2:
                return True
            if msg_type != 3:
                return False
            desc = " ".join(
                [
                    text_value(idx, "HOMEDESCRIPTION"),
                    text_value(idx, "VISITORDESCRIPTION"),
                    text_value(idx, "NEUTRALDESCRIPTION"),
                ]
            ).lower()
            return "miss" in desc

        # --- PATTERN -0.95: Sub/timeout block, rebound, then delayed FG miss ---
        # Some historical feeds log:
        #   ... -> SUB/TIMEOUT... -> REBOUND -> MISS
        # where a delayed field-goal miss really happened before the dead-ball block.
        # Handle both offensive and defensive field-goal rebound variants before the
        # generic "nearest prior miss" fallback can steal the wrong earlier miss.
        # Do not use this for free throws; historical FT placeholder rebounds often
        # sit between missed attempts and should keep the original order.
        if (
            rebound_event_index is not None
            and rebound_event_index == issue_event_index + 1
            and et(issue_event_index) in (8, 9)
        ):
            block_start = issue_event_index
            while block_start - 1 >= 0 and et(block_start - 1) in (8, 9):
                block_start -= 1

            rebound_clock = rows[rebound_event_index].get("PCTIMESTRING")
            search_limit = min(rebound_event_index + 4, len(rows))
            for candidate_idx in range(rebound_event_index + 1, search_limit):
                candidate_row = rows[candidate_idx]
                if rebound_period is not None and candidate_row.get("PERIOD") != rebound_period:
                    break
                candidate_clock = candidate_row.get("PCTIMESTRING")
                if candidate_clock != rebound_clock:
                    break
                if et(candidate_idx) == 2:
                    delayed_miss = rows.pop(candidate_idx)
                    rebound_row = rows.pop(rebound_event_index)
                    rows[block_start:block_start] = [delayed_miss, rebound_row]
                    self.data = rows
                    return

        # --- PATTERN -0.9: Real player rebound stranded behind a foul / FT block ---
        # Some old feeds log:
        #   MISS_FG -> foul / FT / TEAM placeholder block -> PLAYER rebound
        # where the player rebound really belongs to the earlier missed field goal.
        # Keep this narrow:
        #   - only move PLAYER rebounds
        #   - only when EVENTNUM immediately follows the earlier missed FG
        #   - only when the rows in between are a real foul / FT block with
        #     optional TEAM placeholders mixed in
        if rebound_event_index is not None and not is_team_rebound(rebound_event_index):
            rebound_event_number = en(rebound_event_index)
            if rebound_event_number is not None:
                search_start = max(0, rebound_event_index - 10)
                for candidate_idx in range(search_start, rebound_event_index):
                    if rows[candidate_idx].get("PERIOD") != rebound_period:
                        continue
                    if en(candidate_idx) != rebound_event_number - 1:
                        continue
                    if et(candidate_idx) != 2:
                        continue

                    block_range = range(candidate_idx + 1, rebound_event_index)
                    if not block_range:
                        continue

                    block_event_types = [et(block_idx) for block_idx in block_range]
                    if not any(event_type in (3, 6) for event_type in block_event_types):
                        continue

                    if all(
                        event_type in (3, 6)
                        or (event_type == 4 and is_team_rebound(block_idx))
                        for block_idx, event_type in zip(block_range, block_event_types)
                    ):
                        rebound_row = rows.pop(rebound_event_index)
                        if rebound_event_index < candidate_idx:
                            candidate_idx -= 1
                        rows.insert(candidate_idx + 1, rebound_row)
                        self.data = rows
                        return

        # --- PATTERN -0.895: Real player rebound stranded behind a dead-ball FT block ---
        # Some old feeds log:
        #   MISS_FT_A -> foul / timeout / sub / FT block -> PLAYER rebound_B
        # where the player rebound really belongs to the earlier missed free throw.
        # Keep this narrow:
        #   - only move PLAYER rebounds
        #   - only when EVENTNUM immediately follows the earlier missed FT
        #   - only when the rows in between are dead-ball events
        if rebound_event_index is not None and not is_team_rebound(rebound_event_index):
            rebound_event_number = en(rebound_event_index)
            if rebound_event_number is not None:
                search_start = max(0, rebound_event_index - 12)
                for candidate_idx in range(search_start, rebound_event_index):
                    if rows[candidate_idx].get("PERIOD") != rebound_period:
                        continue
                    if en(candidate_idx) != rebound_event_number - 1:
                        continue
                    if et(candidate_idx) != 3 or not is_missed_shot_or_ft(candidate_idx):
                        continue

                    block_range = range(candidate_idx + 1, rebound_event_index)
                    if not block_range:
                        continue
                    if not all(et(block_idx) in (3, 6, 8, 9) for block_idx in block_range):
                        continue

                    rebound_team = effective_team_id(rebound_event_index)
                    missed_ft_team = effective_team_id(candidate_idx)
                    if (
                        rebound_team is None
                        or missed_ft_team is None
                        or rebound_team == missed_ft_team
                    ):
                        continue

                    rebound_row = rows.pop(rebound_event_index)
                    if rebound_event_index < candidate_idx:
                        candidate_idx -= 1
                    rows.insert(candidate_idx + 1, rebound_row)
                    self.data = rows
                    return

        # --- PATTERN -0.85: Made shot, then stranded opponent rebound before future miss ---
        # Some 90s feeds log:
        #   MAKE_A -> REBOUND_B -> MISS_B -> MAKE_B -> MISS_A -> REBOUND_B
        # where the rebound really belongs to MISS_B, not to any earlier miss
        # before MAKE_A. Move only the stranded rebound behind that future MISS_B
        # before the generic "nearest prior miss" repair can steal the wrong shot.
        if (
            rebound_event_index is not None
            and rebound_event_index == issue_event_index + 1
            and et(issue_event_index) == 1
            and et(rebound_event_index) == 4
            and rebound_event_index + 4 < len(rows)
            and et(rebound_event_index + 1) == 2
            and et(rebound_event_index + 2) == 1
            and et(rebound_event_index + 3) == 2
            and et(rebound_event_index + 4) == 4
        ):
            rebound_team = effective_team_id(rebound_event_index)
            issue_team = effective_team_id(issue_event_index)
            rebound_clock = rows[rebound_event_index].get("PCTIMESTRING")
            if (
                rebound_team is not None
                and issue_team is not None
                and rebound_team != issue_team
                and rows[rebound_event_index + 1].get("PCTIMESTRING") == rebound_clock
                and rows[rebound_event_index + 2].get("PCTIMESTRING") == rebound_clock
                and rows[rebound_event_index + 3].get("PCTIMESTRING") == rebound_clock
                and rows[rebound_event_index + 4].get("PCTIMESTRING") == rebound_clock
                and team_id(rebound_event_index + 1) == rebound_team
                and team_id(rebound_event_index + 2) == rebound_team
                and team_id(rebound_event_index + 3) == issue_team
                and effective_team_id(rebound_event_index + 4) == rebound_team
                and not is_team_rebound(rebound_event_index)
            ):
                rebound_row = rows.pop(rebound_event_index)
                rows.insert(rebound_event_index + 1, rebound_row)
                self.data = rows
                return

        # --- PATTERN -0.8: TEAM rebound + turnover stranded ahead of its delayed miss ---
        # Some historical feeds log:
        #   MADE/FT -> TEAM_REBOUND_A -> TURNOVER_A -> delayed MISS_A
        # where the TEAM rebound and turnover really belong after the delayed miss.
        # Handle this before the generic "nearest prior miss" repair can steal an
        # earlier valid rebound from the other team.
        if (
            rebound_event_index is not None
            and rebound_event_index == issue_event_index + 1
            and et(issue_event_index) in (1, 3)
            and et(rebound_event_index) == 4
            and is_team_rebound(rebound_event_index)
            and rebound_event_index + 2 < len(rows)
            and et(rebound_event_index + 1) == 5
            and is_missed_shot_or_ft(rebound_event_index + 2)
        ):
            rebound_team = effective_team_id(rebound_event_index)
            turnover_team = effective_team_id(rebound_event_index + 1)
            delayed_miss_team = effective_team_id(rebound_event_index + 2)
            rebound_event_number = en(rebound_event_index)
            delayed_miss_event_number = en(rebound_event_index + 2)
            if (
                rebound_team is not None
                and rebound_team == turnover_team
                and rebound_team == delayed_miss_team
                and rebound_event_number is not None
                and delayed_miss_event_number == rebound_event_number - 1
            ):
                delayed_miss = rows.pop(rebound_event_index + 2)
                rows.insert(rebound_event_index, delayed_miss)
                self.data = rows
                return

        # --- PATTERN -0.79: Player rebound stranded ahead of a future missed FT ---
        # Some historical feeds log:
        #   MADE/other -> REBOUND_B -> foul / FT block -> MISS_FT_A
        # where the player rebound actually belongs after that missed last FT.
        # Keep this narrow:
        #   - only move PLAYER rebounds
        #   - only when the future missed FT EVENTNUM immediately precedes the rebound
        #   - only when the intervening rows are foul / FT / sub events
        if rebound_event_index is not None and not is_team_rebound(rebound_event_index):
            rebound_event_number = en(rebound_event_index)
            if rebound_event_number is not None:
                search_limit = min(rebound_event_index + 8, len(rows))
                for candidate_idx in range(rebound_event_index + 1, search_limit):
                    if rows[candidate_idx].get("PERIOD") != rebound_period:
                        break
                    if en(candidate_idx) != rebound_event_number - 1:
                        continue
                    if not is_missed_shot_or_ft(candidate_idx):
                        continue

                    block_range = range(rebound_event_index + 1, candidate_idx)
                    if not block_range:
                        continue
                    if not any(et(block_idx) in (3, 6) for block_idx in block_range):
                        continue
                    if not all(et(block_idx) in (3, 6, 8) for block_idx in block_range):
                        continue

                    rebound_team = effective_team_id(rebound_event_index)
                    delayed_miss_team = effective_team_id(candidate_idx)
                    if (
                        rebound_team is None
                        or delayed_miss_team is None
                        or rebound_team == delayed_miss_team
                    ):
                        continue

                    rebound_row = rows.pop(rebound_event_index)
                    if rebound_event_index < candidate_idx:
                        candidate_idx -= 1
                    rows.insert(candidate_idx + 1, rebound_row)
                    self.data = rows
                    return

        # --- PATTERN -0.784: Player rebound ahead of future opponent missed FT block ---
        # Some historical feeds log:
        #   REBOUND_A/turnover -> REBOUND_B -> dead-ball FT block for team A -> MISS_FT_A
        # where REBOUND_B is the real defensive rebound on the missed last free throw.
        # Keep this narrow:
        #   - only move PLAYER rebounds
        #   - only when the future missed FT EVENTNUM immediately precedes the rebound
        #   - only when the in-between rows are dead-ball events and TEAM placeholders
        if (
            rebound_event_index is not None
            and not is_team_rebound(rebound_event_index)
            and et(issue_event_index) in (4, 5)
        ):
            rebound_team = effective_team_id(rebound_event_index)
            rebound_event_number = en(rebound_event_index)
            if rebound_team is not None and rebound_event_number is not None:
                search_limit = min(rebound_event_index + 10, len(rows))
                for candidate_idx in range(rebound_event_index + 1, search_limit):
                    if rows[candidate_idx].get("PERIOD") != rebound_period:
                        break
                    if en(candidate_idx) != rebound_event_number - 1:
                        continue
                    if et(candidate_idx) != 3 or not is_missed_shot_or_ft(candidate_idx):
                        continue

                    missed_ft_team = effective_team_id(candidate_idx)
                    if missed_ft_team is None or missed_ft_team == rebound_team:
                        continue

                    block_range = range(rebound_event_index + 1, candidate_idx)
                    if not block_range:
                        continue
                    if not any(et(block_idx) in (6, 8, 9) for block_idx in block_range):
                        continue
                    if not all(
                        et(block_idx) in (3, 6, 8, 9)
                        or (et(block_idx) == 4 and is_team_rebound(block_idx))
                        for block_idx in block_range
                    ):
                        continue

                    rebound_row = rows.pop(rebound_event_index)
                    if rebound_event_index < candidate_idx:
                        candidate_idx -= 1
                    rows.insert(candidate_idx + 1, rebound_row)
                    self.data = rows
                    return

        # --- PATTERN -0.782: Made shot, then stranded same-team rebound before future miss ---
        # Some historical feeds log:
        #   MAKE_A -> REBOUND_A -> delayed same-clock MISS_B
        # where REBOUND_A actually belongs after the future MISS_B, not immediately
        # after the earlier make. Move only the stranded rebound.
        if (
            rebound_event_index is not None
            and et(issue_event_index) == 1
            and et(rebound_event_index) == 4
            and rebound_event_index == issue_event_index + 1
            and not is_team_rebound(rebound_event_index)
        ):
            issue_team = effective_team_id(issue_event_index)
            rebound_team = effective_team_id(rebound_event_index)
            rebound_clock = rows[rebound_event_index].get("PCTIMESTRING")
            if (
                issue_team is not None
                and rebound_team is not None
                and issue_team == rebound_team
            ):
                search_limit = min(rebound_event_index + 6, len(rows))
                for candidate_idx in range(rebound_event_index + 1, search_limit):
                    if rows[candidate_idx].get("PERIOD") != rebound_period:
                        break
                    if rows[candidate_idx].get("PCTIMESTRING") != rebound_clock:
                        continue
                    if not is_missed_shot_or_ft(candidate_idx):
                        continue
                    candidate_team = effective_team_id(candidate_idx)
                    if candidate_team is None or candidate_team == rebound_team:
                        continue

                    issue_rebound = rows.pop(rebound_event_index)
                    if rebound_event_index < candidate_idx:
                        candidate_idx -= 1
                    rows.insert(candidate_idx + 1, issue_rebound)
                    self.data = rows
                    return

        # --- PATTERN -1: Move an orphan rebound back to the nearest prior miss ---
        if (
            rebound_event_index is not None
            and et(issue_event_index) != 4
        ):
            search_start = rebound_event_index - 1
            search_stop = max(-1, rebound_event_index - 8)
            for candidate_idx in range(search_start, search_stop, -1):
                if rebound_period is not None and rows[candidate_idx].get("PERIOD") != rebound_period:
                    break
                if is_missed_shot_or_ft(candidate_idx):
                    rebound_row = rows.pop(rebound_event_index)
                    rows.insert(candidate_idx + 1, rebound_row)
                    self.data = rows
                    return

        # --- PATTERN -0.75: Start of period, rebound, made shot, then delayed miss ---
        # Some historical feeds open a period with:
        #   START -> REBOUND -> MADE -> MISS
        # where the rebound/make really belong to the later missed shot.
        if (
            rebound_event_index is not None
            and rebound_event_index == issue_event_index + 1
            and et(issue_event_index) == 12
        ):
            rebound_event_number = en(rebound_event_index)
            search_limit = min(rebound_event_index + 12, len(rows))
            if rebound_event_number is not None:
                for candidate_idx in range(rebound_event_index + 1, search_limit):
                    if rows[candidate_idx].get("PERIOD") != rebound_period:
                        break
                    if en(candidate_idx) != rebound_event_number - 1:
                        continue
                    if is_missed_shot_or_ft(candidate_idx):
                        rebound_row = rows.pop(rebound_event_index)
                        if rebound_event_index < candidate_idx:
                            candidate_idx -= 1
                        rows.insert(candidate_idx + 1, rebound_row)
                        self.data = rows
                        return
            for candidate_idx in range(rebound_event_index + 1, min(rebound_event_index + 4, len(rows))):
                if rows[candidate_idx].get("PERIOD") != rebound_period:
                    break
                if is_missed_shot_or_ft(candidate_idx) and team_id(candidate_idx) == team_id(rebound_event_index):
                    missed_shot = rows.pop(candidate_idx)
                    rows.insert(issue_event_index + 1, missed_shot)
                    self.data = rows
                    return

        # --- PATTERN -0.5: Two misses, rebound, rebound ---
        # Historical 90s feeds sometimes tick the clock between a missed putback
        # and the rebound that actually belongs to the first miss, so do not require
        # the miss/rebound pair to share the exact same clock.
        if (
            rebound_event_index is not None
            and rebound_event_index == issue_event_index + 1
            and issue_event_index - 2 >= 0
            and et(issue_event_index) == 4
            and et(rebound_event_index) == 4
            and et(issue_event_index - 1) == 2
            and et(issue_event_index - 2) == 2
            and team_id(issue_event_index - 2) == team_id(issue_event_index - 1)
            and team_id(issue_event_index - 1) == team_id(issue_event_index)
        ):
            first_rebound = rows[issue_event_index]
            missed_putback = rows[issue_event_index - 1]
            rows[issue_event_index - 1], rows[issue_event_index] = first_rebound, missed_putback
            self.data = rows
            return

        # --- PATTERN -0.46: Earlier rebound stranded behind a shooting-foul FT block ---
        # Some historical feeds log:
        #   MISS_A -> foul/free throws -> REBOUND_A -> REBOUND_B
        # where REBOUND_A actually belongs to MISS_A before the foul/FT block,
        # leaving REBOUND_B as the real rebound on the missed last free throw.
        if (
            rebound_event_index is not None
            and et(issue_event_index) == 4
            and et(rebound_event_index) == 4
        ):
            rebound_event_number = en(rebound_event_index)
            if rebound_event_number is not None:
                search_start = max(0, rebound_event_index - 12)
                for candidate_idx in range(search_start, issue_event_index):
                    if rows[candidate_idx].get("PERIOD") != rebound_period:
                        continue
                    if en(candidate_idx) != rebound_event_number - 1:
                        continue
                    if not is_missed_shot_or_ft(candidate_idx):
                        continue
                    block_range = range(candidate_idx + 1, issue_event_index)
                    if block_range and all(et(block_idx) in (3, 6) for block_idx in block_range):
                        rebound_row = rows.pop(rebound_event_index)
                        rows.insert(candidate_idx + 1, rebound_row)
                        self.data = rows
                        return
            first_rebound_team = effective_team_id(issue_event_index)
            second_rebound_team = effective_team_id(rebound_event_index)
            prev_idx = issue_event_index - 1
            if (
                first_rebound_team is not None
                and second_rebound_team is not None
                and first_rebound_team != second_rebound_team
                and prev_idx >= 0
                and rows[prev_idx].get("PERIOD") == rebound_period
                and et(prev_idx) == 3
                and is_missed_shot_or_ft(prev_idx)
            ):
                scan_idx = prev_idx - 1
                saw_foul = False
                while (
                    scan_idx >= 0
                    and rows[scan_idx].get("PERIOD") == rebound_period
                    and et(scan_idx) in (3, 6)
                ):
                    saw_foul = saw_foul or et(scan_idx) == 6
                    scan_idx -= 1

                if (
                    saw_foul
                    and scan_idx - 1 >= 0
                    and rows[scan_idx].get("PERIOD") == rebound_period
                    and et(scan_idx) == 1
                    and effective_team_id(scan_idx) == first_rebound_team
                    and rows[scan_idx - 1].get("PERIOD") == rebound_period
                    and is_missed_shot_or_ft(scan_idx - 1)
                    and team_id(scan_idx - 1) == first_rebound_team
                    and en(scan_idx - 1) is not None
                    and en(issue_event_index) == en(scan_idx - 1) + 1
                ):
                    first_rebound = rows.pop(issue_event_index)
                    rows.insert(scan_idx, first_rebound)
                    self.data = rows
                    return

                if (
                    saw_foul
                    and scan_idx >= 0
                    and rows[scan_idx].get("PERIOD") == rebound_period
                    and is_missed_shot_or_ft(scan_idx)
                    and team_id(scan_idx) == first_rebound_team
                ):
                    first_rebound = rows.pop(issue_event_index)
                    rows.insert(scan_idx + 1, first_rebound)
                    self.data = rows
                    return

        # --- PATTERN -0.455: Stacked same-clock misses/rebounds before opponent rebound ---
        # Some old feeds log a long offensive-rebound chain like:
        #   MISS_A0 -> MISS_A1 -> REBOUND_A0 -> MISS_A2 -> REBOUND_A1 -> REBOUND_B
        # where each same-team rebound actually belongs to the immediately earlier miss.
        # Re-pair the same-team rebounds before the opponent rebound gets treated as orphaned.
        if (
            rebound_event_index is not None
            and issue_event_index - 4 >= 0
            and et(issue_event_index) == 4
            and et(rebound_event_index) == 4
            and et(issue_event_index - 1) == 2
            and et(issue_event_index - 2) == 4
            and et(issue_event_index - 3) == 2
            and et(issue_event_index - 4) == 2
        ):
            current_team = effective_team_id(issue_event_index)
            opponent_team = effective_team_id(rebound_event_index)
            if (
                current_team is not None
                and opponent_team is not None
                and current_team != opponent_team
                and effective_team_id(issue_event_index - 2) == current_team
                and team_id(issue_event_index - 1) == current_team
                and team_id(issue_event_index - 3) == current_team
                and team_id(issue_event_index - 4) == current_team
            ):
                earlier_miss_clock = rows[issue_event_index - 3].get("PCTIMESTRING")
                earlier_rebound_clock = rows[issue_event_index - 2].get("PCTIMESTRING")
                current_miss_clock = rows[issue_event_index - 1].get("PCTIMESTRING")
                current_rebound_clock = rows[issue_event_index].get("PCTIMESTRING")
                if (
                    earlier_miss_clock == earlier_rebound_clock
                    and current_miss_clock == current_rebound_clock
                    and earlier_miss_clock == current_miss_clock
                ):
                    slice_start = issue_event_index - 4
                    slice_end = rebound_event_index + 1
                    prefix = rows[:slice_start]
                    suffix = rows[slice_end:]
                    reordered = [
                        rows[issue_event_index - 4],
                        rows[issue_event_index - 2],
                        rows[issue_event_index - 3],
                        rows[issue_event_index],
                        rows[issue_event_index - 1],
                        rows[rebound_event_index],
                    ]
                    self.data = prefix + reordered + suffix
                    return

        # --- PATTERN -0.45: Rebound, rebound, then delayed same-clock miss ---
        # Historical feeds sometimes log:
        #   MISS -> REBOUND_A -> REBOUND_B -> delayed MISS
        # where REBOUND_B actually belongs after the delayed same-clock miss.
        if (
            rebound_event_index is not None
            and et(issue_event_index) == 4
            and et(rebound_event_index) == 4
        ):
            rebound_clock = rows[rebound_event_index].get("PCTIMESTRING")
            first_rebound_team = effective_team_id(issue_event_index)
            search_limit = min(rebound_event_index + 3, len(rows))
            for candidate_idx in range(rebound_event_index + 1, search_limit):
                candidate_row = rows[candidate_idx]
                if rebound_period is not None and candidate_row.get("PERIOD") != rebound_period:
                    break
                if candidate_row.get("PCTIMESTRING") != rebound_clock:
                    break
                if not is_missed_shot_or_ft(candidate_idx):
                    continue
                if first_rebound_team is not None and team_id(candidate_idx) != first_rebound_team:
                    continue

                rebound_row = rows.pop(rebound_event_index)
                if rebound_event_index < candidate_idx:
                    candidate_idx -= 1
                rows.insert(candidate_idx + 1, rebound_row)
                self.data = rows
                return

        # --- PATTERN -0.4: Shadowing TEAM rebound before a future miss chain ---
        # Some historical feeds place a later TEAM rebound ahead of the missed shot
        # or free throw it actually belongs to. That rebound can "shadow" an earlier
        # valid rebound and cause the valid rebound to be deleted by fallback.
        if (
            rebound_event_index is not None
            and et(issue_event_index) == 4
            and et(rebound_event_index) == 4
            and is_team_rebound(issue_event_index)
        ):
            shadow_clock = rows[issue_event_index].get("PCTIMESTRING")
            rebound_clock = rows[rebound_event_index].get("PCTIMESTRING")
            if rebound_clock == shadow_clock:
                shadow_clock = None
            search_limit = min(issue_event_index + 8, len(rows))
            if shadow_clock is not None:
                for candidate_idx in range(issue_event_index + 1, search_limit):
                    candidate_row = rows[candidate_idx]
                    if rebound_period is not None and candidate_row.get("PERIOD") != rebound_period:
                        break
                    if candidate_idx == rebound_event_index:
                        continue
                    if candidate_row.get("PCTIMESTRING") != shadow_clock:
                        continue
                    if not is_missed_shot_or_ft(candidate_idx):
                        continue

                    insert_after_idx = candidate_idx
                    if (
                        candidate_idx + 1 < len(rows)
                        and et(candidate_idx + 1) == 4
                        and rows[candidate_idx + 1].get("PCTIMESTRING") == shadow_clock
                    ):
                        insert_after_idx = candidate_idx + 1

                    shadow_rebound = rows.pop(issue_event_index)
                    if issue_event_index < insert_after_idx:
                        insert_after_idx -= 1
                    rows.insert(insert_after_idx + 1, shadow_rebound)
                    self.data = rows
                    return

        # --- PATTERN 0: Miss, made putback/layup, rebound ---
        if (
            rebound_event_index is not None
            and rebound_event_index == issue_event_index + 1
            and issue_event_index - 1 >= 0
            and et(issue_event_index - 1) == 2
            and et(issue_event_index) == 1
            and et(rebound_event_index) == 4
            and rows[issue_event_index].get("PCTIMESTRING") == rows[rebound_event_index].get("PCTIMESTRING")
            and team_id(issue_event_index - 1) == team_id(issue_event_index)
            and team_id(issue_event_index) == team_id(rebound_event_index)
        ):
            rebound_event = rows[rebound_event_index]
            made_shot = rows[issue_event_index]
            rows[issue_event_index], rows[rebound_event_index] = rebound_event, made_shot
            self.data = rows
            return

        # --- PATTERN 3: Rebound immediately after, swap them ---
        if (
            issue_event_index + 1 < len(rows)
            and et(issue_event_index + 1) == 4
            and en(issue_event_index + 1) == context_event_num - 1
        ):
            rebound_event = rows[issue_event_index + 1]
            shot_event = rows[issue_event_index]
            rows[issue_event_index], rows[issue_event_index + 1] = (
                rebound_event,
                shot_event,
            )
            self.data = rows
            return

        # --- PATTERN 4: Shot, rebound, rebound (first rebound out of place) ---
        if (
            issue_event_index - 1 >= 0
            and issue_event_index + 1 < len(rows)
            and et(issue_event_index + 1) == 4
            and et(issue_event_index - 1) == 2
            and en(issue_event_index + 1) == context_event_num + 2
            and en(issue_event_index - 1) == context_event_num + 1
        ):
            rebound_event = rows[issue_event_index]
            shot_event = rows[issue_event_index - 1]
            rows[issue_event_index - 1], rows[issue_event_index] = (
                rebound_event,
                shot_event,
            )
            self.data = rows
            return

        # --- PATTERN 5: Shot, rebound, rebound (second rebound out of place) ---
        if (
            issue_event_index - 1 >= 0
            and issue_event_index + 1 < len(rows)
            and et(issue_event_index + 1) == 4
            and et(issue_event_index - 1) == 2
            and en(issue_event_index + 1) == context_event_num - 2
            and en(issue_event_index - 1) == context_event_num - 1
        ):
            first_rebound = rows[issue_event_index + 1]
            second_rebound = rows[issue_event_index]
            shot_event = rows[issue_event_index - 1]
            rows[issue_event_index - 1 : issue_event_index + 2] = [
                first_rebound,
                shot_event,
                second_rebound,
            ]
            self.data = rows
            return

        # --- FALLBACK: delete an orphan rebound (TEAM first, then PLAYER) ---
        prev_period = rows[issue_event_index].get("PERIOD")
        team_candidate_idx: Optional[int] = None
        player_candidate_idx: Optional[int] = None

        for i in range(issue_event_index + 1, min(issue_event_index + 10, len(rows))):
            row = rows[i]
            if row.get("PERIOD") != prev_period:
                break
            if et(i) != 4:  # not a rebound
                continue

            pid = row.get("PLAYER1_ID") or 0
            # TEAM rebound: either explicit team id (16106127xx) or 0
            is_team = pid >= 1610000000 or pid == 0

            if is_team and team_candidate_idx is None:
                team_candidate_idx = i
                break

            if (not is_team) and player_candidate_idx is None:
                player_candidate_idx = i

        def _log_deletion(deleted_row: dict, prev_evnum: int) -> None:
            if self._rebound_deletions_list is None:
                return
            entry = {
                "game_id": str(self.game_id).zfill(10),
                "deleted_EVENTNUM": deleted_row.get("EVENTNUM"),
                "deleted_EVENTMSGTYPE": deleted_row.get("EVENTMSGTYPE"),
                "deleted_PERIOD": deleted_row.get("PERIOD"),
                "deleted_PCTIMESTRING": deleted_row.get("PCTIMESTRING"),
                "deleted_PLAYER1_ID": deleted_row.get("PLAYER1_ID"),
                "deleted_PLAYER1_TEAM_ID": deleted_row.get("PLAYER1_TEAM_ID"),
                "prev_EVENTNUM": prev_evnum,
            }
            self._rebound_deletions_list.append(entry)

        # Prefer deleting a TEAM rebound if we found one
        if team_candidate_idx is not None:
            rebound_index = team_candidate_idx
            deleted_row = rows[rebound_index]
            print(
                f"[REBOUND FIX] Game {self.game_id}: deleting TEAM orphan rebound "
                f"EVENTNUM {en(rebound_index)} after EVENTNUM {context_event_num}"
            )
            _log_deletion(deleted_row, context_event_num)
            del rows[rebound_index]
            self.data = rows
            return

        # Only PLAYER rebounds were seen
        if player_candidate_idx is not None:
            rebound_index = player_candidate_idx
            deleted_row = rows[rebound_index]
            if REBOUND_STRICT_MODE:
                print(
                    f"[REBOUND FIX] Game {self.game_id}: refusing to auto-delete PLAYER rebound "
                    f"EVENTNUM {en(rebound_index)} after EVENTNUM {context_event_num}; re-raising."
                )
                _log_deletion(deleted_row, context_event_num)
                raise exception
            else:
                print(
                    f"[REBOUND FIX] Game {self.game_id}: deleting PLAYER orphan rebound "
                    f"EVENTNUM {en(rebound_index)} after EVENTNUM {context_event_num}"
                )
                _log_deletion(deleted_row, context_event_num)
                del rows[rebound_index]
                self.data = rows
                return

        # No rebound found nearby - give up and let the game fail
        raise exception


def get_possessions_from_df(
    game_df: pd.DataFrame,
    fetch_pbp_v3_fn: Optional[FetchPbpV3Fn] = None,
    rebound_deletions_list: Optional[List[Dict]] = None,
    boxscore_source_loader=None,
    file_directory: Optional[str] = None,
) -> Possessions:
    """
    Build a pbpstats Possessions object from a single-game PBP DataFrame.

    - Optionally uses a fetch_pbp_v3_fn(game_id) -> DataFrame to:
        * filter EVENTNUMs to those present in v3,
        * add missing StartOfPeriod events,
        * preserve the deduped raw row order without forcing a v3 numeric reorder.

    - Optionally propagates a local stats.nba boxscore loader to StartOfPeriod events.
    - Always normalizes NaNs/ints and repairs rebound event ordering via PbpProcessor.
    """
    if game_df.empty:
        raise ValueError("get_possessions_from_df: empty game_df")

    game_id = str(game_df["GAME_ID"].iloc[0]).zfill(10)

    df = game_df.copy()

    # v3-based dedupe if available
    df = dedupe_with_v3(df, game_id, fetch_pbp_v3_fn)

    # Always drop remaining duplicates
    df = df.drop_duplicates(subset=["GAME_ID", "EVENTNUM"], keep="first")

    # Patch missing start-of-period markers
    df = patch_start_of_periods(df, game_id, fetch_pbp_v3_fn)

    # Preserve the post-dedupe row order; numeric re-sorts can break old-game chronology.
    if fetch_pbp_v3_fn is not None:
        try:
            df_ordered = reorder_with_v3(df, game_id, fetch_pbp_v3_fn)
        except Exception:
            df_ordered = _ensure_eventnum_int(df).reset_index(drop=True)
    else:
        df_ordered = _ensure_eventnum_int(df).reset_index(drop=True)

    # As a last fallback, preserve original order if needed
    if df_ordered.empty:
        df_ordered = df.reset_index(drop=True)

    raw_dicts = create_raw_dicts_from_df(df_ordered)
    processor = PbpProcessor(
        game_id,
        raw_dicts,
        rebound_deletions_list,
        boxscore_source_loader=boxscore_source_loader,
        file_directory=file_directory,
    )
    return Possessions(processor.possessions)
