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
    ):
        self.game_id = str(game_id).zfill(10)
        self.league = "nba"
        self.file_directory = None
        self.data = raw_data_dicts
        self.factory = StatsNbaEnhancedPbpFactory()
        # Per-processor log of any fallback rebound deletions
        self._rebound_deletions_list = rebound_deletions_list

        self._process_with_retries(max_retries=20)

    def _build_items_from_data(self) -> None:
        self.items = [
            self.factory.get_event_class(item["EVENTMSGTYPE"])(item, i)
            for i, item in enumerate(self.data)
        ]

    def _process_with_retries(self, max_retries: int) -> None:
        attempts = 0
        while attempts <= max_retries:
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
        msg = str(exception)
        try:
            event_num = int(msg.split("EventNum: ")[-1].split(">")[0])
        except Exception:
            raise exception

        rows = self.data
        issue_event_index: Optional[int] = None
        for i, row in enumerate(rows):
            if row.get("EVENTNUM") == event_num:
                issue_event_index = i
                break
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

        # --- PATTERN 1: Previous event is sub/timeout (type 8 or 9) ---
        if et(issue_event_index) in (8, 9):
            row_index = issue_event_index
            while row_index >= 0 and et(row_index) in (8, 9):
                row_index -= 1

            if row_index >= 0:
                ft_event_num = en(row_index)
                new_rows: List[dict] = []
                row_to_move: Optional[dict] = None
                for row in rows:
                    if row.get("EVENTNUM") == ft_event_num:
                        row_to_move = row
                    elif row.get("EVENTNUM") == event_num:
                        new_rows.append(row)
                        if row_to_move is not None:
                            new_rows.append(row_to_move)
                    else:
                        new_rows.append(row)
                if row_to_move is not None:
                    self.data = new_rows
                    return

        # --- PATTERN 2: Instant replay (type 18) before rebound ---
        if (
            et(issue_event_index) == 18
            and issue_event_index + 1 < len(rows)
            and et(issue_event_index + 1) == 4
        ):
            rows[issue_event_index], rows[issue_event_index + 1] = (
                rows[issue_event_index + 1],
                rows[issue_event_index],
            )
            self.data = rows
            return

        # --- PATTERN 3: Rebound immediately after, swap them ---
        if (
            issue_event_index + 1 < len(rows)
            and et(issue_event_index + 1) == 4
            and en(issue_event_index + 1) == event_num - 1
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
            and en(issue_event_index + 1) == event_num + 2
            and en(issue_event_index - 1) == event_num + 1
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
            and en(issue_event_index + 1) == event_num - 2
            and en(issue_event_index - 1) == event_num - 1
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
                f"EVENTNUM {en(rebound_index)} after EVENTNUM {event_num}"
            )
            _log_deletion(deleted_row, event_num)
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
                    f"EVENTNUM {en(rebound_index)} after EVENTNUM {event_num}; re-raising."
                )
                _log_deletion(deleted_row, event_num)
                raise exception
            else:
                print(
                    f"[REBOUND FIX] Game {self.game_id}: deleting PLAYER orphan rebound "
                    f"EVENTNUM {en(rebound_index)} after EVENTNUM {event_num}"
                )
                _log_deletion(deleted_row, event_num)
                del rows[rebound_index]
                self.data = rows
                return

        # No rebound found nearby - give up and let the game fail
        raise exception


def get_possessions_from_df(
    game_df: pd.DataFrame,
    fetch_pbp_v3_fn: Optional[FetchPbpV3Fn] = None,
    rebound_deletions_list: Optional[List[Dict]] = None,
) -> Possessions:
    """
    Build a pbpstats Possessions object from a single-game PBP DataFrame.

    - Optionally uses a fetch_pbp_v3_fn(game_id) -> DataFrame to:
        * filter EVENTNUMs to those present in v3,
        * add missing StartOfPeriod events,
        * reorder rows by v3 actionId ordering.

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

    # Reorder using v3 if possible; otherwise fall back to EVENTNUM / index
    if fetch_pbp_v3_fn is not None:
        try:
            df_ordered = reorder_with_v3(df, game_id, fetch_pbp_v3_fn)
        except Exception:
            df_ordered = _ensure_eventnum_int(df).sort_values(["PERIOD", "EVENTNUM"])
    else:
        df_ordered = _ensure_eventnum_int(df).sort_values(["PERIOD", "EVENTNUM"])

    # As a last fallback, preserve original order if needed
    if df_ordered.empty:
        df_ordered = df.sort_index()

    raw_dicts = create_raw_dicts_from_df(df_ordered)
    processor = PbpProcessor(game_id, raw_dicts, rebound_deletions_list)
    return Possessions(processor.possessions)
