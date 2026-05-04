import json
import os
from collections import defaultdict

from pbpstats import (
    G_LEAGUE_GAME_ID_PREFIX,
    G_LEAGUE_STRING,
    NBA_GAME_ID_PREFIX,
    NBA_STRING,
    WNBA_GAME_ID_PREFIX,
    WNBA_STRING,
)
from pbpstats.game_id import is_overtime_period, normalize_game_id
from pbpstats.overrides import IntDecoder
from pbpstats.resources.enhanced_pbp import FieldGoal, Foul, FreeThrow, StartOfPeriod
from pbpstats.resources.enhanced_pbp.intraperiod_lineup_repair import (
    build_generated_lineup_override_lookup,
)
from pbpstats.resources.enhanced_pbp.shot_clock import annotate_shot_clock


class NbaEnhancedPbpLoader(object):
    """
    Class for shared methods between :obj:`~pbpstats.data_loader.data_nba.enhanced_pbp_loader.DataNbaEnhancedPbpLoader`
    and :obj:`~pbpstats.data_loader.stats_nba.enhanced_pbp_loader.StatsNbaEnhancedPbpLoader`

    Both :obj:`~pbpstats.data_loader.data_nba.enhanced_pbp_loader.DataNbaEnhancedPbpLoader`
    and :obj:`~pbpstats.data_loader.stats_nba.enhanced_pbp_loader.StatsNbaEnhancedPbpLoader` should inherit from this class

    This class should not be instantiated directly
    """

    def _add_extra_attrs_to_all_events(self):
        """
        adds fouls to give, player fouls, score, next event, previous event and
        approximate shot clock to each event
        """
        self.start_period_indices = []
        self._load_possession_changing_event_overrides()
        self.lineup_window_overrides = self._load_lineup_window_overrides()
        lineup_window_override_lookup = self._build_lineup_window_override_lookup()
        game_id = self.game_id if self.league == NBA_STRING else int(self.game_id)
        change_override_event_nums = self.possession_changing_event_overrides.get(
            game_id, []
        )
        non_change_override_event_nums = (
            self.non_possession_changing_event_overrides.get(game_id, [])
        )
        player_game_fouls = defaultdict(int)
        fouls_to_give = defaultdict(lambda: 4)
        score = defaultdict(int)
        season_year = self._infer_season_year()
        league = (
            getattr(self, "league", None)
            or self._infer_league_from_game_id()
            or NBA_STRING
        )
        for i, event in enumerate(self.items):
            event.previous_event_any_period = self.items[i - 1] if i > 0 else None
            event.next_event_any_period = (
                self.items[i + 1] if i < len(self.items) - 1 else None
            )
            if i == 0 and i == len(self.items) - 1:
                event.previous_event = None
                event.next_event = None
            elif isinstance(event, StartOfPeriod) or i == 0:
                event.previous_event = None
                event.next_event = self.items[i + 1] if i < len(self.items) - 1 else None
                self.start_period_indices.append(i)
                if is_overtime_period(event.period, league, season_year):
                    fouls_to_give = defaultdict(lambda: 3)
                else:
                    fouls_to_give = defaultdict(lambda: 4)
            elif i == len(self.items) - 1 or event.period != self.items[i + 1].period:
                event.previous_event = self.items[i - 1]
                event.next_event = None
            else:
                event.previous_event = self.items[i - 1]
                event.next_event = self.items[i + 1]

            if event.seconds_remaining <= 120:
                if len(fouls_to_give.keys()) == 0:
                    # neither team has fouled yet in the period
                    fouls_to_give = defaultdict(lambda: 1)
                elif len(fouls_to_give.keys()) == 1:
                    # only one team has fouled - other team id key is not in defaultdict
                    team_id = list(fouls_to_give.keys())[0]
                    team_fouls_to_give = min(fouls_to_give[team_id], 1)
                    fouls_to_give = defaultdict(lambda: 1)
                    fouls_to_give[team_id] = team_fouls_to_give
                else:
                    for team_id in fouls_to_give.keys():
                        fouls_to_give[team_id] = min(fouls_to_give[team_id], 1)
            if isinstance(event, Foul):
                if event.counts_towards_penalty and fouls_to_give[event.team_id] > 0:
                    fouls_to_give[event.team_id] -= 1
                if event.counts_as_personal_foul:
                    player_game_fouls[event.player1_id] += 1
            if isinstance(event, (FieldGoal, FreeThrow)) and event.is_made:
                score[event.team_id] += event.shot_value

            event.fouls_to_give = fouls_to_give.copy()
            event.player_game_fouls = player_game_fouls.copy()
            event.score = score.copy()
            event.possession_changing_override = (
                event.event_num in change_override_event_nums
            )
            event.non_possession_changing_override = (
                event.event_num in non_change_override_event_nums
            )
            event.lineup_override_by_team = lineup_window_override_lookup.get(i, {})

        # these need next and previous event to be added to all events
        self._set_period_start_items()
        generated_lineup_window_override_lookup = (
            self._build_generated_intraperiod_lineup_override_lookup()
        )
        self._merge_generated_lineup_override_lookup(
            generated_lineup_window_override_lookup
        )

        # annotate approximate shot clock for every enhanced pbp event.
        # LiveEnhancedPbpLoader recomputes shot clock after normalizing DREBs.
        if getattr(self, "data_provider", None) != "live":
            self._annotate_shot_clock()

    def _set_period_start_items(self):
        """
        sets team starting period with the ball and period starters for each team

        On some older / malformed games, start_period_indices can include
        non-StartOfPeriod events (e.g. JumpBall at index 0). In those cases
        we skip the entry instead of crashing.
        """
        from pbpstats.resources.enhanced_pbp import StartOfPeriod  # local import

        for i in getattr(self, "start_period_indices", []):
            event = self.items[i]
            if not isinstance(event, StartOfPeriod):
                # do no harm: only operate on true StartOfPeriod events
                continue
            previous_period_end_event = None
            for j in range(i - 1, -1, -1):
                if getattr(self.items[j], "period", None) == event.period - 1:
                    previous_period_end_event = self.items[j]
                    break

            # Find the first event of this period (may be before StartOfPeriod marker
            # due to period-start substitutions appearing first in live data)
            first_period_event_idx = i
            for j in range(i - 1, -1, -1):
                if getattr(self.items[j], "period", None) == event.period:
                    first_period_event_idx = j
                else:
                    break
            event.first_period_event = self.items[first_period_event_idx]
            if previous_period_end_event is not None:
                prev_players = getattr(previous_period_end_event, "current_players", None)
                if isinstance(prev_players, dict):
                    snap = {}
                    for team_id, players in prev_players.items():
                        if isinstance(players, (list, tuple, set)):
                            snap[team_id] = list(players)
                    event.previous_period_end_lineups = snap or None
                else:
                    event.previous_period_end_lineups = None
                event.previous_period_end_period = getattr(
                    previous_period_end_event, "period", None
                )
            else:
                event.previous_period_end_lineups = None
                event.previous_period_end_period = None

            if getattr(event, "next_event", None) is None:
                # Some malformed games end with a stray StartOfPeriod marker and
                # no events in the new period. Skip starter / possession inference
                # for those terminal sentinels instead of crashing.
                event.team_starting_with_ball = None
                event.period_starters = {}
                continue

            team_id = event.get_team_starting_with_ball()
            event.team_starting_with_ball = team_id
            period_starters = event.get_period_starters(
                file_directory=self.file_directory
            )
            event.period_starters = period_starters

    def _annotate_shot_clock(self):
        """
        Compute and attach approximate shot clock values to each enhanced pbp event.

        This wraps resources.enhanced_pbp.shot_clock.annotate_shot_clock so that the
        logic is shared by stats_nba / data_nba / live data providers.
        """
        season_year = self._infer_season_year()
        league = (
            getattr(self, "league", None)
            or self._infer_league_from_game_id()
            or NBA_STRING
        )

        annotate_shot_clock(self.items, season_year=season_year, league=league)

    def _infer_season_year(self):
        # Season is stored as e.g. "2019" or "2019-20"; we want the start year.
        season_val = getattr(self, "season", None)
        if isinstance(season_val, int):
            return season_val
        elif isinstance(season_val, str):
            try:
                return int(season_val.strip().split("-")[0])
            except (ValueError, TypeError):
                pass

        return self._infer_season_year_from_game_id()

    def _infer_season_year_from_game_id(self):
        raw_game_id = self._normalize_game_id_for_inference()
        try:
            suffix = int(raw_game_id[3:5])
        except (TypeError, ValueError):
            return None

        # Game ids encode the season start year as YY after the three-character
        # game type prefix, e.g. 0022300001 -> 2023.
        return 2000 + suffix if suffix < 90 else 1900 + suffix

    def _infer_league_from_game_id(self):
        raw_game_id = self._normalize_game_id_for_inference()
        if raw_game_id.startswith(NBA_GAME_ID_PREFIX):
            return NBA_STRING
        if raw_game_id.startswith(WNBA_GAME_ID_PREFIX):
            return WNBA_STRING
        if raw_game_id.startswith(G_LEAGUE_GAME_ID_PREFIX):
            return G_LEAGUE_STRING
        return None

    def _normalize_game_id_for_inference(self):
        return normalize_game_id(
            getattr(self, "game_id", ""),
            league=getattr(self, "league", None),
        )

    def _load_possession_changing_event_overrides(self):
        """
        loads overrides for possession or non possession changing events
        """
        if self.file_directory is not None:
            possession_changing_event_overrides_file_path = f"{self.file_directory}/overrides/possession_change_event_overrides.json"
            if os.path.isfile(possession_changing_event_overrides_file_path):
                with open(possession_changing_event_overrides_file_path) as f:
                    # issues with pbp - force these events to be possession changing events
                    # {GameId: [EventNum]}
                    self.possession_changing_event_overrides = json.loads(
                        f.read(), cls=IntDecoder
                    )
            else:
                self.possession_changing_event_overrides = {}

            non_possession_changing_event_overrides_file_path = f"{self.file_directory}/overrides/non_possession_changing_event_overrides.json"
            if os.path.isfile(non_possession_changing_event_overrides_file_path):
                with open(non_possession_changing_event_overrides_file_path) as f:
                    # issues with pbp - force these events to be not possession changing events
                    # {GameId: [EventNum]}
                    self.non_possession_changing_event_overrides = json.loads(
                        f.read(), cls=IntDecoder
                    )
            else:
                self.non_possession_changing_event_overrides = {}
        else:
            self.possession_changing_event_overrides = {}
            self.non_possession_changing_event_overrides = {}

    def _load_lineup_window_overrides(self):
        if self.file_directory is None:
            return {}

        file_path = f"{self.file_directory}/overrides/lineup_window_overrides.json"
        if not os.path.isfile(file_path):
            return {}

        with open(file_path) as f:
            return json.loads(f.read(), cls=IntDecoder)

    def _build_lineup_window_override_lookup(self):
        overrides = getattr(self, "lineup_window_overrides", {})
        if not overrides:
            return {}

        game_id_keys = [self.game_id]
        try:
            game_id_keys.append(int(self.game_id))
        except (TypeError, ValueError):
            pass

        windows = []
        for game_id in game_id_keys:
            game_windows = overrides.get(game_id, [])
            if isinstance(game_windows, list):
                windows.extend(game_windows)
        if not windows:
            return {}

        event_positions = {}
        for idx, event in enumerate(getattr(self, "items", [])):
            try:
                period = int(getattr(event, "period"))
                event_num = int(getattr(event, "event_num"))
            except (AttributeError, TypeError, ValueError):
                continue
            event_positions.setdefault((period, event_num), []).append(idx)

        lookup = {}
        for window in windows:
            try:
                period = int(window["period"])
                team_id = int(window["team_id"])
                start_event_num = int(window["start_event_num"])
                end_event_num = int(window["end_event_num"])
            except (KeyError, TypeError, ValueError):
                continue

            lineup_player_ids = window.get("lineup_player_ids")
            if not isinstance(lineup_player_ids, list) or len(lineup_player_ids) != 5:
                continue
            try:
                normalized_lineup = [int(player_id) for player_id in lineup_player_ids]
            except (TypeError, ValueError):
                continue
            if len(set(normalized_lineup)) != 5:
                continue

            start_indices = event_positions.get((period, start_event_num), [])
            end_indices = event_positions.get((period, end_event_num), [])
            if not start_indices or not end_indices:
                continue

            low_idx, high_idx = sorted((start_indices[0], end_indices[-1]))
            for idx in range(low_idx, high_idx + 1):
                event = self.items[idx]
                try:
                    event_period = int(getattr(event, "period"))
                except (AttributeError, TypeError, ValueError):
                    continue
                if event_period != period:
                    continue
                lookup.setdefault(idx, {})[team_id] = list(normalized_lineup)
        return lookup

    def _get_explicit_lineup_window_override_period_team_keys(self):
        overrides = getattr(self, "lineup_window_overrides", {})
        if not overrides:
            return set()

        game_id_keys = [self.game_id]
        try:
            game_id_keys.append(int(self.game_id))
        except (TypeError, ValueError):
            pass

        blocked_keys = set()
        for game_id in game_id_keys:
            game_windows = overrides.get(game_id, [])
            if not isinstance(game_windows, list):
                continue
            for window in game_windows:
                try:
                    blocked_keys.add((int(window["period"]), int(window["team_id"])))
                except (KeyError, TypeError, ValueError):
                    continue
        return blocked_keys

    def _build_generated_intraperiod_lineup_override_lookup(self):
        lookup, candidates = build_generated_lineup_override_lookup(
            getattr(self, "items", []),
            game_id=getattr(self, "game_id", None),
        )
        blocked_period_team_keys = (
            self._get_explicit_lineup_window_override_period_team_keys()
        )
        if blocked_period_team_keys:
            filtered_lookup = {}
            for event_index, team_overrides in lookup.items():
                try:
                    period = int(getattr(self.items[event_index], "period"))
                except (AttributeError, TypeError, ValueError, IndexError):
                    continue
                kept_team_overrides = {}
                for team_id, team_players in team_overrides.items():
                    key = (period, int(team_id))
                    if key in blocked_period_team_keys:
                        continue
                    kept_team_overrides[int(team_id)] = list(team_players)
                if kept_team_overrides:
                    filtered_lookup[event_index] = kept_team_overrides
            lookup = filtered_lookup

            for candidate in candidates:
                key = (
                    int(candidate.get("period", 0) or 0),
                    int(candidate.get("team_id", 0) or 0),
                )
                if key in blocked_period_team_keys and candidate.get("auto_apply"):
                    candidate["auto_apply"] = False
                    candidate["promotion_decision"] = "blocked_by_explicit_override"
                    candidate["override_event_indices"] = []
        self.generated_intraperiod_lineup_override_lookup = lookup
        self.generated_intraperiod_lineup_repair_candidates = candidates
        return lookup

    def _merge_generated_lineup_override_lookup(self, generated_lookup):
        if not generated_lookup:
            return

        for event_index, team_overrides in generated_lookup.items():
            if event_index < 0 or event_index >= len(getattr(self, "items", [])):
                continue
            event = self.items[event_index]
            existing_overrides = getattr(event, "lineup_override_by_team", {}) or {}
            merged = {
                team_id: list(team_players)
                for team_id, team_players in existing_overrides.items()
            }
            for team_id, team_players in team_overrides.items():
                if team_id in merged:
                    continue
                merged[team_id] = list(team_players)
            event.lineup_override_by_team = merged
