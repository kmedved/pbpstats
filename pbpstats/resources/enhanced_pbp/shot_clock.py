from collections import defaultdict
from dataclasses import dataclass
from typing import List, Optional

import pbpstats
from pbpstats.resources.enhanced_pbp import (
    FieldGoal,
    FreeThrow,
    Rebound,
    Foul,
    Violation,
    Turnover,
    StartOfPeriod,
    JumpBall,
)


@dataclass(frozen=True)
class ShotClockConfig:
    full_reset: float = 24.0
    rim_retention_reset: float = 14.0
    retained_stop_minimum: float = 14.0
    bump_to_short_on_retained_stop: bool = True
    hard_reset_to_short_on_rim_hit_stop: bool = True
    treat_kicked_ball_as_retained_stop: bool = True


def _infer_rim_hit_from_missed_shot(missed) -> Optional[bool]:
    """
    Best-effort rim contact inference from the missed shot object.

    Provider data does not expose rim contact directly. Blocked shots are treated
    as no-rim misses unless the provider gives a stronger signal.
    """
    if missed is None:
        return None

    if isinstance(missed, FieldGoal) and getattr(missed, "is_blocked", False):
        return False

    desc = (getattr(missed, "description", "") or "").lower()
    if "airball" in desc:
        return False

    return True


def _infer_rim_hit_from_rebound(reb) -> Optional[bool]:
    try:
        missed = reb.missed_shot
    except Exception:
        missed = None
    rim = _infer_rim_hit_from_missed_shot(missed)
    if rim is not None:
        return rim

    # Fallback 1: scan same-timestamp group *before the rebound* for a missed shot
    same_time = _events_at_same_time(reb)
    same_time = sorted(same_time, key=lambda e: getattr(e, "order", 0))
    try:
        reb_idx = same_time.index(reb)
    except ValueError:
        reb_idx = len(same_time)
    for ev in reversed(same_time[:reb_idx]):
        try:
            is_made = getattr(ev, "is_made", None)
        except Exception:
            is_made = None
        if isinstance(ev, (FieldGoal, FreeThrow)) and is_made is False:
            rim2 = _infer_rim_hit_from_missed_shot(ev)
            if rim2 is not None:
                return rim2

    # Fallback 2: backward scan within the same period/time
    prev = getattr(reb, "previous_event", None)
    while prev is not None:
        if getattr(prev, "period", None) != getattr(reb, "period", None):
            break
        if getattr(prev, "seconds_remaining", None) != getattr(
            reb, "seconds_remaining", None
        ):
            break
        try:
            is_made = getattr(prev, "is_made", None)
        except Exception:
            is_made = None
        if isinstance(prev, (FieldGoal, FreeThrow)) and is_made is False:
            rim2 = _infer_rim_hit_from_missed_shot(prev)
            if rim2 is not None:
                return rim2
        prev = getattr(prev, "previous_event", None)

    return None


def _safe_is_real_rebound(ev) -> bool:
    try:
        return bool(getattr(ev, "is_real_rebound", False))
    except Exception:
        return False


def _safe_offense_team_id(event, *, backfill_previous: bool = True) -> Optional[int]:
    """Best-effort offense team id lookup with optional backfill to previous."""
    if event is None:
        return None
    try:
        if hasattr(event, "get_offense_team_id"):
            tid = event.get_offense_team_id()
        else:
            tid = getattr(event, "offense_team_id", None)
    except Exception:
        tid = getattr(event, "offense_team_id", None)

    if tid in (0, None):
        if backfill_previous:
            prev = getattr(event, "previous_event", None)
            if prev is not None and getattr(prev, "period", None) == getattr(
                event, "period", None
            ):
                return _safe_offense_team_id(prev, backfill_previous=True)
        return None
    return tid


def _safe_bool_attr(event, attr: str) -> bool:
    try:
        return bool(getattr(event, attr, False))
    except Exception:
        return False


def _possession_change_override(event) -> Optional[bool]:
    if getattr(event, "possession_changing_override", False):
        return True
    if getattr(event, "non_possession_changing_override", False):
        return False
    return None


def _infer_possession_changed(event) -> bool:
    override = _possession_change_override(event)
    if override is not None:
        return override

    next_event = getattr(event, "next_event", None)
    offense_now = _safe_offense_team_id(event, backfill_previous=True)
    offense_next = _safe_offense_team_id(next_event, backfill_previous=False)
    if offense_now is not None and offense_next is not None:
        return offense_now != offense_next

    try:
        if hasattr(event, "is_possession_ending_event"):
            return bool(event.is_possession_ending_event)
    except Exception:
        pass

    return False


def _normalize_game_id_for_inference(game_id) -> str:
    game_id = str(game_id or "").strip()
    if game_id.isdigit() and len(game_id) < 10:
        return game_id.zfill(10)
    return game_id


def _infer_league_from_game_id(game_id) -> Optional[str]:
    game_id = _normalize_game_id_for_inference(game_id)
    if game_id.startswith(pbpstats.NBA_GAME_ID_PREFIX):
        return pbpstats.NBA_STRING
    if game_id.startswith(pbpstats.WNBA_GAME_ID_PREFIX):
        return pbpstats.WNBA_STRING
    if game_id.startswith(pbpstats.G_LEAGUE_GAME_ID_PREFIX):
        return pbpstats.G_LEAGUE_STRING
    return None


def _infer_season_year_from_game_id(game_id) -> Optional[int]:
    game_id = _normalize_game_id_for_inference(game_id)
    if len(game_id) < 5:
        return None
    try:
        suffix = int(game_id[3:5])
    except ValueError:
        return None
    return 2000 + suffix if suffix < 90 else 1900 + suffix


def _infer_league_from_events(events: List[object]) -> Optional[str]:
    for event in events:
        league = _infer_league_from_game_id(getattr(event, "game_id", None))
        if league is not None:
            return league
    return None


def _infer_season_year_from_events(events: List[object]) -> Optional[int]:
    for event in events:
        season_year = _infer_season_year_from_game_id(
            getattr(event, "game_id", None)
        )
        if season_year is not None:
            return season_year
    return None


def _short_reset_league_thresholds():
    thresholds = {
        pbpstats.NBA_STRING: 2018,
        pbpstats.WNBA_STRING: 2016,
        pbpstats.G_LEAGUE_STRING: 2016,
    }
    if hasattr(pbpstats, "D_LEAGUE_STRING"):
        thresholds[pbpstats.D_LEAGUE_STRING] = 2016
    return thresholds


def _league_supports_retained_stop_minimum(league: Optional[str]) -> bool:
    return league in _short_reset_league_thresholds()


def _get_rim_retention_reset_value(
    league: Optional[str], season_year: Optional[int]
) -> float:
    """
    Returns the shot clock value to use for rim-contact retention resets
    (offensive rebounds, retained loose balls, and retained dead-ball rebounds).

    - NBA: 14s starting with 2018-19 (season_year >= 2018).
    - WNBA: 14s starting with 2016.
    - G-League / D-League: 14s starting with 2016-17.
    - Older seasons and other leagues: no rim-retention short reset; use 24s.
    - Unknown WNBA/G-League seasons use current rules because those loaders do
      not always carry a reliable season field.
    """
    full = 24.0
    thresholds = _short_reset_league_thresholds()
    threshold = thresholds.get(league)
    if threshold is None:
        return full

    if season_year is None:
        if league in {pbpstats.WNBA_STRING, pbpstats.G_LEAGUE_STRING} or (
            hasattr(pbpstats, "D_LEAGUE_STRING")
            and league == pbpstats.D_LEAGUE_STRING
        ):
            return 14.0
        return full

    if season_year >= threshold:
        return 14.0

    return full


def _build_shot_clock_config(
    league: Optional[str], season_year: Optional[int]
) -> ShotClockConfig:
    retained_stop_minimum = 24.0
    if _league_supports_retained_stop_minimum(league):
        retained_stop_minimum = 14.0
    return ShotClockConfig(
        rim_retention_reset=_get_rim_retention_reset_value(league, season_year),
        retained_stop_minimum=retained_stop_minimum,
    )


def _get_short_reset_value(league: Optional[str], season_year: Optional[int]) -> float:
    """
    Backward-compatible private helper for the rim-retention reset value.
    """
    return _get_rim_retention_reset_value(league, season_year)


def _is_retained_missed_shot_deadball(event) -> bool:
    if not isinstance(event, Rebound):
        return False
    if _safe_is_real_rebound(event):
        return False
    if _safe_bool_attr(event, "is_buzzer_beater_placeholder"):
        return False
    if _safe_bool_attr(event, "is_buzzer_beater_rebound_at_shot_time"):
        return False

    try:
        missed = event.missed_shot
    except Exception:
        return False

    if isinstance(missed, FreeThrow) and not getattr(missed, "is_end_ft", False):
        return False

    shot_team_id = getattr(missed, "team_id", None)
    if shot_team_id is None:
        return False

    next_event = getattr(event, "next_event", None)
    next_offense = _safe_offense_team_id(next_event, backfill_previous=False)
    if next_offense == shot_team_id:
        return True

    if getattr(event, "team_id", None) == shot_team_id and not _infer_possession_changed(
        event
    ):
        return True

    return False


def _rim_retention_new_state(cfg: ShotClockConfig) -> float:
    if cfg.rim_retention_reset < cfg.full_reset:
        return cfg.rim_retention_reset
    return cfg.full_reset


def _retained_stop_new_state(
    state: float, cfg: ShotClockConfig, *, rim_hit_context: bool
) -> float:
    if cfg.retained_stop_minimum >= cfg.full_reset:
        return cfg.full_reset

    if cfg.hard_reset_to_short_on_rim_hit_stop and rim_hit_context:
        return _rim_retention_new_state(cfg)

    if cfg.bump_to_short_on_retained_stop:
        return max(cfg.retained_stop_minimum, state)

    return cfg.full_reset


def _retained_technical_or_delay(event) -> bool:
    return (
        getattr(event, "is_technical", False)
        or getattr(event, "is_delay_of_game", False)
        or getattr(event, "is_defensive_3_seconds", False)
    )


def _full_reset_defensive_foul(event) -> bool:
    return _safe_bool_attr(event, "is_flagrant") or _safe_bool_attr(
        event, "is_clear_path_foul"
    )


def _shooting_foul_without_retained_reset(event) -> bool:
    return getattr(event, "is_shooting_foul", False) or getattr(
        event, "is_shooting_block_foul", False
    )


def _events_at_same_time(event) -> List[object]:
    try:
        if hasattr(event, "get_all_events_at_current_time"):
            evs = event.get_all_events_at_current_time()
            return evs or [event]
    except Exception:
        pass
    return [event]


def _rim_hit_context_at_time(event) -> bool:
    events = _events_at_same_time(event)
    events = sorted(events, key=lambda e: getattr(e, "order", 0))
    cur_order = getattr(event, "order", 0)
    prior = [e for e in events if getattr(e, "order", 0) < cur_order]

    for ev in prior:
        if isinstance(ev, Rebound) and _safe_is_real_rebound(ev):
            rim = _infer_rim_hit_from_rebound(ev)
            if rim is True:
                return True

    for ev in prior:
        if isinstance(ev, FieldGoal) and not getattr(ev, "is_made", False):
            rim = _infer_rim_hit_from_missed_shot(ev)
            if rim is True:
                return True

    return False


def annotate_shot_clock(
    events: List[object],
    season_year: Optional[int] = None,
    league: Optional[str] = None,
) -> None:
    """
    Attach an approximate shot_clock attribute to every EnhancedPbpItem in-place.

    For each event, sets:
        event.shot_clock: float   # seconds remaining on shot clock at start of event

    This is data-provider agnostic and only uses the EnhancedPbpItem interface
    and existing event type classes, so it works for stats_nba / data_nba / live.

    It assumes events are already linked via previous_event / next_event so that
    possession changes across events can be inferred.
    Events should also be in chronological order within each period.

    This approximation does not know inbound location. Retained defensive
    fouls/violations are treated as frontcourt retained stops unless the
    provider has already changed possession. Blocked field goal misses are
    treated as no-rim misses because provider feeds do not expose rim contact.
    End-of-period situations clamp the displayed value to the game clock instead
    of exposing an explicit "shot clock off" state.
    """
    if not events:
        return

    if league is None:
        league = _infer_league_from_events(events)
    if season_year is None:
        season_year = _infer_season_year_from_events(events)

    cfg = _build_shot_clock_config(league, season_year)

    # Group by period to keep logic clean; events are already in chronological
    # order within a game (descending clock).
    events_by_period = defaultdict(list)
    for ev in events:
        period = getattr(ev, "period", 0)
        events_by_period[period].append(ev)

    for period, period_events in events_by_period.items():
        if not period_events:
            continue
        _annotate_period_shot_clock(
            period_events,
            cfg=cfg,
            league=league,
            season_year=season_year,
        )


def _annotate_period_shot_clock(
    period_events: List[object],
    *,
    cfg: ShotClockConfig,
    league: Optional[str],
    season_year: Optional[int],
) -> None:
    """
    Run a state machine over all events in a single period and assign
    event.shot_clock at the start of each event.
    """
    if not period_events:
        return

    shot_clock_state: Optional[float] = None
    previous_event = None

    # We assume events are already sorted in chronological order for the period.
    for ev in period_events:
        # 1. Decay from previous event -> shot clock at *start* of this event.
        if previous_event is None or isinstance(ev, StartOfPeriod):
            # New period or explicit StartOfPeriod -> fresh 24
            shot_clock_state = cfg.full_reset
        else:
            # Prefer clock diff over provider-specific deltas.
            prev_sec = getattr(previous_event, "seconds_remaining", None)
            cur_sec = getattr(ev, "seconds_remaining", None)
            if prev_sec is None or cur_sec is None:
                dt = 0.0
            else:
                # Clock counts down, so diff is prev - current within a period.
                dt = max(float(prev_sec) - float(cur_sec), 0.0)

            if shot_clock_state is None:
                shot_clock_state = cfg.full_reset
            shot_clock_state = max(shot_clock_state - dt, 0.0)

        # 2. Compute display shot clock (clamped by game clock).
        raw_sc = max(0.0, min(cfg.full_reset, shot_clock_state))
        seconds_remaining = getattr(ev, "seconds_remaining", None)
        if seconds_remaining is not None:
            display_sc = min(raw_sc, float(seconds_remaining))
        else:
            display_sc = raw_sc

        # Normalize and store; 1 decimal is usually enough.
        ev.shot_clock = round(display_sc, 1)

        # Hard clamp: shot clock violations should read exactly 0.0 on the turnover.
        if isinstance(ev, Turnover) and getattr(ev, "is_shot_clock_violation", False):
            ev.shot_clock = 0.0

        # 3. Apply resets/updates caused BY this event -> state for next event.
        shot_clock_state = _update_shot_clock_after_event(
            ev,
            shot_clock_state,
            cfg=cfg,
            _league=league,
            _season_year=season_year,
        )

        previous_event = ev


def _update_shot_clock_after_event(
    event,
    shot_clock_state: Optional[float],
    *,
    cfg: ShotClockConfig,
    _league: Optional[str],
    _season_year: Optional[int],
) -> float:
    if shot_clock_state is None:
        shot_clock_state = cfg.full_reset

    possession_changed = _infer_possession_changed(event)
    offense_now = _safe_offense_team_id(event)
    team_id = getattr(event, "team_id", None)
    is_defense_event = (
        offense_now is not None and team_id is not None and team_id != offense_now
    )

    # 0) Defensive goaltending ends the possession
    if isinstance(event, Violation) and getattr(event, "is_goaltend_violation", False):
        return cfg.full_reset

    # 1) Rebounds
    if isinstance(event, Rebound):
        if _safe_is_real_rebound(event):
            if getattr(event, "oreb", False):
                rim_hit = _infer_rim_hit_from_rebound(event)
                if rim_hit is False:
                    return shot_clock_state
                return _rim_retention_new_state(cfg)
            return cfg.full_reset

        if _is_retained_missed_shot_deadball(event):
            rim_hit = _infer_rim_hit_from_rebound(event)
            if rim_hit is False:
                return shot_clock_state
            return _rim_retention_new_state(cfg)

    # 2) Made field goals
    if isinstance(event, FieldGoal) and getattr(event, "is_made", False):
        if getattr(event, "is_make_that_does_not_end_possession", False):
            return shot_clock_state
        return cfg.full_reset

    # 3) Turnovers
    if isinstance(event, Turnover) and not getattr(event, "is_no_turnover", False):
        if cfg.treat_kicked_ball_as_retained_stop and getattr(
            event, "is_kicked_ball", False
        ):
            if not possession_changed:
                return _retained_stop_new_state(
                    shot_clock_state, cfg, rim_hit_context=False
                )
        return cfg.full_reset

    # 4) Free throws
    if isinstance(event, FreeThrow) and getattr(event, "is_end_ft", False):
        if possession_changed:
            return cfg.full_reset
        return shot_clock_state

    # 5) Jump balls / held balls
    if isinstance(event, JumpBall):
        if possession_changed:
            return cfg.full_reset
        # Approximation: retained jump balls are treated as defensive held balls.
        return shot_clock_state

    # 6) Defensive non-shooting fouls where offense keeps the ball
    if isinstance(event, Foul):
        if is_defense_event and not possession_changed:
            if _full_reset_defensive_foul(event):
                return cfg.full_reset

            if _retained_technical_or_delay(event):
                return _retained_stop_new_state(
                    shot_clock_state, cfg, rim_hit_context=False
                )

            if _shooting_foul_without_retained_reset(event):
                return shot_clock_state

            rim_hit_ctx = False
            if getattr(event, "is_loose_ball_foul", False):
                rim_hit_ctx = _rim_hit_context_at_time(event)

            return _retained_stop_new_state(
                shot_clock_state, cfg, rim_hit_context=rim_hit_ctx
            )

    # 7) Defensive violations where offense keeps the ball
    if isinstance(event, Violation):
        if is_defense_event and not possession_changed:
            rim_hit_ctx = _rim_hit_context_at_time(event)
            return _retained_stop_new_state(
                shot_clock_state, cfg, rim_hit_context=rim_hit_ctx
            )

    # 8) Fallback: possession changed
    if possession_changed:
        return cfg.full_reset

    return shot_clock_state
