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
    short_reset: float = 14.0
    bump_to_short_on_retained_stop: bool = True
    hard_reset_to_short_on_rim_hit_stop: bool = True
    treat_kicked_ball_as_retained_stop: bool = True


def _infer_rim_hit_from_missed_shot(missed) -> Optional[bool]:
    """Best-effort rim contact inference from the missed shot object."""
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

    # Fallback 2: bounded backward scan within the same period/time
    prev = getattr(reb, "previous_event", None)
    for _ in range(6):
        if prev is None:
            break
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


def _get_short_reset_value(league: Optional[str], season_year: Optional[int]) -> float:
    """
    Returns the shot clock value to use for short resets (offensive rebounds,
    retained-ball defensive fouls/violations).

    - NBA: 14s starting with 2018-19 (season_year >= 2018).
    - WNBA / G-League: approximate as 14s for the same set of retained-ball
      situations.
    - Older NBA seasons and other leagues: no short reset; use 24s everywhere.
    """
    full = 24.0

    # NBA 14-second reset from 2018-19 onward.
    if league == pbpstats.NBA_STRING and season_year is not None and season_year >= 2018:
        return 14.0

    # Leagues that always use a short reset (approximation).
    short_reset_leagues = {pbpstats.WNBA_STRING, pbpstats.G_LEAGUE_STRING}
    # Live G League sometimes uses a different constant.
    if hasattr(pbpstats, "D_LEAGUE_STRING"):
        short_reset_leagues.add(pbpstats.D_LEAGUE_STRING)

    if league in short_reset_leagues:
        return 14.0

    return full


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


def _retained_stop_new_state(state: float, cfg: ShotClockConfig, *, rim_hit_context: bool) -> float:
    if cfg.short_reset >= cfg.full_reset:
        return cfg.full_reset

    if cfg.hard_reset_to_short_on_rim_hit_stop and rim_hit_context:
        return cfg.short_reset

    if cfg.bump_to_short_on_retained_stop:
        return max(cfg.short_reset, state)

    return cfg.full_reset


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
    """
    if not events:
        return

    short_reset = _get_short_reset_value(league, season_year)
    cfg = ShotClockConfig(short_reset=short_reset)

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
            # New period or explicit StartOfPeriod → fresh 24
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

        # 3. Apply resets/updates caused BY this event → state for next event.
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
    if isinstance(event, Rebound) and _safe_is_real_rebound(event):
        if getattr(event, "oreb", False):
            rim_hit = _infer_rim_hit_from_rebound(event)
            if rim_hit is False:
                return shot_clock_state
            return cfg.short_reset if cfg.short_reset < cfg.full_reset else cfg.full_reset
        return cfg.full_reset

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
            if is_defense_event and not possession_changed:
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
        # Held-ball jump balls do not reset the shot clock when offense retains.
        return shot_clock_state

    # 6) Defensive non-shooting fouls where offense keeps the ball
    if isinstance(event, Foul):
        if is_defense_event and not possession_changed:
            if (
                getattr(event, "is_technical", False)
                or getattr(event, "is_double_technical", False)
                or getattr(event, "is_double_foul", False)
            ):
                return shot_clock_state
            if getattr(event, "is_shooting_foul", False) or getattr(
                event, "is_shooting_block_foul", False
            ):
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
            return _retained_stop_new_state(
                shot_clock_state, cfg, rim_hit_context=False
            )

    # 8) Fallback: possession changed
    if possession_changed:
        return cfg.full_reset

    return shot_clock_state
