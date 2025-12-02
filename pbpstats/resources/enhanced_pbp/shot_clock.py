from collections import defaultdict
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
)


def _safe_offense_team_id(event) -> Optional[int]:
    """
    Best-effort way to get the offense team id for an event.

    Uses the EnhancedPbpItem interface when available; falls back to
    .offense_team_id if needed.
    """
    if event is None:
        return None
    if hasattr(event, "get_offense_team_id"):
        try:
            tid = event.get_offense_team_id()
        except Exception:
            tid = getattr(event, "offense_team_id", None)
    else:
        tid = getattr(event, "offense_team_id", None)
    if tid == 0:
        return None
    return tid


def _get_short_reset_value(league: Optional[str], season_year: Optional[int]) -> float:
    """
    Returns the shot clock value to use for short resets (offensive rebounds,
    certain defensive fouls/violations).

    - NBA: 14s starting with 2018-19 (season_year >= 2018).
    - WNBA / G-League: approximate as using 14s on offensive rebounds; other
      resets are currently treated as full 24s.
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
    full_reset = 24.0
    use_short_reset = short_reset < full_reset

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
            full_reset=full_reset,
            short_reset=short_reset,
            use_short_reset=use_short_reset,
            league=league,
            season_year=season_year,
        )


def _annotate_period_shot_clock(
    period_events: List[object],
    *,
    full_reset: float,
    short_reset: float,
    use_short_reset: bool,
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
    last_shot_hit_rim: bool = True  # heuristic; updated on shot/FT descriptions

    previous_event = None

    # We assume events are already sorted in chronological order for the period.
    for ev in period_events:
        # 1. Decay from previous event -> shot clock at *start* of this event.
        if previous_event is None or isinstance(ev, StartOfPeriod):
            # New period or explicit StartOfPeriod → fresh 24
            shot_clock_state = full_reset
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
                shot_clock_state = full_reset
            shot_clock_state = max(shot_clock_state - dt, 0.0)

        # 2. Compute display shot clock (clamped by game clock).
        raw_sc = max(0.0, min(full_reset, shot_clock_state))
        seconds_remaining = getattr(ev, "seconds_remaining", None)
        if seconds_remaining is not None:
            display_sc = min(raw_sc, float(seconds_remaining))
        else:
            display_sc = raw_sc

        # Normalize and store; 1 decimal is usually enough.
        ev.shot_clock = round(display_sc, 1)

        # 3. Apply resets/updates caused BY this event → state for next event.
        shot_clock_state = _update_shot_clock_after_event(
            ev,
            shot_clock_state,
            full_reset=full_reset,
            short_reset=short_reset,
            use_short_reset=use_short_reset,
            league=league,
            season_year=season_year,
            last_shot_hit_rim=last_shot_hit_rim,
        )

        # 4. Update rim-hit heuristic based on this event (for future OREBs).
        if isinstance(ev, (FieldGoal, FreeThrow)):
            desc = getattr(ev, "description", "") or ""
            if "airball" in desc.lower():
                last_shot_hit_rim = False
            else:
                last_shot_hit_rim = True

        previous_event = ev


def _update_shot_clock_after_event(
    event,
    shot_clock_state: Optional[float],
    *,
    full_reset: float,
    short_reset: float,
    use_short_reset: bool,
    league: Optional[str],
    season_year: Optional[int],
    last_shot_hit_rim: bool,
) -> float:
    """
    Returns the shot clock value immediately AFTER `event` resolves, before any
    time elapses to the next event.

    `shot_clock_state` is the value at the *start* of this event (after decay).
    """
    if shot_clock_state is None:
        shot_clock_state = full_reset

    # Helper flags
    is_nba_modern = (
        league == pbpstats.NBA_STRING and season_year is not None and season_year >= 2018
    )

    offense_now = _safe_offense_team_id(event)
    next_event = getattr(event, "next_event", None)
    offense_next = _safe_offense_team_id(next_event)
    offense_changed = (
        offense_now is not None
        and offense_next is not None
        and offense_now != offense_next
    )

    #
    # 1. Rebounds
    #
    if isinstance(event, Rebound) and event.is_real_rebound:
        if event.oreb:
            # Offensive rebound: offense keeps ball.
            #
            # If we believe the prior shot did NOT hit the rim (airball),
            # there should be NO reset in any era; the clock keeps counting down.
            if not last_shot_hit_rim:
                return shot_clock_state

            # Otherwise, reset based on era/league rules.
            if use_short_reset:
                # Modern short-reset era or leagues with 14s resets
                return short_reset
            # Older eras: always reset to 24 on an OREB after a rim hit.
            return full_reset
        else:
            # Defensive rebound: new possession → full reset.
            return full_reset

    #
    # 2. Turnovers
    #
    if isinstance(event, Turnover) and not event.is_no_turnover:
        # Any real turnover creates a new possession for the other team.
        # We treat the reset as happening at the moment of the turnover; the
        # decay to shots on the new possession happens via seconds_since_previous_event.
        return full_reset

    #
    # 3. Made field goals
    #
    if isinstance(event, FieldGoal) and event.is_made:
        # pbpstats already tracks "and-1 / non-possession-ending" makes.
        if getattr(event, "is_make_that_does_not_end_possession", False):
            return shot_clock_state
        # Normal made basket → defense inbounds with fresh clock.
        return full_reset

    #
    # 4. Free throws that end the trip
    #
    if isinstance(event, FreeThrow) and event.is_end_ft:
        # If offense changes after the trip, treat this as possession end.
        if offense_changed:
            return full_reset
        return shot_clock_state

    #
    # 5. Defensive non-shooting fouls where offense keeps the ball.
    #
    if isinstance(event, Foul):
        # Rough "defense vs offense" check.
        team_id = getattr(event, "team_id", None)
        is_defense_foul = (
            offense_now is not None and team_id is not None and team_id != offense_now
        )

        if is_defense_foul:
            # Ignore technical/double fouls for shot clock purposes.
            if event.is_technical or event.is_double_technical or event.is_double_foul:
                return shot_clock_state

            # Ignore clear shooting fouls; those are handled via FTs/rebounds.
            if event.is_shooting_foul or event.is_shooting_block_foul:
                return shot_clock_state

            # Modern NBA frontcourt rule: if below 14, bump to 14; otherwise keep.
            if is_nba_modern and use_short_reset:
                return max(short_reset, shot_clock_state)

            # Other eras/leagues: treat as full reset.
            return full_reset

    #
    # 6. Defensive violations where offense keeps ball (e.g., kicked ball)
    #
    if isinstance(event, Violation):
        team_id = getattr(event, "team_id", None)
        is_defense_violation = (
            offense_now is not None and team_id is not None and team_id != offense_now
        )

        if is_defense_violation:
            if is_nba_modern and use_short_reset:
                return max(short_reset, shot_clock_state)
            return full_reset

    #
    # 7. Fallback: offense changed but we didn't catch the specific mechanism
    #
    if offense_changed and not isinstance(
        event, (FieldGoal, FreeThrow, Rebound, Turnover)
    ):
        return full_reset

    #
    # 8. Everything else (timeouts, subs, replay, techs, etc.) → keep current state.
    #
    return shot_clock_state
