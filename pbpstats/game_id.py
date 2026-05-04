import math
from numbers import Integral, Real

import pbpstats


def _coerce_integral_game_id_string(game_id) -> str:
    if game_id is None:
        return ""
    if isinstance(game_id, Integral) and not isinstance(game_id, bool):
        return str(int(game_id))
    if isinstance(game_id, Real) and not isinstance(game_id, bool):
        value = float(game_id)
        if math.isfinite(value) and value.is_integer():
            return str(int(value))

    raw_game_id = str(game_id).strip()
    try:
        value = float(raw_game_id)
    except (TypeError, ValueError):
        return raw_game_id
    if math.isfinite(value) and value.is_integer():
        return str(int(value))
    return raw_game_id


def normalize_game_id(game_id, league=None) -> str:
    """
    Normalize numeric and string-like NBA-family game ids.

    Short ids are ambiguous without a league. When a league is supplied, use its
    prefix and pad the provider game number to eight digits.
    """
    raw_game_id = _coerce_integral_game_id_string(game_id)
    if not raw_game_id.isdigit():
        return raw_game_id

    league_prefixes = {
        pbpstats.NBA_STRING: pbpstats.NBA_GAME_ID_PREFIX,
        pbpstats.WNBA_STRING: pbpstats.WNBA_GAME_ID_PREFIX,
        pbpstats.G_LEAGUE_STRING: pbpstats.G_LEAGUE_GAME_ID_PREFIX,
    }
    if league in league_prefixes:
        desired_prefix = league_prefixes[league]
        known_prefixes = set(league_prefixes.values())
        if len(raw_game_id) == 10 and raw_game_id[:2] in known_prefixes:
            return desired_prefix + raw_game_id[-8:]
        if len(raw_game_id) <= 8:
            return desired_prefix + raw_game_id.zfill(8)
    if len(raw_game_id) == 10:
        return raw_game_id
    return raw_game_id.zfill(10)


def uses_wnba_twenty_minute_halves(league, season_year) -> bool:
    return (
        league == pbpstats.WNBA_STRING
        and season_year is not None
        and season_year < pbpstats.WNBA_FOUR_QUARTERS_START_SEASON
    )


def regulation_period_count(league, season_year) -> int:
    if uses_wnba_twenty_minute_halves(league, season_year):
        return 2
    return 4


def is_overtime_period(period, league, season_year) -> bool:
    try:
        period = int(period)
    except (TypeError, ValueError):
        return False
    return period > regulation_period_count(league, season_year)
