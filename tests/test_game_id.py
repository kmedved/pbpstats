import pbpstats
from pbpstats.game_id import normalize_game_id


def test_normalize_game_id_honors_explicit_league_for_conflicting_known_prefixes():
    assert normalize_game_id("1022500234", league=pbpstats.NBA_STRING) == "0022500234"
    assert normalize_game_id("2022500234", league=pbpstats.WNBA_STRING) == "1022500234"
    assert (
        normalize_game_id("0022500234", league=pbpstats.G_LEAGUE_STRING)
        == "2022500234"
    )


def test_normalize_game_id_keeps_unknown_full_prefix_unchanged():
    assert normalize_game_id("3022500234", league=pbpstats.WNBA_STRING) == "3022500234"
