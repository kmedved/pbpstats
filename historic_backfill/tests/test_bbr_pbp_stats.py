import pandas as pd

from bbr_pbp_stats import aggregate_bbr_player_stats, normalize_person_name


def test_aggregate_bbr_player_stats_counts_made_shots_assists_and_rebounds():
    rows = [
        {
            "away_play": "",
            "away_player_ids": "",
            "home_play": "A. Horfordmakes 3-pt jump shot from 24 ft (assist byA. Bradley)",
            "home_player_ids": "horfoal01,bradlav01",
        },
        {
            "away_play": "",
            "away_player_ids": "",
            "home_play": "Offensive rebound byA. Bradley",
            "home_player_ids": "bradlav01",
        },
        {
            "away_play": "",
            "away_player_ids": "",
            "home_play": "Defensive rebound byA. Bradley",
            "home_player_ids": "bradlav01",
        },
        {
            "away_play": "T. McConnellmisses 3-pt jump shot from 30 ft",
            "away_player_ids": "mccontj01",
            "home_play": "",
            "home_player_ids": "",
        },
    ]

    stats = aggregate_bbr_player_stats(rows).set_index("bbr_slug")

    assert stats.loc["horfoal01", "FGM"] == 1
    assert stats.loc["horfoal01", "FGA"] == 1
    assert stats.loc["horfoal01", "3PM"] == 1
    assert stats.loc["horfoal01", "3PA"] == 1
    assert stats.loc["horfoal01", "PTS"] == 3
    assert stats.loc["bradlav01", "AST"] == 1
    assert stats.loc["bradlav01", "OREB"] == 1
    assert stats.loc["bradlav01", "DRB"] == 1
    assert stats.loc["bradlav01", "REB"] == 2
    assert stats.loc["mccontj01", "FGA"] == 1
    assert stats.loc["mccontj01", "3PA"] == 1


def test_aggregate_bbr_player_stats_counts_turnovers_steals_blocks_and_free_throws():
    rows = [
        {
            "away_play": "",
            "away_player_ids": "",
            "home_play": "Turnover byN. Batum(bad pass; steal byT. McConnell)",
            "home_player_ids": "batumni01,mccontj01",
        },
        {
            "away_play": "T. McConnellmakes 2-pt layup from 1 ft (assist byJ. Okafor)",
            "away_player_ids": "mccontj01,okafoja01",
            "home_play": "",
            "home_player_ids": "",
        },
        {
            "away_play": "J. Okaformakes free throw 1 of 2",
            "away_player_ids": "okafoja01",
            "home_play": "",
            "home_player_ids": "",
        },
        {
            "away_play": "J. Okaformisses free throw 2 of 2",
            "away_player_ids": "okafoja01",
            "home_play": "",
            "home_player_ids": "",
        },
        {
            "away_play": "H. Thompsonmisses 2-pt layup from 1 ft (block byJ. Okafor)",
            "away_player_ids": "thompho01,okafoja01",
            "home_play": "",
            "home_player_ids": "",
        },
    ]

    stats = aggregate_bbr_player_stats(rows).set_index("bbr_slug")

    assert stats.loc["batumni01", "TOV"] == 1
    assert stats.loc["mccontj01", "STL"] == 1
    assert stats.loc["mccontj01", "FGM"] == 1
    assert stats.loc["mccontj01", "FGA"] == 1
    assert stats.loc["mccontj01", "PTS"] == 2
    assert stats.loc["okafoja01", "AST"] == 1
    assert stats.loc["okafoja01", "FTA"] == 2
    assert stats.loc["okafoja01", "FTM"] == 1
    assert stats.loc["okafoja01", "PTS"] == 1
    assert stats.loc["okafoja01", "BLK"] == 1


def test_aggregate_bbr_player_stats_treats_offensive_fouls_as_turnovers():
    rows = [
        {
            "away_play": "Offensive foul byS. Marciulionis",
            "away_player_ids": "marcisa01",
            "home_play": "",
            "home_player_ids": "",
        }
    ]

    stats = aggregate_bbr_player_stats(rows).set_index("bbr_slug")

    assert stats.loc["marcisa01", "TOV"] == 1


def test_aggregate_bbr_player_stats_dedupes_double_logged_offensive_foul_turnovers():
    rows = [
        {
            "period": 3,
            "game_clock": "2:16.0",
            "away_play": "",
            "away_player_ids": "",
            "home_play": "Offensive foul byK. Olynyk(drawn byT. Maker)",
            "home_player_ids": "olynyke01,makerth01",
        },
        {
            "period": 3,
            "game_clock": "2:16.0",
            "away_play": "",
            "away_player_ids": "",
            "home_play": "Turnover byK. Olynyk(offensive foul)",
            "home_player_ids": "olynyke01",
        },
    ]

    stats = aggregate_bbr_player_stats(rows).set_index("bbr_slug")

    assert stats.loc["olynyke01", "TOV"] == 1


def test_normalize_person_name_repairs_common_bbr_mojibake():
    assert normalize_person_name("Å arÅ«nas MarÄiulionis") == "sarunas marciulionis"
