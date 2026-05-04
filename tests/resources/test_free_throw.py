from collections import defaultdict

import pbpstats

from pbpstats.resources.enhanced_pbp.data_nba.free_throw import DataFreeThrow
from pbpstats.resources.enhanced_pbp.stats_nba.field_goal import StatsFieldGoal
from pbpstats.resources.enhanced_pbp.stats_nba.foul import StatsFoul
from pbpstats.resources.enhanced_pbp.stats_nba.free_throw import StatsFreeThrow


class DummyReboundEvent:
    event_type = 4

    def __init__(self, team_id):
        self.team_id = team_id


class CurrentPlayersSeed:
    def __init__(self, current_players):
        self._players = {
            int(team_id): list(player_ids)
            for team_id, player_ids in current_players.items()
        }
        self.game_id = "0020000001"
        self.period = 3
        self.clock = "3:49"
        self.order = 0
        self.seconds_remaining = 229.0
        self.score = defaultdict(int)
        self.player_game_fouls = defaultdict(int)
        self.is_possession_ending_event = False
        self.previous_event = None
        self.next_event = None

    @property
    def current_players(self):
        return {
            int(team_id): list(player_ids)
            for team_id, player_ids in self._players.items()
        }

    @property
    def _raw_current_players(self):
        return self.current_players


def test_data_made_free_throw():
    item = {
        "evt": 110,
        "cl": "01:09",
        "de": "[NYK 17-27] O'Quinn Free Throw 2 of 2 (3 PTS)",
        "locX": 0,
        "locY": -80,
        "mtype": 12,
        "etype": 3,
        "opid": "",
        "tid": 1610612752,
        "pid": 203124,
        "hs": 17,
        "vs": 27,
        "epid": "",
        "oftid": 1610612752,
    }
    period = 1
    game_id = "0021900001"
    event = DataFreeThrow(item, period, game_id)
    assert event.is_made is True


def test_data_missed_free_throw():
    item = {
        "evt": 108,
        "cl": "01:09",
        "de": "[NYK] O'Quinn Free Throw 1 of 2 Missed",
        "locX": 0,
        "locY": -80,
        "mtype": 11,
        "etype": 3,
        "opid": "",
        "tid": 1610612752,
        "pid": 203124,
        "hs": 16,
        "vs": 27,
        "epid": "",
        "oftid": 1610612752,
    }
    period = 1
    game_id = "0021900001"
    event = DataFreeThrow(item, period, game_id)
    assert event.is_made is False


def test_stats_made_free_throw():
    item = {
        "EVENTNUM": 110,
        "PCTIMESTRING": "01:09",
        "HOMEDESCRIPTION": "O'Quinn Free Throw 2 of 2 (3 PTS)",
        "EVENTMSGACTIONTYPE": 12,
        "EVENTMSGTYPE": 3,
        "PLAYER1_ID": 203124,
        "PLAYER1_TEAM_ID": 1610612752,
        "PLAYER2_ID": None,
        "PLAYER2_TEAM_ID": None,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
    }
    order = 1
    event = StatsFreeThrow(item, order)
    assert event.is_made is True


def test_stats_missed_free_throw():
    item = {
        "EVENTNUM": 108,
        "PCTIMESTRING": "01:09",
        "HOMEDESCRIPTION": "MISS O'Quinn Free Throw 1 of 2",
        "EVENTMSGACTIONTYPE": 11,
        "EVENTMSGTYPE": 3,
        "PLAYER1_ID": 203124,
        "PLAYER1_TEAM_ID": 1610612752,
        "PLAYER2_ID": None,
        "PLAYER2_TEAM_ID": None,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
    }
    order = 1
    event = StatsFreeThrow(item, order)
    assert event.is_made is False


def test_stats_ambiguous_final_free_throw_followed_by_offensive_rebound_is_missed():
    item = {
        "EVENTNUM": 228,
        "PCTIMESTRING": "2:58",
        "VISITORDESCRIPTION": "Free Throw 2 of 2",
        "EVENTMSGACTIONTYPE": 12,
        "EVENTMSGTYPE": 3,
        "PLAYER1_ID": 1924,
        "PLAYER1_TEAM_ID": 1610612739,
        "PLAYER2_ID": None,
        "PLAYER2_TEAM_ID": None,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
    }
    order = 1
    event = StatsFreeThrow(item, order)
    event.next_event = DummyReboundEvent(team_id=1610612739)
    assert event.is_made is False


def test_stats_ambiguous_final_free_throw_followed_by_defensive_rebound_is_missed():
    item = {
        "EVENTNUM": 490,
        "PCTIMESTRING": "0:04",
        "HOMEDESCRIPTION": "Free Throw 2 of 2",
        "EVENTMSGACTIONTYPE": 12,
        "EVENTMSGTYPE": 3,
        "PLAYER1_ID": 157,
        "PLAYER1_TEAM_ID": 1610612742,
        "PLAYER2_ID": None,
        "PLAYER2_TEAM_ID": None,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
    }
    order = 1
    event = StatsFreeThrow(item, order)
    event.next_event = DummyReboundEvent(team_id=1610612755)
    assert event.is_made is False


def test_ft_1_of_2():
    item = {
        "EVENTNUM": 108,
        "PCTIMESTRING": "01:09",
        "HOMEDESCRIPTION": "MISS O'Quinn Free Throw 1 of 2",
        "EVENTMSGACTIONTYPE": 11,
        "EVENTMSGTYPE": 3,
        "PLAYER1_ID": 203124,
        "PLAYER1_TEAM_ID": 1610612752,
        "PLAYER2_ID": None,
        "PLAYER2_TEAM_ID": None,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
    }
    order = 1
    event = StatsFreeThrow(item, order)
    assert event.is_ft_1_of_2 is True


def test_ft_1_of_3():
    item = {
        "EVENTNUM": 108,
        "PCTIMESTRING": "01:09",
        "HOMEDESCRIPTION": "MISS O'Quinn Free Throw 1 of 3",
        "EVENTMSGACTIONTYPE": 13,
        "EVENTMSGTYPE": 3,
        "PLAYER1_ID": 203124,
        "PLAYER1_TEAM_ID": 1610612752,
        "PLAYER2_ID": None,
        "PLAYER2_TEAM_ID": None,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
    }
    order = 1
    event = StatsFreeThrow(item, order)
    assert event.is_ft_1_of_3 is True


def test_ft_2_of_3():
    item = {
        "EVENTNUM": 108,
        "PCTIMESTRING": "01:09",
        "HOMEDESCRIPTION": "MISS O'Quinn Free Throw 2 of 3",
        "EVENTMSGACTIONTYPE": 14,
        "EVENTMSGTYPE": 3,
        "PLAYER1_ID": 203124,
        "PLAYER1_TEAM_ID": 1610612752,
        "PLAYER2_ID": None,
        "PLAYER2_TEAM_ID": None,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
    }
    order = 1
    event = StatsFreeThrow(item, order)
    assert event.is_ft_2_of_3 is True


def test_num_ft_for_trip_is_3():
    item = {
        "EVENTNUM": 108,
        "PCTIMESTRING": "01:09",
        "HOMEDESCRIPTION": "MISS O'Quinn Free Throw 1 of 3",
        "EVENTMSGACTIONTYPE": 13,
        "EVENTMSGTYPE": 3,
        "PLAYER1_ID": 203124,
        "PLAYER1_TEAM_ID": 1610612752,
        "PLAYER2_ID": None,
        "PLAYER2_TEAM_ID": None,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
    }
    order = 1
    event = StatsFreeThrow(item, order)
    assert event.num_ft_for_trip == 3


def test_away_from_play_true():
    foul = {
        "EVENTMSGTYPE": 6,
        "EVENTMSGACTIONTYPE": 6,
        "VISITORDESCRIPTION": "Away From Play Foul",
        "PCTIMESTRING": "0:45",
        "PLAYER1_TEAM_ID": 2,
        "PLAYER1_ID": 2,
        "PLAYER2_ID": 1,
    }
    order = 1
    foul_event = StatsFoul(foul, order)
    ft = {
        "EVENTMSGTYPE": 3,
        "EVENTMSGACTIONTYPE": 10,
        "HOMEDESCRIPTION": "Free Throw 1 of 1",
        "PCTIMESTRING": "0:45",
        "PLAYER1_TEAM_ID": 1,
        "PLAYER1_ID": 1,
    }
    order = 1
    ft_event = StatsFreeThrow(ft, order)
    make = {
        "EVENTMSGTYPE": 1,
        "EVENTMSGACTIONTYPE": 10,
        "VISITORDESCRIPTION": "Made Shot by team that got fouled",
        "PCTIMESTRING": "0:35",
        "PLAYER1_TEAM_ID": 1,
        "PLAYER1_ID": 21,
    }
    order = 1
    make_event = StatsFieldGoal(make, order)
    foul_event.previous_event = None
    foul_event.next_event = ft_event
    ft_event.previous_event = foul_event
    ft_event.next_event = make_event
    make_event.previous_event = ft_event
    make_event.next_event = None
    assert ft_event.is_away_from_play_ft is True


def test_foul_on_made_shot_by_team_that_got_fouled_is_not_away_from_play_ft():
    ft = {
        "EVENTMSGTYPE": 3,
        "EVENTMSGACTIONTYPE": 10,
        "HOMEDESCRIPTION": "Free Throw 1 of 1",
        "PCTIMESTRING": "0:45",
        "PLAYER1_TEAM_ID": 1,
        "PLAYER1_ID": 1,
    }
    order = 1
    ft_event = StatsFreeThrow(ft, order)
    foul = {
        "EVENTMSGTYPE": 6,
        "EVENTMSGACTIONTYPE": 6,
        "VISITORDESCRIPTION": "Away From Play Foul",
        "PCTIMESTRING": "0:45",
        "PLAYER1_TEAM_ID": 2,
        "PLAYER1_ID": 2,
        "PLAYER2_ID": 1,
    }
    order = 1
    foul_event = StatsFoul(foul, order)
    make = {
        "EVENTMSGTYPE": 1,
        "EVENTMSGACTIONTYPE": 10,
        "HOMEDESCRIPTION": "Made Shot by team that got fouled",
        "PCTIMESTRING": "0:45",
        "PLAYER1_TEAM_ID": 1,
        "PLAYER1_ID": 2,
    }
    order = 1
    make_event = StatsFieldGoal(make, order)
    ft_event.previous_event = None
    ft_event.next_event = foul_event
    foul_event.previous_event = ft_event
    foul_event.next_event = make_event
    make_event.previous_event = foul_event
    make_event.next_event = None
    assert ft_event.is_away_from_play_ft is False


def test_foul_on_made_shot_by_team_that_didnt_get_fouled_is_away_from_play_ft():
    ft = {
        "EVENTMSGTYPE": 3,
        "EVENTMSGACTIONTYPE": 10,
        "HOMEDESCRIPTION": "Free Throw 1 of 1",
        "PCTIMESTRING": "0:45",
        "PLAYER1_TEAM_ID": 1,
        "PLAYER1_ID": 1,
    }
    order = 1
    ft_event = StatsFreeThrow(ft, order)
    foul = {
        "EVENTMSGTYPE": 6,
        "EVENTMSGACTIONTYPE": 6,
        "VISITORDESCRIPTION": "Away From Play Foul",
        "PCTIMESTRING": "0:45",
        "PLAYER1_TEAM_ID": 2,
        "PLAYER1_ID": 2,
        "PLAYER2_ID": 1,
    }
    order = 1
    foul_event = StatsFoul(foul, order)
    make = {
        "EVENTMSGTYPE": 1,
        "EVENTMSGACTIONTYPE": 10,
        "HOMEDESCRIPTION": "Made Shot by team that got fouled",
        "PCTIMESTRING": "0:45",
        "PLAYER1_TEAM_ID": 2,
        "PLAYER1_ID": 3,
    }
    order = 1
    make_event = StatsFieldGoal(make, order)
    ft_event.previous_event = None
    ft_event.next_event = foul_event
    foul_event.previous_event = ft_event
    foul_event.next_event = make_event
    make_event.previous_event = foul_event
    make_event.next_event = None
    assert ft_event.is_away_from_play_ft is True


def test_foul_on_made_ft_by_team_that_didnt_get_fouled_is_away_from_play_ft():
    ft_2_of_2 = {
        "EVENTNUM": 607,
        "PCTIMESTRING": "0:25",
        "VISITORDESCRIPTION": "Jackson Free Throw 2 of 2 (16 PTS)",
        "EVENTMSGACTIONTYPE": 12,
        "EVENTMSGTYPE": 3,
        "PLAYER1_ID": 202704,
        "PLAYER1_TEAM_ID": 1610612765,
        "PLAYER2_ID": None,
        "PLAYER2_TEAM_ID": None,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
    }
    order = 1
    ft_2_of_2_event = StatsFreeThrow(ft_2_of_2, order)
    foul = {
        "EVENTNUM": 609,
        "PCTIMESTRING": "0:25",
        "VISITORDESCRIPTION": "Griffin AWAY.FROM.PLAY.FOUL (P5.PN) (M.Davis)",
        "EVENTMSGACTIONTYPE": 6,
        "EVENTMSGTYPE": 6,
        "PLAYER1_ID": 201933,
        "PLAYER1_TEAM_ID": 1610612765,
        "PLAYER2_ID": 201145,
        "PLAYER2_TEAM_ID": 1610612764,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
    }
    order = 1
    foul_event = StatsFoul(foul, order)
    ft_1_of_1 = {
        "EVENTNUM": 611,
        "PCTIMESTRING": "0:25",
        "HOMEDESCRIPTION": "Beal Free Throw 1 of 1 (32 PTS)",
        "EVENTMSGACTIONTYPE": 10,
        "EVENTMSGTYPE": 3,
        "PLAYER1_ID": 203078,
        "PLAYER1_TEAM_ID": 1610612764,
        "PLAYER2_ID": None,
        "PLAYER2_TEAM_ID": None,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
    }
    order = 1
    ft_1_of_1_event = StatsFreeThrow(ft_1_of_1, order)
    fg = {
        "EVENTNUM": 612,
        "PCTIMESTRING": "0:24",
        "VISITORDESCRIPTION": "MISS Green 27' 3PT Jump Shot",
        "EVENTMSGACTIONTYPE": 1,
        "EVENTMSGTYPE": 2,
        "PLAYER1_ID": 201145,
        "PLAYER1_TEAM_ID": 1610612764,
        "PLAYER2_ID": None,
        "PLAYER2_TEAM_ID": None,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
    }
    order = 1
    fg_event = StatsFieldGoal(fg, order)
    ft_2_of_2_event.previous_event = None
    ft_2_of_2_event.next_event = foul_event
    foul_event.previous_event = ft_2_of_2_event
    foul_event.next_event = ft_1_of_1_event
    ft_1_of_1_event.previous_event = foul_event
    ft_1_of_1_event.next_event = fg_event
    fg_event.previous_event = ft_1_of_1_event
    fg_event.next_event = None
    assert ft_1_of_1_event.is_away_from_play_ft is True


def test_inbound_foul_ft_true():
    foul = {
        "EVENTMSGTYPE": 6,
        "EVENTMSGACTIONTYPE": 5,
        "VISITORDESCRIPTION": "Inbound Foul",
        "PCTIMESTRING": "0:45",
        "PLAYER1_TEAM_ID": 2,
    }
    order = 1
    foul_event = StatsFoul(foul, order)
    ft = {
        "EVENTMSGTYPE": 3,
        "EVENTMSGACTIONTYPE": 10,
        "HOMEDESCRIPTION": "Free Throw 1 of 1",
        "PCTIMESTRING": "0:45",
        "PLAYER1_TEAM_ID": 1,
    }
    order = 1
    ft_event = StatsFreeThrow(ft, order)
    foul_event.previous_event = None
    foul_event.next_event = ft_event
    ft_event.previous_event = foul_event
    ft_event.next_event = None
    assert ft_event.is_inbound_foul_ft is True


def test_away_from_play_free_throw_type():
    foul = {
        "EVENTMSGTYPE": 6,
        "EVENTMSGACTIONTYPE": 6,
        "VISITORDESCRIPTION": "Away From Play Foul",
        "PCTIMESTRING": "0:45",
        "PLAYER1_TEAM_ID": 2,
        "PLAYER1_ID": 2,
        "PLAYER2_ID": 1,
    }
    order = 1
    foul_event = StatsFoul(foul, order)
    ft = {
        "EVENTMSGTYPE": 3,
        "EVENTMSGACTIONTYPE": 10,
        "HOMEDESCRIPTION": "Free Throw 1 of 1",
        "PCTIMESTRING": "0:45",
        "PLAYER1_TEAM_ID": 1,
        "PLAYER1_ID": 1,
    }
    order = 1
    ft_event = StatsFreeThrow(ft, order)
    make = {
        "EVENTMSGTYPE": 1,
        "EVENTMSGACTIONTYPE": 10,
        "VISITORDESCRIPTION": "Made Shot by team that got fouled",
        "PCTIMESTRING": "0:35",
        "PLAYER1_TEAM_ID": 1,
        "PLAYER1_ID": 21,
    }
    order = 1
    make_event = StatsFieldGoal(make, order)
    foul_event.previous_event = None
    foul_event.next_event = ft_event
    ft_event.previous_event = foul_event
    ft_event.next_event = make_event
    make_event.previous_event = ft_event
    make_event.next_event = None
    assert ft_event.free_throw_type == "1 Shot Away From Play"


def test_flagrant_free_throw_type():
    foul = {
        "EVENTNUM": 609,
        "PCTIMESTRING": "0:25",
        "VISITORDESCRIPTION": "Griffin Flagrant Foul (P5.PN) (M.Davis)",
        "EVENTMSGACTIONTYPE": 14,
        "EVENTMSGTYPE": 6,
        "PLAYER1_ID": 201933,
        "PLAYER1_TEAM_ID": 1610612765,
        "PLAYER2_ID": 203078,
        "PLAYER2_TEAM_ID": 1610612764,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
    }
    order = 1
    foul_event = StatsFoul(foul, order)
    ft_1_of_2 = {
        "EVENTNUM": 611,
        "PCTIMESTRING": "0:25",
        "HOMEDESCRIPTION": "Beal Free Throw Flagrant 1 of 2 (32 PTS)",
        "EVENTMSGACTIONTYPE": 11,
        "EVENTMSGTYPE": 3,
        "PLAYER1_ID": 203078,
        "PLAYER1_TEAM_ID": 1610612764,
        "PLAYER2_ID": None,
        "PLAYER2_TEAM_ID": None,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
    }
    order = 1
    ft_1_of_2_event = StatsFreeThrow(ft_1_of_2, order)
    ft_2_of_2 = {
        "EVENTNUM": 611,
        "PCTIMESTRING": "0:25",
        "HOMEDESCRIPTION": "Beal Free Throw Flagrant 2 of 2 (32 PTS)",
        "EVENTMSGACTIONTYPE": 12,
        "EVENTMSGTYPE": 3,
        "PLAYER1_ID": 203078,
        "PLAYER1_TEAM_ID": 1610612764,
        "PLAYER2_ID": None,
        "PLAYER2_TEAM_ID": None,
        "PLAYER3_ID": None,
        "PLAYER3_TEAM_ID": None,
    }
    order = 1
    ft_2_of_2_event = StatsFreeThrow(ft_2_of_2, order)

    foul_event.previous_event = None
    foul_event.next_event = ft_1_of_2_event
    ft_1_of_2_event.previous_event = foul_event
    ft_1_of_2_event.next_event = ft_2_of_2_event
    ft_2_of_2_event.previous_event = ft_1_of_2_event
    ft_2_of_2_event.next_event = None

    assert ft_1_of_2_event.free_throw_type == "2 Shot Flagrant"


def _stats_foul_event(event_num, event_action_type, player_id, team_id):
    return StatsFoul(
        {
            "GAME_ID": "0020000001",
            "EVENTNUM": event_num,
            "PERIOD": 3,
            "PCTIMESTRING": "3:49",
            "VISITORDESCRIPTION": "Foul",
            "EVENTMSGACTIONTYPE": event_action_type,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": player_id,
            "PLAYER1_TEAM_ID": team_id,
            "PLAYER2_ID": 20,
            "PLAYER2_TEAM_ID": 200,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
        },
        event_num,
    )


def _stats_free_throw_event(event_num, event_action_type, player_id, team_id, description):
    return StatsFreeThrow(
        {
            "GAME_ID": "0020000001",
            "EVENTNUM": event_num,
            "PERIOD": 3,
            "PCTIMESTRING": "3:49",
            "HOMEDESCRIPTION": description,
            "EVENTMSGACTIONTYPE": event_action_type,
            "EVENTMSGTYPE": 3,
            "PLAYER1_ID": player_id,
            "PLAYER1_TEAM_ID": team_id,
            "PLAYER2_ID": None,
            "PLAYER2_TEAM_ID": None,
            "PLAYER3_ID": None,
            "PLAYER3_TEAM_ID": None,
        },
        event_num,
    )


def _wire_events(events):
    for index, event in enumerate(events):
        event.previous_event = events[index - 1] if index else None
        event.next_event = events[index + 1] if index + 1 < len(events) else None
    return events


def test_event_for_efficiency_stats_keeps_normal_ft_context_after_same_player_technical():
    shooting_foul = _stats_foul_event(388, 2, 14, 100)
    ft1 = _stats_free_throw_event(389, 11, 20, 200, "Griner Free Throw 1 of 2 (1 PTS)")
    technical_foul = _stats_foul_event(393, 11, 14, 100)
    technical_ft = _stats_free_throw_event(
        394, 10, 30, 200, "Taurasi Technical Free Throw 1 of 1 (1 PTS)"
    )
    ft2 = _stats_free_throw_event(395, 12, 20, 200, "Griner Free Throw 2 of 2 (2 PTS)")
    _wire_events([shooting_foul, ft1, technical_foul, technical_ft, ft2])

    assert ft1.event_for_efficiency_stats == shooting_foul
    assert technical_ft.event_for_efficiency_stats == technical_foul
    assert ft2.event_for_efficiency_stats == shooting_foul


def test_interrupted_normal_ft_event_stats_use_original_foul_lineup(monkeypatch):
    original_lineup = {
        100: [10, 11, 12, 13, 14],
        200: [20, 21, 22, 23, 24],
    }
    post_sub_lineup = {
        100: [10, 11, 12, 13, 15],
        200: [20, 21, 22, 23, 24],
    }
    shooting_foul = _stats_foul_event(388, 2, 14, 100)
    ft1 = _stats_free_throw_event(389, 11, 20, 200, "Griner Free Throw 1 of 2 (1 PTS)")
    technical_foul = _stats_foul_event(393, 11, 14, 100)
    technical_ft = _stats_free_throw_event(
        394, 10, 30, 200, "Taurasi Technical Free Throw 1 of 1 (1 PTS)"
    )
    ft2 = _stats_free_throw_event(395, 12, 20, 200, "Griner Free Throw 2 of 2 (2 PTS)")
    _wire_events(
        [
            CurrentPlayersSeed(original_lineup),
            shooting_foul,
            ft1,
            CurrentPlayersSeed(post_sub_lineup),
            technical_foul,
            technical_ft,
            ft2,
        ]
    )
    monkeypatch.setattr(StatsFreeThrow, "base_stats", property(lambda self: []))
    monkeypatch.setattr(StatsFreeThrow, "is_penalty_event", lambda self: False)
    monkeypatch.setattr(StatsFreeThrow, "is_second_chance_event", lambda self: False)

    plus_minus_stats = [
        stat
        for stat in ft2.event_stats
        if stat["stat_key"] == pbpstats.PLUS_MINUS_STRING
    ]

    assert any(
        stat["player_id"] == 14 and stat["team_id"] == 100 and stat["stat_value"] == -1
        for stat in plus_minus_stats
    )
    assert not any(stat["player_id"] == 15 for stat in plus_minus_stats)


def test_event_for_efficiency_stats_keeps_2_of_3_context_after_same_player_technical():
    shooting_foul = _stats_foul_event(388, 2, 14, 100)
    ft1 = _stats_free_throw_event(389, 13, 20, 200, "Griner Free Throw 1 of 3 (1 PTS)")
    ft2 = _stats_free_throw_event(390, 14, 20, 200, "Griner Free Throw 2 of 3 (2 PTS)")
    technical_foul = _stats_foul_event(393, 11, 14, 100)
    technical_ft = _stats_free_throw_event(
        394, 10, 30, 200, "Taurasi Technical Free Throw 1 of 1 (1 PTS)"
    )
    ft3 = _stats_free_throw_event(395, 15, 20, 200, "Griner Free Throw 3 of 3 (3 PTS)")
    _wire_events([shooting_foul, ft1, ft2, technical_foul, technical_ft, ft3])

    assert ft3.event_for_efficiency_stats == shooting_foul


def test_event_for_efficiency_stats_keeps_all_remaining_3shot_fts_after_same_player_technical_after_ft1():
    shooting_foul = _stats_foul_event(388, 2, 14, 100)
    ft1 = _stats_free_throw_event(389, 13, 20, 200, "Griner Free Throw 1 of 3 (1 PTS)")
    technical_foul = _stats_foul_event(393, 11, 14, 100)
    technical_ft = _stats_free_throw_event(
        394, 10, 30, 200, "Taurasi Technical Free Throw 1 of 1 (1 PTS)"
    )
    ft2 = _stats_free_throw_event(395, 14, 20, 200, "Griner Free Throw 2 of 3 (2 PTS)")
    ft3 = _stats_free_throw_event(396, 15, 20, 200, "Griner Free Throw 3 of 3 (3 PTS)")
    _wire_events([shooting_foul, ft1, technical_foul, technical_ft, ft2, ft3])

    assert ft2.event_for_efficiency_stats == shooting_foul
    assert ft3.event_for_efficiency_stats == shooting_foul


def test_event_for_efficiency_stats_does_not_rewrite_first_normal_ft_after_technical():
    shooting_foul = _stats_foul_event(388, 2, 14, 100)
    technical_foul = _stats_foul_event(393, 11, 14, 100)
    ft1 = _stats_free_throw_event(395, 11, 20, 200, "Griner Free Throw 1 of 2 (1 PTS)")
    _wire_events([shooting_foul, technical_foul, ft1])

    assert ft1.event_for_efficiency_stats == technical_foul


def test_event_for_efficiency_stats_does_not_rewrite_ft2_when_technical_precedes_prior_ft():
    shooting_foul = _stats_foul_event(388, 2, 14, 100)
    technical_foul = _stats_foul_event(389, 11, 14, 100)
    technical_ft = _stats_free_throw_event(
        390, 10, 30, 200, "Taurasi Technical Free Throw 1 of 1 (1 PTS)"
    )
    ft1 = _stats_free_throw_event(391, 11, 20, 200, "Griner Free Throw 1 of 2 (1 PTS)")
    ft2 = _stats_free_throw_event(392, 12, 20, 200, "Griner Free Throw 2 of 2 (2 PTS)")
    _wire_events([shooting_foul, technical_foul, technical_ft, ft1, ft2])

    assert ft1.event_for_efficiency_stats == technical_foul
    assert ft2.event_for_efficiency_stats == technical_foul


def test_event_for_efficiency_stats_does_not_rewrite_flagrant_context():
    flagrant_foul = _stats_foul_event(388, 14, 14, 100)
    ft1 = _stats_free_throw_event(
        389, 11, 20, 200, "Griner Free Throw Flagrant 1 of 2 (1 PTS)"
    )
    technical_foul = _stats_foul_event(393, 11, 14, 100)
    ft2 = _stats_free_throw_event(
        395, 12, 20, 200, "Griner Free Throw Flagrant 2 of 2 (2 PTS)"
    )
    _wire_events([flagrant_foul, ft1, technical_foul, ft2])

    assert ft2.event_for_efficiency_stats == technical_foul


def test_event_for_efficiency_stats_does_not_rewrite_special_foul_contexts():
    for action_type, description in [
        (5, "Griner Free Throw Inbound 2 of 2 (2 PTS)"),
        (6, "Griner Free Throw Away From Play 2 of 2 (2 PTS)"),
        (9, "Griner Free Throw Clear Path 2 of 2 (2 PTS)"),
        (31, "Griner Free Throw Transition Take 2 of 2 (2 PTS)"),
    ]:
        special_foul = _stats_foul_event(388, action_type, 14, 100)
        ft1 = _stats_free_throw_event(
            389, 11, 20, 200, description.replace("2 of 2", "1 of 2")
        )
        technical_foul = _stats_foul_event(393, 11, 14, 100)
        ft2 = _stats_free_throw_event(395, 12, 20, 200, description)
        _wire_events([special_foul, ft1, technical_foul, ft2])

        assert ft2.event_for_efficiency_stats == technical_foul


def test_event_for_efficiency_stats_does_not_rewrite_different_player_technical():
    shooting_foul = _stats_foul_event(388, 2, 14, 100)
    ft1 = _stats_free_throw_event(389, 11, 20, 200, "Griner Free Throw 1 of 2 (1 PTS)")
    technical_foul = _stats_foul_event(393, 11, 99, 100)
    ft2 = _stats_free_throw_event(395, 12, 20, 200, "Griner Free Throw 2 of 2 (2 PTS)")
    _wire_events([shooting_foul, ft1, technical_foul, ft2])

    assert ft2.event_for_efficiency_stats == technical_foul


def test_event_for_efficiency_stats_does_not_rewrite_missing_player_technical():
    shooting_foul = _stats_foul_event(388, 2, 0, 100)
    ft1 = _stats_free_throw_event(389, 11, 20, 200, "Griner Free Throw 1 of 2 (1 PTS)")
    technical_foul = _stats_foul_event(393, 11, 0, 100)
    ft2 = _stats_free_throw_event(395, 12, 20, 200, "Griner Free Throw 2 of 2 (2 PTS)")
    _wire_events([shooting_foul, ft1, technical_foul, ft2])

    assert ft2.event_for_efficiency_stats == technical_foul


def test_event_for_efficiency_stats_does_not_rewrite_and_one_after_technical():
    shooting_foul = _stats_foul_event(388, 2, 14, 100)
    technical_foul = _stats_foul_event(393, 11, 14, 100)
    and_one_ft = _stats_free_throw_event(395, 10, 20, 200, "Griner Free Throw 1 of 1 (1 PTS)")
    _wire_events([shooting_foul, technical_foul, and_one_ft])

    assert and_one_ft.event_for_efficiency_stats == technical_foul


def test_event_for_efficiency_stats_does_not_rewrite_without_prior_same_trip_ft():
    shooting_foul = _stats_foul_event(388, 2, 14, 100)
    technical_foul = _stats_foul_event(393, 11, 14, 100)
    ft2 = _stats_free_throw_event(395, 12, 20, 200, "Griner Free Throw 2 of 2 (2 PTS)")
    _wire_events([shooting_foul, technical_foul, ft2])

    assert ft2.event_for_efficiency_stats == technical_foul


def test_event_for_efficiency_stats_when_events_out_of_order():
    ft = {
        "EVENTMSGTYPE": 3,
        "EVENTMSGACTIONTYPE": 10,
        "HOMEDESCRIPTION": "Free Throw 1 of 1",
        "PCTIMESTRING": "0:45",
        "PLAYER1_TEAM_ID": 1,
        "PLAYER1_ID": 1,
    }
    order = 1
    ft_event = StatsFreeThrow(ft, order)
    foul = {
        "EVENTMSGTYPE": 6,
        "EVENTMSGACTIONTYPE": 6,
        "VISITORDESCRIPTION": "Away From Play Foul",
        "PCTIMESTRING": "0:45",
        "PLAYER1_TEAM_ID": 2,
        "PLAYER1_ID": 2,
        "PLAYER2_ID": 1,
    }
    order = 1
    foul_event = StatsFoul(foul, order)

    ft_event.previous_event = None
    ft_event.next_event = foul_event
    foul_event.previous_event = ft_event
    foul_event.next_event = None
    assert ft_event.event_for_efficiency_stats == foul_event
