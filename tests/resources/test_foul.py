import pbpstats
from pbpstats.resources.enhanced_pbp.stats_nba.foul import StatsFoul


def _build_foul(action_type: int) -> StatsFoul:
    return StatsFoul(
        {
            "GAME_ID": "0029700171",
            "EVENTNUM": 1,
            "PCTIMESTRING": "11:06",
            "PERIOD": 4,
            "EVENTMSGACTIONTYPE": action_type,
            "EVENTMSGTYPE": 6,
            "PLAYER1_ID": 891,
            "PLAYER1_TEAM_ID": 1610612752,
            "PLAYER2_ID": 901,
            "VIDEO_AVAILABLE_FLAG": 0,
            "HOMEDESCRIPTION": "Test foul",
            "VISITORDESCRIPTION": "",
        },
        0,
    )


def test_elbow_foul_counts_as_personal_foul():
    foul = _build_foul(7)

    assert foul.is_personal_foul is True
    assert foul.counts_as_personal_foul is True
    assert foul.counts_towards_penalty is True
    assert foul.foul_type_string == pbpstats.PERSONAL_FOUL_TYPE_STRING


def test_punch_foul_counts_as_personal_foul():
    foul = _build_foul(8)

    assert foul.is_personal_foul is True
    assert foul.counts_as_personal_foul is True
    assert foul.counts_towards_penalty is True
    assert foul.foul_type_string == pbpstats.PERSONAL_FOUL_TYPE_STRING
