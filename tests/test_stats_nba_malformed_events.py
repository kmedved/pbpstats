from types import SimpleNamespace

from pbpstats.resources.enhanced_pbp.stats_nba.jump_ball import StatsJumpBall
from pbpstats.resources.enhanced_pbp.stats_nba.start_of_period import StatsStartOfPeriod
from pbpstats.resources.enhanced_pbp.stats_nba.substitution import StatsSubstitution


def _event(
    *,
    game_id: str,
    event_num: int,
    event_type: int,
    clock: str,
    period: int,
    player1_id: int | None = None,
    player1_team_id: int | None = None,
    player2_id: int | None = None,
    player2_team_id: int | None = None,
    player3_id: int | None = None,
    player3_team_id: int | None = None,
    home_description: str | None = "",
    visitor_description: str | None = "",
    neutral_description: str | None = None,
) -> dict:
    return {
        "GAME_ID": game_id,
        "EVENTNUM": event_num,
        "EVENTMSGTYPE": event_type,
        "EVENTMSGACTIONTYPE": 0,
        "PCTIMESTRING": clock,
        "PERIOD": period,
        "PLAYER1_ID": player1_id,
        "PLAYER1_TEAM_ID": player1_team_id,
        "PLAYER2_ID": player2_id,
        "PLAYER2_TEAM_ID": player2_team_id,
        "PLAYER3_ID": player3_id,
        "PLAYER3_TEAM_ID": player3_team_id,
        "HOMEDESCRIPTION": home_description,
        "VISITORDESCRIPTION": visitor_description,
        "NEUTRALDESCRIPTION": neutral_description,
        "VIDEO_AVAILABLE_FLAG": 0,
    }


def test_stats_substitution_missing_incoming_player_becomes_noop_lineup_change():
    sub = StatsSubstitution(
        _event(
            game_id="0029700621",
            event_num=579,
            event_type=8,
            clock="0:00",
            period=5,
            player1_id=688,
            player1_team_id=1610612749,
            home_description="",
            visitor_description="SUB:  FOR Curry",
        ),
        0,
    )
    previous_players = {
        1610612749: [688, 210, 238, 299, 951],
        1610612752: [168, 201, 275, 317, 369],
    }
    sub.previous_event = SimpleNamespace(current_players=previous_players)

    assert sub.incoming_player_id == 688
    assert sub.current_players == previous_players


def test_stats_jump_ball_with_incomplete_lineups_returns_empty_event_stats():
    start = StatsStartOfPeriod(
        _event(
            game_id="0049700045",
            event_num=0,
            event_type=12,
            clock="12:00",
            period=1,
            neutral_description="Start of 1st Period",
        ),
        0,
    )
    start.period_starters = {1610612741: [23, 166, 389, 893, 937]}

    jump_ball = StatsJumpBall(
        _event(
            game_id="0049700045",
            event_num=1,
            event_type=10,
            clock="12:00",
            period=1,
            player1_id=389,
            player1_team_id=1610612741,
            player2_id=124,
            player2_team_id=1610612766,
            player3_id=133,
            player3_team_id=1610612766,
            home_description="Jump Ball Kukoc vs. Divac: Tip to Wesley",
            visitor_description="",
        ),
        1,
    )
    jump_ball.previous_event = start
    jump_ball.next_event = SimpleNamespace(clock="11:43")

    assert jump_ball.event_stats == []
