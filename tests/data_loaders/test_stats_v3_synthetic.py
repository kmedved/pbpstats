import pytest

from pbpstats.data_loader.stats_nba.pbp.v3_synthetic import (
    ENDPOINT_STRATEGY_V3_SYNTHETIC,
    V2_HEADERS,
    StatsNbaV2PbpResponseError,
    StatsNbaV3SyntheticMappingError,
    StatsNbaV3SyntheticRoleError,
    UnsupportedV3SyntheticSchemaError,
    build_synthetic_v2_pbp_response,
)
from pbpstats.data_loader.stats_nba.enhanced_pbp.loader import (
    StatsNbaEnhancedPbpLoader,
)
from pbpstats.data_loader.stats_nba.pbp.web import StatsNbaPbpWebLoader
from pbpstats.resources.enhanced_pbp.stats_nba.enhanced_pbp_factory import (
    StatsNbaEnhancedPbpFactory,
)
from pbpstats.resources.possessions.possession import Possession

GAME_ID = "0022400001"
HOME_ID = 1610612764
AWAY_ID = 1610612739


def _action(
    action_number,
    action_id,
    action_type,
    sub_type="",
    team_id=None,
    location="",
    team_tricode="",
    person_id=0,
    player_name="",
    description="",
    period=1,
    clock="PT12M00.00S",
    score_home=None,
    score_away=None,
):
    return {
        "gameId": GAME_ID,
        "actionNumber": action_number,
        "actionId": action_id,
        "actionType": action_type,
        "subType": sub_type,
        "teamId": team_id,
        "teamTricode": team_tricode,
        "location": location,
        "personId": person_id,
        "playerName": player_name,
        "description": description,
        "period": period,
        "clock": clock,
        "scoreHome": score_home,
        "scoreAway": score_away,
        "videoAvailable": 0,
    }


def _sample_v3_source_data():
    actions = [
        _action(
            5,
            6,
            "substitution",
            team_id=HOME_ID,
            location="h",
            team_tricode="WAS",
            person_id=203484,
            player_name="Corey Kispert",
            description="SUB: Corey Kispert FOR Kyle Kuzma",
        ),
        _action(1, 1, "period", "start", description="Period Start"),
        _action(
            2,
            2,
            "jump ball",
            team_id=HOME_ID,
            location="h",
            team_tricode="WAS",
            person_id=1629655,
            player_name="Daniel Gafford",
            description="Jump Ball Daniel Gafford vs. Jarrett Allen: Tip to Tyus Jones",
        ),
        _action(
            3,
            3,
            "timeout",
            "regular",
            team_id=AWAY_ID,
            location="v",
            team_tricode="CLE",
            person_id=AWAY_ID,
            description="Cavaliers Timeout: Regular",
        ),
        _action(
            4,
            4,
            "instant replay",
            "support ruling",
            description="Instant Replay: Support Ruling",
        ),
        _action(
            5,
            5,
            "substitution",
            team_id=HOME_ID,
            location="h",
            team_tricode="WAS",
            person_id=202693,
            player_name="Kyle Kuzma",
            description="SUB: Corey Kispert FOR Kyle Kuzma",
        ),
        _action(
            6,
            7,
            "made shot",
            "jump shot",
            team_id=HOME_ID,
            location="h",
            team_tricode="WAS",
            person_id=204456,
            player_name="Tyus Jones",
            description="Jones 3PT Jump Shot (3 PTS) (Gafford 1 AST)",
            clock="PT11M42.00S",
            score_home=3,
            score_away=0,
        ),
        _action(
            7,
            8,
            "missed shot",
            "layup shot",
            team_id=AWAY_ID,
            location="v",
            team_tricode="CLE",
            person_id=1628386,
            player_name="Jarrett Allen",
            description="MISS Allen 2' Layup",
            clock="PT11M18.00S",
            score_home=3,
            score_away=0,
        ),
        _action(
            7,
            9,
            "block",
            team_id=HOME_ID,
            location="h",
            team_tricode="WAS",
            person_id=1629655,
            player_name="Daniel Gafford",
            description="Gafford BLOCK (1 BLK)",
            clock="PT11M18.00S",
            score_home=3,
            score_away=0,
        ),
        _action(
            8,
            10,
            "rebound",
            team_id=AWAY_ID,
            location="v",
            team_tricode="CLE",
            person_id=1628386,
            player_name="Jarrett Allen",
            description="Allen REBOUND (Off:1 Def:0)",
            clock="PT11M16.00S",
            score_home=3,
            score_away=0,
        ),
        _action(
            9,
            11,
            "turnover",
            "bad pass",
            team_id=HOME_ID,
            location="h",
            team_tricode="WAS",
            person_id=204456,
            player_name="Tyus Jones",
            description="Jones Bad Pass Turnover (P1.T1)",
            clock="PT10M54.00S",
            score_home=3,
            score_away=0,
        ),
        _action(
            9,
            12,
            "steal",
            team_id=AWAY_ID,
            location="v",
            team_tricode="CLE",
            person_id=1628386,
            player_name="Jarrett Allen",
            description="Allen STEAL (1 STL)",
            clock="PT10M54.00S",
            score_home=3,
            score_away=0,
        ),
        _action(
            10,
            13,
            "foul",
            "personal",
            team_id=HOME_ID,
            location="h",
            team_tricode="WAS",
            person_id=1629655,
            player_name="Daniel Gafford",
            description="Gafford P.FOUL (P1.T1)",
            clock="PT10M30.00S",
            score_home=3,
            score_away=0,
        ),
        _action(
            10,
            14,
            "foul drawn",
            team_id=AWAY_ID,
            location="v",
            team_tricode="CLE",
            person_id=1628386,
            player_name="Jarrett Allen",
            description="Allen DRAWN FOUL",
            clock="PT10M30.00S",
            score_home=3,
            score_away=0,
        ),
        _action(
            11,
            15,
            "free throw",
            "free throw 1 of 2",
            team_id=AWAY_ID,
            location="v",
            team_tricode="CLE",
            person_id=1628386,
            player_name="Jarrett Allen",
            description="Allen Free Throw 1 of 2 (1 PTS)",
            clock="PT10M30.00S",
            score_home=3,
            score_away=1,
        ),
        _action(
            12,
            16,
            "period",
            "end",
            description="Period End",
            clock="PT0M00.00S",
            score_home=3,
            score_away=3,
        ),
    ]
    return {"game": {"actions": actions}}


def _response_rows(response):
    headers = response["resultSets"][0]["headers"]
    return [dict(zip(headers, row)) for row in response["resultSets"][0]["rowSet"]]


def _single_action_row(action_type, sub_type, description=None):
    source_data = {
        "game": {
            "actions": [
                _action(
                    1,
                    1,
                    action_type,
                    sub_type,
                    team_id=HOME_ID,
                    location="h",
                    team_tricode="WAS",
                    person_id=204456,
                    player_name="Tyus Jones",
                    description=description or sub_type,
                )
            ]
        }
    }
    return _response_rows(build_synthetic_v2_pbp_response(GAME_ID, source_data))[0]


def _enhanced_events(response):
    factory = StatsNbaEnhancedPbpFactory()
    events = [
        factory.get_event_class(row["EVENTMSGTYPE"])(row, i)
        for i, row in enumerate(_response_rows(response))
    ]
    for i, event in enumerate(events):
        event.previous_event = events[i - 1] if i else None
        event.next_event = events[i + 1] if i + 1 < len(events) else None
    return events


def test_synthetic_v3_output_has_exact_v2_contract_and_groups_actions():
    response = build_synthetic_v2_pbp_response(GAME_ID, _sample_v3_source_data())

    headers = response["resultSets"][0]["headers"]
    rows = response["resultSets"][0]["rowSet"]

    assert headers == V2_HEADERS
    assert all(len(row) == len(V2_HEADERS) for row in rows)
    assert [row[V2_HEADERS.index("EVENTNUM")] for row in rows] == list(range(1, 13))
    assert len(rows) == 12


def test_synthetic_v3_description_columns_and_score_orientation():
    rows = _response_rows(
        build_synthetic_v2_pbp_response(GAME_ID, _sample_v3_source_data())
    )

    made_shot = rows[5]
    timeout = rows[2]
    replay = rows[3]
    period_end = rows[-1]

    assert made_shot["HOMEDESCRIPTION"] == "Jones 3PT Jump Shot (3 PTS) (Gafford 1 AST)"
    assert made_shot["VISITORDESCRIPTION"] is None
    assert made_shot["SCORE"] == "0 - 3"
    assert made_shot["SCOREMARGIN"] == "3"
    assert timeout["VISITORDESCRIPTION"] == "Cavaliers Timeout: Regular"
    assert replay["NEUTRALDESCRIPTION"] == "Instant Replay: Support Ruling"
    assert period_end["SCORE"] == "3 - 3"
    assert period_end["SCOREMARGIN"] == "TIE"


def test_synthetic_v3_role_confidence_downstream_events():
    events = _enhanced_events(
        build_synthetic_v2_pbp_response(GAME_ID, _sample_v3_source_data())
    )

    jump_ball = events[1]
    substitution = events[4]
    made_shot = events[5]
    missed_shot = events[6]
    rebound = events[7]
    turnover = events[8]
    foul = events[9]
    free_throw = events[10]

    assert jump_ball.team_id == HOME_ID
    assert substitution.outgoing_player_id == 202693
    assert substitution.incoming_player_id == 203484
    assert made_shot.shot_value == 3
    assert made_shot.player2_id == 1629655
    assert missed_shot.player3_id == 1629655
    assert rebound.oreb
    assert turnover.player3_id == 1628386
    assert foul.player3_id == 1628386
    assert free_throw.is_made


@pytest.mark.parametrize(
    "action_type,sub_type,expected_event_type,expected_action_type",
    [
        ("turnover", "Shot Clock Violation", 5, 11),
        ("turnover", "Kicked Ball Violation", 5, 19),
        ("turnover", "Bad Pass Out-of-Bounds", 5, 45),
        ("turnover", "Lost Ball Out-of-Bounds", 5, 40),
        ("turnover", "5 Second Violation", 5, 9),
        ("turnover", "8 Second Violation", 5, 10),
        ("turnover", "Backcourt Violation", 5, 13),
        ("violation", "Kicked Ball Violation", 7, 5),
        ("violation", "Lane Violation", 7, 3),
        ("violation", "Defensive Goaltending Violation", 7, 2),
        ("foul", "Clear Path Foul", 6, 9),
        ("foul", "Flagrant Type 1 Foul", 6, 14),
        ("foul", "Flagrant Type 2 Foul", 6, 15),
        ("foul", "Non-Unsportsmanlike Technical", 6, 12),
        ("foul", "Defensive 3 Seconds Technical", 6, 17),
        ("foul", "Delay Of Game Technical", 6, 18),
        ("free throw", "Free Throw Clear-Path 1 of 2", 3, 11),
        ("free throw", "Flagrant Free Throw 2 of 2", 3, 12),
    ],
)
def test_synthetic_v3_maps_actual_subtype_aliases(
    action_type, sub_type, expected_event_type, expected_action_type
):
    row = _single_action_row(action_type, sub_type)

    assert row["EVENTMSGTYPE"] == expected_event_type
    assert row["EVENTMSGACTIONTYPE"] == expected_action_type


def test_enhanced_repair_does_not_write_synthetic_v3_to_canonical_pbp(tmp_path):
    (tmp_path / "pbp").mkdir()
    loader = StatsNbaEnhancedPbpLoader.__new__(StatsNbaEnhancedPbpLoader)
    loader.file_directory = str(tmp_path)
    loader.game_id = GAME_ID
    loader.source_data = {"resultSets": [{"rowSet": [["synthetic"]]}]}
    loader.loaded_endpoint_strategy = ENDPOINT_STRATEGY_V3_SYNTHETIC

    loader._save_data_to_file()

    assert not (tmp_path / "pbp" / f"stats_{GAME_ID}.json").exists()


def test_synthetic_v3_possession_start_uses_stats_start_period_inference():
    source_data = {
        "game": {
            "actions": [
                _action(1, 1, "period", "start", description="Period Start"),
                _action(
                    2,
                    2,
                    "timeout",
                    "regular",
                    team_id=AWAY_ID,
                    location="v",
                    team_tricode="CLE",
                    person_id=AWAY_ID,
                    description="Cavaliers Timeout: Regular",
                ),
                _action(
                    3,
                    3,
                    "instant replay",
                    "support ruling",
                    description="Instant Replay: Support Ruling",
                ),
                _action(
                    4,
                    4,
                    "substitution",
                    team_id=HOME_ID,
                    location="h",
                    team_tricode="WAS",
                    person_id=202693,
                    player_name="Kyle Kuzma",
                    description="SUB: Corey Kispert FOR Kyle Kuzma",
                ),
                _action(
                    4,
                    5,
                    "substitution",
                    team_id=HOME_ID,
                    location="h",
                    team_tricode="WAS",
                    person_id=203484,
                    player_name="Corey Kispert",
                    description="SUB: Corey Kispert FOR Kyle Kuzma",
                ),
                _action(
                    5,
                    6,
                    "made shot",
                    "jump shot",
                    team_id=HOME_ID,
                    location="h",
                    team_tricode="WAS",
                    person_id=204456,
                    player_name="Tyus Jones",
                    description="Jones Jump Shot (2 PTS)",
                    clock="PT11M45.00S",
                    score_home=2,
                    score_away=0,
                ),
            ]
        }
    }
    events = _enhanced_events(build_synthetic_v2_pbp_response(GAME_ID, source_data))
    start_event = events[0]
    start_event.team_starting_with_ball = start_event.get_team_starting_with_ball()

    possession = Possession(events)

    assert possession._get_head_event_for_offense() is start_event
    assert possession.offense_team_id == HOME_ID


def test_synthetic_v3_substitution_uses_outgoing_player_when_incoming_row_sorts_first():
    source_data = {
        "game": {
            "actions": [
                _action(
                    1,
                    1,
                    "substitution",
                    team_id=HOME_ID,
                    location="h",
                    team_tricode="WAS",
                    person_id=203484,
                    player_name="Corey Kispert",
                    description="SUB: Corey Kispert FOR Kyle Kuzma",
                ),
                _action(
                    1,
                    2,
                    "substitution",
                    team_id=HOME_ID,
                    location="h",
                    team_tricode="WAS",
                    person_id=202693,
                    player_name="Kyle Kuzma",
                    description="SUB: Corey Kispert FOR Kyle Kuzma",
                ),
            ]
        }
    }

    event = _enhanced_events(build_synthetic_v2_pbp_response(GAME_ID, source_data))[0]

    assert event.outgoing_player_id == 202693
    assert event.incoming_player_id == 203484


def test_synthetic_v3_clock_conversion_keeps_fractional_seconds():
    source_data = {
        "game": {
            "actions": [
                _action(
                    1,
                    1,
                    "made shot",
                    "jump shot",
                    team_id=HOME_ID,
                    location="h",
                    team_tricode="WAS",
                    person_id=204456,
                    player_name="Tyus Jones",
                    description="Jones Jump Shot (2 PTS)",
                    clock="PT0M00.50S",
                    score_home=2,
                    score_away=0,
                )
            ]
        }
    }

    row = _response_rows(build_synthetic_v2_pbp_response(GAME_ID, source_data))[0]

    assert row["PCTIMESTRING"] == "0:00.50"


def test_synthetic_v3_unknown_mapping_raises_structured_error():
    source_data = {"game": {"actions": [_action(1, 1, "unknown action")]}}

    with pytest.raises(StatsNbaV3SyntheticMappingError):
        build_synthetic_v2_pbp_response(GAME_ID, source_data)


def test_synthetic_v3_missing_action_numbers_raises_contract_error():
    action = _action(1, 1, "period", "start")
    action["actionNumber"] = None
    source_data = {"game": {"actions": [action]}}

    with pytest.raises(StatsNbaV2PbpResponseError):
        build_synthetic_v2_pbp_response(GAME_ID, source_data)


def test_synthetic_v3_missing_required_role_raises_structured_error():
    source_data = {
        "game": {
            "actions": [
                _action(
                    1,
                    1,
                    "substitution",
                    team_id=HOME_ID,
                    location="h",
                    team_tricode="WAS",
                    person_id=202693,
                    player_name="Kyle Kuzma",
                    description="SUB: Unknown Player FOR Kyle Kuzma",
                )
            ]
        }
    }

    with pytest.raises(StatsNbaV3SyntheticRoleError):
        build_synthetic_v2_pbp_response(GAME_ID, source_data)


def test_synthetic_v3_rejects_wnba_until_role_parity_is_validated():
    with pytest.raises(UnsupportedV3SyntheticSchemaError):
        build_synthetic_v2_pbp_response("1022400001", _sample_v3_source_data())


def test_synthetic_v3_rejects_g_league_until_role_parity_is_validated():
    with pytest.raises(UnsupportedV3SyntheticSchemaError):
        build_synthetic_v2_pbp_response("2022400001", _sample_v3_source_data())


def test_auto_strategy_does_not_fallback_when_valid_v2_cache_write_fails():
    class SaveFailingV2Loader(StatsNbaPbpWebLoader):
        def __init__(self):
            super().__init__(endpoint_strategy="auto")
            self.used_v3_fallback = False

        def _load_v2_data(self):
            return build_synthetic_v2_pbp_response(GAME_ID, _sample_v3_source_data())

        def _save_data_to_file(self):
            raise RuntimeError("cache write failed")

        def _load_v3_synthetic_data(self):
            self.used_v3_fallback = True
            return build_synthetic_v2_pbp_response(GAME_ID, _sample_v3_source_data())

    source_loader = SaveFailingV2Loader()

    with pytest.raises(RuntimeError, match="cache write failed"):
        source_loader.load_data(GAME_ID)
    assert not source_loader.used_v3_fallback
