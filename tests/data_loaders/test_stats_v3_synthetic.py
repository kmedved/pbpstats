import json
from collections import defaultdict
from pathlib import Path

import pytest

from pbpstats.data_loader.stats_nba.pbp.v3_synthetic import (
    ENDPOINT_STRATEGY_V3_SYNTHETIC,
    V2_HEADERS,
    StatsNbaV2PbpResponseError,
    StatsNbaV3SyntheticMappingError,
    StatsNbaV3SyntheticParityError,
    StatsNbaV3SyntheticRoleError,
    StatsNbaV3SyntheticRoleSupplementError,
    UnsupportedV3SyntheticSchemaError,
    build_synthetic_v2_pbp_response,
)
from pbpstats.data_loader.stats_nba.enhanced_pbp.loader import (
    StatsNbaEnhancedPbpLoader,
)
from pbpstats.data_loader.stats_nba.pbp import web as pbp_web_module
from pbpstats.data_loader.stats_nba.pbp.web import StatsNbaPbpWebLoader
from pbpstats.resources.enhanced_pbp.stats_nba.enhanced_pbp_factory import (
    StatsNbaEnhancedPbpFactory,
)
from pbpstats.resources.possessions.possession import Possession

GAME_ID = "0022400001"
HOME_ID = 1610612764
AWAY_ID = 1610612739
WNBA_2025_SYNTHETIC_GAME_IDS = [
    "1022500234",
    "1022500283",
    "1022500284",
    "1022500286",
    "1022500285",
    "1022500282",
]
WNBA_DATA_DIR = Path("tests/data")
WNBA_ROLE_COLUMNS = [
    f"{prefix}{slot}{suffix}"
    for slot in (1, 2, 3)
    for prefix, suffix in [
        ("PERSON", "TYPE"),
        ("PLAYER", "_ID"),
        ("PLAYER", "_NAME"),
        ("PLAYER", "_TEAM_ID"),
        ("PLAYER", "_TEAM_CITY"),
        ("PLAYER", "_TEAM_NICKNAME"),
        ("PLAYER", "_TEAM_ABBREVIATION"),
    ]
]


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


def _load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _wnba_v2_fixture(game_id):
    return _load_json(WNBA_DATA_DIR / "pbp" / f"stats_{game_id}.json")


def _wnba_v3_fixture(game_id):
    return _load_json(WNBA_DATA_DIR / "pbp_v3" / f"stats_pbpv3_{game_id}.json")


def _wnba_shot_fixtures(game_id):
    return [
        _load_json(WNBA_DATA_DIR / "game_details" / f"stats_home_shots_{game_id}.json"),
        _load_json(WNBA_DATA_DIR / "game_details" / f"stats_away_shots_{game_id}.json"),
    ]


def _v2_fixture_rows(source_data):
    headers = source_data["resultSets"][0]["headers"]
    return [dict(zip(headers, row)) for row in source_data["resultSets"][0]["rowSet"]]


def _v3_action_groups(source_data):
    groups = defaultdict(list)
    for action in source_data["game"]["actions"]:
        groups[action["actionNumber"]].append(action)
    return groups


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
    assert (tmp_path / "pbp_synthetic_v3" / f"stats_{GAME_ID}.json").exists()


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


def test_wnba_v3_synthetic_without_v2_role_supplement_rejects():
    game_id = WNBA_2025_SYNTHETIC_GAME_IDS[0]

    with pytest.raises(StatsNbaV3SyntheticRoleSupplementError):
        build_synthetic_v2_pbp_response(game_id, _wnba_v3_fixture(game_id))


@pytest.mark.parametrize("game_id", WNBA_2025_SYNTHETIC_GAME_IDS)
def test_wnba_v3_synthetic_with_v2_supplement_has_exact_v2_contract(game_id):
    response = build_synthetic_v2_pbp_response(
        game_id,
        _wnba_v3_fixture(game_id),
        v2_role_supplement=_wnba_v2_fixture(game_id),
    )
    v2_rows = _v2_fixture_rows(_wnba_v2_fixture(game_id))
    rows = _response_rows(response)

    assert response["resultSets"][0]["headers"] == V2_HEADERS
    assert all(
        len(row) == len(V2_HEADERS) for row in response["resultSets"][0]["rowSet"]
    )
    assert len(rows) == len(v2_rows)
    assert {row["EVENTNUM"] for row in rows} == {row["EVENTNUM"] for row in v2_rows}


@pytest.mark.parametrize("game_id", WNBA_2025_SYNTHETIC_GAME_IDS)
def test_wnba_v3_synthetic_role_columns_match_v2_supplement(game_id):
    response = build_synthetic_v2_pbp_response(
        game_id,
        _wnba_v3_fixture(game_id),
        v2_role_supplement=_wnba_v2_fixture(game_id),
    )
    synthetic_rows = {row["EVENTNUM"]: row for row in _response_rows(response)}
    v2_rows = {
        row["EVENTNUM"]: row for row in _v2_fixture_rows(_wnba_v2_fixture(game_id))
    }

    for event_num, synthetic_row in synthetic_rows.items():
        for column in WNBA_ROLE_COLUMNS:
            assert synthetic_row[column] == v2_rows[event_num][column]


def test_wnba_v3_synthetic_fills_drawn_foul_roles_from_v2_supplement():
    foul_drawn_count = 0
    for game_id in WNBA_2025_SYNTHETIC_GAME_IDS:
        response = build_synthetic_v2_pbp_response(
            game_id,
            _wnba_v3_fixture(game_id),
            v2_role_supplement=_wnba_v2_fixture(game_id),
        )
        v2_rows = {
            row["EVENTNUM"]: row for row in _v2_fixture_rows(_wnba_v2_fixture(game_id))
        }
        for row in _response_rows(response):
            if row["EVENTMSGTYPE"] == 6 and v2_rows[row["EVENTNUM"]]["PLAYER2_ID"]:
                foul_drawn_count += 1
                assert row["PLAYER2_ID"] == v2_rows[row["EVENTNUM"]]["PLAYER2_ID"]

    assert foul_drawn_count == 171


def test_wnba_v3_synthetic_fills_unparseable_jump_ball_roles_from_v2_supplement():
    jump_ball_count = 0
    blank_v3_jump_ball_count = 0
    for game_id in WNBA_2025_SYNTHETIC_GAME_IDS:
        response = build_synthetic_v2_pbp_response(
            game_id,
            _wnba_v3_fixture(game_id),
            v2_role_supplement=_wnba_v2_fixture(game_id),
        )
        groups = _v3_action_groups(_wnba_v3_fixture(game_id))
        for row in _response_rows(response):
            if row["EVENTMSGTYPE"] != 10:
                continue
            jump_ball_count += 1
            primary = groups[row["EVENTNUM"]][0]
            if "jump ball" not in (primary.get("description") or "").lower():
                blank_v3_jump_ball_count += 1
            assert row["PLAYER1_ID"]
            assert row["PLAYER2_ID"]
            assert row["PLAYER3_ID"]

    assert jump_ball_count == 14
    assert blank_v3_jump_ball_count == 2


def test_wnba_v3_duplicate_block_and_steal_side_actor_rows_collapse_to_v2_roles():
    game_id = "1022500234"
    response = build_synthetic_v2_pbp_response(
        game_id,
        _wnba_v3_fixture(game_id),
        v2_role_supplement=_wnba_v2_fixture(game_id),
    )
    rows = {row["EVENTNUM"]: row for row in _response_rows(response)}
    groups = _v3_action_groups(_wnba_v3_fixture(game_id))

    blocked_event_num = next(
        event_num
        for event_num, group in groups.items()
        if any("BLOCK" in (action.get("description") or "") for action in group)
    )
    steal_event_num = next(
        event_num
        for event_num, group in groups.items()
        if any("STEAL" in (action.get("description") or "") for action in group)
    )

    assert len(groups[blocked_event_num]) > 1
    assert rows[blocked_event_num]["EVENTMSGTYPE"] == 2
    assert rows[blocked_event_num]["PLAYER3_ID"]
    assert len(groups[steal_event_num]) > 1
    assert rows[steal_event_num]["EVENTMSGTYPE"] == 5
    assert rows[steal_event_num]["PLAYER2_ID"]

    events = _enhanced_events(response)
    blocked_event = next(
        event for event in events if event.event_num == blocked_event_num
    )
    steal_event = next(event for event in events if event.event_num == steal_event_num)
    assert blocked_event.player3_id == rows[blocked_event_num]["PLAYER3_ID"]
    assert steal_event.player3_id == rows[steal_event_num]["PLAYER2_ID"]


def test_wnba_extra_subtype_mappings_match_v2_fixture_codes():
    expected_codes = {
        ("made shot", "alley oop layup shot"): 43,
        ("made shot", "driving finger roll layup shot"): 75,
        ("made shot", "driving hook shot"): 57,
        ("made shot", "finger roll layup shot"): 71,
        ("made shot", "jump bank shot"): 66,
        ("made shot", "putback layup shot"): 72,
        ("made shot", "running finger roll layup shot"): 76,
        ("made shot", "running pull up jump shot"): 103,
        ("made shot", "running reverse layup shot"): 74,
        ("made shot", "turnaround bank hook shot"): 96,
        ("missed shot", "cutting finger roll layup shot"): 99,
        ("missed shot", "driving bank hook shot"): 93,
        ("missed shot", "driving finger roll layup shot"): 75,
        ("missed shot", "jump bank shot"): 66,
        ("missed shot", "putback layup shot"): 72,
        ("missed shot", "running pull up jump shot"): 103,
        ("missed shot", "running reverse layup shot"): 74,
        ("missed shot", "turnaround bank hook shot"): 96,
        ("timeout", "coach challenge"): 7,
        ("turnover", "offensive foul turnover"): 37,
        ("turnover", "out of bounds bad pass turnover"): 45,
        ("turnover", "out of bounds lost ball turnover"): 40,
        ("turnover", "shot clock turnover"): 11,
    }
    seen_codes = {}
    for game_id in WNBA_2025_SYNTHETIC_GAME_IDS:
        response = build_synthetic_v2_pbp_response(
            game_id,
            _wnba_v3_fixture(game_id),
            v2_role_supplement=_wnba_v2_fixture(game_id),
        )
        rows = {row["EVENTNUM"]: row for row in _response_rows(response)}
        for action in _wnba_v3_fixture(game_id)["game"]["actions"]:
            action_type = (action.get("actionType") or "").strip().lower()
            subtype = (
                (action.get("subType") or "")
                .strip()
                .lower()
                .replace("-", " ")
                .replace("_", " ")
            )
            subtype = " ".join(subtype.split())
            key = (action_type, subtype)
            if key in expected_codes and key not in seen_codes:
                seen_codes[key] = rows[action["actionNumber"]]["EVENTMSGACTIONTYPE"]

    assert seen_codes == expected_codes


def test_wnba_v3_synthetic_rejects_when_v3_group_missing_v2_eventnum():
    game_id = WNBA_2025_SYNTHETIC_GAME_IDS[0]
    v3_source_data = _wnba_v3_fixture(game_id)
    v3_source_data["game"]["actions"] = v3_source_data["game"]["actions"][1:]

    with pytest.raises(StatsNbaV3SyntheticParityError):
        build_synthetic_v2_pbp_response(
            game_id,
            v3_source_data,
            v2_role_supplement=_wnba_v2_fixture(game_id),
        )


def test_wnba_shotchart_validation_does_not_fill_non_shot_roles():
    game_id = WNBA_2025_SYNTHETIC_GAME_IDS[0]

    with pytest.raises(StatsNbaV3SyntheticRoleSupplementError):
        build_synthetic_v2_pbp_response(
            game_id,
            _wnba_v3_fixture(game_id),
            shotchartdetail=_wnba_shot_fixtures(game_id),
        )


def test_nba_v3_synthetic_does_not_require_v2_role_supplement():
    response = build_synthetic_v2_pbp_response(GAME_ID, _sample_v3_source_data())

    assert len(response["resultSets"][0]["rowSet"]) == 12


def test_wnba_auto_strategy_prefers_valid_v2_and_does_not_fetch_v3():
    game_id = WNBA_2025_SYNTHETIC_GAME_IDS[0]

    class WnbaAutoLoader(StatsNbaPbpWebLoader):
        def __init__(self):
            super().__init__(endpoint_strategy="auto")
            self.used_v3_fallback = False

        def _load_v2_data(self):
            return _wnba_v2_fixture(game_id)

        def _load_v3_synthetic_data(self):
            self.used_v3_fallback = True
            return super()._load_v3_synthetic_data()

    source_loader = WnbaAutoLoader()

    source_data = source_loader.load_data(game_id)

    assert source_data == _wnba_v2_fixture(game_id)
    assert not source_loader.used_v3_fallback


def test_wnba_auto_strategy_rejects_when_v2_missing_and_no_role_supplement(
    monkeypatch,
):
    game_id = WNBA_2025_SYNTHETIC_GAME_IDS[0]

    class FakeV3WebLoader:
        def __init__(self, file_directory=None):
            self.file_directory = file_directory

        def load_data(self, game_id):
            return _wnba_v3_fixture(game_id)

    class WnbaAutoLoader(StatsNbaPbpWebLoader):
        def _load_v2_data(self):
            return {}

    monkeypatch.setattr(pbp_web_module, "StatsNbaPbpV3WebLoader", FakeV3WebLoader)
    source_loader = WnbaAutoLoader(endpoint_strategy="auto")

    with pytest.raises(StatsNbaV3SyntheticRoleSupplementError):
        source_loader.load_data(game_id)


def test_wnba_v3_synthetic_web_caches_true_v2_raw_v3_and_synthetic_separately(
    tmp_path, monkeypatch
):
    game_id = WNBA_2025_SYNTHETIC_GAME_IDS[0]

    class FakeV3WebLoader:
        def __init__(self, file_directory=None):
            self.file_directory = file_directory

        def load_data(self, game_id):
            source_data = _wnba_v3_fixture(game_id)
            if self.file_directory is not None:
                raw_dir = Path(self.file_directory, "pbp_v3")
                raw_dir.mkdir(parents=True, exist_ok=True)
                with open(
                    raw_dir / f"stats_pbpv3_{game_id}.json",
                    "w",
                    encoding="utf-8",
                ) as f:
                    json.dump(source_data, f)
            return source_data

    class WnbaSyntheticLoader(StatsNbaPbpWebLoader):
        def _load_v2_data(self):
            return _wnba_v2_fixture(game_id)

    monkeypatch.setattr(pbp_web_module, "StatsNbaPbpV3WebLoader", FakeV3WebLoader)
    source_loader = WnbaSyntheticLoader(str(tmp_path), endpoint_strategy="v3_synthetic")

    source_loader.load_data(game_id)

    assert Path(tmp_path, "pbp", f"stats_{game_id}.json").exists()
    assert Path(tmp_path, "pbp_v3", f"stats_pbpv3_{game_id}.json").exists()
    assert Path(tmp_path, "pbp_synthetic_v3", f"stats_{game_id}.json").exists()


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
