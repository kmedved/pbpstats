import json
from pathlib import Path
import sqlite3

import pandas as pd
import pytest

from historic_backfill.catalogs.boxscore_source_overrides import BOXSCORE_SOURCE_COLUMNS
from historic_backfill.common.period_boxscore_source_loader import (
    STARTER_LOOKUP_COLUMNS,
)
from historic_backfill.runners.validate import validate_scope


def _playbyplay_rows(
    game_ids: list[str] | None = None,
    seasons: list[int] | None = None,
) -> pd.DataFrame:
    game_ids = game_ids or ["0029700001"]
    seasons = seasons or [1998] * len(game_ids)
    return pd.DataFrame(
        {
            "GAME_ID": game_ids,
            "EVENTNUM": list(range(1, len(game_ids) + 1)),
            "EVENTMSGTYPE": [12] * len(game_ids),
            "EVENTMSGACTIONTYPE": [0] * len(game_ids),
            "PERIOD": [1] * len(game_ids),
            "SEASON": seasons,
            "PCTIMESTRING": ["12:00"] * len(game_ids),
            "HOMEDESCRIPTION": ["Start Period"] * len(game_ids),
            "VISITORDESCRIPTION": [""] * len(game_ids),
            "PLAYER1_ID": [0] * len(game_ids),
            "PLAYER2_ID": [0] * len(game_ids),
            "PLAYER3_ID": [0] * len(game_ids),
            "PLAYER1_TEAM_ID": [0] * len(game_ids),
            "PLAYER2_TEAM_ID": [0] * len(game_ids),
            "PLAYER3_TEAM_ID": [0] * len(game_ids),
        }
    )


def _boxscore_row(
    game_id: str,
    team_id: int,
    player_id: int,
    player_name: str,
    start_position: str,
) -> list[object]:
    values: dict[str, object] = {
        "GAME_ID": game_id,
        "TEAM_ID": team_id,
        "TEAM_ABBREVIATION": "HOM" if team_id == 1610612740 else "AWY",
        "TEAM_CITY": "Home" if team_id == 1610612740 else "Away",
        "PLAYER_ID": player_id,
        "PLAYER_NAME": player_name,
        "NICKNAME": player_name.split()[0],
        "START_POSITION": start_position,
        "COMMENT": "",
        "MIN": "12:00",
        "FGM": 0,
        "FGA": 0,
        "FG_PCT": 0.0,
        "FG3M": 0,
        "FG3A": 0,
        "FG3_PCT": 0.0,
        "FTM": 0,
        "FTA": 0,
        "FT_PCT": 0.0,
        "OREB": 0,
        "DREB": 0,
        "REB": 0,
        "AST": 0,
        "STL": 0,
        "BLK": 0,
        "TO": 0,
        "PF": 0,
        "PTS": 0,
        "PLUS_MINUS": 0,
    }
    return [values[column] for column in BOXSCORE_SOURCE_COLUMNS]


def _payload_for_endpoint(endpoint: str) -> bytes:
    if endpoint == "boxscore":
        payload = {
            "resultSets": [
                {
                    "headers": BOXSCORE_SOURCE_COLUMNS,
                    "rowSet": [
                        _boxscore_row(
                            "0029700001",
                            1610612740,
                            123,
                            "Home Player",
                            "G",
                        ),
                        _boxscore_row(
                            "0029700001",
                            1610612741,
                            456,
                            "Away Player",
                            "F",
                        ),
                    ],
                }
            ]
        }
        return json.dumps(payload).encode("utf-8")
    if endpoint == "summary":
        return (
            b'{"resultSets":[{"headers":["GAME_ID","HOME_TEAM_ID","VISITOR_TEAM_ID"],'
            b'"rowSet":[["0029700001",1610612740,1610612741]]}]}'
        )
    if endpoint == "pbpv3":
        return b'{"game":{"actions":[]}}'
    return b"{}"


def _boxscore_payload(rows: list[list[object]], headers: list[str] | None = None) -> bytes:
    return json.dumps(
        {
            "resultSets": [
                {
                    "headers": headers or BOXSCORE_SOURCE_COLUMNS,
                    "rowSet": rows,
                }
            ]
        }
    ).encode("utf-8")


def _write_valid_nba_db(
    root: Path,
    endpoints: tuple[str, ...] = ("boxscore", "summary", "pbpv3"),
    game_ids: tuple[str, ...] = ("0029700001",),
) -> None:
    path = root / "data" / "nba_raw.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
        )
        for game_id in game_ids:
            for endpoint in endpoints:
                conn.execute(
                    "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, NULL, ?)",
                    (game_id, endpoint, _payload_for_endpoint(endpoint)),
                )
        conn.commit()
    finally:
        conn.close()


def _write_period_starters_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {column: [1] for column in STARTER_LOOKUP_COLUMNS}
    row["game_id"] = ["0029700001"]
    row["period"] = [1]
    row["resolved"] = [True]
    row["away_team_id"] = [1610612740]
    row["home_team_id"] = [1610612741]
    for index in range(1, 6):
        row[f"away_player{index}"] = [100 + index]
        row[f"home_player{index}"] = [200 + index]
    pd.DataFrame(row).to_parquet(path)


def _write_core_inputs(root: Path) -> None:
    _write_valid_nba_db(root)
    parquet_path = root / "data" / "playbyplayv2.parq"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    _playbyplay_rows().to_parquet(parquet_path)
    for rel_path in (
        "data/period_starters_v6.parquet",
        "data/period_starters_v5.parquet",
    ):
        _write_period_starters_parquet(root / rel_path)


def _write_core_catalogs(root: Path) -> None:
    catalogs = root / "catalogs"
    (catalogs / "overrides").mkdir(parents=True, exist_ok=True)
    (catalogs / "pbp_row_overrides.csv").write_text(
        "game_id,action,event_num,anchor_event_num,notes,period,pctimestring,wctimestring,description_side,"
        "player_out_id,player_out_name,player_out_team_id,player_in_id,player_in_name,player_in_team_id\n"
        "0020400335,insert_sub_before,148,149,canary,2,7:59,,home,"
        "2747,JR Smith,1610612740,2454,Junior Harrington,1610612740\n",
        encoding="utf-8",
    )
    (catalogs / "pbp_stat_overrides.csv").write_text(
        "game_id,team_id,player_id,stat_key,stat_value,notes\n",
        encoding="utf-8",
    )
    (catalogs / "validation_overrides.csv").write_text(
        "game_id,action,tolerance,notes\n",
        encoding="utf-8",
    )
    (catalogs / "boxscore_source_overrides.csv").write_text(
        "game_id,GAME_ID,TEAM_ID,TEAM_ABBREVIATION,TEAM_CITY,PLAYER_ID,PLAYER_NAME,"
        "NICKNAME,START_POSITION,COMMENT,MIN,FGM,FGA,FG_PCT,FG3M,FG3A,FG3_PCT,"
        "FTM,FTA,FT_PCT,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS,PLUS_MINUS,notes\n",
        encoding="utf-8",
    )
    (catalogs / "overrides" / "correction_manifest.json").write_text(
        '{"manifest_version": "test", "corrections": [], "residual_annotations": []}\n',
        encoding="utf-8",
    )
    (catalogs / "overrides" / "period_starters_overrides.json").write_text(
        "{}\n",
        encoding="utf-8",
    )
    (catalogs / "overrides" / "lineup_window_overrides.json").write_text(
        "{}\n",
        encoding="utf-8",
    )


def test_core_validation_requires_nba_inputs_without_checking_cross_source(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)

    result = validate_scope("core", root=tmp_path)

    assert result.ok is True
    assert result.validation_level == "input_preflight"
    assert result.missing_required == []
    assert result.skipped_optional == []
    assert result.validation_errors == []


def test_core_validation_reports_invalid_catalogs(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "catalogs" / "pbp_row_overrides.csv").write_text(
        "game_id,action,event_num,anchor_event_num,notes\n"
        "0020400335,teleport,148,149,bad action\n",
        encoding="utf-8",
    )

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert result.validation_errors


def test_core_validation_reports_invalid_catalogs_even_when_nba_inputs_are_missing(
    tmp_path,
):
    _write_core_catalogs(tmp_path)
    (tmp_path / "catalogs" / "validation_overrides.csv").write_text(
        "game_id\n",
        encoding="utf-8",
    )

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert "data/nba_raw.db" in result.missing_required
    assert result.validation_errors
    assert any(
        "validation_overrides.csv" in error for error in result.validation_errors
    )


def test_core_validation_reports_invalid_nba_raw_db_contents(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "data" / "nba_raw.db").unlink()
    _write_valid_nba_db(tmp_path, endpoints=("boxscore", "summary"))

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("pbpv3" in error for error in result.validation_errors)


def test_core_validation_requires_runtime_endpoint_rows_to_have_null_team_id(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "data" / "nba_raw.db").unlink()
    path = tmp_path / "data" / "nba_raw.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
        )
        for endpoint in ("boxscore", "summary", "pbpv3"):
            conn.execute(
                "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, ?, ?)",
                ("0029700001", endpoint, 1610612740, _payload_for_endpoint(endpoint)),
            )
        conn.commit()
    finally:
        conn.close()

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("team_id IS NULL" in error for error in result.validation_errors)


def test_core_validation_rejects_noncanonical_raw_response_game_ids(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "data" / "nba_raw.db").unlink()
    path = tmp_path / "data" / "nba_raw.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
        )
        for endpoint in ("boxscore", "summary", "pbpv3"):
            conn.execute(
                "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, NULL, ?)",
                ("29700001", endpoint, _payload_for_endpoint(endpoint)),
            )
        conn.commit()
    finally:
        conn.close()

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("non-canonical" in error for error in result.validation_errors)


def test_core_validation_rejects_whitespace_raw_response_game_ids(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "data" / "nba_raw.db").unlink()
    path = tmp_path / "data" / "nba_raw.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
        )
        for endpoint in ("boxscore", "summary", "pbpv3"):
            conn.execute(
                "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, NULL, ?)",
                ("0029700001 ", endpoint, _payload_for_endpoint(endpoint)),
            )
        conn.commit()
    finally:
        conn.close()

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("surrounding whitespace" in error for error in result.validation_errors)


def test_core_validation_reports_missing_raw_response_game_coverage(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    _playbyplay_rows(
        game_ids=["0029700001", "0029700002"],
        seasons=[1998, 1998],
    ).to_parquet(tmp_path / "data" / "playbyplayv2.parq")

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any(
        "missing boxscore responses" in error for error in result.validation_errors
    )


def test_core_validation_reports_corrupt_raw_response_blobs_after_first_game(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    _playbyplay_rows(
        game_ids=["0029700001", "0029700002"],
        seasons=[1998, 1998],
    ).to_parquet(tmp_path / "data" / "playbyplayv2.parq")
    (tmp_path / "data" / "nba_raw.db").unlink()
    path = tmp_path / "data" / "nba_raw.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
        )
        for game_id in ("0029700001", "0029700002"):
            for endpoint in ("boxscore", "summary", "pbpv3"):
                blob = (
                    b"{not json"
                    if (game_id, endpoint) == ("0029700002", "pbpv3")
                    else _payload_for_endpoint(endpoint)
                )
                conn.execute(
                    "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, NULL, ?)",
                    (game_id, endpoint, blob),
                )
        conn.commit()
    finally:
        conn.close()

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("invalid pbpv3 blobs" in error for error in result.validation_errors)


def test_core_validation_reports_duplicate_raw_response_rows(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    path = tmp_path / "data" / "nba_raw.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, NULL, ?)",
            ("0029700001", "pbpv3", _payload_for_endpoint("pbpv3")),
        )
        conn.commit()
    finally:
        conn.close()

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("duplicate team_id IS NULL pbpv3" in error for error in result.validation_errors)


def test_core_validation_rejects_shape_invalid_raw_response_payloads(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "data" / "nba_raw.db").unlink()
    path = tmp_path / "data" / "nba_raw.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
        )
        for endpoint in ("boxscore", "summary", "pbpv3"):
            blob = b"{}" if endpoint == "boxscore" else _payload_for_endpoint(endpoint)
            conn.execute(
                "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, NULL, ?)",
                ("0029700001", endpoint, blob),
            )
        conn.commit()
    finally:
        conn.close()

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any(
        "boxscore" in error and "resultSets" in error
        for error in result.validation_errors
    )


@pytest.mark.parametrize(
    ("boxscore_payload", "expected"),
    [
        (
            b'{"resultSets":[{"headers":["TEAM_ID","PLAYER_ID","PTS"],"rowSet":[]}]}',
            "rowSet is empty",
        ),
        (
            _boxscore_payload([[1610612740, 123]]),
            "does not match headers length",
        ),
        (
            _boxscore_payload(
                [
                    _boxscore_row("0029700001", 1610612740, 0, "No Player", ""),
                    _boxscore_row("0029700001", 1610612741, 0, "No Player 2", ""),
                ]
            ),
            "no positive PLAYER_ID rows",
        ),
        (
            _boxscore_payload(
                [[1610612740, 123, 0], [1610612741, 456, 0]],
                headers=["TEAM_ID", "PLAYER_ID", "PTS"],
            ),
            "missing required headers",
        ),
        (
            _boxscore_payload(
                [
                    _boxscore_row(
                        "0029700001",
                        1610612740,
                        123,
                        "Home Player",
                        "G",
                    )
                ]
            ),
            "fewer than two positive TEAM_ID values",
        ),
    ],
)
def test_core_validation_rejects_unusable_boxscore_payload_shapes(
    tmp_path,
    boxscore_payload,
    expected,
):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "data" / "nba_raw.db").unlink()
    path = tmp_path / "data" / "nba_raw.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
        )
        for endpoint in ("boxscore", "summary", "pbpv3"):
            blob = (
                boxscore_payload
                if endpoint == "boxscore"
                else _payload_for_endpoint(endpoint)
            )
            conn.execute(
                "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, NULL, ?)",
                ("0029700001", endpoint, blob),
            )
        conn.commit()
    finally:
        conn.close()

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any(expected in error for error in result.validation_errors)


@pytest.mark.parametrize(
    ("summary_payload", "expected"),
    [
        (b'{"resultSets":[1]}', "first resultSet is invalid"),
        (b'{"resultSets":[{"headers":["GAME_ID"]}]}', "rowSet must be a list"),
        (
            b'{"resultSets":[{"headers":["GAME_ID"],"rowSet":[["0029700001"]]}]}',
            "missing required headers",
        ),
        (
            b'{"resultSets":[{"headers":["GAME_ID","HOME_TEAM_ID","VISITOR_TEAM_ID"],'
            b'"rowSet":[["0029700001",1610612740,1610612740]]}]}',
            "HOME_TEAM_ID and VISITOR_TEAM_ID must differ",
        ),
    ],
)
def test_core_validation_rejects_unusable_summary_payload_shapes(
    tmp_path,
    summary_payload,
    expected,
):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "data" / "nba_raw.db").unlink()
    path = tmp_path / "data" / "nba_raw.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
        )
        for endpoint in ("boxscore", "summary", "pbpv3"):
            blob = (
                summary_payload
                if endpoint == "summary"
                else _payload_for_endpoint(endpoint)
            )
            conn.execute(
                "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, NULL, ?)",
                ("0029700001", endpoint, blob),
            )
        conn.commit()
    finally:
        conn.close()

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any(expected in error for error in result.validation_errors)


def test_core_validation_rejects_summary_teams_missing_from_boxscore(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "data" / "nba_raw.db").unlink()
    path = tmp_path / "data" / "nba_raw.db"
    summary_payload = (
        b'{"resultSets":[{"headers":["GAME_ID","HOME_TEAM_ID","VISITOR_TEAM_ID"],'
        b'"rowSet":[["0029700001",1610612740,1610612999]]}]}'
    )
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
        )
        for endpoint in ("boxscore", "summary", "pbpv3"):
            blob = (
                summary_payload
                if endpoint == "summary"
                else _payload_for_endpoint(endpoint)
            )
            conn.execute(
                "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, NULL, ?)",
                ("0029700001", endpoint, blob),
            )
        conn.commit()
    finally:
        conn.close()

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("summary team ids do not match" in error for error in result.validation_errors)


@pytest.mark.parametrize("missing_header", ["FGM", "FGA", "AST", "MIN", "PLUS_MINUS"])
def test_core_validation_rejects_boxscores_missing_runtime_headers(
    tmp_path,
    missing_header,
):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    headers = [header for header in BOXSCORE_SOURCE_COLUMNS if header != missing_header]
    rows = [
        [
            value
            for header, value in zip(
                BOXSCORE_SOURCE_COLUMNS,
                _boxscore_row("0029700001", 1610612740, 123, "Home Player", "G"),
            )
            if header != missing_header
        ],
        [
            value
            for header, value in zip(
                BOXSCORE_SOURCE_COLUMNS,
                _boxscore_row("0029700001", 1610612741, 456, "Away Player", "F"),
            )
            if header != missing_header
        ],
    ]
    boxscore_payload = _boxscore_payload(rows, headers=headers)
    (tmp_path / "data" / "nba_raw.db").unlink()
    path = tmp_path / "data" / "nba_raw.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
        )
        for endpoint in ("boxscore", "summary", "pbpv3"):
            blob = (
                boxscore_payload
                if endpoint == "boxscore"
                else _payload_for_endpoint(endpoint)
            )
            conn.execute(
                "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, NULL, ?)",
                ("0029700001", endpoint, blob),
            )
        conn.commit()
    finally:
        conn.close()

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any(missing_header in error for error in result.validation_errors)


def test_core_validation_reports_invalid_playbyplay_v2_parquet(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "data" / "playbyplayv2.parq").write_text(
        "not parquet\n",
        encoding="utf-8",
    )

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("playbyplayv2.parq" in error for error in result.validation_errors)


def test_core_validation_reports_missing_playbyplay_v2_columns(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    pd.DataFrame(
        {
            "game_id": ["0029700001"],
            "eventnum": [1],
            "eventmsgtype": [12],
            "period": [1],
        }
    ).to_parquet(tmp_path / "data" / "playbyplayv2.parq")

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("SEASON" in error for error in result.validation_errors)


def test_core_validation_rejects_minimal_legacy_playbyplay_v2_schema(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    pd.DataFrame(
        {
            "GAME_ID": ["0029700001"],
            "EVENTNUM": [1],
            "EVENTMSGTYPE": [12],
            "PERIOD": [1],
            "SEASON": [1997],
        }
    ).to_parquet(tmp_path / "data" / "playbyplayv2.parq")

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("PCTIMESTRING" in error for error in result.validation_errors)


def test_core_validation_rejects_string_playbyplay_v2_seasons(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    rows = _playbyplay_rows()
    rows["SEASON"] = ["1997"]
    rows.to_parquet(tmp_path / "data" / "playbyplayv2.parq")

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("string SEASON" in error for error in result.validation_errors)


def test_core_validation_rejects_playbyplay_v2_season_game_id_mismatch(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    rows = _playbyplay_rows(seasons=[1997])
    rows.to_parquet(tmp_path / "data" / "playbyplayv2.parq")

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("GAME_ID-derived season" in error for error in result.validation_errors)


def test_core_validation_reports_empty_playbyplay_v2_parquet(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    pd.DataFrame(columns=_playbyplay_rows().columns).to_parquet(
        tmp_path / "data" / "playbyplayv2.parq"
    )

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("contains no playbyplayv2 rows" in error for error in result.validation_errors)


def test_core_validation_reports_invalid_period_starters_parquet(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "data" / "period_starters_v6.parquet").write_text(
        "not parquet\n",
        encoding="utf-8",
    )

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any(
        "period_starters_v6.parquet" in error for error in result.validation_errors
    )


def test_core_validation_reports_missing_period_starter_columns(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    pd.DataFrame(
        {
            "game_id": ["0029700001"],
            "period": [1],
            "resolved": [True],
        }
    ).to_parquet(tmp_path / "data" / "period_starters_v6.parquet")

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("away_player1" in error for error in result.validation_errors)


def test_core_validation_reports_empty_period_starters_parquet(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    pd.DataFrame(columns=STARTER_LOOKUP_COLUMNS).to_parquet(
        tmp_path / "data" / "period_starters_v6.parquet"
    )

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any(
        "contains no period starter rows" in error for error in result.validation_errors
    )


def test_core_validation_reports_unresolved_period_starters_parquet(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    row = {column: [1] for column in STARTER_LOOKUP_COLUMNS}
    row["game_id"] = ["0029700001"]
    row["period"] = [1]
    row["resolved"] = [False]
    row["away_team_id"] = [1610612740]
    row["home_team_id"] = [1610612741]
    for index in range(1, 6):
        row[f"away_player{index}"] = [100 + index]
        row[f"home_player{index}"] = [200 + index]
    pd.DataFrame(row).to_parquet(tmp_path / "data" / "period_starters_v6.parquet")

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("contains no resolved period starter rows" in error for error in result.validation_errors)


def test_core_validation_reports_incomplete_resolved_period_starter_rows(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    row = {column: [1] for column in STARTER_LOOKUP_COLUMNS}
    row["game_id"] = ["0029700001"]
    row["period"] = [1]
    row["resolved"] = [True]
    row["away_team_id"] = [1610612740]
    row["home_team_id"] = [1610612741]
    for index in range(1, 6):
        row[f"away_player{index}"] = [100 + index]
        row[f"home_player{index}"] = [200 + index]
    row["away_player5"] = [None]
    pd.DataFrame(row).to_parquet(tmp_path / "data" / "period_starters_v6.parquet")

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("away_player5" in error for error in result.validation_errors)


def test_core_validation_reports_duplicate_resolved_period_starter_rows(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    rows = []
    for _ in range(2):
        row = {column: 1 for column in STARTER_LOOKUP_COLUMNS}
        row["game_id"] = "0029700001"
        row["period"] = 1
        row["resolved"] = True
        row["away_team_id"] = 1610612740
        row["home_team_id"] = 1610612741
        for index in range(1, 6):
            row[f"away_player{index}"] = 100 + index
            row[f"home_player{index}"] = 200 + index
        rows.append(row)
    pd.DataFrame(rows).to_parquet(tmp_path / "data" / "period_starters_v6.parquet")

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("duplicates starter key" in error for error in result.validation_errors)


@pytest.mark.parametrize(
    ("rel_path", "bad_contents"),
    [
        ("catalogs/pbp_stat_overrides.csv", "game_id\n"),
        (
            "catalogs/pbp_stat_overrides.csv",
            "game_id,team_id,player_id,stat_key,stat_value,notes\n"
            "0029600332,team,769,UnknownDistance2ptDefRebounds,1,bad team\n",
        ),
        ("catalogs/validation_overrides.csv", "game_id\n"),
        (
            "catalogs/validation_overrides.csv",
            "game_id,action,tolerance,notes\n" "29600370,teleport,5,bad action\n",
        ),
        (
            "catalogs/validation_overrides.csv",
            "game_id,action,tolerance,notes\n" "29600370,skip,0,bad skip\n",
        ),
        (
            "catalogs/validation_overrides.csv",
            "game_id,action,tolerance,notes\n" "29600370,allow,-1,bad tolerance\n",
        ),
        ("catalogs/boxscore_source_overrides.csv", "game_id\n"),
        (
            "catalogs/boxscore_source_overrides.csv",
            "game_id,GAME_ID,TEAM_ID,TEAM_ABBREVIATION,TEAM_CITY,PLAYER_ID,PLAYER_NAME,"
            "NICKNAME,START_POSITION,COMMENT,MIN,FGM,FGA,FG_PCT,FG3M,FG3A,FG3_PCT,"
            "FTM,FTA,FT_PCT,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS,PLUS_MINUS,notes\n"
            "0029700001,0029700002,1610612740,NOP,New Orleans,123,Player,,,,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,bad game id\n",
        ),
        (
            "catalogs/boxscore_source_overrides.csv",
            "game_id,GAME_ID,TEAM_ID,TEAM_ABBREVIATION,TEAM_CITY,PLAYER_ID,PLAYER_NAME,"
            "NICKNAME,START_POSITION,COMMENT,MIN,FGM,FGA,FG_PCT,FG3M,FG3A,FG3_PCT,"
            "FTM,FTA,FT_PCT,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS,PLUS_MINUS,notes\n"
            "0029700001,0029700001,team,NOP,New Orleans,123,Player,,,,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,bad team\n",
        ),
        ("catalogs/overrides/correction_manifest.json", "{not json\n"),
        (
            "catalogs/overrides/correction_manifest.json",
            '{"manifest_version": "test"}\n',
        ),
        ("catalogs/overrides/period_starters_overrides.json", "{not json\n"),
        ("catalogs/overrides/lineup_window_overrides.json", "{not json\n"),
    ],
)
def test_core_validation_reports_invalid_non_pbp_row_catalogs(
    tmp_path, rel_path, bad_contents
):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / rel_path).write_text(bad_contents, encoding="utf-8")

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert result.validation_errors


def test_core_validation_reports_invalid_correction_manifest_semantics(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "catalogs" / "overrides" / "correction_manifest.json").write_text(
        """
{
  "manifest_version": "test",
  "corrections": [
    {
      "correction_id": "bad_lineup",
      "status": "active",
      "domain": "lineup",
      "scope_type": "period_start",
      "authoring_mode": "explicit",
      "game_id": "0029700001",
      "period": 1,
      "team_id": 1610612740,
      "lineup_player_ids": [1, 2, 3, 4],
      "reason_code": "test",
      "evidence_summary": "test",
      "source_primary": "manual_trace",
      "preferred_source": "manual_trace"
    }
  ],
  "residual_annotations": []
}
""",
        encoding="utf-8",
    )

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any(
        "does not resolve to 5 players" in error for error in result.validation_errors
    )


def test_core_validation_reports_invalid_delta_correction_schema(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "catalogs" / "overrides" / "correction_manifest.json").write_text(
        """
{
  "manifest_version": "test",
  "corrections": [
    {
      "correction_id": "bad_delta",
      "status": "active",
      "domain": "lineup",
      "scope_type": "period_start",
      "authoring_mode": "delta",
      "game_id": "0029700001",
      "period": 1,
      "team_id": 1610612740,
      "swap_out_player_id": "abc",
      "reason_code": "test",
      "evidence_summary": "test",
      "source_primary": "manual_trace",
      "preferred_source": "manual_trace"
    }
  ],
  "residual_annotations": []
}
""",
        encoding="utf-8",
    )

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any("swap_out_player_id" in error for error in result.validation_errors)


def test_core_validation_reports_stale_compiled_runtime_views(tmp_path):
    _write_core_inputs(tmp_path)
    _write_core_catalogs(tmp_path)
    (tmp_path / "catalogs" / "overrides" / "correction_manifest.json").write_text(
        """
{
  "manifest_version": "test",
  "corrections": [
    {
      "correction_id": "period_start_fix",
      "status": "active",
      "domain": "lineup",
      "scope_type": "period_start",
      "authoring_mode": "explicit",
      "game_id": "0029700001",
      "period": 1,
      "team_id": 1610612740,
      "lineup_player_ids": [1, 2, 3, 4, 5],
      "reason_code": "test",
      "evidence_summary": "test",
      "source_primary": "manual_trace",
      "preferred_source": "manual_trace"
    }
  ],
  "residual_annotations": []
}
""",
        encoding="utf-8",
    )

    result = validate_scope("core", root=tmp_path)

    assert result.ok is False
    assert any(
        "period_starters_overrides.json does not match" in error
        for error in result.validation_errors
    )


def test_cross_source_validation_skips_missing_optional_inputs(tmp_path):
    result = validate_scope("cross-source", root=tmp_path)

    assert result.ok is True
    assert result.validation_level == "optional_diagnostic_preflight"
    assert "data/bbr/bbref_boxscores.db" in result.skipped_optional
    assert "data/tpdev/full_pbp_new.parq" in result.skipped_optional


def test_provenance_validation_fails_when_evidence_inputs_are_missing(tmp_path):
    result = validate_scope("provenance", root=tmp_path)

    assert result.ok is False
    assert result.validation_level == "provenance_evidence_preflight"
    assert "data/bbr/bbref_boxscores.db" in result.missing_required
    assert "data/tpdev/tpdev_box.parq" in result.missing_required
