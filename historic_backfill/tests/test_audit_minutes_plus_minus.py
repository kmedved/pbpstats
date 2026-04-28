import json
import sqlite3
import sys
import zlib
from pathlib import Path

import pandas as pd

from historic_backfill.audits.core.minutes_plus_minus import (
    build_minutes_plus_minus_audit,
    load_official_boxscore_batch_df,
    load_official_boxscore_df,
    parse_official_minutes,
    summarize_minutes_plus_minus_audit,
)


def test_parse_official_minutes_handles_boxscore_strings():
    assert parse_official_minutes("33:08") == 33 + (8 / 60.0)
    assert parse_official_minutes("DNP") == 0.0
    assert parse_official_minutes("") == 0.0
    assert parse_official_minutes(12.5) == 12.5


def test_build_minutes_plus_minus_audit_flags_material_outliers(monkeypatch):
    darko_df = pd.DataFrame(
        [
            {
                "Game_SingleGame": 21900291,
                "NbaDotComID": 203497,
                "Team_SingleGame": 1610612762,
                "FullName": "Rudy Gobert",
                "Minutes": 40.433333,
                "Plus_Minus": -44.0,
            },
            {
                "Game_SingleGame": 20100810,
                "NbaDotComID": 258,
                "Team_SingleGame": 1610612748,
                "FullName": "Brian Grant",
                "Minutes": 33.15,
                "Plus_Minus": -11.0,
            },
        ]
    )

    official = pd.DataFrame(
        [
            {
                "game_id": "0021900291",
                "player_id": 203497,
                "team_id": 1610612762,
                "player_name": "Rudy Gobert",
                "Minutes_official": 28.433333,
                "Plus_Minus_official": -21.0,
            },
            {
                "game_id": "0020100810",
                "player_id": 258,
                "team_id": 1610612748,
                "player_name": "Brian Grant",
                "Minutes_official": 33.133333,
                "Plus_Minus_official": -11.0,
            },
        ]
    )

    def fake_batch_loader(_db_path: Path, game_ids, **_kwargs) -> pd.DataFrame:
        requested = {str(game_id) for game_id in game_ids}
        return official[official["game_id"].isin(requested)].copy()

    monkeypatch.setattr(
        "historic_backfill.audits.core.minutes_plus_minus.load_official_boxscore_batch_df",
        fake_batch_loader,
    )

    audit_df = build_minutes_plus_minus_audit(darko_df, db_path=Path("/tmp/fake.db"))
    summary = summarize_minutes_plus_minus_audit(audit_df)

    gobert = audit_df[audit_df["player_id"] == 203497].iloc[0]
    grant = audit_df[audit_df["player_id"] == 258].iloc[0]

    assert gobert["is_minutes_outlier"]
    assert gobert["has_plus_minus_mismatch"]
    assert not grant["is_minutes_outlier"]
    assert not grant["has_plus_minus_mismatch"]
    assert summary["minutes_outliers"] == 1
    assert summary["plus_minus_mismatches"] == 1


def test_load_official_boxscore_batch_df_matches_single_game_loader(tmp_path: Path) -> None:
    db_path = tmp_path / "nba_raw.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
    )

    payload_one = {
        "resultSets": [
            {
                "headers": ["PLAYER_ID", "TEAM_ID", "PLAYER_NAME", "MIN", "PLUS_MINUS"],
                "rowSet": [[101, 1, "Player One", "20:30", -3]],
            }
        ]
    }
    payload_two = {
        "resultSets": [
            {
                "headers": ["PLAYER_ID", "TEAM_ID", "PLAYER_NAME", "MIN", "PLUS_MINUS"],
                "rowSet": [[202, 2, "Player Two", "18:00", 5]],
            }
        ]
    }
    conn.executemany(
        "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, ?, ?)",
        [
            ("0021900001", "boxscore", None, zlib.compress(json.dumps(payload_one).encode("utf-8"))),
            ("0021900002", "boxscore", None, zlib.compress(json.dumps(payload_two).encode("utf-8"))),
        ],
    )
    conn.commit()
    conn.close()

    batch_df = load_official_boxscore_batch_df(db_path, ["0021900001", "0021900002"])
    single_df = pd.concat(
        [
            load_official_boxscore_df(db_path, "0021900001"),
            load_official_boxscore_df(db_path, "0021900002"),
        ],
        ignore_index=True,
    ).sort_values(["game_id", "team_id", "player_id"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(batch_df, single_df)


def test_build_minutes_plus_minus_audit_uses_boxscore_source_overrides(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "nba_raw.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE raw_responses (game_id TEXT, endpoint TEXT, team_id INTEGER, data BLOB)"
    )
    payload = {
        "resultSets": [
            {
                "headers": ["PLAYER_ID", "TEAM_ID", "PLAYER_NAME", "MIN", "PLUS_MINUS"],
                "rowSet": [[247, 1610612765, "Joe Dumars", "37:54", 0]],
            }
        ]
    }
    conn.execute(
        "INSERT INTO raw_responses (game_id, endpoint, team_id, data) VALUES (?, ?, ?, ?)",
        (
            "0029600957",
            "boxscore",
            None,
            zlib.compress(json.dumps(payload).encode("utf-8")),
        ),
    )
    conn.commit()
    conn.close()

    darko_df = pd.DataFrame(
        [
            {
                "Game_SingleGame": "0029600957",
                "NbaDotComID": 247,
                "Team_SingleGame": 1610612765,
                "FullName": "Joe Dumars",
                "Minutes": 37.9,
                "Plus_Minus": 6,
            }
        ]
    )
    override = {
        "game_id": "0029600957",
        "GAME_ID": "0029600957",
        "TEAM_ID": 1610612765,
        "TEAM_ABBREVIATION": "DET",
        "TEAM_CITY": "Detroit",
        "PLAYER_ID": 247,
        "PLAYER_NAME": "Joe Dumars",
        "NICKNAME": "Joe",
        "START_POSITION": "G",
        "COMMENT": "",
        "MIN": "37:54",
        "FGM": 1,
        "FGA": 6,
        "FG_PCT": 0.167,
        "FG3M": 1,
        "FG3A": 5,
        "FG3_PCT": 0.2,
        "FTM": 5,
        "FTA": 5,
        "FT_PCT": 1.0,
        "OREB": 1,
        "DREB": 0,
        "REB": 1,
        "AST": 8,
        "STL": 0,
        "BLK": 0,
        "TO": 1,
        "PF": 0,
        "PTS": 8,
        "PLUS_MINUS": 6,
        "notes": "test override",
    }

    without_override = build_minutes_plus_minus_audit(
        darko_df,
        db_path=db_path,
        boxscore_source_overrides=pd.DataFrame(),
    )
    with_override = build_minutes_plus_minus_audit(
        darko_df,
        db_path=db_path,
        boxscore_source_overrides=pd.DataFrame([override]),
    )

    assert without_override["has_plus_minus_mismatch"].iloc[0]
    assert not with_override["has_plus_minus_mismatch"].iloc[0]


def test_minutes_plus_minus_cli_forwards_boxscore_source_overrides_path(
    monkeypatch,
    tmp_path: Path,
) -> None:
    import historic_backfill.audits.core.minutes_plus_minus as mod

    input_path = tmp_path / "darko.parquet"
    input_path.write_text("placeholder", encoding="utf-8")
    db_path = tmp_path / "nba_raw.db"
    overrides_path = tmp_path / "boxscore_source_overrides.csv"
    output_dir = tmp_path / "out"
    observed: dict[str, object] = {}

    monkeypatch.setattr(mod, "_discover_parquet_paths", lambda path: [path])
    monkeypatch.setattr(
        mod,
        "_load_darko_frames",
        lambda _paths: pd.DataFrame({"Game_SingleGame": ["0029700001"]}),
    )

    def fake_build_minutes_plus_minus_audit(darko_df, **kwargs):
        observed["darko_df"] = darko_df
        observed.update(kwargs)
        return pd.DataFrame()

    monkeypatch.setattr(
        mod, "build_minutes_plus_minus_audit", fake_build_minutes_plus_minus_audit
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "minutes_plus_minus.py",
            str(input_path),
            "--db-path",
            str(db_path),
            "--output-dir",
            str(output_dir),
            "--boxscore-source-overrides-path",
            str(overrides_path),
        ],
    )

    mod.main()

    assert observed["db_path"] == db_path
    assert observed["boxscore_source_overrides_path"] == overrides_path
