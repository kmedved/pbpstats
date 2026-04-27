from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from historic_backfill.audits.cross_source.bbr_boxscore_loader import load_bbr_boxscore_df
from historic_backfill.audits.cross_source.bbr_pbp_lookup import BbrGameMatch, NbaGameContext


def _create_player_basic_db(path: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE player_basic (
                game_id TEXT,
                team TEXT,
                row_index INTEGER,
                home_away TEXT,
                player TEXT,
                player_id TEXT,
                mp TEXT,
                plus_minus INTEGER
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO player_basic (
                game_id, team, row_index, home_away, player, player_id, mp, plus_minus
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def test_load_bbr_boxscore_df_uses_crosswalk_mapping(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "bbref_boxscores.db"
    _create_player_basic_db(
        db_path,
        [
            ("202001010BOS", "ATL", 0, "away", "Player One", "playeon01", "33:08", -5),
            ("202001010BOS", "BOS", 0, "home", "Player Two", "playetw01", "28:10", 7),
        ],
    )
    crosswalk_path = tmp_path / "crosswalk.csv"
    pd.DataFrame(
        [
            {"bbr_id": "playeon01", "nba_id": 101, "alt_nba_id": None},
            {"bbr_id": "playetw01", "nba_id": 202, "alt_nba_id": None},
        ]
    ).to_csv(crosswalk_path, index=False)

    monkeypatch.setattr(
        "historic_backfill.audits.cross_source.bbr_boxscore_loader.find_bbr_game_for_nba_game",
        lambda *_args, **_kwargs: (
            NbaGameContext(
                nba_game_id="0021900001",
                game_date=pd.Timestamp("2020-01-01").date(),
                home_team_id=2,
                away_team_id=1,
                home_team_abbr="BOS",
                away_team_abbr="ATL",
            ),
            [BbrGameMatch("202001010BOS", "", "ATL", "BOS")],
        ),
    )
    monkeypatch.setattr(
        "historic_backfill.audits.cross_source.bbr_boxscore_loader.load_official_boxscore_df",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )

    report = load_bbr_boxscore_df(
        "0021900001",
        nba_raw_db_path=tmp_path / "fake.db",
        bbr_db_path=db_path,
        crosswalk_path=crosswalk_path,
    )

    assert report["player_id"].tolist() == [101, 202]
    assert report["team_id"].tolist() == [1, 2]
    assert report["Minutes_bbr_box"].tolist() == [33 + 8 / 60.0, 28 + 10 / 60.0]
    assert report["Plus_Minus_bbr_box"].tolist() == [-5.0, 7.0]


def test_load_bbr_boxscore_df_falls_back_to_official_name_match(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "bbref_boxscores.db"
    _create_player_basic_db(
        db_path,
        [
            ("202001010ATL", "ATL", 0, "away", "Joe Harris", "harrijo01", "31:00", -9),
        ],
    )
    crosswalk_path = tmp_path / "crosswalk.csv"
    pd.DataFrame([{"bbr_id": "otherguy", "nba_id": 999, "alt_nba_id": None}]).to_csv(
        crosswalk_path, index=False
    )

    monkeypatch.setattr(
        "historic_backfill.audits.cross_source.bbr_boxscore_loader.find_bbr_game_for_nba_game",
        lambda *_args, **_kwargs: (
            NbaGameContext(
                nba_game_id="0021900002",
                game_date=pd.Timestamp("2020-01-01").date(),
                home_team_id=2,
                away_team_id=1,
                home_team_abbr="BOS",
                away_team_abbr="ATL",
            ),
            [BbrGameMatch("202001010ATL", "", "ATL", "BOS")],
        ),
    )
    monkeypatch.setattr(
        "historic_backfill.audits.cross_source.bbr_boxscore_loader.load_official_boxscore_df",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "game_id": "0021900002",
                    "player_id": 303,
                    "team_id": 1,
                    "player_name": "Joe Harris",
                    "Minutes_official": 31.0,
                    "Plus_Minus_official": -9.0,
                }
            ]
        ),
    )

    report = load_bbr_boxscore_df(
        "0021900002",
        nba_raw_db_path=tmp_path / "fake.db",
        bbr_db_path=db_path,
        crosswalk_path=crosswalk_path,
    )

    assert len(report) == 1
    row = report.iloc[0]
    assert row["player_id"] == 303
    assert row["team_id"] == 1
    assert row["Minutes_bbr_box"] == 31.0
    assert row["Plus_Minus_bbr_box"] == -9.0
