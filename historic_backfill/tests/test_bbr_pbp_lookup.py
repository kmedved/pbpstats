from __future__ import annotations

import sqlite3
from datetime import date

from bbr_pbp_lookup import (
    BbrGameMatch,
    candidate_bbr_team_codes,
    load_bbr_play_by_play_rows,
)


def test_candidate_bbr_team_codes_handles_historical_aliases():
    assert candidate_bbr_team_codes(1610612756, date(2019, 12, 1)) == ["PHO"]
    assert candidate_bbr_team_codes(1610612766, date(1998, 1, 1)) == ["CHH"]
    assert candidate_bbr_team_codes(1610612766, date(2010, 1, 1)) == ["CHA"]
    assert candidate_bbr_team_codes(1610612766, date(2019, 1, 1)) == ["CHO"]
    assert candidate_bbr_team_codes(1610612740, date(2006, 1, 1)) == ["NOH", "NOK"]
    assert candidate_bbr_team_codes(1610612764, date(1996, 12, 1)) == ["WSB"]
    assert candidate_bbr_team_codes(1610612764, date(1998, 12, 1)) == ["WAS"]


def test_bbr_games_query_pattern_matches_expected_row():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE games (
            game_id TEXT PRIMARY KEY,
            season INTEGER NOT NULL,
            date TEXT NOT NULL,
            url TEXT NOT NULL,
            away_team TEXT NOT NULL,
            home_team TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO games (game_id, season, date, url, away_team, home_team)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            "201912010LAL",
            2020,
            "201912010LAL",
            "https://example.com/201912010LAL",
            "DAL",
            "LAL",
        ),
    )
    rows = conn.execute(
        """
        SELECT game_id, url, away_team, home_team
        FROM games
        WHERE game_id LIKE ?
          AND home_team IN (?)
          AND away_team IN (?)
        ORDER BY game_id
        """,
        ("20191201%", "LAL", "DAL"),
    ).fetchall()
    conn.close()

    assert rows == [
        (
            "201912010LAL",
            "https://example.com/201912010LAL",
            "DAL",
            "LAL",
        )
    ]
    match = BbrGameMatch(
        bbr_game_id=rows[0][0],
        game_url=rows[0][1],
        away_team=rows[0][2],
        home_team=rows[0][3],
    )
    assert match.bbr_game_id == "201912010LAL"


def test_load_bbr_play_by_play_rows_matches_clock_without_decimal_suffix(tmp_path):
    db_path = tmp_path / "bbref.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE play_by_play (
            game_id TEXT NOT NULL,
            event_index INTEGER NOT NULL,
            period INTEGER NOT NULL,
            game_clock TEXT,
            score_away INTEGER,
            score_home INTEGER,
            away_play TEXT,
            home_play TEXT,
            away_player_ids TEXT,
            home_player_ids TEXT,
            is_colspan_row INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (game_id, event_index)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO play_by_play (
            game_id, event_index, period, game_clock, score_away, score_home,
            away_play, home_play, away_player_ids, home_player_ids, is_colspan_row
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "201912010LAL",
            100,
            4,
            "5:23.0",
            95,
            99,
            "Defensive rebound byL. James",
            None,
            "jamesle01",
            "",
            0,
        ),
    )
    conn.commit()
    conn.close()

    rows = load_bbr_play_by_play_rows(
        "201912010LAL",
        bbr_db_path=db_path,
        period=4,
        clock="5:23",
    )

    assert len(rows) == 1
    assert rows[0]["event_index"] == 100
