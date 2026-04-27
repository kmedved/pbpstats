from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from build_minutes_cross_source_report import (
    MINUTE_MATCH_TOLERANCE,
    build_minutes_cross_source_report,
    summarize_minutes_cross_source_report,
)


def test_build_minutes_cross_source_report_compares_official_and_tpdev(monkeypatch, tmp_path: Path):
    darko_df = pd.DataFrame(
        [
            {
                "Game_SingleGame": 21900001,
                "NbaDotComID": 101,
                "Team_SingleGame": 1,
                "FullName": "Player One",
                "Minutes": 20.8,
                "Plus_Minus": -6,
            }
        ]
    )

    def fake_official_batch(_db_path: Path, _game_ids) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "game_id": "0021900001",
                    "player_id": 101,
                    "team_id": 1,
                    "player_name": "Player One",
                    "Minutes_official": 20.7,
                    "Plus_Minus_official": -7.0,
                }
            ]
    )

    monkeypatch.setattr(
        "build_minutes_cross_source_report.load_official_boxscore_batch_df",
        fake_official_batch,
    )
    monkeypatch.setattr(
        "build_minutes_cross_source_report.load_bbr_boxscore_df",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "game_id": "0021900001",
                    "player_id": 101,
                    "team_id": 1,
                    "player_name_bbr_box": "Player One",
                    "Minutes_bbr_box": 20.7,
                    "Plus_Minus_bbr_box": -7.0,
                    "bbr_game_id": "202001010AAA",
                }
            ]
        ),
    )

    tpdev_box = pd.DataFrame(
        [
            {
                "Game_SingleGame": 21900001,
                "Team_SingleGame": 1,
                "NbaDotComID": 101,
                "FullName": "Player One",
                "Minutes": 20.7,
                "Plus_Minus": -4,
            }
        ]
    )
    tpdev_box_path = tmp_path / "tpdev_box.parq"
    tpdev_box.to_parquet(tpdev_box_path, index=False)

    report = build_minutes_cross_source_report(
        darko_df=darko_df,
        db_path=tmp_path / "fake.db",
        tpdev_box_path=tpdev_box_path,
        tpdev_box_new_path=tmp_path / "missing_new.parq",
        tpdev_box_cdn_path=tmp_path / "missing_cdn.parq",
    )

    assert len(report) == 1
    row = report.iloc[0]
    assert row["Minutes_diff_vs_official"] == pytest.approx(0.1)
    assert bool(row["Minutes_match_vs_official"]) is False
    assert bool(row["Official_minutes_match_vs_tpdev_box"]) is True
    assert bool(row["Minutes_match_vs_tpdev_box"]) is False
    assert bool(row["Official_minutes_match_vs_bbr_box"]) is True
    assert bool(row["Official_plus_minus_match_vs_bbr_box"]) is True
    assert row["Plus_Minus_diff_vs_official"] == pytest.approx(1.0)


def test_summarize_minutes_cross_source_report_counts_source_agreement():
    report_df = pd.DataFrame(
        [
            {
                "Minutes_match_vs_official": False,
                "Plus_Minus_match_vs_official": True,
                "Minutes_abs_diff_vs_official": 0.1,
                "Minutes_match_vs_tpdev_box": False,
                "Plus_Minus_match_vs_tpdev_box": False,
                "Official_minutes_match_vs_tpdev_box": True,
                "Official_plus_minus_match_vs_tpdev_box": False,
                "Minutes_match_vs_tpdev_box_new": False,
                "Plus_Minus_match_vs_tpdev_box_new": False,
                "Official_minutes_match_vs_tpdev_box_new": True,
                "Official_plus_minus_match_vs_tpdev_box_new": False,
                "Minutes_match_vs_tpdev_box_cdn": False,
                "Plus_Minus_match_vs_tpdev_box_cdn": False,
                "Official_minutes_match_vs_tpdev_box_cdn": False,
                "Official_plus_minus_match_vs_tpdev_box_cdn": False,
                "Minutes_match_vs_bbr_box": False,
                "Plus_Minus_match_vs_bbr_box": False,
                "Official_minutes_match_vs_bbr_box": True,
                "Official_plus_minus_match_vs_bbr_box": True,
            },
            {
                "Minutes_match_vs_official": True,
                "Plus_Minus_match_vs_official": False,
                "Minutes_abs_diff_vs_official": MINUTE_MATCH_TOLERANCE,
                "Minutes_match_vs_tpdev_box": True,
                "Plus_Minus_match_vs_tpdev_box": True,
                "Official_minutes_match_vs_tpdev_box": True,
                "Official_plus_minus_match_vs_tpdev_box": True,
                "Minutes_match_vs_tpdev_box_new": True,
                "Plus_Minus_match_vs_tpdev_box_new": True,
                "Official_minutes_match_vs_tpdev_box_new": True,
                "Official_plus_minus_match_vs_tpdev_box_new": True,
                "Minutes_match_vs_tpdev_box_cdn": True,
                "Plus_Minus_match_vs_tpdev_box_cdn": True,
                "Official_minutes_match_vs_tpdev_box_cdn": True,
                "Official_plus_minus_match_vs_tpdev_box_cdn": True,
                "Minutes_match_vs_bbr_box": True,
                "Plus_Minus_match_vs_bbr_box": False,
                "Official_minutes_match_vs_bbr_box": True,
                "Official_plus_minus_match_vs_bbr_box": True,
            },
        ]
    )

    summary = summarize_minutes_cross_source_report(report_df)

    assert summary["rows"] == 2
    assert summary["output_minutes_match_official"] == 1
    assert summary["rows_where_output_minutes_differs_from_official"] == 1
    assert summary["official_minutes_match_tpdev_box"] == 2
    assert summary["rows_where_official_and_tpdev_box_agree_but_output_minutes_differs"] == 1
    assert summary["official_plus_minus_match_bbr_box"] == 2
    assert summary["rows_where_output_matches_bbr_box_not_official_minutes"] == 0
    assert summary["minute_diff_buckets_vs_official"]["seconds_2_to_6"] == 1
