from __future__ import annotations

import pandas as pd

from historic_backfill.audits.cross_source.build_large_minute_outlier_family_register import (
    _choose_primary_family,
    _classify_large_outlier_row,
    _consensus_label,
    _parse_list_like,
)


def test_parse_list_like_handles_python_literal_strings():
    assert _parse_list_like("[1, 2, 3]") == [1, 2, 3]
    assert _parse_list_like("['a', 'b']") == ["a", "b"]
    assert _parse_list_like("") == []


def test_consensus_label_prefers_full_official_agreement():
    row = pd.Series(
        {
            "Official_minutes_match_vs_tpdev_box": True,
            "Official_minutes_match_vs_bbr_box": True,
        }
    )
    assert _consensus_label(row) == "official_tpdev_bbr"


def test_classify_large_outlier_row_marks_simple_starter_candidate():
    row = pd.Series(
        {
            "game_id": "0021700482",
            "Minutes_diff_seconds": 300.0,
            "triage_period_sized_candidate": True,
            "triage_simple_case": True,
            "Official_minutes_match_vs_tpdev_box": True,
            "Official_minutes_match_vs_bbr_box": True,
            "v2_v3_periods_with_diff": 0,
        }
    )
    assert _classify_large_outlier_row(row) == "starter_simple_candidate"


def test_classify_large_outlier_row_marks_known_v3_anomaly():
    row = pd.Series(
        {
            "game_id": "0029600585",
            "Minutes_diff_seconds": 147.0,
            "triage_period_sized_candidate": False,
            "triage_simple_case": False,
            "Official_minutes_match_vs_tpdev_box": True,
            "Official_minutes_match_vs_bbr_box": True,
            "v2_v3_periods_with_diff": 4,
        }
    )
    assert _classify_large_outlier_row(row) == "v3_ordering_candidate"


def test_choose_primary_family_prioritizes_v3_then_starter():
    assert (
        _choose_primary_family(
            ["other_large_outlier", "starter_complex_candidate", "v3_ordering_candidate"]
        )
        == "v3_ordering_candidate"
    )
