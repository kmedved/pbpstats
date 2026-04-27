from __future__ import annotations

import pandas as pd

from historic_backfill.audits.cross_source.build_override_consensus_report import (
    _classify_row_override,
    _classify_stat_like_override,
    _stat_match_flags,
)


def test_stat_match_flags_tracks_tpdev_alignment() -> None:
    matches = pd.DataFrame(
        [
            {"check_stat": "TOV", "parser_value": "2", "official_value": "2", "bbr_value": "3"},
        ]
    )
    flags = _stat_match_flags(matches, ["TOV"], "TOV:2")
    assert flags["parser_matches_official_all"] is True
    assert flags["parser_matches_bbr_pbp_all"] is False
    assert flags["tpdev_matches_parser_all"] is True
    assert flags["stat_pairs"] == ["TOV: parser=2 official=2 bbr_pbp=3 tpdev_box=2"]


def test_classify_stat_like_override_detects_documented_source_conflict() -> None:
    flags = {
        "parser_matches_official_all": True,
        "parser_matches_bbr_pbp_all": False,
        "official_matches_bbr_pbp_all": False,
        "tpdev_present": True,
        "tpdev_matches_parser_all": False,
        "tpdev_matches_official_all": False,
    }
    consensus, action = _classify_stat_like_override(
        "pbp_stat_overrides",
        "Manual source fix: the local official shots cache assigns the q1 5:36 missed 10-footer to Nicolas Batum not Kemba Walker",
        flags,
    )
    assert consensus == "documented_shot_source_conflict"
    assert action == "keep_production_override_and_document"


def test_classify_row_override_keeps_semantic_fix_when_bbr_logs_event() -> None:
    consensus, action = _classify_row_override(
        "bbr_keeps_target_like_event",
        "Drop impossible Laettner rebound logged after a turnover with no missed shot context",
        "audit_player_rows:1->0",
    )
    assert consensus == "semantic_fix_despite_bbr_event_presence"
    assert action == "keep_row_override"
