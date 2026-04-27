from historic_backfill.audits.cross_source.check_pbp_row_override_windows_against_bbr import (
    _classify_status,
    _extract_tokens,
    _normalize_text,
)


def test_extract_tokens_captures_last_names_and_event_phrases():
    row = {
        "PLAYER1_NAME": "Christian Laettner",
        "PLAYER2_NAME": "",
        "PLAYER3_NAME": "",
    }

    tokens = _extract_tokens(row, "Defensive rebound by C. Laettner")

    assert "laettner" in tokens
    assert "defensive rebound" in tokens


def test_classify_status_supports_move_before_when_target_precedes_anchor():
    status = _classify_status("move_before", target_hit=0, anchor_hit=1, bbr_rows=[{"home_play": "a"}, {"home_play": "b"}])

    assert status == "bbr_supports_move_before"


def test_normalize_text_removes_punctuation():
    assert _normalize_text("K. Walker misses 2-pt jump shot from 10 ft") == "k walker misses 2 pt jump shot from 10 ft"
