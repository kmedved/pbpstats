from __future__ import annotations

from historic_backfill.audits.core.fork_repair_usage import (
    _load_game_ids_from_file,
    _instrument_fix_event_order_source,
    _instrument_silent_ft_source,
    _pattern_rule_id,
    summarize_rule_hits,
    summarize_rule_comparison,
)


class _Dummy:
    def _fix_event_order(self):
        # --- PATTERN -1: Move an orphan rebound back to the nearest prior miss ---
        self.data = []
        return

    def _repair_silent_ft_rebound_windows(self):
        # Reversed and-one / 1-of-1 block:
        changed = True


def test_pattern_rule_id_prefix() -> None:
    assert _pattern_rule_id("# --- PATTERN -0.4: Shadowing TEAM rebound before a future miss chain ---").startswith("processor.")


def test_load_game_ids_from_file_normalizes_and_dedupes(tmp_path) -> None:
    path = tmp_path / "games.txt"
    path.write_text("21700041\n0021700041\n21700012\n\n", encoding="utf-8")
    assert _load_game_ids_from_file(path) == ["0021700012", "0021700041"]


def test_instrument_fix_event_order_inserts_record_call() -> None:
    instrumented = _instrument_fix_event_order_source(_Dummy._fix_event_order)
    assert "_record_repair" in instrumented.__code__.co_names


def test_instrument_silent_ft_inserts_record_call() -> None:
    instrumented = _instrument_silent_ft_source(_Dummy._repair_silent_ft_rebound_windows)
    assert "_record_repair" in instrumented.__code__.co_names


def test_summarize_rule_comparison_classifies_usage() -> None:
    import pandas as pd

    summary_df = pd.DataFrame(
        [
            {"mode": "current_production", "rule_id": "processor.a", "games_hit": 3},
            {"mode": "raw_no_row_overrides", "rule_id": "processor.a", "games_hit": 5},
            {"mode": "raw_no_row_overrides", "rule_id": "processor.b", "games_hit": 2},
        ]
    )
    comparison = summarize_rule_comparison(summary_df)
    usage = dict(zip(comparison["rule_id"], comparison["usage_class"]))
    assert usage["processor.a"] == "active_with_current_overrides"
    assert usage["processor.b"] == "only_active_without_row_overrides"


def test_summarize_rule_hits_tolerates_missing_audit_columns() -> None:
    import pandas as pd

    hit_df = pd.DataFrame(
        [
            {
                "mode": "current_production",
                "game_id": "0021700041",
                "hit_index": 1,
                "rule_id": "processor.x",
                "error": "",
                "rebound_deletions": 0,
            }
        ]
    )
    summary = summarize_rule_hits(hit_df)
    row = summary.iloc[0].to_dict()
    assert row["games_hit"] == 1
    assert row["games_with_audit_rows"] == 0
