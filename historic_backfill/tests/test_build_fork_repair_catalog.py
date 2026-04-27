from __future__ import annotations

from historic_backfill.runners.build_fork_repair_catalog import build_catalog


def test_catalog_contains_turnover_gate_and_processor_patterns() -> None:
    catalog, summary = build_catalog()
    assert not catalog.empty
    assert "turnover.no_turnover_dead_ball_gate" in set(catalog["rule_id"])
    assert any(rule_id.startswith("processor.") for rule_id in catalog["rule_id"])
    assert summary["counts_by_category"]["ordering_repair"] >= 1
