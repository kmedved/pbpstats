from historic_backfill.audits.core.no_turnover_gate import _comparison_label
from historic_backfill.common.override_necessity_utils import GameVariantMetrics


def test_comparison_label_marks_variant_worse_when_audit_rows_increase():
    current = GameVariantMetrics(audit_player_rows=0)
    variant = GameVariantMetrics(audit_player_rows=2)

    assert _comparison_label(current, variant) == "variant_worse"


def test_comparison_label_marks_variant_better_when_error_clears():
    current = GameVariantMetrics(error="ValueError: boom")
    variant = GameVariantMetrics()

    assert _comparison_label(current, variant) == "variant_better"
