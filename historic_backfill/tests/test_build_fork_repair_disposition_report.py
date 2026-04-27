from __future__ import annotations

import json

import pandas as pd

from historic_backfill.runners.build_fork_repair_disposition_report import build_report


def test_build_report_classifies_turnover_gate_and_manualization_candidates(tmp_path):
    catalog = pd.DataFrame(
        [
            {
                "rule_id": "turnover.no_turnover_dead_ball_gate",
                "file": "/tmp/turnover.py",
                "line": 71,
                "category": "feed_semantics_guard",
                "scope": "2017_plus_gate",
                "proof_level": "full_impacted_game_audit",
                "default_recommendation": "keep_in_fork",
                "description": "dead-ball gate",
            },
            {
                "rule_id": "processor.m0_785_player_rebound_ahead_of_future_samemteam_missed_ft_placeholder",
                "file": "/tmp/processor.py",
                "line": 659,
                "category": "ordering_repair",
                "scope": "historical_general",
                "proof_level": "season_canary_plus_regression_tests",
                "default_recommendation": "keep_in_fork_existing_only",
                "description": "placeholder repair",
            },
            {
                "rule_id": "processor.m1_move_an_orphan_rebound_back_to_the_nearest_prior_miss",
                "file": "/tmp/processor.py",
                "line": 789,
                "category": "ordering_repair",
                "scope": "historical_general",
                "proof_level": "season_canary_plus_regression_tests",
                "default_recommendation": "keep_in_fork_existing_only",
                "description": "orphan rebound repair",
            },
        ]
    )
    usage_comparison = pd.DataFrame(
        [
            {
                "rule_id": "processor.m0_785_player_rebound_ahead_of_future_samemteam_missed_ft_placeholder",
                "current_production": 0,
                "raw_no_row_overrides": 2,
                "usage_class": "only_active_without_row_overrides",
            },
            {
                "rule_id": "processor.m1_move_an_orphan_rebound_back_to_the_nearest_prior_miss",
                "current_production": 31,
                "raw_no_row_overrides": 52,
                "usage_class": "active_with_current_overrides",
            },
        ]
    )
    usage_summary = pd.DataFrame(
        [
            {
                "mode": "raw_no_row_overrides",
                "rule_id": "processor.m0_785_player_rebound_ahead_of_future_samemteam_missed_ft_placeholder",
                "sample_games": "0021700012|0029600401",
            },
            {
                "mode": "current_production",
                "rule_id": "processor.m1_move_an_orphan_rebound_back_to_the_nearest_prior_miss",
                "sample_games": "0020300257|0020700184",
            },
        ]
    )
    no_turnover_summary = {
        "variant_comparison_counts": {
            "always": {"same": 51, "variant_worse": 7},
            "never": {"same": 16, "variant_worse": 236},
        }
    }

    catalog_path = tmp_path / "catalog.csv"
    usage_dir = tmp_path / "usage"
    usage_dir.mkdir()
    no_turnover_path = tmp_path / "no_turnover_summary.json"

    catalog.to_csv(catalog_path, index=False)
    usage_comparison.to_csv(usage_dir / "fork_repair_usage_rule_comparison.csv", index=False)
    usage_summary.to_csv(usage_dir / "fork_repair_usage_rule_summary.csv", index=False)
    no_turnover_path.write_text(json.dumps(no_turnover_summary), encoding="utf-8")

    report, summary = build_report(catalog_path, usage_dir, no_turnover_path)

    turnover_row = report.loc[report["rule_id"] == "turnover.no_turnover_dead_ball_gate"].iloc[0]
    assert turnover_row["recommended_disposition"] == "keep_in_fork_proven_by_impacted_game_audit"

    candidate_row = report.loc[
        report["rule_id"] == "processor.m0_785_player_rebound_ahead_of_future_samemteam_missed_ft_placeholder"
    ].iloc[0]
    assert candidate_row["recommended_disposition"] == "manual_override_candidate"
    assert candidate_row["raw_no_row_sample_games"] == "0021700012|0029600401"

    active_row = report.loc[
        report["rule_id"] == "processor.m1_move_an_orphan_rebound_back_to_the_nearest_prior_miss"
    ].iloc[0]
    assert active_row["recommended_disposition"] == "keep_in_fork_broadly_active"
    assert active_row["current_production_sample_games"] == "0020300257|0020700184"

    assert summary["manual_override_candidate_rules"] == [
        "processor.m0_785_player_rebound_ahead_of_future_samemteam_missed_ft_placeholder"
    ]
    assert summary["manual_override_candidate_games"] == ["0021700012", "0029600401"]
