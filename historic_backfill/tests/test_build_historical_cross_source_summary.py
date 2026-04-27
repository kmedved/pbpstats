import json
from pathlib import Path

import pandas as pd

from historic_backfill.audits.cross_source.build_historical_cross_source_summary import build_summary


def test_build_summary_aggregates_key_counts(tmp_path: Path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "season_range": {"start": 1997, "stop": 2020},
                "seasons_frozen": 24,
                "missing_clean_seasons": [],
                "seasons": {"1997": {}, "1998": {}},
            }
        )
    )

    provenance = pd.DataFrame(
        [
            {"override_file": "pbp_row_overrides", "kind": "row_order", "scope": "production_output", "basis_tag": "raw_pbp_order_fix"},
            {"override_file": "boxscore_audit_overrides", "kind": "audit_exception", "scope": "audit_only", "basis_tag": "bbr_boxscore"},
        ]
    )
    provenance_path = tmp_path / "provenance.csv"
    provenance.to_csv(provenance_path, index=False)

    consensus = pd.DataFrame(
        [
            {"consensus_class": "strong_bbr_window_support", "recommended_action": "keep_row_override"},
            {"consensus_class": "documented_box_vs_pbp_source_conflict", "recommended_action": "keep_production_override_and_document"},
        ]
    )
    consensus_path = tmp_path / "consensus.csv"
    consensus.to_csv(consensus_path, index=False)

    conflicts = pd.DataFrame(
        [
            {
                "season": "2017",
                "game_id": "21600096",
                "override_file": "pbp_stat_overrides",
                "override_key": "UnknownDistance2ptDefRebounds:-1",
                "consensus_class": "documented_box_vs_pbp_source_conflict",
                "tpdev_status": "player_present",
                "recommended_action": "keep_production_override_and_document",
                "notes": "example",
            }
        ]
    )
    conflicts_path = tmp_path / "conflicts.csv"
    conflicts.to_csv(conflicts_path, index=False)

    row_bbr = pd.DataFrame(
        [{"game_id": "21600096", "bbr_status": "bbr_supports_move_after"}]
    )
    row_bbr_path = tmp_path / "row_bbr.csv"
    row_bbr.to_csv(row_bbr_path, index=False)

    fork = pd.DataFrame(
        [
            {
                "rule_id": "processor.1_previous_event_is_sub_timeout_(type_8_or_9)",
                "recommended_disposition": "manual_override_candidate",
                "raw_no_row_sample_games": "0021700041|0021800715",
            },
            {
                "rule_id": "turnover.no_turnover_gate",
                "recommended_disposition": "keep_in_fork_feed_semantics_guard",
                "raw_no_row_sample_games": "",
            },
        ]
    )
    fork_path = tmp_path / "fork.csv"
    fork.to_csv(fork_path, index=False)

    summary, grouped = build_summary(
        manifest_path=manifest_path,
        provenance_path=provenance_path,
        consensus_path=consensus_path,
        source_conflict_path=conflicts_path,
        row_bbr_audit_path=row_bbr_path,
        fork_disposition_path=fork_path,
    )

    assert summary["seasons_frozen"] == 24
    assert summary["override_provenance"]["counts_by_file"]["pbp_row_overrides"] == 1
    assert summary["documented_source_conflicts"]["games"] == 1
    assert summary["fork_repair_disposition"]["manual_override_candidate_games"] == ["0021700041", "0021800715"]
    assert grouped.iloc[0]["game_id"] == "21600096"
