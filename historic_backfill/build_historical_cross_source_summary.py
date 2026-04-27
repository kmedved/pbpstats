from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_MANIFEST_PATH = ROOT / "historical_baseline_manifest_20260315_v2" / "historical_baseline_manifest.json"
DEFAULT_PROVENANCE_PATH = ROOT / "override_provenance_20260315_v2" / "override_provenance_report.csv"
DEFAULT_CONSENSUS_PATH = ROOT / "override_consensus_20260315_v2" / "override_consensus_report.csv"
DEFAULT_SOURCE_CONFLICT_PATH = ROOT / "source_conflict_register_20260315_v2" / "source_conflict_register.csv"
DEFAULT_ROW_BBR_AUDIT_PATH = ROOT / "pbp_row_override_bbr_window_audit_20260315_v2" / "pbp_row_override_bbr_window_audit.csv"
DEFAULT_FORK_DISPOSITION_PATH = ROOT / "fork_repair_disposition_20260315_v2" / "fork_repair_disposition_report.csv"
DEFAULT_OUTPUT_DIR = ROOT / "historical_cross_source_summary_20260315_v1"

TPDEV_DATA_DIR = (
    ROOT.parent / "fixed_data" / "raw_input_data" / "tpdev_data"
).resolve()


def _series_counts(series: pd.Series) -> dict[str, int]:
    return {str(key): int(value) for key, value in series.value_counts(dropna=False).sort_index().items()}


def _first_present(df: pd.DataFrame, *names: str) -> pd.Series:
    for name in names:
        if name in df.columns:
            return df[name]
    raise KeyError(names[0])


def _split_counts(series: pd.Series, delimiter: str = "|") -> dict[str, int]:
    counts: dict[str, int] = {}
    for raw_value in series.fillna(""):
        for part in str(raw_value).split(delimiter):
            token = part.strip()
            if not token:
                continue
            counts[token] = counts.get(token, 0) + 1
    return dict(sorted(counts.items()))


def build_summary(
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    provenance_path: Path = DEFAULT_PROVENANCE_PATH,
    consensus_path: Path = DEFAULT_CONSENSUS_PATH,
    source_conflict_path: Path = DEFAULT_SOURCE_CONFLICT_PATH,
    row_bbr_audit_path: Path = DEFAULT_ROW_BBR_AUDIT_PATH,
    fork_disposition_path: Path = DEFAULT_FORK_DISPOSITION_PATH,
) -> tuple[dict, pd.DataFrame]:
    manifest = json.loads(manifest_path.read_text())
    provenance = pd.read_csv(provenance_path, dtype=str).fillna("")
    consensus = pd.read_csv(consensus_path, dtype=str).fillna("")
    source_conflicts = pd.read_csv(source_conflict_path, dtype=str).fillna("")
    row_bbr_audit = pd.read_csv(row_bbr_audit_path, dtype=str).fillna("")
    fork_disposition = pd.read_csv(fork_disposition_path, dtype=str).fillna("")

    grouped_conflicts = (
        source_conflicts.groupby(["season", "game_id"], dropna=False)
        .agg(
            rows=("override_key", "size"),
            override_files=("override_file", lambda s: "|".join(sorted(set(s)))),
            consensus_classes=("consensus_class", lambda s: "|".join(sorted(set(s)))),
            tpdev_statuses=("tpdev_status", lambda s: "|".join(sorted(set(v for v in s if v)))),
            recommended_actions=("recommended_action", lambda s: "|".join(sorted(set(s)))),
            notes=("notes", lambda s: " || ".join(sorted(set(v for v in s if v)))),
        )
        .reset_index()
        .sort_values(["season", "game_id"])
    )

    summary = {
        "season_range": manifest.get("season_range", {}),
        "seasons_frozen": int(manifest.get("seasons_frozen", 0)),
        "missing_clean_seasons": manifest.get("missing_clean_seasons", []),
        "tpdev_data_paths": {
            "directory": str(TPDEV_DATA_DIR),
            "full_pbp_new": str(TPDEV_DATA_DIR / "full_pbp_new.parq"),
            "tpdev_box": str(TPDEV_DATA_DIR / "tpdev_box.parq"),
            "tpdev_box_new": str(TPDEV_DATA_DIR / "tpdev_box_new.parq"),
            "tpdev_box_cdn": str(TPDEV_DATA_DIR / "tpdev_box_cdn.parq"),
        },
        "override_provenance": {
            "rows": int(len(provenance)),
            "counts_by_file": _series_counts(provenance["override_file"]),
            "counts_by_kind": _series_counts(_first_present(provenance, "override_kind", "kind")),
            "counts_by_scope": _series_counts(_first_present(provenance, "production_scope", "scope")),
            "counts_by_basis_tag": _split_counts(_first_present(provenance, "basis_tags", "basis_tag")),
        },
        "override_consensus": {
            "rows": int(len(consensus)),
            "counts_by_consensus_class": _series_counts(consensus["consensus_class"]),
            "counts_by_recommended_action": _series_counts(consensus["recommended_action"]),
        },
        "documented_source_conflicts": {
            "rows": int(len(source_conflicts)),
            "games": int(source_conflicts["game_id"].nunique()),
            "counts_by_consensus_class": _series_counts(source_conflicts["consensus_class"]),
            "counts_by_tpdev_status": _series_counts(source_conflicts["tpdev_status"]),
            "counts_by_recommended_action": _series_counts(source_conflicts["recommended_action"]),
        },
        "row_override_bbr_window_audit": {
            "rows": int(len(row_bbr_audit)),
            "games": int(row_bbr_audit["game_id"].nunique()),
            "counts_by_bbr_status": _series_counts(row_bbr_audit["bbr_status"]),
        },
        "fork_repair_disposition": {
            "rows": int(len(fork_disposition)),
            "counts_by_recommended_disposition": _series_counts(fork_disposition["recommended_disposition"]),
            "manual_override_candidate_rules": sorted(
                fork_disposition.loc[
                    fork_disposition["recommended_disposition"] == "manual_override_candidate", "rule_id"
                ].tolist()
            ),
            "manual_override_candidate_games": sorted(
                {
                    game_id
                    for sample in fork_disposition.loc[
                        fork_disposition["recommended_disposition"] == "manual_override_candidate",
                        "raw_no_row_sample_games",
                    ]
                    for game_id in str(sample).split("|")
                    if game_id and game_id.lower() != "nan"
                }
            ),
        },
    }

    return summary, grouped_conflicts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a single cross-source summary for the cleaned 1997-2020 historical path."
    )
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--provenance-path", type=Path, default=DEFAULT_PROVENANCE_PATH)
    parser.add_argument("--consensus-path", type=Path, default=DEFAULT_CONSENSUS_PATH)
    parser.add_argument("--source-conflict-path", type=Path, default=DEFAULT_SOURCE_CONFLICT_PATH)
    parser.add_argument("--row-bbr-audit-path", type=Path, default=DEFAULT_ROW_BBR_AUDIT_PATH)
    parser.add_argument("--fork-disposition-path", type=Path, default=DEFAULT_FORK_DISPOSITION_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    summary, grouped_conflicts = build_summary(
        manifest_path=args.manifest_path,
        provenance_path=args.provenance_path,
        consensus_path=args.consensus_path,
        source_conflict_path=args.source_conflict_path,
        row_bbr_audit_path=args.row_bbr_audit_path,
        fork_disposition_path=args.fork_disposition_path,
    )
    (args.output_dir / "historical_cross_source_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True)
    )
    grouped_conflicts.to_csv(args.output_dir / "documented_source_conflicts_by_game.csv", index=False)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
