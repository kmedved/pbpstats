from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent
DEFAULT_CATALOG_PATH = ROOT / "fork_repair_catalog_20260315_v1" / "fork_repair_catalog.csv"
DEFAULT_USAGE_DIR = ROOT / "fork_repair_usage_20260315_v1"
DEFAULT_NO_TURNOVER_SUMMARY = ROOT / "audit_no_turnover_gate_20260315_v1" / "summary.json"
DEFAULT_OUTPUT_DIR = ROOT / "fork_repair_disposition_20260315_v1"


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_usage(catalog: pd.DataFrame, usage: pd.DataFrame) -> pd.DataFrame:
    cols = ["rule_id", "current_production", "raw_no_row_overrides", "usage_class"]
    if usage.empty:
        usage = pd.DataFrame(columns=cols)
    else:
        usage = usage[cols].copy()

    merged = catalog.merge(usage, on="rule_id", how="left")
    merged["current_production"] = pd.to_numeric(merged["current_production"], errors="coerce").fillna(0).astype(int)
    merged["raw_no_row_overrides"] = (
        pd.to_numeric(merged["raw_no_row_overrides"], errors="coerce").fillna(0).astype(int)
    )
    merged["usage_class"] = merged["usage_class"].fillna("not_observed_in_row_override_audit")
    return merged


def _build_sample_map(rule_summary: pd.DataFrame) -> dict[tuple[str, str], str]:
    if rule_summary.empty:
        return {}
    sample_map: dict[tuple[str, str], str] = {}
    for _, row in rule_summary.iterrows():
        sample_map[(str(row["mode"]), str(row["rule_id"]))] = str(row.get("sample_games", ""))
    return sample_map


def _classify_row_rule(row: pd.Series) -> tuple[str, str]:
    rule_id = str(row["rule_id"])
    current_hits = int(row["current_production"])
    raw_hits = int(row["raw_no_row_overrides"])

    if rule_id == "processor.fallback.delete_orphan_rebound":
        return (
            "keep_safety_fallback",
            "Not active in the cleaned production path, but still catches 31 broken raw-no-row variants and should remain as the last-resort guardrail.",
        )

    if current_hits > 0:
        if current_hits >= 5:
            return (
                "keep_in_fork_broadly_active",
                f"Still fires in {current_hits} cleaned production games, so it is doing real work beyond manual row surgery.",
            )
        if current_hits >= 2:
            return (
                "keep_in_fork_narrow_but_active",
                f"Still fires in {current_hits} cleaned production games, which is narrow but enough to justify keeping it in the fork for now.",
            )
        return (
            "keep_in_fork_single_game_active",
            "Still fires in one cleaned production game, so it remains part of the effective production behavior even if it is very narrow.",
        )

    if raw_hits > 0:
        return (
            "manual_override_candidate",
            f"Only activates after row overrides are stripped out ({raw_hits} raw-no-row hits), so this is the clearest candidate to replace with manual row surgery if we want a smaller fork.",
        )

    return (
        "keep_in_fork_unobserved",
        "Not observed in the row-override audit, but it remains cataloged as a historical repair and should stay unless separately disproven.",
    )


def _classify_rule(row: pd.Series, no_turnover_summary: dict[str, Any]) -> tuple[str, str]:
    rule_id = str(row["rule_id"])
    category = str(row["category"])

    if rule_id == "turnover.no_turnover_dead_ball_gate":
        counts = no_turnover_summary.get("variant_comparison_counts", {})
        always = counts.get("always", {})
        never = counts.get("never", {})
        return (
            "keep_in_fork_proven_by_impacted_game_audit",
            "Full impacted-game audit supports this gate: disabling it is worse in "
            f"{never.get('variant_worse', 0)} of {never.get('variant_worse', 0) + never.get('same', 0)} never-apply variants, "
            "while always-applying it is usually neutral and only rarely worse.",
        )

    if category == "feed_semantics_guard":
        return (
            "keep_in_fork_feed_semantics_guard",
            "This is a feed-semantics rule rather than historical row surgery, so it belongs in the fork unless a broader contradiction is found.",
        )

    if category == "malformed_event_guard":
        return (
            "keep_in_fork_malformed_event_guard",
            "This protects the parser from malformed legacy events and should stay in the fork as a general guardrail.",
        )

    if rule_id == "ordering.patch_start_of_periods_preserve_raw_order":
        return (
            "keep_in_fork_structural_ordering_fix",
            "This is a structural ordering fix that prevents synthetic period starts from globally re-sorting the raw game chronology.",
        )

    return _classify_row_rule(row)


def build_report(
    catalog_path: Path = DEFAULT_CATALOG_PATH,
    usage_dir: Path = DEFAULT_USAGE_DIR,
    no_turnover_summary_path: Path = DEFAULT_NO_TURNOVER_SUMMARY,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    catalog = _load_csv(catalog_path)
    usage = _load_csv(usage_dir / "fork_repair_usage_rule_comparison.csv")
    rule_summary = _load_csv(usage_dir / "fork_repair_usage_rule_summary.csv")
    no_turnover_summary = _load_json(no_turnover_summary_path)

    if catalog.empty:
        raise FileNotFoundError(f"Missing fork repair catalog: {catalog_path}")

    report = _normalize_usage(catalog, usage)
    sample_map = _build_sample_map(rule_summary)

    dispositions: list[str] = []
    rationales: list[str] = []
    current_samples: list[str] = []
    raw_samples: list[str] = []

    for _, row in report.iterrows():
        disposition, rationale = _classify_rule(row, no_turnover_summary)
        dispositions.append(disposition)
        rationales.append(rationale)
        current_samples.append(sample_map.get(("current_production", str(row["rule_id"])), ""))
        raw_samples.append(sample_map.get(("raw_no_row_overrides", str(row["rule_id"])), ""))

    report["recommended_disposition"] = dispositions
    report["disposition_rationale"] = rationales
    report["current_production_sample_games"] = current_samples
    report["raw_no_row_sample_games"] = raw_samples

    manual_candidates = report[report["recommended_disposition"] == "manual_override_candidate"].copy()
    summary = {
        "rows": int(len(report)),
        "counts_by_disposition": report["recommended_disposition"].value_counts(dropna=False).to_dict(),
        "manual_override_candidate_rules": manual_candidates["rule_id"].tolist(),
        "manual_override_candidate_games": sorted(
            {
                game_id
                for sample_str in manual_candidates["raw_no_row_sample_games"].astype(str)
                for game_id in sample_str.split("|")
                if game_id
            }
        ),
    }
    return report, summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a disposition report for pbpstats historical fork repair families"
    )
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG_PATH)
    parser.add_argument("--usage-dir", type=Path, default=DEFAULT_USAGE_DIR)
    parser.add_argument("--no-turnover-summary", type=Path, default=DEFAULT_NO_TURNOVER_SUMMARY)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    report, summary = build_report(args.catalog, args.usage_dir, args.no_turnover_summary)
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    report.to_csv(output_dir / "fork_repair_disposition_report.csv", index=False)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
