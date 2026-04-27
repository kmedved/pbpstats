from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_OVERLAY_CSV = ROOT / "reviewed_frontier_policy_overlay_20260322_v1.csv"
DEFAULT_OVERLAY_SUMMARY_JSON = ROOT / "reviewed_frontier_policy_overlay_20260322_v1.summary.json"
DEFAULT_FRONTIER_INVENTORY_CSV = ROOT / "phase6_open_blocker_inventory_20260322_v1.csv"
DEFAULT_FRONTIER_SUMMARY_JSON = ROOT / "phase6_reviewed_frontier_inventory_20260322_v1/summary.json"
DEFAULT_PM_SUMMARY_JSON = ROOT / "phase6_reviewed_pm_reference_report_ABCDE_20260322_v1/summary.json"
DEFAULT_PM_CHARACTERIZATION_CSV = ROOT / "phase6_reviewed_pm_reference_report_ABCDE_20260322_v1/pm_reference_characterization.csv"
DEFAULT_RESIDUAL_ROOT = ROOT / "phase6_reviewed_release_policy_residuals_20260322_v1"
DEFAULT_SIDECAR_SUMMARY_JSON = ROOT / "reviewed_release_quality_sidecar_20260322_v1/summary.json"
DEFAULT_SIDECAR_JOIN_CONTRACT_JSON = ROOT / "reviewed_release_quality_sidecar_20260322_v1/join_contract.json"
DEFAULT_COMPILE_SUMMARY_JSON = ROOT / "overrides/correction_manifest_compile_summary.json"
DEFAULT_EXPECTED_OUTSIDE_OVERLAY_GAME_ID = "0029800606"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate the reviewed release-policy artifact set.")
    parser.add_argument("--overlay-csv", type=Path, default=DEFAULT_OVERLAY_CSV)
    parser.add_argument("--overlay-summary-json", type=Path, default=DEFAULT_OVERLAY_SUMMARY_JSON)
    parser.add_argument("--frontier-inventory-csv", type=Path, default=DEFAULT_FRONTIER_INVENTORY_CSV)
    parser.add_argument("--frontier-summary-json", type=Path, default=DEFAULT_FRONTIER_SUMMARY_JSON)
    parser.add_argument("--pm-summary-json", type=Path, default=DEFAULT_PM_SUMMARY_JSON)
    parser.add_argument("--pm-characterization-csv", type=Path, default=DEFAULT_PM_CHARACTERIZATION_CSV)
    parser.add_argument("--residual-root", type=Path, default=DEFAULT_RESIDUAL_ROOT)
    parser.add_argument("--sidecar-summary-json", type=Path, default=DEFAULT_SIDECAR_SUMMARY_JSON)
    parser.add_argument("--sidecar-join-contract-json", type=Path, default=DEFAULT_SIDECAR_JOIN_CONTRACT_JSON)
    parser.add_argument("--compile-summary-json", type=Path, default=DEFAULT_COMPILE_SUMMARY_JSON)
    parser.add_argument("--expected-outside-overlay-game-id", default=DEFAULT_EXPECTED_OUTSIDE_OVERLAY_GAME_ID)
    return parser.parse_args()


def _normalize_game_id(value: object) -> str:
    return str(int(value)).zfill(10)


def _load_game_quality_df(residual_root: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    candidate_paths = []
    root_game_quality = residual_root / "game_quality.csv"
    if root_game_quality.exists():
        candidate_paths.append(root_game_quality)
    candidate_paths.extend(sorted(residual_root.glob("*/game_quality.csv")))
    for path in candidate_paths:
        df = pd.read_csv(path, dtype={"game_id": str})
        if df.empty:
            continue
        df["game_id"] = df["game_id"].map(_normalize_game_id)
        df["source_file"] = str(path.resolve())
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_optional_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return _load_json(path)


def _parse_boolish(value: object) -> bool:
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n", ""}:
        return False
    raise ValueError(f"Unsupported boolean value: {value!r}")


def main() -> int:
    args = parse_args()
    overlay_df = pd.read_csv(args.overlay_csv, dtype={"game_id": str})
    overlay_df["game_id"] = overlay_df["game_id"].map(_normalize_game_id)
    overlay_summary = _load_optional_json(args.overlay_summary_json)
    frontier_summary = _load_json(args.frontier_summary_json)
    pm_summary = _load_json(args.pm_summary_json)
    sidecar_summary = _load_json(args.sidecar_summary_json)
    sidecar_join_contract = _load_json(args.sidecar_join_contract_json)
    pm_df = pd.read_csv(args.pm_characterization_csv, dtype={"game_id": str})
    if not pm_df.empty:
        pm_df["game_id"] = pm_df["game_id"].map(_normalize_game_id)
    game_quality_df = _load_game_quality_df(args.residual_root)
    compile_summary = _load_json(args.compile_summary_json)

    errors: list[str] = []
    overlay_versions = sorted(
        value for value in overlay_df.get("policy_decision_id", pd.Series(dtype=str)).astype(str).unique().tolist() if value
    )
    if len(overlay_versions) > 1:
        errors.append(f"Overlay contains multiple policy_decision_id values: {overlay_versions}")
    expected_policy_overlay_version = overlay_versions[0] if overlay_versions else ""
    expected_frontier_inventory_snapshot_id = args.frontier_inventory_csv.resolve().stem
    expected_release_blocking = sorted(
        overlay_df.loc[overlay_df["blocks_release"].map(_parse_boolish), "game_id"].astype(str).tolist()
    ) if not overlay_df.empty else []
    expected_research_open = sorted(
        overlay_df.loc[overlay_df["research_open"].map(_parse_boolish), "game_id"].astype(str).tolist()
    ) if not overlay_df.empty else []

    if overlay_df["game_id"].duplicated().any():
        errors.append(
            "Overlay contains duplicate game_ids: "
            f"{sorted(overlay_df.loc[overlay_df['game_id'].duplicated(), 'game_id'].unique().tolist())}"
        )
    if (overlay_df["game_id"] == str(args.expected_outside_overlay_game_id)).any():
        errors.append(f"Overlay unexpectedly contains {args.expected_outside_overlay_game_id}")
    if overlay_summary and overlay_summary.get("reviewed_policy_overlay_version") != expected_policy_overlay_version:
        errors.append(
            "Unexpected reviewed_policy_overlay_version in overlay summary: "
            f"{overlay_summary.get('reviewed_policy_overlay_version')}"
        )
    if overlay_summary and overlay_summary.get("frontier_inventory_snapshot_id") != expected_frontier_inventory_snapshot_id:
        errors.append(
            "Unexpected frontier_inventory_snapshot_id in overlay summary: "
            f"{overlay_summary.get('frontier_inventory_snapshot_id')}"
        )
    if overlay_summary and sorted(overlay_summary.get("release_blocking_game_ids") or []) != expected_release_blocking:
        errors.append(
            "Unexpected overlay release_blocking_game_ids: "
            f"{overlay_summary.get('release_blocking_game_ids')}"
        )
    if overlay_summary and sorted(overlay_summary.get("research_open_game_ids") or []) != expected_research_open:
        errors.append(
            f"Unexpected overlay research_open_game_ids: {overlay_summary.get('research_open_game_ids')}"
        )
    if frontier_summary.get("release_blocking_game_count") != len(expected_release_blocking):
        errors.append(
            "Expected release_blocking_game_count="
            f"{len(expected_release_blocking)}, found {frontier_summary.get('release_blocking_game_count')}"
        )
    if frontier_summary.get("research_open_game_count") != len(expected_research_open):
        errors.append(
            "Expected research_open_game_count="
            f"{len(expected_research_open)}, found {frontier_summary.get('research_open_game_count')}"
        )
    if sorted(frontier_summary.get("release_blocking_game_ids") or []) != expected_release_blocking:
        errors.append(
            "Unexpected frontier release_blocking_game_ids: "
            f"{frontier_summary.get('release_blocking_game_ids')}"
        )
    if sorted(frontier_summary.get("research_open_game_ids") or []) != expected_research_open:
        errors.append(
            f"Unexpected research_open_game_ids: {frontier_summary.get('research_open_game_ids')}"
        )
    if frontier_summary.get("reviewed_policy_overlay_version") != expected_policy_overlay_version:
        errors.append(
            "Unexpected reviewed_policy_overlay_version in frontier summary: "
            f"{frontier_summary.get('reviewed_policy_overlay_version')}"
        )
    if frontier_summary.get("frontier_inventory_snapshot_id") != expected_frontier_inventory_snapshot_id:
        errors.append(
            "Unexpected frontier_inventory_snapshot_id in frontier summary: "
            f"{frontier_summary.get('frontier_inventory_snapshot_id')}"
        )
    if frontier_summary.get("tier1_release_ready") is not True:
        errors.append("Expected tier1_release_ready=true")
    expected_tier2_frontier_closed = not expected_research_open
    if frontier_summary.get("tier2_frontier_closed") is not expected_tier2_frontier_closed:
        errors.append(
            "Expected tier2_frontier_closed="
            f"{expected_tier2_frontier_closed}, found {frontier_summary.get('tier2_frontier_closed')}"
        )

    release_blocking_games = sorted(
        game_quality_df.loc[
            game_quality_df["blocks_release"].astype(str).str.lower() == "true",
            "game_id",
        ].tolist()
    ) if not game_quality_df.empty else []
    if release_blocking_games != expected_release_blocking:
        errors.append(
            "Expected residual release-blocking games "
            f"{expected_release_blocking}, found {release_blocking_games}"
        )

    if game_quality_df.empty:
        errors.append("No game_quality.csv files found under residual root")
    else:
        match_outside_overlay = game_quality_df.loc[
            game_quality_df["game_id"] == str(args.expected_outside_overlay_game_id)
        ]
        if match_outside_overlay.empty:
            errors.append(
                f"Expected {args.expected_outside_overlay_game_id} in release-policy residual game_quality outputs"
            )
        else:
            policy_sources = sorted(match_outside_overlay["policy_source"].astype(str).unique().tolist())
            if policy_sources != ["auto_default"]:
                errors.append(
                    f"Expected {args.expected_outside_overlay_game_id} policy_source=auto_default, found {policy_sources}"
                )

    if pm_summary.get("release_blocker_game_count") != len(expected_release_blocking):
        errors.append(
            "Expected PM release_blocker_game_count="
            f"{len(expected_release_blocking)}, found {pm_summary.get('release_blocker_game_count')}"
        )
    if sorted(pm_summary.get("release_blocking_game_ids") or []) != expected_release_blocking:
        errors.append(
            "Unexpected PM release_blocking_game_ids: "
            f"{pm_summary.get('release_blocking_game_ids')}"
        )
    if pm_summary.get("reviewed_policy_overlay_version") != expected_policy_overlay_version:
        errors.append(
            "Unexpected reviewed_policy_overlay_version in PM summary: "
            f"{pm_summary.get('reviewed_policy_overlay_version')}"
        )
    if pm_summary.get("frontier_inventory_snapshot_id") != expected_frontier_inventory_snapshot_id:
        errors.append(
            "Unexpected frontier_inventory_snapshot_id in PM summary: "
            f"{pm_summary.get('frontier_inventory_snapshot_id')}"
        )

    actual_raw_pm_counts = pm_summary.get("class_counts") or {}
    if "pm_residual_class" not in pm_df.columns:
        errors.append("PM characterization CSV is missing raw pm_residual_class column")
    elif "pm_characterization" in pm_df.columns and not pm_df["pm_residual_class"].equals(pm_df["pm_characterization"]):
        errors.append("PM characterization CSV mutated raw pm_residual_class away from pm_characterization")

    release_blocking_pm_games = sorted(
        pm_df.loc[pm_df["release_pm_class"] == "open_actionable_lineup_blocker", "game_id"].unique().tolist()
    ) if not pm_df.empty else []
    if not set(release_blocking_pm_games).issubset(set(release_blocking_games)):
        errors.append(
            "Release-blocking PM games must be a subset of release-blocking game_quality set: "
            f"pm={release_blocking_pm_games}, game_quality={release_blocking_games}"
        )
    for game_id in release_blocking_games:
        game_rows = pm_df.loc[pm_df["game_id"] == game_id] if not pm_df.empty else pd.DataFrame()
        if not game_rows.empty and "open_actionable_lineup_blocker" not in set(game_rows["release_pm_class"].astype(str)):
            errors.append(
                "Release-blocking game with PM rows must contribute open_actionable_lineup_blocker rows: "
                f"{game_id}"
            )

    if sidecar_summary.get("row_count") != int(len(game_quality_df)):
        errors.append(
            f"Expected sidecar row_count={len(game_quality_df)}, found {sidecar_summary.get('row_count')}"
        )
    if sidecar_summary.get("unique_game_count") != int(game_quality_df["game_id"].nunique()):
        errors.append(
            "Expected sidecar unique_game_count="
            f"{game_quality_df['game_id'].nunique()}, found {sidecar_summary.get('unique_game_count')}"
        )
    if sidecar_summary.get("release_blocking_game_count") != len(expected_release_blocking):
        errors.append(
            "Expected sidecar release_blocking_game_count="
            f"{len(expected_release_blocking)}, found {sidecar_summary.get('release_blocking_game_count')}"
        )
    if sidecar_summary.get("reviewed_policy_overlay_version") != expected_policy_overlay_version:
        errors.append(
            "Unexpected reviewed_policy_overlay_version in sidecar summary: "
            f"{sidecar_summary.get('reviewed_policy_overlay_version')}"
        )
    if sidecar_summary.get("frontier_inventory_snapshot_id") != expected_frontier_inventory_snapshot_id:
        errors.append(
            "Unexpected frontier_inventory_snapshot_id in sidecar summary: "
            f"{sidecar_summary.get('frontier_inventory_snapshot_id')}"
        )
    if sorted(sidecar_summary.get("release_blocking_game_ids") or []) != expected_release_blocking:
        errors.append(
            "Unexpected sidecar release_blocking_game_ids: "
            f"{sidecar_summary.get('release_blocking_game_ids')}"
        )
    if sorted(sidecar_summary.get("research_open_game_ids") or []) != expected_research_open:
        errors.append(
            f"Unexpected sidecar research_open_game_ids: {sidecar_summary.get('research_open_game_ids')}"
        )
    if sidecar_summary.get("reviewed_override_game_count") != len(overlay_df):
        errors.append(
            "Expected sidecar reviewed_override_game_count="
            f"{len(overlay_df)}, found {sidecar_summary.get('reviewed_override_game_count')}"
        )
    default_absent = sidecar_join_contract.get("default_absent_row_values") or {}
    if default_absent.get("release_gate_status") != "exact":
        errors.append(f"Expected sidecar default absent release_gate_status=exact, found {default_absent.get('release_gate_status')}")
    if default_absent.get("blocks_release") is not False:
        errors.append(f"Expected sidecar default absent blocks_release=false, found {default_absent.get('blocks_release')}")
    if default_absent.get("research_open") is not False:
        errors.append(f"Expected sidecar default absent research_open=false, found {default_absent.get('research_open')}")

    for key in [
        "active_corrections",
        "active_period_start_corrections",
        "active_window_corrections",
    ]:
        value = compile_summary.get(key)
        if value is None:
            errors.append(f"Compile summary missing required key: {key}")
        else:
            try:
                if int(value) < 0:
                    errors.append(f"Compile summary has negative value for {key}: {value}")
            except Exception:
                errors.append(f"Compile summary has non-integer value for {key}: {value!r}")

    if errors:
        raise SystemExit("Reviewed release-policy validation failed:\n- " + "\n- ".join(errors))

    summary = {
        "overlay_row_count": int(len(overlay_df)),
        "reviewed_policy_overlay_version": expected_policy_overlay_version,
        "frontier_inventory_snapshot_id": expected_frontier_inventory_snapshot_id,
        "release_blocking_game_count": frontier_summary["release_blocking_game_count"],
        "release_blocking_game_ids": frontier_summary["release_blocking_game_ids"],
        "research_open_game_ids": frontier_summary["research_open_game_ids"],
        "pm_raw_class_counts": actual_raw_pm_counts,
        "pm_release_blocker_game_count": pm_summary["release_blocker_game_count"],
        "sidecar_row_count": sidecar_summary["row_count"],
        "sidecar_reviewed_override_game_count": sidecar_summary["reviewed_override_game_count"],
        f"policy_source_{args.expected_outside_overlay_game_id}": (
            sorted(
                game_quality_df.loc[
                    game_quality_df["game_id"] == str(args.expected_outside_overlay_game_id),
                    "policy_source",
                ]
                .astype(str)
                .unique()
                .tolist()
            )
            if not game_quality_df.empty
            else []
        ),
        "active_correction_counts": {
            "active_corrections": compile_summary["active_corrections"],
            "active_period_start_corrections": compile_summary["active_period_start_corrections"],
            "active_window_corrections": compile_summary["active_window_corrections"],
        },
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
