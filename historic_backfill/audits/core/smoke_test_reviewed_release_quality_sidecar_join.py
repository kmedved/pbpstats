from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_DARKO_PARQUET = ROOT / "darko_1997_2020.parquet"
DEFAULT_SIDECAR_CSV = ROOT / "reviewed_release_quality_sidecar_20260322_v1/game_quality_sparse.csv"
DEFAULT_SIDECAR_SUMMARY_JSON = ROOT / "reviewed_release_quality_sidecar_20260322_v1/summary.json"
DEFAULT_JOIN_CONTRACT_JSON = ROOT / "reviewed_release_quality_sidecar_20260322_v1/join_contract.json"


RELEASE_COLUMNS = [
    "primary_quality_status",
    "release_gate_status",
    "release_reason_code",
    "execution_lane",
    "blocks_release",
    "research_open",
    "policy_source",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke test the reviewed release-quality sidecar join against a historical player-row parquet sample."
    )
    parser.add_argument("--darko-parquet", type=Path, default=DEFAULT_DARKO_PARQUET)
    parser.add_argument("--sidecar-csv", type=Path, default=DEFAULT_SIDECAR_CSV)
    parser.add_argument("--sidecar-summary-json", type=Path, default=DEFAULT_SIDECAR_SUMMARY_JSON)
    parser.add_argument("--join-contract-json", type=Path, default=DEFAULT_JOIN_CONTRACT_JSON)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def _normalize_game_id(value: object) -> str:
    return str(int(value)).zfill(10)


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin({"true", "1", "yes", "y"})


def _read_sample_rows(darko_parquet: Path, sample_game_ids: list[str]) -> pd.DataFrame:
    filters = [("Game_SingleGame", "in", [int(game_id) for game_id in sample_game_ids])]
    try:
        df = pd.read_parquet(darko_parquet, filters=filters)
    except Exception:
        df = pd.read_parquet(darko_parquet)
        df = df.loc[df["Game_SingleGame"].astype(int).isin([int(game_id) for game_id in sample_game_ids])].copy()
    return df


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    sidecar_df = pd.read_csv(args.sidecar_csv.resolve(), dtype={"game_id": str})
    sidecar_summary = _load_json(args.sidecar_summary_json.resolve())
    join_contract = _load_json(args.join_contract_json.resolve())

    if sidecar_df.empty:
        raise ValueError("Sidecar CSV must be non-empty for join smoke test")
    if sidecar_df["game_id"].duplicated().any():
        duplicates = sorted(sidecar_df.loc[sidecar_df["game_id"].duplicated(), "game_id"].astype(str).unique().tolist())
        raise ValueError(f"Sidecar CSV contains duplicate game_id rows: {duplicates}")

    reviewed_override_ids = sorted(
        sidecar_df.loc[sidecar_df["policy_source"].astype(str) == "reviewed_override", "game_id"].astype(str).tolist()
    )
    if not reviewed_override_ids:
        raise ValueError("Expected at least one reviewed_override row in sidecar CSV")

    research_open_game_ids = sorted(sidecar_summary.get("research_open_game_ids") or [])
    default_absent = join_contract.get("default_absent_row_values") or {}
    if not default_absent:
        raise ValueError("Join contract is missing default_absent_row_values")

    all_game_ids = pd.read_parquet(args.darko_parquet.resolve(), columns=["Game_SingleGame"])
    all_game_ids = sorted({_normalize_game_id(value) for value in all_game_ids["Game_SingleGame"].tolist()})
    absent_game_id = next((game_id for game_id in all_game_ids if game_id not in set(sidecar_df["game_id"])), "")
    if not absent_game_id:
        raise ValueError("Could not find an absent game_id for sidecar join smoke test")

    sample_game_ids = reviewed_override_ids + [absent_game_id]
    player_rows = _read_sample_rows(args.darko_parquet.resolve(), sample_game_ids)
    if player_rows.empty:
        raise ValueError("Historical parquet sample returned zero rows")

    player_rows = player_rows.copy()
    player_rows["game_id"] = player_rows["Game_SingleGame"].map(_normalize_game_id)
    merged = player_rows.merge(sidecar_df, on="game_id", how="left")

    for column in RELEASE_COLUMNS:
        merged[column] = merged[column].where(merged[column].notna(), default_absent[column])
    for column in ["blocks_release", "research_open"]:
        merged[column] = _bool_series(merged[column])

    keep_columns = ["game_id"] + [column for column in ["Game_SingleGame", "Player_SingleGame", "player_name"] if column in merged.columns] + RELEASE_COLUMNS
    merged[keep_columns].to_csv(output_dir / "joined_sample.csv", index=False)

    absent_rows = merged.loc[merged["game_id"] == absent_game_id].copy()
    if absent_rows.empty:
        raise ValueError(f"Expected absent game_id {absent_game_id} in joined sample")
    for column, expected in default_absent.items():
        observed = absent_rows[column].iloc[0]
        if column in {"blocks_release", "research_open"}:
            if bool(observed) is not bool(expected):
                raise ValueError(f"Absent game default mismatch for {column}: expected {expected}, found {observed}")
        elif str(observed) != str(expected):
            raise ValueError(f"Absent game default mismatch for {column}: expected {expected}, found {observed}")

    joined_reviewed = merged.loc[merged["game_id"].isin(reviewed_override_ids), ["game_id", *RELEASE_COLUMNS]].drop_duplicates()
    expected_reviewed = sidecar_df.loc[sidecar_df["game_id"].isin(reviewed_override_ids), ["game_id", *RELEASE_COLUMNS]].copy()
    for column in ["blocks_release", "research_open"]:
        joined_reviewed[column] = _bool_series(joined_reviewed[column])
        expected_reviewed[column] = _bool_series(expected_reviewed[column])
    joined_reviewed = joined_reviewed.sort_values("game_id").reset_index(drop=True)
    expected_reviewed = expected_reviewed.sort_values("game_id").reset_index(drop=True)
    if not joined_reviewed.equals(expected_reviewed):
        raise ValueError("Reviewed sidecar rows did not survive the join unchanged")

    joined_research_open_ids = sorted(
        merged.loc[merged["research_open"], "game_id"].astype(str).drop_duplicates().tolist()
    )
    if joined_research_open_ids != research_open_game_ids:
        raise ValueError(
            "Joined research_open game_ids do not match sidecar summary: "
            f"expected {research_open_game_ids}, found {joined_research_open_ids}"
        )

    summary = {
        "darko_parquet": str(args.darko_parquet.resolve()),
        "sidecar_csv": str(args.sidecar_csv.resolve()),
        "sidecar_summary_json": str(args.sidecar_summary_json.resolve()),
        "join_contract_json": str(args.join_contract_json.resolve()),
        "reviewed_policy_overlay_version": sidecar_summary.get("reviewed_policy_overlay_version", ""),
        "frontier_inventory_snapshot_id": sidecar_summary.get("frontier_inventory_snapshot_id", ""),
        "sample_game_count": len(sample_game_ids),
        "sample_player_row_count": int(len(merged)),
        "reviewed_override_game_count": len(reviewed_override_ids),
        "reviewed_override_game_ids": reviewed_override_ids,
        "research_open_game_ids": joined_research_open_ids,
        "absent_game_id": absent_game_id,
        "absent_defaults_verified": True,
        "reviewed_rows_survive_join_unchanged": True,
        "join_passed": True,
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
