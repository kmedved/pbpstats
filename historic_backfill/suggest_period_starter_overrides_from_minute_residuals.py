from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path

import pandas as pd


def _normalize_game_id(game_id) -> str:
    return str(game_id).zfill(10)


def _override_game_key(game_id) -> str:
    return str(int(_normalize_game_id(game_id)))


def _parse_list(value):
    if isinstance(value, list):
        return value
    if pd.isna(value):
        return []
    try:
        parsed = ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return []
    return parsed if isinstance(parsed, list) else []


def _load_parquet_lookup(parquet_path: Path) -> dict[tuple[str, int], dict]:
    df = pd.read_parquet(parquet_path)
    lookup: dict[tuple[str, int], dict] = {}
    for row in df.itertuples(index=False):
        game_id = _normalize_game_id(row.game_id)
        period = int(row.period)
        lookup[(game_id, period)] = row._asdict()
    return lookup


def _team_players_from_parquet_row(row: dict, team_id: int) -> list[int]:
    if int(row["away_team_id"]) == int(team_id):
        return [int(row[f"away_player{i}"]) for i in range(1, 6)]
    if int(row["home_team_id"]) == int(team_id):
        return [int(row[f"home_player{i}"]) for i in range(1, 6)]
    return []


def build_candidates(
    period_audit_csv: Path,
    stints_dir: Path,
    parquet_path: Path,
    threshold_seconds: float,
    pair_tolerance_seconds: float,
) -> list[dict]:
    audit_df = pd.read_csv(period_audit_csv)
    parquet_lookup = _load_parquet_lookup(parquet_path)
    candidates: list[dict] = []

    mismatch_rows = audit_df[~audit_df["starter_sets_match"]].copy()
    for row in mismatch_rows.itertuples(index=False):
        game_id = _normalize_game_id(row.game_id)
        period = int(row.period)
        team_id = int(row.team_id)

        parquet_row = parquet_lookup.get((game_id, period))
        if parquet_row is None:
            continue

        parquet_players = _team_players_from_parquet_row(parquet_row, team_id)
        if len(parquet_players) != 5:
            continue

        current_players = [int(pid) for pid in _parse_list(row.current_starter_ids)]
        if set(current_players) == set(parquet_players):
            continue

        recon_path = stints_dir / game_id / "player_minutes_recon.csv"
        if not recon_path.exists():
            continue

        recon_df = pd.read_csv(recon_path)
        team_df = recon_df[recon_df["team_id"].astype(int) == team_id].copy()
        if team_df.empty:
            continue

        team_df["output_diff_vs_official_seconds"] = pd.to_numeric(
            team_df["output_diff_vs_official_seconds"], errors="coerce"
        )
        large_df = team_df[
            team_df["output_diff_vs_official_seconds"].abs() >= threshold_seconds
        ].copy()
        if large_df.empty:
            continue

        pos_df = large_df[large_df["output_diff_vs_official_seconds"] >= threshold_seconds]
        neg_df = large_df[large_df["output_diff_vs_official_seconds"] <= -threshold_seconds]
        if pos_df.empty or neg_df.empty:
            continue

        pos_total = float(pos_df["output_diff_vs_official_seconds"].sum())
        neg_total = float(neg_df["output_diff_vs_official_seconds"].sum())
        if abs(pos_total + neg_total) > pair_tolerance_seconds:
            continue

        candidates.append(
            {
                "game_id": game_id,
                "override_game_id": _override_game_key(game_id),
                "period": period,
                "team_id": team_id,
                "current_starter_ids": current_players,
                "current_starter_names": _parse_list(row.current_starter_names),
                "parquet_starter_ids": parquet_players,
                "missing_from_current_names": _parse_list(row.missing_from_current_names),
                "extra_in_current_names": _parse_list(row.extra_in_current_names),
                "large_positive_players": pos_df[
                    ["player_name", "output_diff_vs_official_seconds"]
                ].to_dict(orient="records"),
                "large_negative_players": neg_df[
                    ["player_name", "output_diff_vs_official_seconds"]
                ].to_dict(orient="records"),
                "resolver_mode": parquet_row.get("resolver_mode"),
                "window_seconds": parquet_row.get("window_seconds"),
                "pos_total_seconds": pos_total,
                "neg_total_seconds": neg_total,
            }
        )

    return candidates


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Suggest period starter overrides from large official minute residuals."
    )
    parser.add_argument("--period-audit-csv", type=Path, required=True)
    parser.add_argument("--stints-dir", type=Path, required=True)
    parser.add_argument("--parquet-path", type=Path, required=True)
    parser.add_argument("--threshold-seconds", type=float, default=290.0)
    parser.add_argument("--pair-tolerance-seconds", type=float, default=5.0)
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args()

    candidates = build_candidates(
        period_audit_csv=args.period_audit_csv,
        stints_dir=args.stints_dir,
        parquet_path=args.parquet_path,
        threshold_seconds=args.threshold_seconds,
        pair_tolerance_seconds=args.pair_tolerance_seconds,
    )

    print(json.dumps(candidates, indent=2))
    if args.output_json is not None:
        args.output_json.write_text(json.dumps(candidates, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
