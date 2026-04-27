from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
DEFAULT_CONSENSUS_PATH = ROOT / "override_consensus_20260315_v1" / "override_consensus_report.csv"
DEFAULT_OUTPUT_DIR = ROOT / "source_conflict_register_20260315_v1"


def build_register(consensus_path: Path = DEFAULT_CONSENSUS_PATH) -> tuple[pd.DataFrame, dict]:
    df = pd.read_csv(consensus_path, dtype=str).fillna("")
    conflict_mask = (
        df["consensus_class"].str.startswith("documented_")
        | (df["recommended_action"] == "keep_production_override_and_document")
    )
    register = df.loc[conflict_mask].copy()
    register = register.sort_values(
        ["override_file", "game_id", "team_id", "player_id", "override_key"]
    ).reset_index(drop=True)
    summary = {
        "rows": int(len(register)),
        "games": int(register["game_id"].nunique()),
        "counts_by_file": register["override_file"].value_counts(dropna=False).to_dict(),
        "counts_by_consensus_class": register["consensus_class"].value_counts(dropna=False).to_dict(),
    }
    return register, summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a dedicated register of documented historical source conflicts")
    parser.add_argument("--consensus-path", type=Path, default=DEFAULT_CONSENSUS_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    register, summary = build_register(args.consensus_path)
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    register.to_csv(output_dir / "source_conflict_register.csv", index=False)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
