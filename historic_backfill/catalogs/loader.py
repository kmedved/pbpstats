from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Dict, List

import pandas as pd

from pbpstats.offline.row_overrides import (
    VALID_PBP_ROW_OVERRIDE_ACTIONS,
    apply_pbp_row_overrides,
    load_pbp_row_overrides,
)


CATALOGS_ROOT = Path(__file__).resolve().parent
DEFAULT_HISTORIC_PBP_ROW_OVERRIDES_PATH = CATALOGS_ROOT / "pbp_row_overrides.csv"
PBP_ROW_OVERRIDE_REQUIRED_COLUMNS = {
    "game_id",
    "action",
    "event_num",
    "anchor_event_num",
    "notes",
}


@lru_cache(maxsize=1)
def load_historic_pbp_row_overrides(
    path: str | Path = DEFAULT_HISTORIC_PBP_ROW_OVERRIDES_PATH,
) -> Dict[str, List[dict]]:
    return load_pbp_row_overrides(path)


def validate_historic_pbp_row_override_catalog(
    path: str | Path = DEFAULT_HISTORIC_PBP_ROW_OVERRIDES_PATH,
) -> None:
    """Validate the committed historic row override catalog fails loudly."""
    catalog_path = Path(path)
    raw_df = pd.read_csv(catalog_path, dtype=str).fillna("")
    missing_columns = PBP_ROW_OVERRIDE_REQUIRED_COLUMNS - set(raw_df.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Historic PBP row override catalog missing columns: {missing}")

    parsed = load_pbp_row_overrides(catalog_path, strict=True)
    parsed_row_count = sum(len(rows) for rows in parsed.values())
    if parsed_row_count != len(raw_df):
        raise ValueError(
            "Historic PBP row override catalog parsed row count "
            f"{parsed_row_count} does not match CSV row count {len(raw_df)}"
        )

    unknown_actions = sorted(set(raw_df["action"].str.strip().str.lower()) - VALID_PBP_ROW_OVERRIDE_ACTIONS)
    if unknown_actions:
        raise ValueError(f"Unknown historic PBP row override actions: {unknown_actions}")

    canaries = [
        row
        for row in parsed.get("0020400335", [])
        if row["event_num"] == 148 and row["action"] == "insert_sub_before"
    ]
    if len(canaries) != 1:
        raise ValueError("Synthetic substitution canary 0020400335 event 148 is missing")


def apply_historic_pbp_row_overrides(
    game_df: pd.DataFrame,
    overrides: Dict[str, List[dict]] | None = None,
) -> pd.DataFrame:
    return apply_pbp_row_overrides(
        game_df,
        overrides if overrides is not None else load_historic_pbp_row_overrides(),
    )
