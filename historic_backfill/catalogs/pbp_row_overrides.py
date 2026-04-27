"""Compatibility wrappers for the historic PBP row override catalog."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pandas as pd

from historic_backfill.catalogs.loader import (
    DEFAULT_HISTORIC_PBP_ROW_OVERRIDES_PATH,
    apply_historic_pbp_row_overrides,
    load_historic_pbp_row_overrides,
)
from pbpstats.offline.row_overrides import (
    PBP_ROW_OVERRIDE_ACTION_COLUMN,
    PBP_ROW_OVERRIDE_NOTES_COLUMN,
    apply_pbp_row_overrides as apply_pbp_row_overrides_with_catalog,
    load_pbp_row_overrides,
)


def _resolve_default_pbp_row_overrides_path(module_file: str | Path = __file__) -> Path:
    _ignored_module_file = module_file
    return DEFAULT_HISTORIC_PBP_ROW_OVERRIDES_PATH


def apply_pbp_row_overrides(
    game_df: pd.DataFrame,
    overrides: Dict[str, List[dict]] | None = None,
) -> pd.DataFrame:
    if overrides is not None:
        return apply_pbp_row_overrides_with_catalog(game_df, overrides)
    return apply_historic_pbp_row_overrides(game_df)
