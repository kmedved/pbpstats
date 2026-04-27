from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Dict, List

import pandas as pd

from pbpstats.offline.row_overrides import (
    apply_pbp_row_overrides,
    load_pbp_row_overrides,
)


CATALOGS_ROOT = Path(__file__).resolve().parent
DEFAULT_HISTORIC_PBP_ROW_OVERRIDES_PATH = CATALOGS_ROOT / "pbp_row_overrides.csv"


@lru_cache(maxsize=1)
def load_historic_pbp_row_overrides(
    path: str | Path = DEFAULT_HISTORIC_PBP_ROW_OVERRIDES_PATH,
) -> Dict[str, List[dict]]:
    return load_pbp_row_overrides(path)


def apply_historic_pbp_row_overrides(
    game_df: pd.DataFrame,
    overrides: Dict[str, List[dict]] | None = None,
) -> pd.DataFrame:
    return apply_pbp_row_overrides(
        game_df,
        overrides if overrides is not None else load_historic_pbp_row_overrides(),
    )
