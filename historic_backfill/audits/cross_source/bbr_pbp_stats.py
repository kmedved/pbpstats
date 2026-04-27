from __future__ import annotations

import re
import unicodedata
from collections import Counter, defaultdict
from typing import Iterable

import pandas as pd


BBR_BASIC_STATS = [
    "PTS",
    "AST",
    "STL",
    "BLK",
    "TOV",
    "FGA",
    "FGM",
    "3PA",
    "3PM",
    "FTA",
    "FTM",
    "OREB",
    "DRB",
    "REB",
]

_THREE_POINT_PATTERN = re.compile(r"3-pt|3pt", re.IGNORECASE)
_MOJIBAKE_HINT_PATTERN = re.compile(r"[ÃÅÄÆÐÑØÞ]")


def _repair_common_mojibake(value: str) -> str:
    text = str(value)
    if not _MOJIBAKE_HINT_PATTERN.search(text):
        return text
    try:
        repaired = text.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text
    return repaired


def normalize_person_name(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", _repair_common_mojibake(str(value)))
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.lower().replace(".", " ").replace("'", "").replace("-", " ")
    normalized = re.sub(r"[^a-z0-9 ]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def parse_bbr_player_slugs(value: str | None) -> list[str]:
    if not value:
        return []
    return [slug.strip() for slug in str(value).split(",") if slug.strip()]


def _is_three_point_attempt(text: str) -> bool:
    return bool(_THREE_POINT_PATTERN.search(text))


def _increment(counter: dict[str, Counter], slug: str, stat: str, value: int = 1) -> None:
    if not slug:
        return
    counter[slug][stat] += value


def _parse_single_play(text: str, slugs: list[str], counters: dict[str, Counter]) -> None:
    if not text:
        return

    lowered = text.lower()
    primary = slugs[0] if slugs else ""
    secondary = slugs[1] if len(slugs) > 1 else ""

    if "offensive rebound by team" in lowered or "defensive rebound by team" in lowered:
        return
    if "offensive rebound by" in lowered:
        _increment(counters, primary, "OREB")
        _increment(counters, primary, "REB")
        return
    if "defensive rebound by" in lowered:
        _increment(counters, primary, "DRB")
        _increment(counters, primary, "REB")
        return

    if "turnover" in lowered:
        _increment(counters, primary, "TOV")
        if "steal by" in lowered:
            _increment(counters, secondary, "STL")
        return

    if "free throw" in lowered:
        _increment(counters, primary, "FTA")
        if "makes" in lowered:
            _increment(counters, primary, "FTM")
            _increment(counters, primary, "PTS")
        return

    if "makes" in lowered:
        _increment(counters, primary, "FGA")
        _increment(counters, primary, "FGM")
        if _is_three_point_attempt(lowered):
            _increment(counters, primary, "3PA")
            _increment(counters, primary, "3PM")
            _increment(counters, primary, "PTS", 3)
        else:
            _increment(counters, primary, "PTS", 2)
        if "assist by" in lowered:
            _increment(counters, secondary, "AST")
        return

    if "misses" in lowered:
        _increment(counters, primary, "FGA")
        if _is_three_point_attempt(lowered):
            _increment(counters, primary, "3PA")
        if "block by" in lowered:
            _increment(counters, secondary, "BLK")
        return


def aggregate_bbr_player_stats(play_rows: Iterable[dict]) -> pd.DataFrame:
    counters: dict[str, Counter] = defaultdict(Counter)
    counted_offensive_foul_turnovers: set[tuple[object, object, str]] = set()

    for row in play_rows:
        for play_key, slug_key in (("away_play", "away_player_ids"), ("home_play", "home_player_ids")):
            text = str(row.get(play_key) or "").strip()
            if not text:
                continue
            slugs = parse_bbr_player_slugs(row.get(slug_key))
            primary = slugs[0] if slugs else ""
            lowered = text.lower()
            turnover_key = (row.get("period"), row.get("game_clock"), primary)

            if "offensive foul by" in lowered:
                _increment(counters, primary, "TOV")
                counted_offensive_foul_turnovers.add(turnover_key)
                continue
            if "turnover" in lowered and "offensive foul" in lowered and turnover_key in counted_offensive_foul_turnovers:
                continue
            _parse_single_play(text, slugs, counters)

    records = []
    for slug, stat_counter in sorted(counters.items()):
        record = {"bbr_slug": slug}
        for stat in BBR_BASIC_STATS:
            record[stat] = int(stat_counter.get(stat, 0))
        records.append(record)

    return pd.DataFrame(records, columns=["bbr_slug", *BBR_BASIC_STATS])
