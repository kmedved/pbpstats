from __future__ import annotations

import argparse
import json
import sqlite3
import textwrap
import zlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Sequence


DEFAULT_NBA_RAW_DB_PATH = Path(__file__).resolve().parent / "nba_raw.db"
DEFAULT_BBR_DB_PATH = (
    Path(__file__).resolve().parent.parent / "33_wowy_rapm" / "bbref_boxscores.db"
)


@dataclass(frozen=True)
class TeamAlias:
    team_id: int
    bbr_code: str
    start: date
    stop: date


TEAM_ALIASES: tuple[TeamAlias, ...] = (
    TeamAlias(1610612737, "ATL", date(1968, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612738, "BOS", date(1946, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612751, "NJN", date(1977, 1, 1), date(2012, 6, 30)),
    TeamAlias(1610612751, "BRK", date(2012, 7, 1), date(9999, 12, 31)),
    TeamAlias(1610612766, "CHH", date(1988, 1, 1), date(2002, 6, 30)),
    TeamAlias(1610612766, "CHA", date(2004, 1, 1), date(2014, 6, 30)),
    TeamAlias(1610612766, "CHO", date(2014, 7, 1), date(9999, 12, 31)),
    TeamAlias(1610612741, "CHI", date(1966, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612739, "CLE", date(1970, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612742, "DAL", date(1980, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612743, "DEN", date(1976, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612765, "DET", date(1957, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612745, "HOU", date(1971, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612754, "IND", date(1976, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612746, "SDC", date(1978, 1, 1), date(1984, 6, 30)),
    TeamAlias(1610612746, "LAC", date(1984, 7, 1), date(9999, 12, 31)),
    TeamAlias(1610612747, "LAL", date(1960, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612763, "VAN", date(1995, 1, 1), date(2001, 6, 30)),
    TeamAlias(1610612763, "MEM", date(2001, 7, 1), date(9999, 12, 31)),
    TeamAlias(1610612748, "MIA", date(1988, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612749, "MIL", date(1968, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612750, "MIN", date(1989, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612740, "NOH", date(2002, 1, 1), date(2013, 6, 30)),
    TeamAlias(1610612740, "NOK", date(2005, 1, 1), date(2007, 6, 30)),
    TeamAlias(1610612740, "NOP", date(2013, 7, 1), date(9999, 12, 31)),
    TeamAlias(1610612752, "NYK", date(1946, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612760, "SEA", date(1967, 1, 1), date(2008, 6, 30)),
    TeamAlias(1610612760, "OKC", date(2008, 7, 1), date(9999, 12, 31)),
    TeamAlias(1610612753, "ORL", date(1989, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612755, "PHI", date(1963, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612756, "PHO", date(1968, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612757, "POR", date(1970, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612758, "KCK", date(1975, 1, 1), date(1985, 6, 30)),
    TeamAlias(1610612758, "SAC", date(1985, 7, 1), date(9999, 12, 31)),
    TeamAlias(1610612759, "SAS", date(1976, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612744, "GSW", date(1971, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612761, "TOR", date(1995, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612762, "UTA", date(1979, 1, 1), date(9999, 12, 31)),
    TeamAlias(1610612764, "WSB", date(1974, 1, 1), date(1997, 6, 30)),
    TeamAlias(1610612764, "WAS", date(1997, 7, 1), date(9999, 12, 31)),
)


@dataclass(frozen=True)
class NbaGameContext:
    nba_game_id: str
    game_date: date
    home_team_id: int
    away_team_id: int
    home_team_abbr: str | None
    away_team_abbr: str | None


@dataclass(frozen=True)
class BbrGameMatch:
    bbr_game_id: str
    game_url: str
    away_team: str
    home_team: str


def _open_sqlite_readonly(path: Path | str) -> sqlite3.Connection:
    db_path = Path(path)
    return sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)


def _load_json_blob(raw_value: bytes | str | memoryview) -> dict:
    if isinstance(raw_value, memoryview):
        raw_value = raw_value.tobytes()
    if isinstance(raw_value, bytes):
        try:
            raw_value = zlib.decompress(raw_value).decode("utf-8")
        except zlib.error:
            raw_value = raw_value.decode("utf-8")
    if isinstance(raw_value, str):
        return json.loads(raw_value)
    raise TypeError(f"Unsupported raw response type: {type(raw_value)!r}")


def _result_set_by_name(payload: dict, name: str) -> dict | None:
    for result_set in payload.get("resultSets", []):
        if result_set.get("name") == name:
            return result_set
    return None


def _rows_as_dicts(result_set: dict | None) -> list[dict]:
    if not result_set:
        return []
    headers = result_set.get("headers", [])
    return [dict(zip(headers, row)) for row in result_set.get("rowSet", [])]


def load_nba_game_context(
    nba_game_id: str,
    *,
    nba_raw_db_path: Path | str = DEFAULT_NBA_RAW_DB_PATH,
) -> NbaGameContext:
    conn = _open_sqlite_readonly(nba_raw_db_path)
    try:
        row = conn.execute(
            "SELECT data FROM raw_responses WHERE game_id = ? AND endpoint = 'summary' LIMIT 1",
            (str(nba_game_id).zfill(10),),
        ).fetchone()
        if row is None:
            raise ValueError(f"No summary payload found for NBA game {nba_game_id}")
        payload = _load_json_blob(row[0])
    finally:
        conn.close()

    summary_rows = _rows_as_dicts(_result_set_by_name(payload, "GameSummary"))
    if not summary_rows:
        raise ValueError(f"Summary payload for {nba_game_id} is missing GameSummary")
    summary = summary_rows[0]

    line_rows = _rows_as_dicts(_result_set_by_name(payload, "LineScore"))
    home_team_id = int(summary["HOME_TEAM_ID"])
    away_team_id = int(summary["VISITOR_TEAM_ID"])

    home_team_abbr = None
    away_team_abbr = None
    for row_dict in line_rows:
        team_id = int(row_dict["TEAM_ID"])
        if team_id == home_team_id:
            home_team_abbr = row_dict.get("TEAM_ABBREVIATION")
        elif team_id == away_team_id:
            away_team_abbr = row_dict.get("TEAM_ABBREVIATION")

    game_date = date.fromisoformat(str(summary["GAME_DATE_EST"])[:10])
    return NbaGameContext(
        nba_game_id=str(nba_game_id).zfill(10),
        game_date=game_date,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        home_team_abbr=home_team_abbr,
        away_team_abbr=away_team_abbr,
    )


def candidate_bbr_team_codes(team_id: int, game_date: date) -> list[str]:
    matches = [
        alias.bbr_code
        for alias in TEAM_ALIASES
        if alias.team_id == team_id and alias.start <= game_date <= alias.stop
    ]
    if not matches:
        raise ValueError(f"No BBR team alias match for team_id={team_id} on {game_date.isoformat()}")
    return sorted(set(matches))


def find_bbr_game_for_nba_game(
    nba_game_id: str,
    *,
    nba_raw_db_path: Path | str = DEFAULT_NBA_RAW_DB_PATH,
    bbr_db_path: Path | str = DEFAULT_BBR_DB_PATH,
) -> tuple[NbaGameContext, list[BbrGameMatch]]:
    context = load_nba_game_context(nba_game_id, nba_raw_db_path=nba_raw_db_path)
    home_codes = candidate_bbr_team_codes(context.home_team_id, context.game_date)
    away_codes = candidate_bbr_team_codes(context.away_team_id, context.game_date)
    date_prefix = context.game_date.strftime("%Y%m%d")

    home_placeholders = ",".join("?" for _ in home_codes)
    away_placeholders = ",".join("?" for _ in away_codes)
    sql = textwrap.dedent(
        f"""
        SELECT game_id, url, away_team, home_team
        FROM games
        WHERE game_id LIKE ?
          AND home_team IN ({home_placeholders})
          AND away_team IN ({away_placeholders})
        ORDER BY game_id
        """
    ).strip()
    params: list[str] = [f"{date_prefix}%"] + home_codes + away_codes

    conn = _open_sqlite_readonly(bbr_db_path)
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    matches = [
        BbrGameMatch(
            bbr_game_id=row[0],
            game_url=row[1],
            away_team=row[2],
            home_team=row[3],
        )
        for row in rows
    ]
    return context, matches


def load_bbr_play_by_play_rows(
    bbr_game_id: str,
    *,
    bbr_db_path: Path | str = DEFAULT_BBR_DB_PATH,
    period: int | None = None,
    clock: str | None = None,
    contains: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    clauses = ["game_id = ?"]
    params: list[object] = [bbr_game_id]
    if period is not None:
        clauses.append("period = ?")
        params.append(period)
    if clock is not None:
        clauses.append("REPLACE(COALESCE(game_clock, ''), '.0', '') = ?")
        params.append(str(clock).replace(".0", ""))
    if contains:
        clauses.append("(COALESCE(away_play, '') || ' ' || COALESCE(home_play, '')) LIKE ?")
        params.append(f"%{contains}%")

    sql = textwrap.dedent(
        f"""
        SELECT event_index, period, game_clock, score_away, score_home,
               away_play, home_play, away_player_ids, home_player_ids, is_colspan_row
        FROM play_by_play
        WHERE {" AND ".join(clauses)}
        ORDER BY event_index
        """
    ).strip()
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    conn = _open_sqlite_readonly(bbr_db_path)
    try:
        cursor = conn.execute(sql, params)
        headers = [col[0] for col in cursor.description]
        return [dict(zip(headers, row)) for row in cursor.fetchall()]
    finally:
        conn.close()


def format_bbr_rows(rows: Sequence[dict]) -> str:
    lines: list[str] = []
    for row in rows:
        play_text = row["away_play"] or row["home_play"] or ""
        lines.append(
            f"{row['event_index']:>4}  P{row['period']}  {row['game_clock']:>7}  "
            f"{play_text}"
        )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Map an NBA game id to a BBR game id and dump BBR play-by-play rows."
    )
    parser.add_argument("nba_game_id", help="NBA game id, e.g. 0021900287")
    parser.add_argument("--nba-raw-db", default=str(DEFAULT_NBA_RAW_DB_PATH))
    parser.add_argument("--bbr-db", default=str(DEFAULT_BBR_DB_PATH))
    parser.add_argument("--period", type=int)
    parser.add_argument("--clock")
    parser.add_argument("--contains")
    parser.add_argument("--limit", type=int, default=40)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    context, matches = find_bbr_game_for_nba_game(
        args.nba_game_id,
        nba_raw_db_path=args.nba_raw_db,
        bbr_db_path=args.bbr_db,
    )

    print(
        f"NBA {context.nba_game_id}: {context.game_date.isoformat()} "
        f"home_team_id={context.home_team_id} away_team_id={context.away_team_id} "
        f"home_abbr={context.home_team_abbr} away_abbr={context.away_team_abbr}"
    )
    if not matches:
        print("No BBR match found.")
        return 1

    print("BBR matches:")
    for match in matches:
        print(f"  {match.bbr_game_id}  {match.away_team} @ {match.home_team}  {match.game_url}")

    if len(matches) != 1:
        return 0

    rows = load_bbr_play_by_play_rows(
        matches[0].bbr_game_id,
        bbr_db_path=args.bbr_db,
        period=args.period,
        clock=args.clock,
        contains=args.contains,
        limit=args.limit,
    )
    if rows:
        print("\nPlay-by-play rows:")
        print(format_bbr_rows(rows))
    else:
        print("\nNo BBR play-by-play rows matched the requested filters.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
