import pandas as pd

from historic_backfill.audits.cross_source.recheck_overrides_against_bbr_pbp import _load_player_crosswalk, _row_override_game_rows


class _StubContext:
    def __init__(self, merged_by_game):
        self._merged_by_game = merged_by_game

    def merged_player_stats(self, game_id: str) -> pd.DataFrame:
        return self._merged_by_game[game_id].copy()


def test_row_override_game_rows_marks_pure_row_games_separately():
    row_overrides = pd.DataFrame(
        [
            {"game_id": "29600001", "action": "move_before", "event_num": "10", "anchor_event_num": "11"},
            {"game_id": "29600001", "action": "drop", "event_num": "12", "anchor_event_num": ""},
            {"game_id": "29600002", "action": "move_after", "event_num": "20", "anchor_event_num": "21"},
        ]
    )
    merged = {
        "0029600001": pd.DataFrame(
            [
                {
                    "player_id": 1,
                    "team_id": 1,
                    "PARSER_PTS": 10,
                    "BBR_PTS": 10,
                    "PARSER_AST": 2,
                    "BBR_AST": 2,
                    "PARSER_STL": 0,
                    "BBR_STL": 0,
                    "PARSER_BLK": 0,
                    "BBR_BLK": 0,
                    "PARSER_TOV": 1,
                    "BBR_TOV": 1,
                    "PARSER_FGA": 5,
                    "BBR_FGA": 5,
                    "PARSER_FGM": 4,
                    "BBR_FGM": 4,
                    "PARSER_3PA": 1,
                    "BBR_3PA": 1,
                    "PARSER_3PM": 0,
                    "BBR_3PM": 0,
                    "PARSER_FTA": 2,
                    "BBR_FTA": 2,
                    "PARSER_FTM": 2,
                    "BBR_FTM": 2,
                    "PARSER_OREB": 1,
                    "BBR_OREB": 1,
                    "PARSER_DRB": 3,
                    "BBR_DRB": 3,
                    "PARSER_REB": 4,
                    "BBR_REB": 4,
                }
            ]
        ),
        "0029600002": pd.DataFrame(
            [
                {
                    "player_id": 2,
                    "team_id": 2,
                    "PARSER_PTS": 8,
                    "BBR_PTS": 7,
                    "PARSER_AST": 1,
                    "BBR_AST": 1,
                    "PARSER_STL": 0,
                    "BBR_STL": 0,
                    "PARSER_BLK": 0,
                    "BBR_BLK": 0,
                    "PARSER_TOV": 1,
                    "BBR_TOV": 1,
                    "PARSER_FGA": 4,
                    "BBR_FGA": 4,
                    "PARSER_FGM": 3,
                    "BBR_FGM": 3,
                    "PARSER_3PA": 1,
                    "BBR_3PA": 1,
                    "PARSER_3PM": 0,
                    "BBR_3PM": 0,
                    "PARSER_FTA": 2,
                    "BBR_FTA": 2,
                    "PARSER_FTM": 2,
                    "BBR_FTM": 2,
                    "PARSER_OREB": 0,
                    "BBR_OREB": 0,
                    "PARSER_DRB": 2,
                    "BBR_DRB": 2,
                    "PARSER_REB": 2,
                    "BBR_REB": 2,
                }
            ]
        ),
    }
    context = _StubContext(merged)

    rows = _row_override_game_rows(
        row_overrides,
        context,
        stat_override_games={"0029600002"},
        audit_override_games=set(),
        source_override_games=set(),
        validation_override_games=set(),
    )
    rows_by_game = {row["game_id"]: row for row in rows}

    assert rows_by_game["0029600001"]["row_override_only"] is True
    assert rows_by_game["0029600001"]["other_override_files"] == ""
    assert rows_by_game["0029600001"]["status"] == "parser_matches_bbr"

    assert rows_by_game["0029600002"]["row_override_only"] is False
    assert rows_by_game["0029600002"]["other_override_files"] == "pbp_stat_overrides"
    assert rows_by_game["0029600002"]["status"] == "needs_review_mixed_overrides"


def test_load_player_crosswalk_reads_nba_and_alt_ids(tmp_path):
    path = tmp_path / "player_master_crosswalk.csv"
    path.write_text(
        "\n".join(
            [
                "player_name,nba_id,alt_nba_id,bbr_id",
                "Player One,123,,onepl01",
                "Player Two,,456,twopl01",
                "Player Three,789,790,threep01",
                "Bad Row,,,",
            ]
        ),
        encoding="utf-8",
    )

    result = _load_player_crosswalk(path)

    assert set(result.columns) == {"player_id", "bbr_slug"}
    assert set(map(tuple, result.sort_values(["player_id", "bbr_slug"]).to_records(index=False))) == {
        (123, "onepl01"),
        (456, "twopl01"),
        (789, "threep01"),
        (790, "threep01"),
    }
