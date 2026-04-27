import pandas as pd

from historic_backfill.catalogs.pbp_stat_overrides import apply_pbp_stat_overrides, load_pbp_stat_overrides


def test_load_pbp_stat_overrides_normalizes_rows(tmp_path):
    path = tmp_path / "pbp_stat_overrides.csv"
    path.write_text(
        "\n".join(
            [
                "game_id,team_id,player_id,stat_key,stat_value,notes",
                "29600370,1610612742,45,AssistedArc3,1,add missing McCloud make",
                "29600370,1610612742,469,Arc3Assists,1,add missing Mashburn assist",
                "bad,1610612742,45,AssistedArc3,1,ignored",
            ]
        ),
        encoding="utf-8",
    )

    overrides = load_pbp_stat_overrides(path)

    assert list(overrides) == ["0029600370"]
    assert overrides["0029600370"][0]["player_id"] == 45
    assert overrides["0029600370"][0]["stat_key"] == "AssistedArc3"
    assert overrides["0029600370"][1]["player_id"] == 469


def test_apply_pbp_stat_overrides_appends_matching_game_rows():
    base_rows = [
        {
            "player_id": 45,
            "team_id": 1610612742,
            "stat_key": "AssistedArc3",
            "stat_value": 6.0,
        }
    ]
    overrides = {
        "0029600370": [
            {
                "player_id": 45,
                "team_id": 1610612742,
                "stat_key": "AssistedArc3",
                "stat_value": 1.0,
                "notes": "add missing McCloud make",
            },
            {
                "player_id": 469,
                "team_id": 1610612742,
                "stat_key": "Arc3Assists",
                "stat_value": 1.0,
                "notes": "add missing Mashburn assist",
            },
        ]
    }

    result = apply_pbp_stat_overrides("0029600370", base_rows, overrides=overrides)
    result_df = pd.DataFrame(result)

    assert len(result) == 3
    assert (
        result_df.groupby(["player_id", "team_id", "stat_key"])["stat_value"].sum().loc[(45, 1610612742, "AssistedArc3")]
        == 7.0
    )
    assert (
        result_df.groupby(["player_id", "team_id", "stat_key"])["stat_value"].sum().loc[(469, 1610612742, "Arc3Assists")]
        == 1.0
    )


def test_load_pbp_stat_overrides_preserves_negative_adjustments(tmp_path):
    path = tmp_path / "pbp_stat_overrides.csv"
    path.write_text(
        "\n".join(
            [
                "game_id,team_id,player_id,stat_key,stat_value,notes",
                "29900545,1610612737,1898,UnknownDistance2ptOffRebounds,-1,shift rebound side",
                "29900545,1610612737,1898,UnknownDistance2ptDefRebounds,1,shift rebound side",
            ]
        ),
        encoding="utf-8",
    )

    overrides = load_pbp_stat_overrides(path)

    assert list(overrides) == ["0029900545"]
    assert overrides["0029900545"][0]["stat_value"] == -1.0
    assert overrides["0029900545"][1]["stat_value"] == 1.0
