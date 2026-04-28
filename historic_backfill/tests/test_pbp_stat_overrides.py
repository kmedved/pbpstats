import pandas as pd
import pytest

from historic_backfill.catalogs.pbp_stat_overrides import (
    apply_pbp_stat_overrides,
    get_pbp_stat_overrides,
    load_pbp_stat_overrides,
    set_pbp_stat_overrides,
)


def test_pbp_stat_overrides_has_no_import_time_catalog_cache():
    import historic_backfill.catalogs.pbp_stat_overrides as mod

    assert mod._PBP_STAT_OVERRIDES is None


def test_get_pbp_stat_overrides_cache_tracks_requested_path(tmp_path):
    import historic_backfill.catalogs.pbp_stat_overrides as mod

    path_a = tmp_path / "a.csv"
    path_b = tmp_path / "b.csv"
    path_a.write_text(
        "\n".join(
            [
                "game_id,team_id,player_id,stat_key,stat_value,notes",
                "29600370,1610612742,45,PTS,1,a",
            ]
        ),
        encoding="utf-8",
    )
    path_b.write_text(
        "\n".join(
            [
                "game_id,team_id,player_id,stat_key,stat_value,notes",
                "29600371,1610612742,46,AST,2,b",
            ]
        ),
        encoding="utf-8",
    )

    try:
        assert list(get_pbp_stat_overrides(path_a)) == ["0029600370"]
        assert list(get_pbp_stat_overrides(path_b)) == ["0029600371"]
    finally:
        mod._PBP_STAT_OVERRIDES = None
        mod._PBP_STAT_OVERRIDE_PATH = None


def test_set_pbp_stat_overrides_none_installs_explicit_empty_catalog(tmp_path):
    import historic_backfill.catalogs.pbp_stat_overrides as mod

    path = tmp_path / "pbp_stat_overrides.csv"
    path.write_text(
        "\n".join(
            [
                "game_id,team_id,player_id,stat_key,stat_value,notes",
                "29600370,1610612742,45,PTS,1,a",
            ]
        ),
        encoding="utf-8",
    )

    try:
        set_pbp_stat_overrides(None)
        assert mod._PBP_STAT_OVERRIDE_PATH is None
        assert get_pbp_stat_overrides() == {}
        assert get_pbp_stat_overrides(path) == {"0029600370": [
            {
                "team_id": 1610612742,
                "player_id": 45,
                "stat_key": "PTS",
                "stat_value": 1.0,
                "notes": "a",
            }
        ]}
    finally:
        mod._PBP_STAT_OVERRIDES = None
        mod._PBP_STAT_OVERRIDE_PATH = None


def test_load_pbp_stat_overrides_normalizes_rows(tmp_path):
    path = tmp_path / "pbp_stat_overrides.csv"
    path.write_text(
        "\n".join(
            [
                "game_id,team_id,player_id,stat_key,stat_value,notes",
                "29600370,1610612742,45,AssistedArc3,1,add missing McCloud make",
                "29600370,1610612742,469,Arc3Assists,1,add missing Mashburn assist",
            ]
        ),
        encoding="utf-8",
    )

    overrides = load_pbp_stat_overrides(path)

    assert list(overrides) == ["0029600370"]
    assert overrides["0029600370"][0]["player_id"] == 45
    assert overrides["0029600370"][0]["stat_key"] == "AssistedArc3"
    assert overrides["0029600370"][1]["player_id"] == 469


def test_load_pbp_stat_overrides_rejects_bad_rows_by_default(tmp_path):
    path = tmp_path / "pbp_stat_overrides.csv"
    path.write_text(
        "\n".join(
            [
                "game_id,team_id,player_id,stat_key,stat_value,notes",
                "bad,1610612742,45,AssistedArc3,1,invalid game",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="invalid game_id"):
        load_pbp_stat_overrides(path)


def test_load_pbp_stat_overrides_can_skip_bad_rows_when_permissive(tmp_path):
    path = tmp_path / "pbp_stat_overrides.csv"
    path.write_text(
        "\n".join(
            [
                "game_id,team_id,player_id,stat_key,stat_value,notes",
                "29600370,1610612742,45,AssistedArc3,1,add missing McCloud make",
                "bad,1610612742,45,AssistedArc3,1,ignored",
            ]
        ),
        encoding="utf-8",
    )

    overrides = load_pbp_stat_overrides(path, strict=False)

    assert list(overrides) == ["0029600370"]
    assert len(overrides["0029600370"]) == 1


def test_load_pbp_stat_overrides_rejects_duplicate_keys(tmp_path):
    path = tmp_path / "pbp_stat_overrides.csv"
    path.write_text(
        "\n".join(
            [
                "game_id,team_id,player_id,stat_key,stat_value,notes",
                "29600370,1610612742,45,AssistedArc3,1,first",
                "29600370,1610612742,45,AssistedArc3,1,duplicate",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicates PBP stat override"):
        load_pbp_stat_overrides(path)


@pytest.mark.parametrize("bad_value", ["nan", "inf", "-inf"])
def test_load_pbp_stat_overrides_rejects_non_finite_values(tmp_path, bad_value):
    path = tmp_path / "pbp_stat_overrides.csv"
    path.write_text(
        "\n".join(
            [
                "game_id,team_id,player_id,stat_key,stat_value,notes",
                f"29600370,1610612742,45,AssistedArc3,{bad_value},bad",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="non-finite"):
        load_pbp_stat_overrides(path)


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

    float_like_result = apply_pbp_stat_overrides(
        "29600370.0",
        [],
        overrides=overrides,
    )
    assert len(float_like_result) == 2


def test_apply_pbp_stat_overrides_explicit_empty_catalog_suppresses_global(
    monkeypatch,
):
    import historic_backfill.catalogs.pbp_stat_overrides as mod

    monkeypatch.setattr(
        mod,
        "_PBP_STAT_OVERRIDES",
        {
            "0029600370": [
                {
                    "player_id": 45,
                    "team_id": 1610612742,
                    "stat_key": "PTS",
                    "stat_value": 99.0,
                    "notes": "global should not apply",
                }
            ]
        },
    )

    assert mod.apply_pbp_stat_overrides("0029600370", [], overrides={}) == []


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
