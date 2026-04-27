from __future__ import annotations

import json
from pathlib import Path

import historic_backfill.runners.run_lineup_correction_probe as probe


def test_set_correction_statuses_activates_by_episode() -> None:
    manifest = {
        "corrections": [
            {
                "correction_id": "c1",
                "episode_id": "ep1",
                "status": "proposed",
            },
            {
                "correction_id": "c2",
                "episode_id": "ep1",
                "status": "proposed",
            },
            {
                "correction_id": "c3",
                "episode_id": "ep2",
                "status": "active",
            },
        ]
    }

    delta = probe._set_correction_statuses(
        manifest,
        activate_correction_ids=set(),
        activate_episode_ids={"ep1"},
        deactivate_correction_ids=set(),
        deactivate_episode_ids={"ep2"},
    )

    assert delta["activated_correction_ids"] == ["c1", "c2"]
    assert delta["deactivated_correction_ids"] == ["c3"]
    statuses = {row["correction_id"]: row["status"] for row in manifest["corrections"]}
    assert statuses == {"c1": "active", "c2": "active", "c3": "retired"}


def test_main_compile_only_writes_scratch_manifest_and_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    live_file_directory = tmp_path / "live"
    live_overrides_dir = live_file_directory / "overrides"
    live_overrides_dir.mkdir(parents=True)
    (live_overrides_dir / "period_starters_overrides.json").write_text("{}\n", encoding="utf-8")
    (live_overrides_dir / "lineup_window_overrides.json").write_text("{}\n", encoding="utf-8")
    (live_overrides_dir / "period_starters_override_notes.csv").write_text("", encoding="utf-8")
    (live_overrides_dir / "lineup_window_override_notes.csv").write_text("", encoding="utf-8")
    manifest = {
        "manifest_version": "test",
        "corrections": [
            {
                "correction_id": "starter__x",
                "episode_id": "episode__x",
                "status": "proposed",
                "domain": "lineup",
                "scope_type": "period_start",
                "authoring_mode": "explicit",
                "game_id": "29700438",
                "period": 2,
                "team_id": 1610612760,
                "lineup_player_ids": [56, 64, 107, 766, 1425],
                "reason_code": "test",
                "evidence_summary": "test",
                "source_primary": "manual_trace",
                "source_secondary": "unknown",
                "preferred_source": "manual_trace",
                "confidence": "high",
            }
        ],
        "residual_annotations": [],
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    def fake_compile_runtime_views(manifest_obj, *, output_dir, db_path, parquet_path, file_directory):
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "period_starters_overrides.json").write_text('{"29700438":{"2":{"1610612760":[56,64,107,766,1425]}}}\n', encoding="utf-8")
        (output_dir / "lineup_window_overrides.json").write_text("{}\n", encoding="utf-8")
        (output_dir / "period_starters_override_notes.csv").write_text("", encoding="utf-8")
        (output_dir / "lineup_window_override_notes.csv").write_text("", encoding="utf-8")
        (output_dir / "correction_manifest_compile_summary.json").write_text('{"active_corrections":1}\n', encoding="utf-8")
        return {"active_corrections": 1, "output_dir": str(output_dir), "file_directory": str(file_directory)}

    monkeypatch.setattr(probe, "compile_runtime_views", fake_compile_runtime_views)

    exit_code = probe.main(
        [
            "--manifest-path",
            str(manifest_path),
            "--live-file-directory",
            str(live_file_directory),
            "--output-dir",
            str(tmp_path / "probe"),
            "--game-ids",
            "0029700438",
            "--activate-correction-id",
            "starter__x",
            "--compile-only",
        ]
    )

    assert exit_code == 0
    probe_summary = json.loads((tmp_path / "probe" / "probe_summary.json").read_text(encoding="utf-8"))
    assert probe_summary["activated_correction_ids"] == ["starter__x"]
    scratch_manifest = json.loads(
        (tmp_path / "probe" / "file_directory" / "overrides" / "correction_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert scratch_manifest["corrections"][0]["status"] == "active"
