import json
from pathlib import Path

import pytest

from scripts import run_replace_tpdev_compatibility_smoke as smoke


def test_resolve_replace_tpdev_root_uses_sibling_checkout(tmp_path, monkeypatch):
    pbpstats_root = tmp_path / "pbpstats"
    replace_root = tmp_path / "replace_tpdev"
    pbpstats_root.mkdir()
    replace_root.mkdir()
    (replace_root / "run_golden_canary_suite.py").write_text(
        "print('ok')\n", encoding="utf-8"
    )

    monkeypatch.setattr(smoke, "DEFAULT_REPLACE_TPDEV_ROOT", replace_root)

    resolved = smoke.resolve_replace_tpdev_root()

    assert resolved == replace_root.resolve()


def test_build_command_includes_editable_fork_and_runtime_args(tmp_path):
    replace_root = tmp_path / "replace_tpdev"
    replace_root.mkdir()
    runner = replace_root / "run_golden_canary_suite.py"
    runner.write_text("print('ok')\n", encoding="utf-8")
    output_dir = tmp_path / "out"
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{}", encoding="utf-8")
    pbpstats_root = tmp_path / "pbpstats"
    pbpstats_root.mkdir()

    command = smoke.build_command(
        replace_tpdev_root=replace_root,
        output_dir=output_dir,
        manifest_path=manifest_path,
        pbpstats_root=pbpstats_root,
        runtime_input_cache_mode="fresh-copy",
        max_workers=8,
        python_executable="python",
    )

    assert command == [
        "python",
        str(runner.resolve()),
        "--output-dir",
        str(output_dir),
        "--manifest-path",
        str(manifest_path),
        "--pbpstats-repo",
        str(pbpstats_root.resolve()),
        "--runtime-input-cache-mode",
        "fresh-copy",
        "--max-workers",
        "8",
    ]


def test_validate_summary_requires_green_gate():
    summary = {
        "failed_games": 0,
        "event_stats_errors": 0,
        "suite_pass": True,
        "suite_pass_all_cases": True,
        "suite_pass_stable_cases_only": True,
    }

    smoke.validate_summary(summary)


def test_validate_summary_rejects_any_failed_check():
    summary = {
        "failed_games": 1,
        "event_stats_errors": 0,
        "suite_pass": False,
        "suite_pass_all_cases": False,
        "suite_pass_stable_cases_only": True,
    }

    with pytest.raises(smoke.ReplaceTpdevCompatibilityError):
        smoke.validate_summary(summary)


def test_run_compatibility_smoke_reads_and_validates_summary(tmp_path, monkeypatch):
    replace_root = tmp_path / "replace_tpdev"
    replace_root.mkdir()
    (replace_root / "run_golden_canary_suite.py").write_text(
        "print('ok')\n", encoding="utf-8"
    )
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{}", encoding="utf-8")
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    summary_path = output_dir / "summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "failed_games": 0,
                "event_stats_errors": 0,
                "suite_pass": True,
                "suite_pass_all_cases": True,
                "suite_pass_stable_cases_only": True,
            }
        ),
        encoding="utf-8",
    )

    class DummyResult:
        returncode = 0
        stdout = "ok"
        stderr = ""

    def fake_run(command, cwd, text, capture_output):
        assert cwd == replace_root
        return DummyResult()

    monkeypatch.setattr(smoke.subprocess, "run", fake_run)

    summary = smoke.run_compatibility_smoke(
        replace_tpdev_root=replace_root,
        output_dir=output_dir,
        manifest_path=manifest_path,
        pbpstats_root=tmp_path / "pbpstats",
        runtime_input_cache_mode="fresh-copy",
        max_workers=8,
        python_executable="python",
    )

    assert summary["suite_pass"] is True


def test_run_compatibility_smoke_raises_on_failed_subprocess(tmp_path, monkeypatch):
    replace_root = tmp_path / "replace_tpdev"
    replace_root.mkdir()
    (replace_root / "run_golden_canary_suite.py").write_text(
        "print('ok')\n", encoding="utf-8"
    )
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{}", encoding="utf-8")
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    class DummyResult:
        returncode = 1
        stdout = "bad"
        stderr = "worse"

    monkeypatch.setattr(smoke.subprocess, "run", lambda *args, **kwargs: DummyResult())

    with pytest.raises(smoke.ReplaceTpdevCompatibilityError):
        smoke.run_compatibility_smoke(
            replace_tpdev_root=replace_root,
            output_dir=output_dir,
            manifest_path=manifest_path,
            pbpstats_root=tmp_path / "pbpstats",
            runtime_input_cache_mode="fresh-copy",
            max_workers=8,
            python_executable="python",
        )
