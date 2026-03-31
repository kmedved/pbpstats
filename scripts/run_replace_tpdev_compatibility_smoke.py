from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Iterable


PBPSTATS_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPLACE_TPDEV_ROOT = PBPSTATS_ROOT.parent / "replace_tpdev"
DEFAULT_MANIFEST_NAME = "golden_canary_manifest_20260321_v1.json"
REQUIRED_SUMMARY_FIELDS = (
    "failed_games",
    "event_stats_errors",
    "suite_pass",
    "suite_pass_all_cases",
    "suite_pass_stable_cases_only",
)


class ReplaceTpdevCompatibilityError(RuntimeError):
    """Raised when the replace_tpdev compatibility smoke gate fails."""


def _validate_replace_tpdev_root(root: Path) -> Path:
    resolved = root.resolve()
    runner = resolved / "run_golden_canary_suite.py"
    if not runner.exists():
        raise ReplaceTpdevCompatibilityError(
            f"replace_tpdev root does not contain run_golden_canary_suite.py: {resolved}"
        )
    return resolved


def resolve_replace_tpdev_root(root: Path | None = None) -> Path:
    target = DEFAULT_REPLACE_TPDEV_ROOT if root is None else root
    return _validate_replace_tpdev_root(target)


def resolve_manifest_path(
    replace_tpdev_root: Path,
    manifest_path: Path | None = None,
) -> Path:
    target = (
        replace_tpdev_root / DEFAULT_MANIFEST_NAME
        if manifest_path is None
        else manifest_path
    ).resolve()
    if not target.exists():
        raise ReplaceTpdevCompatibilityError(
            f"Golden Canary manifest not found: {target}"
        )
    return target


def resolve_output_dir(output_dir: Path | None = None) -> Path:
    if output_dir is not None:
        resolved = output_dir.resolve()
        resolved.mkdir(parents=True, exist_ok=True)
        return resolved
    return Path(tempfile.mkdtemp(prefix="pbpstats_replace_tpdev_canary_"))


def build_command(
    *,
    replace_tpdev_root: Path,
    output_dir: Path,
    manifest_path: Path,
    pbpstats_root: Path,
    runtime_input_cache_mode: str,
    max_workers: int,
    python_executable: str,
) -> list[str]:
    return [
        python_executable,
        str((replace_tpdev_root / "run_golden_canary_suite.py").resolve()),
        "--output-dir",
        str(output_dir),
        "--manifest-path",
        str(manifest_path),
        "--pbpstats-repo",
        str(pbpstats_root.resolve()),
        "--runtime-input-cache-mode",
        runtime_input_cache_mode,
        "--max-workers",
        str(max_workers),
    ]


def validate_summary(summary: dict) -> None:
    missing_fields = [field for field in REQUIRED_SUMMARY_FIELDS if field not in summary]
    if missing_fields:
        raise ReplaceTpdevCompatibilityError(
            f"Compatibility smoke summary missing fields: {missing_fields}"
        )

    checks = {
        "failed_games": summary["failed_games"] == 0,
        "event_stats_errors": summary["event_stats_errors"] == 0,
        "suite_pass": bool(summary["suite_pass"]),
        "suite_pass_all_cases": bool(summary["suite_pass_all_cases"]),
        "suite_pass_stable_cases_only": bool(summary["suite_pass_stable_cases_only"]),
    }
    failed_checks = [name for name, passed in checks.items() if not passed]
    if failed_checks:
        raise ReplaceTpdevCompatibilityError(
            f"replace_tpdev compatibility smoke failed checks: {failed_checks}"
        )


def run_compatibility_smoke(
    *,
    replace_tpdev_root: Path,
    output_dir: Path,
    manifest_path: Path,
    pbpstats_root: Path,
    runtime_input_cache_mode: str,
    max_workers: int,
    python_executable: str = sys.executable,
) -> dict:
    command = build_command(
        replace_tpdev_root=replace_tpdev_root,
        output_dir=output_dir,
        manifest_path=manifest_path,
        pbpstats_root=pbpstats_root,
        runtime_input_cache_mode=runtime_input_cache_mode,
        max_workers=max_workers,
        python_executable=python_executable,
    )
    result = subprocess.run(
        command,
        cwd=replace_tpdev_root,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise ReplaceTpdevCompatibilityError(
            "Golden Canary command failed.\n"
            f"Command: {' '.join(command)}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

    summary_path = output_dir / "summary.json"
    if not summary_path.exists():
        raise ReplaceTpdevCompatibilityError(
            f"Golden Canary did not produce summary.json at {summary_path}"
        )

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    validate_summary(summary)
    return summary


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the replace_tpdev Golden Canary compatibility smoke gate "
            "against the current editable pbpstats checkout."
        )
    )
    parser.add_argument(
        "--replace-tpdev-root",
        type=Path,
        default=DEFAULT_REPLACE_TPDEV_ROOT,
        help="Path to the sibling replace_tpdev checkout.",
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        help="Optional explicit Golden Canary manifest path.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Optional directory for smoke outputs. Defaults to a temp directory.",
    )
    parser.add_argument(
        "--runtime-input-cache-mode",
        choices=["fresh-copy", "reuse-latest-global-cache"],
        default="fresh-copy",
    )
    parser.add_argument("--max-workers", type=int, default=8)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    replace_tpdev_root = resolve_replace_tpdev_root(args.replace_tpdev_root)
    manifest_path = resolve_manifest_path(replace_tpdev_root, args.manifest_path)
    output_dir = resolve_output_dir(args.output_dir)
    summary = run_compatibility_smoke(
        replace_tpdev_root=replace_tpdev_root,
        output_dir=output_dir,
        manifest_path=manifest_path,
        pbpstats_root=PBPSTATS_ROOT,
        runtime_input_cache_mode=args.runtime_input_cache_mode,
        max_workers=args.max_workers,
    )
    concise_summary = {
        "output_dir": str(output_dir),
        "manifest_path": str(manifest_path),
        "failed_games": summary["failed_games"],
        "event_stats_errors": summary["event_stats_errors"],
        "suite_pass": summary["suite_pass"],
        "suite_pass_all_cases": summary["suite_pass_all_cases"],
        "suite_pass_stable_cases_only": summary["suite_pass_stable_cases_only"],
    }
    print(json.dumps(concise_summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
