import ast
import subprocess
import sys
from pathlib import Path


HISTORIC_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = HISTORIC_ROOT.parent
NO_CROSS_SOURCE_DIRS = [
    HISTORIC_ROOT / "audits" / "core",
    HISTORIC_ROOT / "catalogs",
    HISTORIC_ROOT / "common",
]


def _imported_modules(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            prefix = "." * node.level
            module = f"{prefix}{node.module or ''}"
            modules.append(module)
            modules.extend(f"{module}.{alias.name}" for alias in node.names)
    return modules


def test_runtime_backfill_modules_do_not_import_cross_source_modules():
    offenders = []
    for root in NO_CROSS_SOURCE_DIRS:
        for path in root.rglob("*.py"):
            for module in _imported_modules(path):
                if "cross_source" in module:
                    offenders.append((path.relative_to(HISTORIC_ROOT).as_posix(), module))
                if "audit_period_starters_against_tpdev" in module:
                    offenders.append((path.relative_to(HISTORIC_ROOT).as_posix(), module))

    assert offenders == []


def test_importing_pbpstats_does_not_import_historic_backfill():
    code = """
import sys
import pbpstats
assert not any(name.startswith("historic_backfill") for name in sys.modules)
"""
    subprocess.run([sys.executable, "-c", code], check=True, cwd=REPO_ROOT)


def test_pbpstats_source_does_not_import_historic_backfill():
    offenders = []
    for path in (REPO_ROOT / "pbpstats").rglob("*.py"):
        for module in _imported_modules(path):
            if module.startswith("historic_backfill"):
                offenders.append((path.relative_to(REPO_ROOT).as_posix(), module))

    assert offenders == []
