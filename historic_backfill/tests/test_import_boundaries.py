import ast
from pathlib import Path


CORE_AUDIT_DIR = Path(__file__).resolve().parents[1] / "audits" / "core"


def _imported_modules(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.append(node.module)
    return modules


def test_core_audits_do_not_import_cross_source_modules():
    offenders = []
    for path in CORE_AUDIT_DIR.glob("*.py"):
        for module in _imported_modules(path):
            if module.startswith("historic_backfill.audits.cross_source"):
                offenders.append((path.name, module))

    assert offenders == []
