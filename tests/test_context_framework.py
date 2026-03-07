import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.context_framework import (
    BUNDLE_CONTRACT,
    build_sync_data,
    collect_behavior_snapshot,
    collect_file_inventory,
    collect_module_dependencies,
    collect_public_contracts,
    count_tokens,
    render_bundle,
    render_checked_in_artifacts,
    render_context_budget,
    read_version,
)


def _read(relative_path):
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def _sync_json():
    return json.loads(_read("context/REPO_ARCHITECTURE_SYNC.json"))


def test_checked_in_context_artifacts_match_renderer():
    expected = render_checked_in_artifacts(build_sync_data())
    for relative_path, contents in expected.items():
        assert _read(relative_path) == contents


def test_version_matches_source_of_truth_and_doc():
    version = read_version()
    sync_data = _sync_json()
    architecture = _read("context/REPO_ARCHITECTURE.md")
    assert sync_data["arch_version"] == version
    assert "Architecture sync version: %s" % version in architecture


def test_file_inventory_matches_scoped_walk():
    assert _sync_json()["file_inventory"] == collect_file_inventory()


def test_behavior_snapshot_matches_live_source():
    assert _sync_json()["behavior_snapshot"] == collect_behavior_snapshot()


def test_module_dependencies_match_static_analysis():
    assert _sync_json()["module_dependencies"] == collect_module_dependencies()


def test_public_contracts_match_introspection_snapshot():
    assert _sync_json()["public_contracts"] == collect_public_contracts()


def test_required_headings_exist():
    architecture = _read("context/REPO_ARCHITECTURE.md")
    required_headings = [
        "## TL;DR",
        "## Behavior / Routing Matrix",
        "### Critical Invariants",
        "### Conventions",
        "## Public Contract Snapshot",
        "## Core Abstractions",
        "## Module Dependency Map",
        "## Where To Edit",
        "## Bundle Picker",
    ]
    for heading in required_headings:
        assert heading in architecture


def test_architecture_doc_fits_token_budget():
    architecture = _read("context/REPO_ARCHITECTURE.md")
    assert count_tokens(architecture) <= 4000


def test_start_here_mentions_guided_and_oracle_workflows():
    start_here = _read("context/START_HERE.md")
    assert "Default:" in start_here
    assert "Oracle:" in start_here
    assert "raw source" in start_here


def test_bundle_contract_names_match_architecture_doc_tables():
    architecture = _read("context/REPO_ARCHITECTURE.md")
    declared_bundle_names = {bundle["name"] for bundle in BUNDLE_CONTRACT["bundles"]}
    referenced_bundle_names = set(
        re.findall(r"`(COMPRESSED_[^`]+\.md)`", architecture)
    )
    assert declared_bundle_names.issubset(referenced_bundle_names)

    where_to_edit_rows = [
        line
        for line in architecture.splitlines()
        if line.startswith("|")
        and "Primary bundle" not in line
        and "Task area" not in line
        and "COMPRESSED_" in line
    ]
    for row in where_to_edit_rows:
        bundle_names = re.findall(r"`(COMPRESSED_[^`]+\.md)`", row)
        for bundle_name in bundle_names:
            assert bundle_name in declared_bundle_names


def test_bundle_renderer_and_budget_use_stable_names():
    architecture = _read("context/REPO_ARCHITECTURE.md")
    rendered_bundles = {}
    for bundle in BUNDLE_CONTRACT["bundles"]:
        rendered = render_bundle(bundle["name"], bundle["purpose"])
        assert rendered.startswith("Use this as a navigation bundle")
        rendered_bundles[bundle["name"]] = rendered
    budget = render_context_budget(architecture, rendered_bundles, include_src=False)
    assert "Default guided bundle" in budget
    for bundle_name in rendered_bundles:
        assert bundle_name in budget
