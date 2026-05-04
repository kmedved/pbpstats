import json
import re

from scripts.context_framework import (
    BUNDLE_CONTRACT,
    build_sync_data,
    count_tokens,
    render_bundle,
    render_checked_in_artifacts,
    render_context_budget,
    read_version,
)


def _rendered_artifacts():
    return render_checked_in_artifacts(build_sync_data())


def _rendered_architecture():
    return _rendered_artifacts()["context/REPO_ARCHITECTURE.md"]


def _rendered_sync_json():
    return json.loads(_rendered_artifacts()["context/REPO_ARCHITECTURE_SYNC.json"])


def test_context_artifact_renderer_returns_expected_files():
    artifacts = _rendered_artifacts()
    assert set(artifacts) == {
        "context/FILE_INDEX.md",
        "context/REPO_ARCHITECTURE.md",
        "context/REPO_ARCHITECTURE_SYNC.json",
        "context/START_HERE.md",
    }
    for contents in artifacts.values():
        assert contents.endswith("\n")


def test_rendered_version_matches_source_of_truth_and_doc():
    version = read_version()
    sync_data = _rendered_sync_json()
    architecture = _rendered_architecture()
    assert sync_data["arch_version"] == version
    assert "Architecture sync version: %s" % version in architecture


def test_build_sync_data_populates_expected_sections():
    sync_data = build_sync_data()
    assert "pbpstats/client.py" in sync_data["file_inventory"]
    assert sync_data["behavior_snapshot"]["supported_sources"] == ["file", "web"]
    assert "client" in sync_data["public_contracts"]
    assert "resources.core" in sync_data["module_dependencies"]
    assert sync_data["bundle_contract"] == BUNDLE_CONTRACT


def test_behavior_snapshot_covers_current_offline_semantics():
    behavior = build_sync_data()["behavior_snapshot"]
    assert "enrich_clocks_with_v3" in behavior["offline_pipeline"]
    assert "overrides/lineup_window_overrides.json" in behavior["override_files"]
    assert "overrides/period_starters_overrides.json" in behavior["override_files"]


def test_required_headings_exist():
    architecture = _rendered_architecture()
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
    architecture = _rendered_architecture()
    assert count_tokens(architecture) <= 4000


def test_start_here_mentions_guided_and_oracle_workflows():
    start_here = _rendered_artifacts()["context/START_HERE.md"]
    assert "Default:" in start_here
    assert "Oracle:" in start_here
    assert "raw source" in start_here


def test_bundle_contract_names_match_architecture_doc_tables():
    architecture = _rendered_architecture()
    declared_bundle_names = {bundle["name"] for bundle in BUNDLE_CONTRACT["bundles"]}
    referenced_bundle_names = set(re.findall(r"`(COMPRESSED_[^`]+\.md)`", architecture))
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
    architecture = _rendered_architecture()
    rendered_bundles = {}
    for bundle in BUNDLE_CONTRACT["bundles"]:
        rendered = render_bundle(bundle["name"], bundle["purpose"])
        assert rendered.startswith("Use this as a navigation bundle")
        rendered_bundles[bundle["name"]] = rendered
    budget = render_context_budget(architecture, rendered_bundles, include_src=False)
    assert "Default guided bundle" in budget
    for bundle_name in rendered_bundles:
        assert bundle_name in budget
