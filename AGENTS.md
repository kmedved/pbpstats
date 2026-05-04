# Agent Guidance

LLM context artifacts live in `context/`. Use `context/REPO_ARCHITECTURE.md` as the starting map when repo-level context is useful.

For guided context, add one matching `context/COMPRESSED_*.md` bundle.
For oracle workflows, add `context/FILE_INDEX.md`.
For implementation tasks, include raw source of the touched files; do not rely on compressed bundles alone.

Context artifacts are optional navigation aids. Do not update or regenerate them solely because runtime code changed.
When intentionally refreshing context docs, use `python scripts/generate_repo_architecture_sync.py`.
When intentionally refreshing local bundles, use `python scripts/build_context_bundle.py`.

Version policy: Policy B. Only shipped/runtime behavior changes require a version bump.
