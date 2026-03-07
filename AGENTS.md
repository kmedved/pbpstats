# Agent Guidance

LLM context artifacts live in `context/`. Always pass `context/REPO_ARCHITECTURE.md` first.

For guided context, add one matching `context/COMPRESSED_*.md` bundle.
For oracle workflows, add `context/FILE_INDEX.md`.
For implementation tasks, include raw source of the touched files; do not rely on compressed bundles alone.

If a change touches exposed contracts, module boundaries, routing, or a listed invariant, update `context/REPO_ARCHITECTURE.md` and `context/REPO_ARCHITECTURE_SYNC.json` in the same change.
Refresh checked-in context artifacts with `python scripts/generate_repo_architecture_sync.py`.
Refresh local bundles with `python scripts/build_context_bundle.py`.

Version policy: Policy B. Only shipped/runtime behavior changes require a version bump.
