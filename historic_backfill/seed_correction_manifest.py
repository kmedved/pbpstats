from __future__ import annotations

import argparse
from pathlib import Path

from lineup_correction_manifest import DEFAULT_MANIFEST_PATH, DEFAULT_OVERRIDES_DIR, seed_manifest_from_runtime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed the canonical correction manifest from the live runtime override artifacts."
    )
    parser.add_argument("--overrides-dir", type=Path, default=DEFAULT_OVERRIDES_DIR)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = seed_manifest_from_runtime(
        overrides_dir=args.overrides_dir.resolve(),
        manifest_path=args.manifest_path.resolve(),
    )
    print(
        f"Seeded {len(manifest.get('corrections', []))} corrections into {args.manifest_path.resolve()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
