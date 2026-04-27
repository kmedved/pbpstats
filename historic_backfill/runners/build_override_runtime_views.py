from __future__ import annotations

import argparse
from pathlib import Path

from historic_backfill.catalogs.lineup_correction_manifest import (
    DEFAULT_DB_PATH,
    DEFAULT_FILE_DIRECTORY,
    DEFAULT_MANIFEST_PATH,
    DEFAULT_OVERRIDES_DIR,
    DEFAULT_PARQUET_PATH,
    compile_runtime_views,
    load_manifest,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compile the canonical lineup correction manifest into the runtime override JSON/CSV views."
    )
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OVERRIDES_DIR)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--file-directory", type=Path, default=DEFAULT_FILE_DIRECTORY)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = load_manifest(args.manifest_path.resolve())
    summary = compile_runtime_views(
        manifest,
        output_dir=args.output_dir.resolve(),
        db_path=args.db_path.resolve(),
        parquet_path=args.parquet_path.resolve(),
        file_directory=args.file_directory.resolve(),
    )
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
