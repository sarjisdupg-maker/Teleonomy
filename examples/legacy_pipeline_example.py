"""Example usage for the legacy ETL pipeline via SchemaForge."""
from __future__ import annotations

from pathlib import Path

from SchemaForge import LegacyPipeline, generate_legacy_configs, run_legacy_batch_etl

DB_URI = "postgresql+psycopg2://user:password@localhost:5432/dbname"  # TODO: update
CSV_DIR = Path("./csv")  # TODO: update
CONFIG_PATH = CSV_DIR / "config.yml"


def main() -> None:
    # 1) Generate per-folder config.yml files (uses profiling to infer PKs)
    generate_legacy_configs(CSV_DIR, force=False)

    # 2) Single-file usage via wrapper
    pipeline = LegacyPipeline(DB_URI, add_missing_cols=True)
    pipeline.auto(CSV_DIR / "sample.csv", "sample_table")

    # 3) Batch run using config.yml (updates executed_at on success)
    run_legacy_batch_etl(DB_URI, CSV_DIR, config_path=str(CONFIG_PATH))


if __name__ == "__main__":
    main()
