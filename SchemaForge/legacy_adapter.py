"""Adapters for the legacy ETL pipeline (ported ETLTelco)."""
from __future__ import annotations

from typing import Any

from legacy.etl import (
    ETL as LegacyETL,
    run_etl as _run_etl,
    generate_configs as _generate_configs,
    derive_config_for_csv as _derive_config_for_csv,
    run_batch_etl as _run_batch_etl,
)


class LegacyPipeline:
    """Compact wrapper around the legacy ETL pipeline."""

    def __init__(self, db_uri: str, **kwargs: Any):
        self.db_uri = db_uri
        self.etl = LegacyETL(db_uri, **kwargs)

    def create_load(self, *args: Any, **kwargs: Any):
        return self.etl.create_load(*args, **kwargs)

    def append(self, *args: Any, **kwargs: Any):
        return self.etl.append(*args, **kwargs)

    def upsert(self, *args: Any, **kwargs: Any):
        return self.etl.upsert(*args, **kwargs)

    def update(self, *args: Any, **kwargs: Any):
        return self.etl.update(*args, **kwargs)

    def auto(self, *args: Any, **kwargs: Any):
        """Alias for auto_etl."""
        return self.etl.auto_etl(*args, **kwargs)

    def run_batch(self, csv_dir: str, config_path: str = "csv/config.yml"):
        return _run_batch_etl(self.db_uri, csv_dir, config_path=config_path)


def run_legacy_etl(*args: Any, **kwargs: Any):
    """Run the legacy ETL pipeline using the same signature as legacy.etl.run_etl."""
    return _run_etl(*args, **kwargs)


def generate_legacy_configs(*args: Any, **kwargs: Any):
    """Generate folder-level config.yml files for CSV batches."""
    return _generate_configs(*args, **kwargs)


def derive_legacy_config_for_csv(*args: Any, **kwargs: Any):
    """Derive config settings for a single CSV path."""
    return _derive_config_for_csv(*args, **kwargs)


def run_legacy_batch_etl(*args: Any, **kwargs: Any):
    """Run batch ETL using a config.yml file."""
    return _run_batch_etl(*args, **kwargs)


__all__ = [
    "LegacyETL",
    "LegacyPipeline",
    "run_legacy_etl",
    "generate_legacy_configs",
    "derive_legacy_config_for_csv",
    "run_legacy_batch_etl",
]
