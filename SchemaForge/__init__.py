"""Mini ETL library (legacy-compatible adapters)."""
from __future__ import annotations

from .config import AlignPolicy, CastPolicy
from .cleaner import clean_series, clean_dataframe, sanitize_columns, normalize_sentinels
from .caster import cast_series, cast_dataframe
from .inspector import (
    list_tables,
    has_table,
    get_columns,
    get_pk,
    get_unique_constraints,
    get_foreign_keys,
    get_table_info,
)
from .aligner import align_dataframe
from .ddl import infer_sql_type, build_create_table, build_add_column, execute_ddl
from .executor import execute_etl, ExecutionResult
from .legacy_adapter import (
    LegacyETL,
    LegacyPipeline,
    run_legacy_etl,
    generate_legacy_configs,
    derive_legacy_config_for_csv,
    run_legacy_batch_etl,
)

__all__ = [
    "AlignPolicy",
    "CastPolicy",
    "clean_series",
    "clean_dataframe",
    "sanitize_columns",
    "normalize_sentinels",
    "cast_series",
    "cast_dataframe",
    "list_tables",
    "has_table",
    "get_columns",
    "get_pk",
    "get_unique_constraints",
    "get_foreign_keys",
    "get_table_info",
    "align_dataframe",
    "infer_sql_type",
    "build_create_table",
    "build_add_column",
    "execute_ddl",
    "execute_etl",
    "ExecutionResult",
    "LegacyETL",
    "LegacyPipeline",
    "run_legacy_etl",
    "generate_legacy_configs",
    "derive_legacy_config_for_csv",
    "run_legacy_batch_etl",
]
