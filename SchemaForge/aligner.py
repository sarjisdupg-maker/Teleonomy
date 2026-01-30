"""Schema alignment adapters."""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import pandas as pd
from sqlalchemy.engine import Connection, Engine

from legacy.schema_corrector import SchemaAligner
from legacy import df_align_to_sql as legacy_align
from .config import AlignPolicy
from .diagnostics import AlignReport


def align_dataframe(
    df: pd.DataFrame,
    engine: Engine | Connection,
    table: str,
    schema: Optional[str] = None,
    policy: AlignPolicy | None = None,
    semantic_type_meta: Dict[str, str] | None = None,
    use_simple: bool = False,
) -> Tuple[pd.DataFrame, AlignReport]:
    """Align DataFrame to DB schema using legacy aligners."""
    policy = policy or AlignPolicy()
    if use_simple:
        aligned, report = legacy_align.align_dataframe_to_schema(
            df,
            engine,
            table,
            schema=schema,
            threshold=policy.failure_threshold * 100,
            fix_outliers=True,
            auto_alter=policy.allow_schema_evolution,
        )
        return aligned, AlignReport(status="ok", details=report)

    aligner = SchemaAligner(
        engine,
        on_error=policy.on_error,
        failure_threshold=policy.failure_threshold,
        validate_fk=policy.validate_fk,
        add_missing_cols=policy.allow_schema_evolution,
        col_map=policy.col_map,
    )
    aligned = aligner.align(
        df,
        table,
        schema=schema,
        on_error=policy.on_error,
        failure_threshold=policy.failure_threshold,
        validate_fk=policy.validate_fk,
        add_missing_cols=policy.allow_schema_evolution,
        col_map=policy.col_map,
        semantic_type_meta=semantic_type_meta,
    )
    return aligned, AlignReport(status="ok", details={"method": "schema_corrector"})


def align_column(series: pd.Series, sql_type, threshold: float = 10.0) -> Tuple[pd.Series, Dict[str, Any]]:
    """Align a single column using legacy simple aligner."""
    return legacy_align.align_column(series, sql_type, threshold=threshold)
