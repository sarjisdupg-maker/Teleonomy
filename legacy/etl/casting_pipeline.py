from __future__ import annotations

from typing import Dict, Any, Optional, Union
from pathlib import Path
import logging

import pandas as pd

from ..data_cleaner import quick_clean
from ..casting import cast_df
from ..schema_manager import SchemaManager
from .io import _to_dataframe
from .pk import _process_pk

logger = logging.getLogger(__name__)


def _filter_cast_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Filters kwargs to only include those valid for CastConfig."""
    valid_keys = {
        "use_transform",
        "use_ml",
        "infer_threshold",
        "nan_threshold",
        "max_sample_size",
        "parallel",
        "validate_conversions",
        "max_null_increase",
        "chunk_size",
        "dtype",
    }
    return {k: v for k, v in kwargs.items() if k in valid_keys}


def _get_schema_guided_dtypes(ctx, table: str) -> Dict[str, str]:
    """Queries the database for existing column types to guide the casting engine."""
    sm = SchemaManager(ctx.engine)
    if not sm.has_table(table):
        return {}

    logger.info("Retrieving schema hints for '%s'...", table)
    try:
        cols = sm.get_columns(table)
        hints: Dict[str, str] = {}
        for c in cols:
            name = c["name"]
            st = c["type"]

            from sqlalchemy.sql import sqltypes as sat

            if isinstance(st, (sat.Integer, sat.BIGINT, sat.SmallInteger)):
                hints[name] = "integer"
            elif isinstance(st, (sat.Float, sat.Numeric, sat.REAL)):
                hints[name] = "float"
            elif isinstance(st, (sat.Boolean,)):
                hints[name] = "boolean"
            elif isinstance(st, (sat.DateTime, sat.Date, sat.TIMESTAMP)):
                hints[name] = "timestamp"
            elif isinstance(st, (sat.String, sat.Text, sat.Unicode)):
                hints[name] = "string"
        return hints
    except Exception as e:
        logger.warning("Could not retrieve schema hints for '%s': %s", table, e)
        return {}


def _load_and_sanitize_data(
    ctx,
    source: Union[Path, str, pd.DataFrame, list],
    table: str,
    pk_arg: Union[str, list],
    failure_threshold: Optional[int] = None,
    **kwargs,
):
    """Full data preparation: extraction, type casting, and PK validation/derivation."""
    df_raw = _to_dataframe(source)
    df_raw = quick_clean(df_raw)

    if df_raw.shape[1] == 0:
        raise ValueError("No columns found in dataset after cleaning.")

    ft = failure_threshold if failure_threshold is not None else ctx.default_failure_threshold
    current_cast_kwargs = ctx.cast_kwargs.copy()
    current_cast_kwargs.update(_filter_cast_kwargs(kwargs))
    current_cast_kwargs["infer_threshold"] = ft

    if ctx.schema_aware_casting:
        hints = _get_schema_guided_dtypes(ctx, table)
        if hints:
            current_cast_kwargs["dtype"] = hints

    df_cast, semantic_meta = cast_df(df_raw, return_dtype_meta=True, **current_cast_kwargs)
    pk_cols = _process_pk(ctx, df_cast, table, pk_arg)
    clean_meta = _extract_semantic_meta(semantic_meta)
    return df_raw, df_cast, pk_cols, clean_meta


def _extract_semantic_meta(meta: Dict[str, Any]) -> Dict[str, str]:
    """Extracts simple semantic type strings from the complex cast_df metadata."""
    if not meta or not isinstance(meta, dict):
        return {}

    result: Dict[str, str] = {}
    for col, info in meta.items():
        if isinstance(info, dict):
            semantic_type = info.get("object_semantic_type")
            if semantic_type:
                result[col] = semantic_type
    return result
