from __future__ import annotations

from typing import Optional, Dict, Any

import pandas as pd

from ..crud_v2 import auto_insert as insert, auto_upsert as upsert
from ..schema_manager import SchemaManager


def _load_and_validate(
    ctx,
    sm: SchemaManager,
    table: str,
    df: pd.DataFrame,
    op_type: str = "insert",
    pk: Optional[list] = None,
    chunksize: Optional[int] = None,
    add_missing_cols: Optional[bool] = None,
    failure_threshold: Optional[int] = None,
    semantic_meta: Optional[Dict[str, Any]] = None,
    trace_sql: bool = False,
):
    """Orchestrates the actual data load and performs final row count validation."""
    csize = chunksize if chunksize is not None else ctx.default_chunksize
    amc = add_missing_cols if add_missing_cols is not None else ctx.default_add_missing_cols
    ft = failure_threshold if failure_threshold is not None else ctx.default_failure_threshold

    print(f"Performing '{op_type}' on '{table}' (chunksize={csize})...")

    start_count = sm.get_row_count(table)

    try:
        if op_type == "upsert":
            res = upsert(
                ctx.engine,
                df,
                table,
                pk,
                chunk=csize,
                add_missing_cols=amc,
                failure_threshold=ft,
                semantic_meta=semantic_meta,
                trace_sql=trace_sql,
            )
            if isinstance(res, dict):
                rows_done = res.get("success", 0)
            else:
                rows_done = res
        else:
            rows_done = insert(
                ctx.engine,
                df,
                table,
                chunk_size=csize,
                add_missing_cols=amc,
                failure_threshold=ft,
                semantic_meta=semantic_meta,
                trace_sql=trace_sql,
            )

        print(f"Load complete. {rows_done} rows processed for '{table}'.")

        print(f"Validating row counts for '{table}'...")
        end_count = sm.get_row_count(table)
        delta = end_count - start_count
        df_count = len(df)

        if op_type == "insert":
            if delta < 0:
                raise ValueError(f"Validation FAILED: Row count decreased (delta={delta}).")
            if start_count == 0 and delta != df_count:
                print(
                    f"Validation WARNING: New table delta ({delta}) != Input count ({df_count}). "
                    "Some rows may have been dropped or failed silent constraints."
                )
        elif op_type == "upsert":
            if delta < 0:
                raise ValueError(f"Validation FAILED: Row count decreased during upsert (delta={delta}).")

        print(f"Validation finished. Final DB count: {end_count} (Delta: +{delta})")
        return True

    except Exception as e:
        import logging

        logger = logging.getLogger(__name__)
        logger.exception("Error during data loading: %s", e)
        raise
