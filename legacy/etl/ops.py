from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

import pandas as pd

from ..ddl_create import sanitize_cols
from ..crud_v2 import auto_insert as insert, auto_upsert as upsert, auto_update as crud_update
from ..data_cleaner import quick_clean
from ..casting import cast_df
from ..schema_manager import SchemaManager
from .. import logger as etl_logger
from .context import ETLContext
from .io import _to_dataframe
from .casting_pipeline import (
    _filter_cast_kwargs,
    _get_schema_guided_dtypes,
    _load_and_sanitize_data,
    _extract_semantic_meta,
)
from .ddl_sync import _prepare_ddl_and_sync, _save_ddl_schema, _setup_db_table
from .load_execute import _load_and_validate


class ETL:
    """
    Class-based ETL engine. Handles database state and provides methods
    for full replacement (create_load), data addition (append),
    and record updates (upsert).
    """

    def __init__(
        self,
        db_uri: str,
        schema_dir: Optional[Union[Path, str]] = None,
        logging_enabled: bool = True,
        pk: Union[str, list] = "verify",
        chunksize: int = 10000,
        add_missing_cols: bool = False,
        failure_threshold: int = 1,
        rename_column: bool = False,
        schema_aware_casting: bool = True,
        **cast_kwargs,
    ):
        self.ctx = ETLContext(
            db_uri,
            schema_dir,
            logging_enabled,
            pk,
            chunksize,
            add_missing_cols,
            failure_threshold,
            rename_column,
            schema_aware_casting,
            cast_kwargs,
        )
        etl_logger.ENABLE_LOGGING = self.ctx.logging_enabled
        self.engine = self.ctx.engine
        self.dialect = self.ctx.dialect
        self.schema_dir = self.ctx.schema_dir
        self.logging_enabled = self.ctx.logging_enabled
        self.default_pk = self.ctx.default_pk
        self.default_chunksize = self.ctx.default_chunksize
        self.default_add_missing_cols = self.ctx.default_add_missing_cols
        self.default_failure_threshold = self.ctx.default_failure_threshold
        self.default_rename_column = self.ctx.default_rename_column
        self.schema_aware_casting = self.ctx.schema_aware_casting
        self.cast_kwargs = self.ctx.cast_kwargs
        self.sm = self.ctx.sm

    def create_load(
        self,
        source: Union[Path, str, pd.DataFrame, list],
        table: str,
        pk: Optional[Union[str, list]] = None,
        drop_if_exists: bool = True,
        rename_column: Optional[bool] = None,
        chunksize: Optional[int] = None,
        add_missing_cols: Optional[bool] = None,
        failure_threshold: Optional[int] = None,
        fk=None,
        unique=None,
        autoincrement=None,
        varchar_sizes=None,
        trace_sql: bool = False,
        **kwargs,
    ) -> bool:
        """Full Lifecycle: Extractions -> Sanitize -> DDL -> Drop/Create -> Insert."""
        res_rc = rename_column if rename_column is not None else self.default_rename_column

        df_raw, df_cast, pk_cols, semantic_meta = _load_and_sanitize_data(
            self.ctx, source, table, pk, failure_threshold, **kwargs
        )
        df_final, ddl, meta = _prepare_ddl_and_sync(
            self.ctx,
            df_raw,
            df_cast,
            table,
            pk_cols,
            res_rc,
            source,
            fk,
            unique,
            autoincrement,
            varchar_sizes,
        )

        _save_ddl_schema(self.ctx, table, ddl, meta)
        sm, setup_ok = _setup_db_table(self.ctx, table, ddl, drop_if_exists)
        if not setup_ok:
            return False

        return _load_and_validate(
            self.ctx,
            sm,
            table,
            df_final,
            "insert",
            None,
            chunksize,
            add_missing_cols,
            failure_threshold,
            semantic_meta,
            trace_sql=trace_sql,
        )

    def append(
        self,
        source: Union[Path, str, pd.DataFrame, list],
        table: str,
        rename_column: Optional[bool] = None,
        chunksize: Optional[int] = None,
        add_missing_cols: Optional[bool] = None,
        failure_threshold: Optional[int] = None,
        trace_sql: bool = False,
        **kwargs,
    ) -> bool:
        """Add data to an existing table. Skips DDL generation and table management."""
        res_rc = rename_column if rename_column is not None else self.default_rename_column

        df_raw = _to_dataframe(source)

        ft = failure_threshold if failure_threshold is not None else self.default_failure_threshold
        current_cast_kwargs = self.cast_kwargs.copy()
        current_cast_kwargs.update(_filter_cast_kwargs(kwargs))
        current_cast_kwargs["infer_threshold"] = ft

        if self.schema_aware_casting:
            hints = _get_schema_guided_dtypes(self.ctx, table)
            if hints:
                current_cast_kwargs["dtype"] = hints

        df_cast, semantic_meta = cast_df(df_raw, return_dtype_meta=True, **current_cast_kwargs)
        clean_meta = _extract_semantic_meta(semantic_meta)

        df_final = sanitize_cols(df_cast, dialect=self.dialect) if res_rc else df_cast

        sm = SchemaManager(self.engine)
        if not sm.has_table(table):
            print(f"Append: table '{table}' not found. Creating via create_load().")
            return self.create_load(
                source,
                table,
                pk=None,
                drop_if_exists=False,
                rename_column=rename_column,
                chunksize=chunksize,
                add_missing_cols=add_missing_cols,
                failure_threshold=failure_threshold,
                trace_sql=trace_sql,
                **kwargs,
            )

        return _load_and_validate(
            self.ctx,
            sm,
            table,
            df_final,
            "insert",
            None,
            chunksize,
            add_missing_cols,
            ft,
            clean_meta,
            trace_sql=trace_sql,
        )

    def upsert(
        self,
        source: Union[Path, str, pd.DataFrame, list],
        table: str,
        pk: Optional[Union[str, list]] = None,
        rename_column: Optional[bool] = None,
        chunksize: Optional[int] = None,
        add_missing_cols: Optional[bool] = None,
        failure_threshold: Optional[int] = None,
        trace_sql: bool = False,
        **kwargs,
    ) -> bool:
        """Update existing records or insert new ones based on the primary key."""
        res_pk = pk if pk is not None else self.default_pk
        res_rc = rename_column if rename_column is not None else self.default_rename_column

        df_raw, df_cast, pk_cols, semantic_meta = _load_and_sanitize_data(
            self.ctx, source, table, res_pk, failure_threshold, **kwargs
        )
        df_final = sanitize_cols(df_cast, dialect=self.dialect) if res_rc else df_cast

        sm = SchemaManager(self.engine)
        if not sm.has_table(table):
            print(f"Upsert: table '{table}' not found. Creating via create_load().")
            return self.create_load(
                source,
                table,
                pk=res_pk,
                drop_if_exists=False,
                rename_column=rename_column,
                chunksize=chunksize,
                add_missing_cols=add_missing_cols,
                failure_threshold=failure_threshold,
                trace_sql=trace_sql,
                **kwargs,
            )

        ft = failure_threshold if failure_threshold is not None else self.default_failure_threshold
        if not pk_cols:
            print(f"Upsert: No valid PK for '{table}'. Falling back to insert.")
            return _load_and_validate(
                self.ctx,
                sm,
                table,
                df_final,
                "insert",
                None,
                chunksize,
                add_missing_cols,
                ft,
                semantic_meta,
                trace_sql=trace_sql,
            )
        return _load_and_validate(
            self.ctx,
            sm,
            table,
            df_final,
            "upsert",
            pk_cols,
            chunksize,
            add_missing_cols,
            ft,
            semantic_meta,
            trace_sql=trace_sql,
        )

    def update(
        self,
        source: Union[Path, str, pd.DataFrame, list],
        table: str,
        where: list,
        expression: Optional[str] = None,
        rename_column: Optional[bool] = None,
        add_missing_cols: Optional[bool] = None,
        failure_threshold: Optional[int] = None,
        trace_sql: bool = False,
    ) -> bool:
        """Update records based on a specific where condition."""
        res_rc = rename_column if rename_column is not None else self.default_rename_column
        amc = add_missing_cols if add_missing_cols is not None else self.default_add_missing_cols
        ft = failure_threshold if failure_threshold is not None else self.default_failure_threshold

        df_raw = _to_dataframe(source)
        df_raw = quick_clean(df_raw)

        current_cast_kwargs = self.cast_kwargs.copy()
        current_cast_kwargs["infer_threshold"] = ft

        if self.schema_aware_casting:
            hints = _get_schema_guided_dtypes(self.ctx, table)
            if hints:
                current_cast_kwargs["dtype"] = hints

        df_cast, semantic_meta = cast_df(df_raw, return_dtype_meta=True, **current_cast_kwargs)
        clean_meta = _extract_semantic_meta(semantic_meta)

        df_final = sanitize_cols(df_cast, dialect=self.dialect) if res_rc else df_cast

        sm = SchemaManager(self.engine)
        if not sm.has_table(table):
            print(f"Update: table '{table}' not found. Creating via create_load().")
            created = self.create_load(
                source,
                table,
                pk=None,
                drop_if_exists=False,
                rename_column=rename_column,
                chunksize=None,
                add_missing_cols=add_missing_cols,
                failure_threshold=failure_threshold,
                trace_sql=trace_sql,
            )
            if not created:
                return False

        return crud_update(
            self.engine,
            table,
            df_final,
            where,
            expression,
            add_missing_cols=amc,
            failure_threshold=ft,
            semantic_meta=clean_meta,
            trace_sql=trace_sql,
        )

    def auto_etl(
        self,
        source: Union[Path, str, pd.DataFrame, list],
        table: str,
        pk: Optional[Union[str, list]] = None,
        trace_sql: bool = False,
        **kwargs,
    ) -> bool:
        """Smart dispatcher: Choose between create_load, upsert, or append based on state."""
        if not table or not isinstance(table, str):
            raise ValueError(f"Table name must be a non-empty string. Got: {table}")

        res_pk = pk if pk is not None else self.default_pk

        sm = SchemaManager(self.engine)
        exists = sm.has_table(table)

        if not exists:
            return self.create_load(source, table, pk=res_pk, trace_sql=trace_sql, **kwargs)

        try:
            ft = kwargs.get("failure_threshold", self.default_failure_threshold)
            _, _, pk_cols, _ = _load_and_sanitize_data(
                self.ctx, source, table, res_pk, ft, **kwargs
            )
            if pk_cols:
                return self.upsert(source, table, pk=res_pk, trace_sql=trace_sql, **kwargs)
        except ValueError as e:
            import logging

            logger = logging.getLogger(__name__)
            logger.info(
                "AutoETL: Could not determine PK for upsert (%s). Falling back to append.",
                e,
            )

        return self.append(source, table, trace_sql=trace_sql, **kwargs)


def run_etl(
    source: Union[Path, str, pd.DataFrame, list],
    table: str,
    db_uri: str,
    pk: Optional[Union[str, list]] = None,
    schema_dir: Optional[Path] = None,
    logging_enabled: bool = True,
    **kwargs,
) -> bool:
    """Unified entry point using the smart auto_etl dispatcher."""
    etl = ETL(db_uri, schema_dir=schema_dir, logging_enabled=logging_enabled, **kwargs)
    return etl.auto_etl(source, table, pk=pk, **kwargs)


__all__ = ["ETL", "run_etl"]
