from __future__ import annotations

from typing import Dict, Any, Union, Optional
import json
from pathlib import Path

import pandas as pd

from ..ddl_create import df_ddl
from ..schema_manager import SchemaManager


def _save_ddl_schema(ctx, table: str, ddl: Union[str, list], meta: Dict[str, Any]):
    """Saves generated DDL and metadata to the schema directory if configured."""
    if not ctx.schema_dir:
        return
    ctx.schema_dir.mkdir(parents=True, exist_ok=True)
    ddl_file = ctx.schema_dir / f"{table}.txt"
    json_file = ctx.schema_dir / f"{table}.json"
    ddl_text = "\n\n".join(ddl) if isinstance(ddl, list) else ddl
    with ddl_file.open("w", encoding="utf-8") as f:
        f.write(ddl_text)
    with json_file.open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, default=str)
    print(f"Saved DDL and Metadata to {ctx.schema_dir}")


def _prepare_ddl_and_sync(
    ctx,
    df_raw: pd.DataFrame,
    df_cast: pd.DataFrame,
    table: str,
    pk_cols: list,
    rename_column: bool,
    source: Union[Path, str, pd.DataFrame, list],
    fk=None,
    unique=None,
    autoincrement=None,
    varchar_sizes=None,
):
    """Generates DDL and syncs source file to disk if column renaming is active."""
    print(f"Generating DDL for {ctx.dialect}...")

    df_final, create_sql, meta = df_ddl(
        df_cast,
        table,
        server=ctx.dialect,
        pk=pk_cols,
        fk=fk,
        unique=unique,
        autoincrement=autoincrement,
        varchar_sizes=varchar_sizes,
        rename_column=rename_column,
        sanitize=rename_column,
    )

    ddl_statements = [create_sql]

    if rename_column and isinstance(source, (Path, str)):
        path = Path(source)
        if path.suffix.lower() == ".csv" and not list(df_raw.columns) == list(df_final.columns):
            print(f"Column names changed during sanitization. Updating CSV: {path}")
            df_final.to_csv(path, index=False)

    return df_final, ddl_statements, meta


def _setup_db_table(ctx, table: str, ddl: Union[str, list], drop_if_exists: bool):
    """Ensures the target table exists, dropping and recreating it if requested."""
    sm = SchemaManager(ctx.engine)
    if drop_if_exists and sm.has_table(table):
        print(f"Table '{table}' exists and drop_if_exists=True. Dropping...")
        try:
            sm.drop_table(table)
            sm.refresh()
        except Exception as e:
            print(f"Warning: Failed to drop table '{table}': {e}")

    if not sm.has_table(table):
        print(f"Table '{table}' not found in {ctx.dialect}. Creating...")
        try:
            statements = ddl if isinstance(ddl, list) else [ddl]
            for stmt in statements:
                sm.execute_ddl(stmt, f"Executed DDL for '{table}'")
            sm.refresh()
        except Exception as e:
            print(f"Error creating table: {e}")
            return sm, False
    else:
        print(f"Table '{table}' already exists.")
    return sm, True
