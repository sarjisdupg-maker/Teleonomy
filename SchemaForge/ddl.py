"""DDL helpers backed by legacy generators."""
from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from legacy import ddl_create as legacy_ddl
from .registry import TypeRegistry


def _normalize_dialect(dialect: str) -> str:
    return "postgresql" if dialect.lower() == "postgres" else dialect.lower()


def infer_sql_type(series: pd.Series, dialect: str = "postgresql", registry: TypeRegistry | None = None) -> str:
    """Infer SQL type from Series and registry fallback."""
    dialect = _normalize_dialect(dialect)
    registry = registry or TypeRegistry.default()
    logical = registry.infer_logical_type(series)
    try:
        return registry.resolve_sql(logical, dialect)
    except KeyError:
        return legacy_ddl.get_sql_type(series.dtype, dialect, col_name=series.name)


def build_create_table(
    df: pd.DataFrame,
    table: str,
    dialect: str = "postgresql",
    schema: Optional[str] = None,
    pk=None,
    fk=None,
    unique=None,
    autoincrement=None,
    varchar_sizes: Optional[Dict[str, int]] = None,
    dtype_semantics: Optional[Dict[str, str]] = None,
) -> str:
    """Build CREATE TABLE DDL using legacy generator."""
    dialect = _normalize_dialect(dialect)
    return legacy_ddl.build_create_table_statement(
        df,
        table,
        schema,
        pk,
        fk,
        unique,
        autoincrement,
        dialect,
        varchar_sizes=varchar_sizes,
        dtype_semantics=dtype_semantics,
    )


def build_add_column(
    table: str,
    column: str,
    sql_type: str,
    dialect: str = "postgresql",
    schema: Optional[str] = None,
) -> str:
    """Build ALTER TABLE ADD COLUMN DDL string."""
    dialect = _normalize_dialect(dialect)
    q = legacy_ddl.escape_identifier
    pre = f"{q(schema, dialect)}." if schema else ""
    fmt = "({0} {1})" if dialect == "oracle" else "{0} {1}"
    return f"ALTER TABLE {pre}{q(table, dialect)} ADD {fmt.format(q(column, dialect), sql_type)}"


def execute_ddl(engine: Engine, ddl_sql: str) -> None:
    """Execute DDL statement."""
    with engine.begin() as conn:
        conn.execute(text(ddl_sql))
