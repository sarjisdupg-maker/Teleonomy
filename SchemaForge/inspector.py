"""SQL schema inspection utilities."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from legacy.schema_inspector import SchemaInspector

_INSPECTOR = SchemaInspector()


def list_tables(engine: Engine, schema: Optional[str] = None) -> List[str]:
    """List tables in schema."""
    return _INSPECTOR.get_table_names(engine, schema)


def has_table(engine: Engine, table: str, schema: Optional[str] = None) -> bool:
    """Check table existence."""
    return _INSPECTOR.has_table(engine, table, schema)


def get_columns(engine: Engine, table: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get column metadata."""
    return _INSPECTOR.get_columns(engine, table, schema)


def get_pk(engine: Engine, table: str, schema: Optional[str] = None) -> List[str]:
    """Get primary key columns."""
    return _INSPECTOR.get_primary_keys(engine, table, schema)


def get_unique_constraints(engine: Engine, table: str, schema: Optional[str] = None) -> List[List[str]]:
    """Get unique constraint columns."""
    return _INSPECTOR.get_unique_constraints(engine, table, schema)


def get_foreign_keys(engine: Engine, table: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get foreign key metadata."""
    return inspect(engine).get_foreign_keys(table, schema=schema)


def get_table_info(engine: Engine, table: str, schema: Optional[str] = None) -> Dict[str, Any]:
    """Consolidated table metadata."""
    return {
        "columns": get_columns(engine, table, schema),
        "primary_key": get_pk(engine, table, schema),
        "unique": get_unique_constraints(engine, table, schema),
        "foreign_keys": get_foreign_keys(engine, table, schema),
    }
