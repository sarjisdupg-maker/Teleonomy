"""
Schema inspection utilities: Table/column metadata, identity columns, schema management.
Extracted from dfsql.py for modularity and reusability.
"""
import logging
from typing import Dict, Any, List, Set, Optional
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.types import TypeEngine

logger = logging.getLogger(__name__)


class SchemaInspector:
    """Database schema inspection and management utilities (stateless)."""
    
    def __init__(self):
        """Initialize stateless schema inspector."""
        pass
    
    def refresh(self, engine: Engine) -> None:
        """Refresh inspector to clear cached metadata."""
        # Force new inspector creation by clearing cache
        pass
    
    def get_table_names(self, engine: Engine, schema: Optional[str] = None) -> List[str]:
        """Get list of tables in current schema."""
        inspector = inspect(engine)
        return inspector.get_table_names(schema=schema)
    
    def get_columns(self, engine: Engine, table: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get column metadata for table."""
        inspector = inspect(engine)
        return inspector.get_columns(table, schema=schema)
    
    def has_table(self, engine: Engine, table: str, schema: Optional[str] = None) -> bool:
        """Check if table exists in current schema."""
        return table in self.get_table_names(engine, schema)
    
    def get_identity_columns(self, engine: Engine, table: str, schema: Optional[str] = None) -> Set[str]:
        """Detect identity/autoincrement columns via inspector."""
        db_type = engine.dialect.name.lower()
        if db_type == 'postgresql':
            db_type = 'postgres'
        is_oracle = db_type == 'oracle'
        
        info = self.get_columns(engine, table, schema)
        identity_cols = set()
        
        for col in info:
            if col.get('autoincrement', False):
                identity_cols.add(col['name'])
            elif is_oracle and 'identity' in str(col.get('default', '')):
                identity_cols.add(col['name'])
        
        return identity_cols
    
    def get_primary_keys(self, engine: Engine, table: str, schema: Optional[str] = None) -> List[str]:
        """Get primary key columns for table."""
        inspector = inspect(engine)
        pk_constraint = inspector.get_pk_constraint(table, schema=schema)
        return pk_constraint.get('constrained_columns', []) if pk_constraint else []
    
    def get_unique_constraints(self, engine: Engine, table: str, schema: Optional[str] = None) -> List[List[str]]:
        """Get unique constraint columns for table."""
        inspector = inspect(engine)
        unique_constraints = inspector.get_unique_constraints(table, schema=schema)
        return [uc.get('column_names', []) for uc in unique_constraints]
    
    def validate_upsert_constraints(self, engine: Engine, table: str, key_cols: List[str], schema: Optional[str] = None) -> None:
        """Validate that PK or UNIQUE constraint exists on key columns."""
        db_type = engine.dialect.name.lower()
        if db_type == 'postgresql':
            db_type = 'postgres'
        
        if db_type not in ('postgres', 'mysql', 'sqlite'):
            return  # Oracle/MSSQL MERGE doesn't require constraints
        
        pk_cols = set(self.get_primary_keys(engine, table, schema))
        unique_constraints = self.get_unique_constraints(engine, table, schema)
        unique_cols = set()
        for uc in unique_constraints:
            unique_cols.update(uc)
        
        key_set = set(key_cols)
        if not (key_set <= pk_cols or key_set <= unique_cols):
            raise ValueError(
                f"Upsert requires PK or UNIQUE constraint on {key_cols} for {db_type}. "
                f"Found PK: {list(pk_cols)}, UNIQUE: {list(unique_cols)}"
            )
    
    def add_columns(
        self,
        engine: Engine,
        table: str,
        new_columns: Dict[str, str],
        schema: Optional[str],
        quote_fn,
        execute_fn
    ) -> None:
        """Add new columns to existing table."""
        db_type = engine.dialect.name.lower()
        is_oracle = db_type == 'oracle'
        
        schema_name = f"{schema}." if schema else ""
        table_quoted = quote_fn(table)
        
        for col, dtype_str in new_columns.items():
            col_quoted = quote_fn(col)
            alter_sql = f"ALTER TABLE {schema_name}{table_quoted} ADD {col_quoted} {dtype_str}"
            
            if is_oracle:
                alter_sql = f"ALTER TABLE {schema_name}{table_quoted} ADD ({col_quoted} {dtype_str})"
            
            execute_fn(text(alter_sql))
            logger.info(f"Added column {col} to {table}")
    
    def alter_column_type(
        self,
        engine: Engine,
        table: str,
        column: str,
        new_type: str,
        schema: Optional[str],
        quote_fn,
        execute_fn
    ) -> None:
        """Alter column type (MODIFY/ALTER COLUMN)."""
        db_type = engine.dialect.name.lower()
        if db_type == 'postgresql':
            db_type = 'postgres'
        is_oracle = db_type == 'oracle'
        
        schema_name = f"{schema}." if schema else ""
        table_quoted = quote_fn(table)
        col_quoted = quote_fn(column)
        
        if db_type == 'postgres':
            alter_sql = f"ALTER TABLE {schema_name}{table_quoted} ALTER COLUMN {col_quoted} TYPE {new_type}"
        elif db_type in ('mssql', 'mysql'):
            keyword = 'MODIFY' if db_type == 'mysql' else 'ALTER'
            alter_sql = f"ALTER TABLE {schema_name}{table_quoted} {keyword} COLUMN {col_quoted} {new_type}"
        elif is_oracle:
            alter_sql = f"ALTER TABLE {schema_name}{table_quoted} MODIFY {col_quoted} {new_type}"
        elif db_type == 'sqlite':
            logger.warning(f"Skipping column type alter for SQLite (limited support)")
            return
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        execute_fn(text(alter_sql))
        logger.info(f"Altered column {column} in {table}")
    
    def get_column_info_map(self, engine: Engine, table: str, schema: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """Get column info as dictionary keyed by column name."""
        columns = self.get_columns(engine, table, schema)
        return {col['name']: col for col in columns}
