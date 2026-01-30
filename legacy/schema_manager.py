from __future__ import annotations
import logging, re
from typing import Optional, List, Set, Dict, Any, Union
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

def normalize_db_type(dialect_name: str) -> str:
    """Normalize database dialect name"""
    name = dialect_name.lower()
    if name == 'postgresql':
        return 'postgres'
    return name

def get_logger(name: str) -> logging.Logger:
    """Get logger instance"""
    return logging.getLogger(name)

logger = get_logger(__name__)

class SchemaManager:
    def __init__(self, engine: Engine): self.engine = engine; self.refresh()

    @property
    def db_type(self) -> str: return normalize_db_type(self.engine.dialect.name)
    def refresh(self): self._inspector = inspect(self.engine) # Cache buster

    # --- Inspection & Exploration ---
    def list_tables(self, schema: str=None, pattern: str=None) -> List[str]:
        t = self._inspector.get_table_names(schema=schema)
        return [x for x in t if re.search(pattern, x, re.I)] if pattern else t

    def get_columns(self, table: str, schema: str=None) -> List[Dict[str, Any]]:
        """Return raw column metadata for a table."""
        return self._inspector.get_columns(table, schema=schema)

    def get_primary_keys(self, table: str, schema: str=None) -> List[str]:
        """Return primary key columns for a table."""
        pk_constraint = self._inspector.get_pk_constraint(table, schema=schema)
        return pk_constraint.get('constrained_columns', []) if pk_constraint else []

    def get_unique_constraints(self, table: str, schema: str=None) -> List[List[str]]:
        """Return unique constraint column sets for a table."""
        constraints = self._inspector.get_unique_constraints(table, schema=schema)
        return [uc.get('column_names', []) for uc in constraints]

    def find_column(self, col_pattern: str, schema: str=None) -> Dict[str, List[str]]:
        """Find tables containing columns matching pattern."""
        res, pat = {}, re.compile(col_pattern, re.I)
        for t in self.list_tables(schema):
            matches = [c['name'] for c in self._inspector.get_columns(t, schema=schema) if pat.search(c['name'])]
            if matches: res[t] = matches
        return res

    def get_table_details(self, table: str, schema: str=None) -> Dict[str, Any]:
        """Deep inspection of table structure."""
        insp = self._inspector
        return {
            'columns': {c['name']: {k:v for k,v in c.items() if k!='name'} for c in insp.get_columns(table, schema=schema)},
            'pk': insp.get_pk_constraint(table, schema=schema).get('constrained_columns', []),
            'fk': insp.get_foreign_keys(table, schema=schema),
            'indexes': insp.get_indexes(table, schema=schema),
            'constraints': insp.get_unique_constraints(table, schema=schema),
            'identity': self.get_identity_columns(table, schema)
        }

    # --- Identity & Constraints ---
    def get_identity_columns(self, table: str, schema: str=None) -> Set[str]:
        is_oracle = self.db_type == 'oracle'
        try: cols = self._inspector.get_columns(table, schema=schema)
        except Exception: return set()
        return {c['name'] for c in cols if c.get('autoincrement', False) or (is_oracle and 'identity' in str(c.get('default', '') or '').lower())}

    def validate_upsert_constraints(self, table: str, key_cols: List[str], schema: str=None) -> None:
        if self.db_type not in ('postgres', 'mysql', 'sqlite'): return
        pk_cols = set(self._inspector.get_pk_constraint(table, schema=schema).get('constrained_columns', []))
        unique_cons = [set(uc.get('column_names', [])) for uc in self._inspector.get_unique_constraints(table, schema=schema)]
        if not (set(key_cols) <= pk_cols or any(set(key_cols) <= uc for uc in unique_cons)):
            raise ValueError(f"Upsert safety failed: {key_cols} not covered by PK/Unique on '{table}'")

    # --- Diff & Modification ---
    def compare_to_structure(self, table: str, structure: Dict[str, str], schema: str=None) -> Dict[str, Any]:
        """Compare table columns against {col_name: type_str} expectation."""
        curr = {c['name']: str(c['type']) for c in self._inspector.get_columns(table, schema=schema)}
        return {
            'missing_in_db': [k for k in structure if k not in curr],
            'extra_in_db': [k for k in curr if k not in structure],
            'type_mismatch': {k: f"{curr[k]} != {structure[k]}" for k in structure if k in curr and str(structure[k]).lower() not in str(curr[k]).lower()}
        }

    def has_table(self, table: str, schema: Optional[str]=None) -> bool:
        with self.engine.connect() as conn: return self.engine.dialect.has_table(conn, table, schema=schema)

    def add_column(self, table: str, col_name: str, col_type: str, schema: str=None) -> None:
        q = self.engine.dialect.identifier_preparer.quote
        pre = f"{q(schema)}." if schema else ""
        fmt = "({0} {1})" if self.db_type == 'oracle' else "{0} {1}"
        self._exec_ddl(f"ALTER TABLE {pre}{q(table)} ADD {fmt.format(q(col_name), col_type)}", f"Added column {col_name} to {table}")

    def alter_column_type(self, table: str, col_name: str, new_type: str, schema: str=None) -> None:
        q = self.engine.dialect.identifier_preparer.quote
        pre = f"{q(schema)}." if schema else ""
        db, tq, cq = self.db_type, q(table), q(col_name)
        if db == 'postgres': sql = f"ALTER TABLE {pre}{tq} ALTER COLUMN {cq} TYPE {new_type}"
        elif db == 'mysql': sql = f"ALTER TABLE {pre}{tq} MODIFY COLUMN {cq} {new_type}"
        elif db == 'oracle': sql = f"ALTER TABLE {pre}{tq} MODIFY ({cq} {new_type})"
        elif db == 'mssql': sql = f"ALTER TABLE {pre}{tq} ALTER COLUMN {cq} {new_type}"
        else: sql = f"ALTER TABLE {pre}{tq} MODIFY {cq} {new_type}"
        self._exec_ddl(sql, f"Altered column {col_name} to {new_type} on {table}")

    def drop_table(self, table: str, schema: str=None) -> None:
        q = self.engine.dialect.identifier_preparer.quote
        pre = f"{q(schema)}." if schema else ""
        self.execute_ddl(f"DROP TABLE {pre}{q(table)}", f"Dropped table {table}")

    def drop_table_if_exists(self, table: str, schema: str=None) -> None:
        if self.has_table(table, schema=schema):
            self.drop_table(table, schema=schema)

    def get_row_count(self, table: str, schema: str=None) -> int:
        q = self.engine.dialect.identifier_preparer.quote
        pre = f"{q(schema)}." if schema else ""
        sql = f"SELECT COUNT(*) FROM {pre}{q(table)}"
        with self.engine.connect() as conn:
            result = conn.execute(text(sql))
            return int(result.scalar() or 0)

    def execute_ddl(self, sql: str, msg: str) -> None:
        """Execute DDL statement with logging and error handling."""
        self._exec_ddl(sql, msg)

    def _exec_ddl(self, sql: str, msg: str):
        try:
            with self.engine.begin() as conn: conn.execute(text(sql))
            logger.info(msg)
        except SQLAlchemyError as e:
            logger.error(f"DDL failed: {e}"); raise
