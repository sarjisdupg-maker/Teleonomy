from __future__ import annotations

from typing import Iterable, Mapping, Sequence, Union, List, Dict, Any, Tuple, Callable
from contextlib import contextmanager
from itertools import islice
import re

import sqlalchemy as sa
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.dialects import oracle, sqlite, mysql, postgresql, mssql

try:
    import pandas as pd
    import numpy as np
except ImportError:
    pd = None
    np = None

from schema_align.core import DataAligner
from schema_align.config import AlignmentConfig
from .logger import log_call, log_json
from .where_build import build_update


DataItem = Mapping[str, Any]
DataLike = Union[Sequence[DataItem], DataItem, "pd.DataFrame"]


def _write_sql_to_file(func_name: str, table_name: str, statement: Any, engine: Engine):
    """Writes the generated SQL statement to a file for tracing."""
    filename = f"{func_name}_{table_name}.txt"
    try:
        # Try to compile with literal binds to show values
        if hasattr(statement, 'compile'):
            sql_str = str(statement.compile(dialect=engine.dialect, compile_kwargs={"literal_binds": True}))
        else:
            sql_str = str(statement)
            
        with open(filename, "w", encoding="utf-8") as f:
            f.write(sql_str)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Could not write SQL trace to {filename}: {e}")


def _normalize_data(data: DataLike) -> List[Dict[str, Any]]:
    """Normalize input data into list[dict], with Ultra-Safe null handling for Oracle/MSSQL."""
    if pd is not None and isinstance(data, pd.DataFrame):
        # Stage 1: Vectorized cleaning for speed
        data_clean = data.astype(object).where(pd.notnull(data), None)
        records = data_clean.to_dict(orient="records")
    elif isinstance(data, Mapping):
        records = [dict(data)]
    elif isinstance(data, Sequence):
        records = [dict(row) for row in data]
    else:
        raise TypeError("Unsupported data type; expected dict, list[dict], or DataFrame")
        
    # Stage 2: Recursive sweep for pure Python types and Timestamp conversion
    import datetime as dt
    for r in records:
        for k, v in r.items():
            # Handle Pandas-specific types
            if hasattr(v, 'to_pydatetime'):
                r[k] = v.to_pydatetime()
            elif pd is not None and v is pd.NaT:
                r[k] = None
            # Aggressive null check for all varieties of nan
            elif v is None:
                r[k] = None
            elif pd is not None and pd.isna(v):
                r[k] = None
            elif np is not None and isinstance(v, (float, np.floating)) and np.isnan(v):
                r[k] = None
                
    return records


def _chunk_iter(rows: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    """Yield chunks of rows with given size."""
    it = iter(rows)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk


@contextmanager
def _ensure_connection(eng_or_conn: Union[Engine, Connection]) -> Iterable[Connection]:
    """Yield a Connection; do not manage transaction if caller passed Connection."""
    if isinstance(eng_or_conn, Connection):
        yield eng_or_conn
        return
    engine = eng_or_conn
    with engine.begin() as conn:
        yield conn


def _get_table(conn: Connection, table: Union[str, sa.Table]) -> sa.Table:
    """Resolve table name or return Table as-is, reflecting if needed."""
    if isinstance(table, sa.Table):
        return table
    meta = sa.MetaData()
    return sa.Table(table, meta, autoload_with=conn)


def _ensure_data_columns_in_table(rows: List[Dict[str, Any]], table: sa.Table) -> None:
    """Ensure all keys in rows exist in table columns; raise if not."""
    table_cols = {c.name for c in table.columns}
    invalid = set()
    for r in rows:
        invalid.update(k for k in r.keys() if k not in table_cols)
    if invalid:
        raise ValueError(f"Columns not found in table '{table.name}': {sorted(invalid)}")


def _reflect_unique_sets(inspector: Inspector, table: sa.Table) -> List[Tuple[str, ...]]:
    """Return list of unique/PK column name tuples for table."""
    pk = inspector.get_pk_constraint(table.name)
    pk_cols = tuple(pk.get("constrained_columns") or [])
    unique_sets: List[Tuple[str, ...]] = []
    if pk_cols:
        unique_sets.append(pk_cols)
    for uq in inspector.get_unique_constraints(table.name):
        cols = tuple(uq.get("column_names") or [])
        if cols:
            unique_sets.append(cols)
    return unique_sets


def _validate_constrain_unique(conn: Connection, table: sa.Table, constrain: List[str]) -> Tuple[str, ...]:
    """
    Validate that `constrain` corresponds to a known PK/unique constraint if possible.

    Note: Some DBs or permissions may not expose all metadata; in that case we logically
    trust the caller but still ensure columns exist. Uses case-insensitive matching.
    """
    # Case-insensitive column matching
    cols_map = {c.name.lower(): c.name for c in table.columns}
    resolved_constrain = []
    for c in constrain:
        if c.lower() not in cols_map:
            raise ValueError(f"constrain column '{c}' not found in table '{table.name}'. Available: {list(cols_map.values())}")
        resolved_constrain.append(cols_map[c.lower()])
    
    if not resolved_constrain:
        raise ValueError("constrain must contain at least one column name")
    
    inspector = sa.inspect(conn)
    try:
        unique_sets = _reflect_unique_sets(inspector, table)
    except Exception as e:
        # [FIXED] BUG #13: Log reflection failure
        import logging
        logging.getLogger(__name__).warning(f"Constraint reflection failed: {e}. Trusting caller.")
        return tuple(resolved_constrain)
    
    # Case-insensitive comparison for unique sets
    cset = tuple(sorted(c.lower() for c in resolved_constrain))
    for u in unique_sets:
        # Comparison must be case-insensitive to handle SQLite/Oracle variations
        if tuple(sorted(c.lower() for c in u)) == cset:
            return tuple(resolved_constrain)
            
    # Not strictly unique according to catalog; warn via exception, forcing caller to think
    raise ValueError(
        f"constrain={constrain} does not match any primary/unique key on '{table.name}'. "
        f"Found unique sets: {unique_sets}. "
        "Upsert requires a unique constraint to be safe."
    )


def _execute_with_row_isolation(
    conn: Connection,
    rows: List[Dict[str, Any]],
    bulk_exec: Callable[[List[Dict[str, Any]]], None],
    row_exec: Callable[[Dict[str, Any]], None],
    tolerance: int,
) -> Dict[str, Any]:
    """
    Execute in two phases: try bulk; if fails, retry row-by-row with attribution.
    Returns execution statistics.

    - If bulk_exec succeeds, done.
    - If bulk_exec fails, do row_exec for each row; any failing row is collected and
      reported in an aggregated exception with per-row errors.
    """
    if not rows:
        return {"total": 0, "success": 0, "failed": 0, "method": "none"}
    
    try:
        bulk_exec(rows)
        return {"total": len(rows), "success": len(rows), "failed": 0, "method": "bulk"}
    except Exception as bulk_err:
        # Fallback to row-by-row (lazy method)
        success_count = 0
        bad_rows: List[Tuple[int, Dict[str, Any], Exception]] = []
        failures_in_row = 0
        idx = -1
        
        for idx, r in enumerate(rows):
            try:
                row_exec(r)
                success_count += 1
            except Exception as row_err:
                bad_rows.append((idx, r, row_err))
                failures_in_row += 1
                if failures_in_row >= tolerance:
                    break
        
        stats = {
            "total": len(rows),
            "success": success_count,
            "failed": len(bad_rows),
            "method": "lazy_fallback",
            "bulk_error": str(bulk_err)
        }
        
        # [FIXED] BUG #6: Clearer abort message
        if failures_in_row >= tolerance:
            stats["aborted"] = True
            stats["unprocessed"] = len(rows) - (idx + 1)
        
        if bad_rows:
            messages = []
            for idx_inner, r, err in bad_rows:
                messages.append(
                    f"row_index={idx_inner}, row_keys={sorted(r.keys())}, error={type(err).__name__}: {err}"
                )
            error_msg = (
                "Bulk operation failed, lazy fallback partially succeeded.\n"
                f"Bulk error: {type(bulk_err).__name__}: {bulk_err}\n"
                f"Stats: {success_count}/{len(rows)} succeeded\n"
            )
            if failures_in_row >= tolerance:
                 error_msg += f"\n[!] Processing aborted after {tolerance} failures. {len(rows) - idx - 1} rows not attempted."

            
            error_msg += "Failing rows (first subset):\n" + "\n".join(messages)
            if success_count == 0:  # Total failure
                raise RuntimeError(error_msg) from bulk_err
            # Partial success - log warning but don't raise
            import logging
            logging.getLogger(__name__).warning(error_msg)
        
        return stats


# ---------- ORACLE ----------

def _oracle_merge_sql(table_name: str, key_cols: Tuple[str, ...], sample_row: Dict[str, Any]) -> str:
    """Build Oracle MERGE statement as raw SQL text."""
    src_cols = list(sample_row.keys())
    
    # Build source SELECT from DUAL
    select_parts = [f":{c} AS {c}" for c in src_cols]
    src_sql = f"SELECT {', '.join(select_parts)} FROM DUAL"
    
    # ON clause
    on_parts = [f"tgt.{c} = src.{c}" for c in key_cols]
    on_sql = " AND ".join(on_parts)
    
    # UPDATE SET clause (non-key columns, case-insensitive comparison)
    key_cols_lower = {k.lower() for k in key_cols}
    update_cols = [c for c in src_cols if c.lower() not in key_cols_lower]
    update_sql = ", ".join(f"tgt.{c} = src.{c}" for c in update_cols) if update_cols else None
    
    # INSERT clause
    insert_cols_sql = ", ".join(src_cols)
    insert_vals_sql = ", ".join(f"src.{c}" for c in src_cols)
    
    # Build full MERGE
    merge_sql = f"MERGE INTO {table_name} tgt USING ({src_sql}) src ON ({on_sql})"
    if update_sql:
        merge_sql += f" WHEN MATCHED THEN UPDATE SET {update_sql}"
    merge_sql += f" WHEN NOT MATCHED THEN INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql})"
    
    return merge_sql


def oracle_upsert(engine: Engine, data: DataLike, table: Union[str, sa.Table], constrain: List[str], chunk: int = 10_000, tolerance: int = 5, trace_sql: bool = False) -> Dict[str, Any]:
    """Oracle upsert using MERGE with row-level fallback and error attribution."""
    rows = _normalize_data(data)
    if not rows:
        return {"total": 0, "success": 0, "failed": 0, "method": "none"}
    
    stats = {"total": len(rows), "success": 0, "failed": 0, "chunks": []}
    
    with _ensure_connection(engine) as conn:
        tbl = _get_table(conn, table)
        _ensure_data_columns_in_table(rows, tbl)
        key_cols = _validate_constrain_unique(conn, tbl, constrain)
        
        # Build raw SQL MERGE statement
        merge_sql = _oracle_merge_sql(tbl.name, key_cols, rows[0])
        if trace_sql:
            _write_sql_to_file("oracle_upsert", tbl.name, merge_sql, engine)
        
        def exec_chunk(part: List[Dict[str, Any]]) -> None:
            for row in part:
                conn.execute(sa.text(merge_sql), row)
        
        def exec_row(row: Dict[str, Any]) -> None:
            conn.execute(sa.text(merge_sql), row)
        
        for part in _chunk_iter(rows, chunk):
            chunk_stats = _execute_with_row_isolation(conn, part, exec_chunk, exec_row, tolerance)
            stats["chunks"].append(chunk_stats)
            stats["success"] += chunk_stats["success"]
            stats["failed"] += chunk_stats["failed"]
    
    return stats


def oracle_insert(engine: Engine, data: DataLike, table: Union[str, sa.Table], chunk_size: int = 10_000, tolerance: int = 5, trace_sql: bool = False) -> int:
    """Oracle bulk insert with row-level fallback and error attribution."""
    rows = _normalize_data(data)
    if not rows:
        return 0
    total_success = 0
    with _ensure_connection(engine) as conn:
        tbl = _get_table(conn, table)
        _ensure_data_columns_in_table(rows, tbl)
        stmt = tbl.insert()
        if trace_sql:
            _write_sql_to_file("oracle_insert", tbl.name, stmt, engine)
        def exec_chunk(part: List[Dict[str, Any]]) -> None:
            conn.execute(stmt, part)
        def exec_row(row: Dict[str, Any]) -> None:
            conn.execute(stmt, [row])
        for part in _chunk_iter(rows, chunk_size):
            stats = _execute_with_row_isolation(conn, part, exec_chunk, exec_row, tolerance)
            total_success += stats["success"]
    return total_success


# ---------- SQLITE ----------

def _sqlite_upsert_stmt(table: sa.Table, key_cols: Tuple[str, ...], sample_row: Dict[str, Any]) -> sa.sql.dml.Insert:
    """Build SQLite INSERT .. ON CONFLICT .. DO UPDATE."""
    ins = sqlite.insert(table)
    update_cols = {c: ins.excluded[c] for c in sample_row.keys() if c not in key_cols}
    return ins.on_conflict_do_update(index_elements=list(key_cols), set_=update_cols)


def sqlite_upsert(engine: Engine, data: DataLike, table: Union[str, sa.Table], constrain: List[str], chunk: int = 10_000, tolerance: int = 5, trace_sql: bool = False) -> Dict[str, Any]:
    """SQLite upsert using ON CONFLICT with row-level fallback and error attribution."""
    rows = _normalize_data(data)
    if not rows:
        return {"total": 0, "success": 0, "failed": 0, "method": "none"}
    stats = {"total": len(rows), "success": 0, "failed": 0, "chunks": []}
    with _ensure_connection(engine) as conn:
        tbl = _get_table(conn, table)
        _ensure_data_columns_in_table(rows, tbl)
        key_cols = _validate_constrain_unique(conn, tbl, constrain)
        def exec_chunk(part: List[Dict[str, Any]]) -> None:
            stmt = _sqlite_upsert_stmt(tbl, key_cols, part[0])
            if trace_sql:
                _write_sql_to_file("sqlite_upsert", tbl.name, stmt, engine)
            conn.execute(stmt, part)
        def exec_row(row: Dict[str, Any]) -> None:
            stmt = _sqlite_upsert_stmt(tbl, key_cols, row)
            conn.execute(stmt, [row])
        for part in _chunk_iter(rows, chunk):
            chunk_stats = _execute_with_row_isolation(conn, part, exec_chunk, exec_row, tolerance)
            stats["chunks"].append(chunk_stats)
            stats["success"] += chunk_stats["success"]
            stats["failed"] += chunk_stats["failed"]
    return stats


def sqlite_insert(engine: Engine, data: DataLike, table: Union[str, sa.Table], chunk_size: int = 10_000, tolerance: int = 5, trace_sql: bool = False) -> int:
    """SQLite bulk insert with row-level fallback and error attribution."""
    rows = _normalize_data(data)
    if not rows:
        return 0
    total_success = 0
    with _ensure_connection(engine) as conn:
        tbl = _get_table(conn, table)
        _ensure_data_columns_in_table(rows, tbl)
        stmt = tbl.insert()
        if trace_sql:
            _write_sql_to_file("sqlite_insert", tbl.name, stmt, engine)
        def exec_chunk(part: List[Dict[str, Any]]) -> None:
            conn.execute(stmt, part)
        def exec_row(row: Dict[str, Any]) -> None:
            conn.execute(stmt, [row])
        for part in _chunk_iter(rows, chunk_size):
            stats = _execute_with_row_isolation(conn, part, exec_chunk, exec_row, tolerance)
            total_success += stats["success"]
    return total_success


# ---------- MYSQL ----------

def _mysql_upsert_stmt(table: sa.Table, sample_row: Dict[str, Any]) -> sa.sql.dml.Insert:
    """
    Build MySQL INSERT .. ON DUPLICATE KEY UPDATE.

    Note: MySQL chooses the key (PK or any unique index); we don't control which one.
    Caller must ensure data conforms to schema's uniqueness rules.
    """
    ins = mysql.insert(table)
    update_cols = {c: ins.inserted[c] for c in sample_row.keys()}
    return ins.on_duplicate_key_update(**update_cols)


def mysql_upsert(engine: Engine, data: DataLike, table: Union[str, sa.Table], constrain: List[str], chunk: int = 10_000, tolerance: int = 5, trace_sql: bool = False) -> Dict[str, Any]:
    """
    MySQL upsert using ON DUPLICATE KEY UPDATE with row-level fallback.

    NOTE: `constrain` is accepted for API symmetry but is not used to define conflict
    target; MySQL uses whatever unique/PK constraint is violated.
    """
    rows = _normalize_data(data)
    if not rows:
        return {"total": 0, "success": 0, "failed": 0, "method": "none"}
    stats = {"total": len(rows), "success": 0, "failed": 0, "chunks": []}
    with _ensure_connection(engine) as conn:
        tbl = _get_table(conn, table)
        _ensure_data_columns_in_table(rows, tbl)
        _ = constrain  # unused, see docstring
        def exec_chunk(part: List[Dict[str, Any]]) -> None:
            stmt = _mysql_upsert_stmt(tbl, part[0])
            if trace_sql:
                _write_sql_to_file("mysql_upsert", tbl.name, stmt, engine)
            conn.execute(stmt, part)
        def exec_row(row: Dict[str, Any]) -> None:
            stmt = _mysql_upsert_stmt(tbl, row)
            conn.execute(stmt, [row])
        for part in _chunk_iter(rows, chunk):
            chunk_stats = _execute_with_row_isolation(conn, part, exec_chunk, exec_row, tolerance)
            stats["chunks"].append(chunk_stats)
            stats["success"] += chunk_stats["success"]
            stats["failed"] += chunk_stats["failed"]
    return stats


def mysql_insert(engine: Engine, data: DataLike, table: Union[str, sa.Table], chunk_size: int = 10_000, tolerance: int = 5, trace_sql: bool = False) -> int:
    """MySQL bulk insert with row-level fallback and error attribution."""
    rows = _normalize_data(data)
    if not rows:
        return 0
    total_success = 0
    with _ensure_connection(engine) as conn:
        tbl = _get_table(conn, table)
        _ensure_data_columns_in_table(rows, tbl)
        stmt = tbl.insert()
        if trace_sql:
            _write_sql_to_file("mysql_insert", tbl.name, stmt, engine)
        def exec_chunk(part: List[Dict[str, Any]]) -> None:
            conn.execute(stmt, part)
        def exec_row(row: Dict[str, Any]) -> None:
            conn.execute(stmt, [row])
        for part in _chunk_iter(rows, chunk_size):
            stats = _execute_with_row_isolation(conn, part, exec_chunk, exec_row, tolerance)
            total_success += stats["success"]
    return total_success


# ---------- POSTGRESQL ----------

def _postgres_upsert_stmt(table: sa.Table, key_cols: Tuple[str, ...], sample_row: Dict[str, Any]) -> sa.sql.dml.Insert:
    """Build PostgreSQL INSERT .. ON CONFLICT .. DO UPDATE."""
    ins = postgresql.insert(table)
    update_cols = {c: ins.excluded[c] for c in sample_row.keys() if c not in key_cols}
    return ins.on_conflict_do_update(index_elements=list(key_cols), set_=update_cols)


def postgres_upsert(engine: Engine, data: DataLike, table: Union[str, sa.Table], constrain: List[str], chunk: int = 10_000, tolerance: int = 5, trace_sql: bool = False) -> Dict[str, Any]:
    """PostgreSQL upsert with row-level fallback and explicit conflict target validation."""
    rows = _normalize_data(data)
    if not rows:
        return {"total": 0, "success": 0, "failed": 0, "method": "none"}
    
    stats = {"total": len(rows), "success": 0, "failed": 0, "chunks": []}
    
    with _ensure_connection(engine) as conn:
        tbl = _get_table(conn, table)
        _ensure_data_columns_in_table(rows, tbl)
        key_cols = _validate_constrain_unique(conn, tbl, constrain)
        
        def exec_chunk(part: List[Dict[str, Any]]) -> None:
            stmt = _postgres_upsert_stmt(tbl, key_cols, part[0])
            if trace_sql:
                _write_sql_to_file("postgres_upsert", tbl.name, stmt, engine)
            conn.execute(stmt, part)
        
        def exec_row(row: Dict[str, Any]) -> None:
            stmt = _postgres_upsert_stmt(tbl, key_cols, row)
            conn.execute(stmt, [row])
        
        for part in _chunk_iter(rows, chunk):
            chunk_stats = _execute_with_row_isolation(conn, part, exec_chunk, exec_row, tolerance)
            stats["chunks"].append(chunk_stats)
            stats["success"] += chunk_stats["success"]
            stats["failed"] += chunk_stats["failed"]
    
    return stats


def postgres_insert(engine: Engine, data: DataLike, table: Union[str, sa.Table], chunk_size: int = 10_000, tolerance: int = 5, trace_sql: bool = False) -> int:
    """PostgreSQL bulk insert with row-level fallback and error attribution."""
    rows = _normalize_data(data)
    if not rows:
        return 0
    total_success = 0
    with _ensure_connection(engine) as conn:
        tbl = _get_table(conn, table)
        _ensure_data_columns_in_table(rows, tbl)
        stmt = tbl.insert()
        if trace_sql:
            _write_sql_to_file("postgres_insert", tbl.name, stmt, engine)
        def exec_chunk(part: List[Dict[str, Any]]) -> None:
            conn.execute(stmt, part)
        def exec_row(row: Dict[str, Any]) -> None:
            conn.execute(stmt, [row])
        for part in _chunk_iter(rows, chunk_size):
            stats = _execute_with_row_isolation(conn, part, exec_chunk, exec_row, tolerance)
            total_success += stats["success"]
    return total_success


# ---------- ENHANCED LAZY FALLBACK UTILITIES ----------

@log_call
def auto_upsert(
    engine: Engine, 
    data: DataLike, 
    table: Union[str, sa.Table], 
    constrain: List[str], 
    chunk: int = 10_000, 
    tolerance: int = 5,
    add_missing_cols: bool = True,
    failure_threshold: float = 3,
    semantic_meta: Dict[str, str] | None = None,
    trace_sql: bool = False
) -> Dict[str, Any]:
    """Auto-detect database dialect and perform upsert with Schema Alignment and lazy fallback."""
    # 1. Align Data
    # 1. Align Data
    table_name = table.name if isinstance(table, sa.Table) else table
    # Create configuration
    config = AlignmentConfig(
        failure_threshold=failure_threshold if failure_threshold is not None else 0.1
    )
    # Instantiate DataAligner
    aligner = DataAligner(db_type=engine.dialect.name, config=config)
    
    # Ensure data is DataFrame
    if pd is not None and not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)
    elif pd is None:
         # Fallback or error if pd is missing but DataAligner requires it (DataAligner imports pd top level so it should be there)
         pass

    # Perform alignment (returns a DataFrame or similar structure)
    data = aligner.align(
        conn=engine,
        df=data,
        table=table_name,
        add_missing_cols=add_missing_cols if add_missing_cols is not None else True
    )
    
    dialect = engine.dialect.name.lower()
    if dialect == 'postgresql':
        return postgres_upsert(engine, data, table, constrain, chunk, tolerance, trace_sql=trace_sql)
    elif dialect == 'oracle':
        return oracle_upsert(engine, data, table, constrain, chunk, tolerance, trace_sql=trace_sql)
    elif dialect == 'mysql':
        return mysql_upsert(engine, data, table, constrain, chunk, tolerance, trace_sql=trace_sql)
    elif dialect == 'sqlite':
        return sqlite_upsert(engine, data, table, constrain, chunk, tolerance, trace_sql=trace_sql)
    elif 'mssql' in dialect or 'sqlserver' in dialect:
        return mssql_upsert(engine, data, table, constrain, chunk, tolerance, trace_sql=trace_sql)
    else:
        raise ValueError(f"Unsupported database dialect: {dialect}")


@log_call
def auto_insert(
    engine: Engine, 
    data: DataLike, 
    table: Union[str, sa.Table], 
    chunk_size: int = 10_000, 
    tolerance: int = 5,
    add_missing_cols: bool = True,
    failure_threshold: float = 3,
    semantic_meta: Dict[str, str] | None = None,
    trace_sql: bool = False
) -> int:
    """Auto-detect database dialect and perform bulk insert with Schema Alignment and lazy fallback."""
    # 1. Align Data
    table_name = table.name if isinstance(table, sa.Table) else table
    # Create configuration
    config = AlignmentConfig(
        failure_threshold=failure_threshold if failure_threshold is not None else 0.1
    )
    # Instantiate DataAligner
    aligner = DataAligner(db_type=engine.dialect.name, config=config)

    # Ensure data is DataFrame
    if pd is not None and not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)

    # Perform alignment
    data = aligner.align(
        conn=engine,
        df=data,
        table=table_name,
        add_missing_cols=add_missing_cols if add_missing_cols is not None else True
    )

    dialect = engine.dialect.name.lower()
    if dialect == 'postgresql':
        return postgres_insert(engine, data, table, chunk_size, tolerance, trace_sql=trace_sql)
    elif dialect == 'oracle':
        return oracle_insert(engine, data, table, chunk_size, tolerance, trace_sql=trace_sql)
    elif dialect == 'mysql':
        return mysql_insert(engine, data, table, chunk_size, tolerance, trace_sql=trace_sql)
    elif dialect == 'sqlite':
        return sqlite_insert(engine, data, table, chunk_size, tolerance, trace_sql=trace_sql)
    elif 'mssql' in dialect or 'sqlserver' in dialect:
        return mssql_insert(engine, data, table, chunk_size, tolerance, trace_sql=trace_sql)
    else:
        raise ValueError(f"Unsupported database dialect: {dialect}")


@log_call
def auto_update(
    engine: Engine,
    table: Union[str, sa.Table],
    data: DataLike,
    where: List[Union[str, Tuple[str, str, Any]]],
    expression: str | None = None,
    add_missing_cols: bool = True,
    failure_threshold: float = 3,
    semantic_meta: Dict[str, str] | None = None,
    trace_sql: bool = False
) -> int:
    """Perform targeted updates using where clauses with Schema Alignment."""
    # 1. Align Data
    table_name = table.name if isinstance(table, sa.Table) else table
    # Create configuration
    config = AlignmentConfig(
        failure_threshold=failure_threshold if failure_threshold is not None else 0.1
    )
    # Instantiate DataAligner
    # Instantiate DataAligner
    aligner = DataAligner(db_type=engine.dialect.name, config=config)

    # Ensure data is DataFrame
    if pd is not None and not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)

    # Perform alignment
    data = aligner.align(
        conn=engine,
        df=data,
        table=table_name,
        add_missing_cols=add_missing_cols if add_missing_cols is not None else True
    )
    
    records = _normalize_data(data)
    if not records:
        return 0
    
    # We only update using the first record for targeted updates
    record = records[0]
    
    # Align WHERE clause column names to match database schema
    # This is critical for case-sensitive databases like SQLite
    with _ensure_connection(engine) as conn:
        tbl = _get_table(conn, table_name)
        db_cols_map = {c.name.lower(): c.name for c in tbl.columns}
        
        aligned_where = []
        for condition in where:
            if isinstance(condition, tuple) and len(condition) == 3:
                field, op, value = condition
                # Clean field name (strip BOM and lowercase)
                field_clean = str(field).lstrip('\ufeff').lower()
                # Find the actual database column name (case-insensitive match)
                aligned_field = db_cols_map.get(field_clean, field)
                aligned_where.append((aligned_field, op, value))
            else:
                aligned_where.append(condition)
    
    # v2 Update Builder (Using where_build)
    dialect = engine.dialect.name.lower()
    sql, params = build_update(record, table_name, aligned_where, dialect=dialect, expression=expression)
    
    if trace_sql:
        _write_sql_to_file("auto_update", table_name, sql, engine)
    
    with _ensure_connection(engine) as conn:
        result = conn.execute(sa.text(sql), params)
        return result.rowcount




def lazy_upsert_only(engine: Engine, data: DataLike, table: Union[str, sa.Table], constrain: List[str], trace_sql: bool = False) -> Dict[str, Any]:
    """Force row-by-row upsert (skip bulk, use lazy method only)."""
    rows = _normalize_data(data)
    if not rows:
        return {"total": 0, "success": 0, "failed": 0, "method": "lazy_only"}
    
    dialect = engine.dialect.name.lower()
    success_count = 0
    failed_count = 0
    errors = []
    
    with _ensure_connection(engine) as conn:
        tbl = _get_table(conn, table)
        _ensure_data_columns_in_table(rows, tbl)
        key_cols = _validate_constrain_unique(conn, tbl, constrain)
        
        for idx, row in enumerate(rows):
            try:
                if dialect == 'postgresql':
                    stmt = _postgres_upsert_stmt(tbl, key_cols, row)
                elif dialect == 'oracle':
                    # Use raw SQL for Oracle merge, wrapped in text()
                    raw_sql = _oracle_merge_sql(tbl.name, key_cols, row)
                    stmt = sa.text(raw_sql)
                elif dialect == 'mysql':
                    stmt = _mysql_upsert_stmt(tbl, key_cols, row)
                elif dialect == 'sqlite':
                    stmt = _sqlite_upsert_stmt(tbl, key_cols, row)
                elif 'mssql' in dialect:
                    stmt = _mssql_merge_statement(tbl, key_cols, row)
                else:
                    raise ValueError(f"Unsupported dialect: {dialect}")
                
                if trace_sql:
                    _write_sql_to_file("lazy_upsert_only", tbl.name, stmt, engine)
                conn.execute(stmt, [row])
                success_count += 1
            except Exception as e:
                failed_count += 1
                errors.append(f"Row {idx}: {type(e).__name__}: {e}")
    
    return {
        "total": len(rows),
        "success": success_count,
        "failed": failed_count,
        "method": "lazy_only",
        "errors": errors[:10]  # First 10 errors
    }


# ---------- MSSQL ----------

def _mssql_merge_statement(table: sa.Table, key_cols: Tuple[str, ...], sample_row: Dict[str, Any]) -> mssql.dml.Merge:
    src_cols = list(sample_row.keys())
    src_alias = sa.alias(
        sa.select(*[sa.bindparam(c, key=None, unique=True) for c in src_cols]).subquery(),
        name="src",
    )
    merge = mssql.dml.Merge(table, src_alias)
    on_expr = sa.and_(*[table.c[c] == src_alias.c[c] for c in key_cols])
    merge = merge.on(on_expr)
    update_cols = {c: src_alias.c[c] for c in src_cols if c not in key_cols}
    if update_cols:
        merge = merge.when_matched_then_update(set_=update_cols)
    insert_cols = {c: src_alias.c[c] for c in src_cols}
    merge = merge.when_not_matched_then_insert(values=insert_cols)
    return merge


def mssql_upsert(engine: Engine, data: DataLike, table: Union[str, sa.Table], constrain: List[str], chunk: int = 10_000, tolerance: int = 5, trace_sql: bool = False) -> Dict[str, Any]:
    """MS SQL Server upsert using MERGE with row-level fallback and validated match key."""
    rows = _normalize_data(data)
    if not rows:
        return {"total": 0, "success": 0, "failed": 0, "method": "none"}
    stats = {"total": len(rows), "success": 0, "failed": 0, "chunks": []}
    with _ensure_connection(engine) as conn:
        tbl = _get_table(conn, table)
        _ensure_data_columns_in_table(rows, tbl)
        key_cols = _validate_constrain_unique(conn, tbl, constrain)
        def exec_chunk(part: List[Dict[str, Any]]) -> None:
            stmt = _mssql_merge_statement(tbl, key_cols, part[0])
            if trace_sql:
                _write_sql_to_file("mssql_upsert", tbl.name, stmt, engine)
            conn.execute(stmt, part)
        def exec_row(row: Dict[str, Any]) -> None:
            stmt = _mssql_merge_statement(tbl, key_cols, row)
            conn.execute(stmt, [row])
        for part in _chunk_iter(rows, chunk):
            chunk_stats = _execute_with_row_isolation(conn, part, exec_chunk, exec_row, tolerance)
            stats["chunks"].append(chunk_stats)
            stats["success"] += chunk_stats["success"]
            stats["failed"] += chunk_stats["failed"]
    return stats


def mssql_insert(engine: Engine, data: DataLike, table: Union[str, sa.Table], chunk_size: int = 10_000, tolerance: int = 5, trace_sql: bool = False) -> int:
    """MS SQL Server bulk insert with row-level fallback and error attribution."""
    rows = _normalize_data(data)
    if not rows:
        return 0
    total_success = 0
    with _ensure_connection(engine) as conn:
        tbl = _get_table(conn, table)
        _ensure_data_columns_in_table(rows, tbl)
        stmt = tbl.insert()
        if trace_sql:
            _write_sql_to_file("mssql_insert", tbl.name, stmt, engine)
        def exec_chunk(part: List[Dict[str, Any]]) -> None:
            conn.execute(stmt, part)
        def exec_row(row: Dict[str, Any]) -> None:
            conn.execute(stmt, [row])
        for part in _chunk_iter(rows, chunk_size):
            stats = _execute_with_row_isolation(conn, part, exec_chunk, exec_row, tolerance)
            total_success += stats["success"]
    return total_success


if __name__ == "__main__":
    from sqlalchemy import Column, Integer, String, MetaData, Table, create_engine, text

    eng = create_engine("sqlite:///:memory:")
    meta = MetaData()
    users = Table(
        "users",
        meta,
        Column("id", Integer, primary_key=True),
        Column("username", String(50), unique=True),
        Column("city", String(50)),
    )
    meta.create_all(eng)

    initial = [
        {"id": 1, "username": "alice", "city": "Paris"},
        {"id": 2, "username": "bob", "city": "London"},
    ]
    sqlite_insert(eng, initial, users)

    upserts = [
        {"id": 1, "username": "alice", "city": "Lyon"},     # update
        {"id": 3, "username": "charlie", "city": "Berlin"}, # insert
    ]
    sqlite_upsert(eng, upserts, users, constrain=["id"])

    with eng.connect() as c:
        rows = c.execute(text("select id, username, city from users order by id")).fetchall()
        for r in rows:
            print(dict(r._mapping))
