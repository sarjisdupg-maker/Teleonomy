"""
Enhanced WHERE clause generator with column name alignment and validation.

This module addresses the following issues from the original built_where.py:
1. Column name mismatch between WHERE clause and aligned DataFrame
2. Missing validation for WHERE clause columns
3. Proper handling of case-insensitive column matching
4. Type safety for parameter binding

Key improvements:
- Aligns WHERE clause column names to match DataFrame columns
- Validates that WHERE clause columns exist in the data
- Handles case-insensitive matching consistently
- Provides clear error messages for debugging
"""
from __future__ import annotations
import re
from typing import Any, Dict, List, Tuple, Union, Optional
import pandas as pd
from .sanitization import escape_identifier

# Logging Configuration
DEBUG_LOG_ENABLED = True
LOG_FILE = "where_build.log"


def _log(func_name: str, msg: Any):
    """Write debug log entry."""
    if not DEBUG_LOG_ENABLED:
        return
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{func_name}] {msg}\n")
    except:
        pass


# Regex patterns for parsing SQL conditions
_WD = r"[A-Za-z_][\w$]*"
_OP = r"BETWEEN|IN|LIKE|<=|>=|!=|=|>|<"
_rx_plain = re.compile(rf"^(?P<field>{_WD})\s*(?P<operator>{_OP})\s*(?P<values>.+)$", re.I)
_rx_where = re.compile(rf"^(?P<field>{_WD})\s*(?P<operator>{_OP})\s*(?P<val>.+)$", re.I)


def _escape_like(val: str, dialect: str) -> Tuple[str, str]:
    """Escape %, _ and \\ so LIKE never over-matches; Oracle & PG need explicit ESCAPE."""
    esc = val.replace("\\", r"\\").replace("%", r"\%").replace("_", r"\_")
    needs_escape = dialect in ('postgres', 'postgresql', 'oracle', 'mysql', 'mssql')
    return esc, (" ESCAPE '\\'" if needs_escape else '')


def _process_values(op: str, raw: str) -> Union[List[str], str]:
    """Process values based on operator type."""
    op = op.lower()
    if op == 'in':
        import csv
        import io
        if not (raw.startswith('(') and raw.endswith(')')):
            raise ValueError("IN expects parentheses e.g. col IN ('a','b')")
        inner = raw[1:-1].strip()
        if not inner:
            return []
        reader = csv.reader(io.StringIO(inner), quotechar="'", skipinitialspace=True)
        try:
            return next(reader)
        except StopIteration:
            return []
    if op == 'between':
        parts = re.split(r'\band\b', raw, flags=re.I)
        if len(parts) != 2:
            raise ValueError("BETWEEN needs exactly two values")
        return [p.strip().strip("'").strip('"') for p in parts]
    return raw.strip().strip("'").strip('"')


def parse_sql_condition(cond: str) -> Dict[str, Any]:
    """Parse a SQL condition string into components."""
    m = _rx_plain.match(cond.strip())
    if not m:
        raise ValueError(f"Invalid SQL condition format: {cond}")
    field, op, values = m.group('field'), m.group('operator').upper(), m.group('values')
    return {'field': field, 'operator': op, 'value': _process_values(op, values)}


def _param_name(base: str, c_id: int, idx: Optional[int] = None) -> str:
    """Generate parameter name for SQL binding."""
    return f"{base}_{c_id}" if idx is None else f"{base}_{c_id}_{idx}"


def _build_condition(cd: Dict[str, Any], params: Dict[str, Any], dialect: str) -> Tuple[str, Dict[str, Any]]:
    """Build a single SQL condition with parameterized values."""
    op, fld, val, cid = cd['operator'], cd['field'], cd['value'], cd['id']
    p_fld = escape_identifier(fld, dialect)

    if op == 'BETWEEN':
        if not isinstance(val, (list, tuple)) or len(val) != 2:
            val = [val, val]  # Fallback
        p1, p2 = _param_name(fld, cid, 0), _param_name(fld, cid, 1)
        params[p1], params[p2] = val[0], val[1]
        return f"{p_fld} BETWEEN :{p1} AND :{p2}", params
    
    if op == 'IN':
        if not isinstance(val, (list, tuple)):
            val = [val]
        ph = []
        for idx, v in enumerate(val):
            pn = _param_name(fld, cid, idx)
            ph.append(f":{pn}")
            params[pn] = v
        return f"{p_fld} IN ({', '.join(ph)})", params
    
    if op == 'LIKE':
        lit, esc = _escape_like(str(val), dialect)
        pn = _param_name(fld, cid)
        params[pn] = f"%{lit}%"
        return f"{p_fld} LIKE :{pn}{esc}", params
    
    if op in ('=', '!=', '>', '>=', '<', '<='):
        pn = _param_name(fld, cid)
        params[pn] = val
        return f"{p_fld} {op} :{pn}", params
    
    raise ValueError(f"Unsupported operator: {op}")


def _handle_single(condition: Any, idx: int) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """Handle a single condition and assign it an ID."""
    if isinstance(condition, str):
        d = parse_sql_condition(condition)
        d['id'] = idx
        return d
    if isinstance(condition, dict):
        res = dict(condition)
        res['id'] = idx
        return res
    if isinstance(condition, tuple):
        if len(condition) != 3:
            raise ValueError("Tuple condition must be (field, op, value)")
        fld, op, v = condition
        return {'field': fld, 'operator': op, 'value': v, 'id': idx}
    if isinstance(condition, list):
        return [_handle_single(c, idx) for c in condition]
    raise TypeError(f"Unsupported condition type: {type(condition)}")


def _flatten(conds: Any) -> List[Dict[str, Any]]:
    """Flatten nested conditions into a list."""
    if isinstance(conds, (str, dict, tuple)):
        conds = [conds]
    elif not hasattr(conds, '__iter__'):
        conds = [conds]
        
    res = []
    for i, c in enumerate(conds, 1):
        norm = _handle_single(c, i)
        if isinstance(norm, list):
            res.extend(norm)
        else:
            res.append(norm)
    return res


def _single_list_query(conds: list, dialect: str) -> Tuple[Dict[int, str], Dict[str, Any]]:
    """Build SQL conditions from a list of condition dictionaries."""
    parsed = _flatten(conds)
    q_map, params = {}, {}
    for cd in parsed:
        q, params = _build_condition(cd, params, dialect)
        q_map[cd['id']] = q
    return q_map, params


def sql_where(conditions: Any, expression: Optional[str] = None, dialect: str = 'sqlite') -> Tuple[str, Dict[str, Any]]:
    """
    Build WHERE clause SQL and parameters.
    
    Args:
        conditions: List of conditions (tuples, dicts, or strings)
        expression: Optional expression to combine conditions (e.g., "1 AND 2")
        dialect: Database dialect (sqlite, postgres, oracle, mysql, mssql)
    
    Returns:
        Tuple of (SQL string, parameters dict)
    """
    _log("sql_where", f"expr={expression}, dialect={dialect}")
    if dialect not in ('sqlite', 'postgres', 'postgresql', 'oracle', 'mysql', 'mssql'):
        raise ValueError(f"Unsupported dialect: {dialect}")
    
    q_map, params = _single_list_query(conditions, dialect)
    if not expression:
        return " AND ".join(q_map.values()), params

    def _repl(m):
        idx_str = m.group(0)
        idx = int(idx_str)
        if idx not in q_map:
            return idx_str
        return q_map[idx]

    res_sql = re.sub(r"\b\d+\b", _repl, expression)
    _log("sql_where", f"result_sql={res_sql}")
    return res_sql, params


# ═══════════════════════════════════════════════════════════════════════════
# DataFrame-based Query Building with Column Alignment
# ═══════════════════════════════════════════════════════════════════════════

def _ensure_df(data) -> pd.DataFrame:
    """Convert data to DataFrame."""
    if isinstance(data, pd.DataFrame):
        return data.copy()
    if isinstance(data, list):
        return pd.DataFrame(data)
    if isinstance(data, dict):
        return pd.DataFrame([data])
    raise TypeError("data must be DataFrame / list[dict] / dict")


def _align_column_name(field: str, df_cols: set[str]) -> Tuple[str, bool]:
    # Create case-insensitive mapping
    df_cols_map = {c.upper(): c for c in df_cols}
    
    # Check if field exists (case-insensitive)
    if field.upper() in df_cols_map:
        return df_cols_map[field.upper()], True
    
    return field, False


def _parse_where_item(item: Any, df_cols: set[str], allow_missing: bool = False) -> Optional[Dict[str, Any]]:
    if isinstance(item, tuple):
        field, op, raw_val = item
    else:
        m = _rx_where.match(str(item).strip())
        if not m:
            raise ValueError(f"Invalid where clause: {item}")
        field, op, raw_val = m.group('field'), m.group('operator'), m.group('val')

    # Align column name to DataFrame
    aligned_field, found = _align_column_name(field, df_cols)
    
    # Handle placeholder values (?)
    is_placeholder = isinstance(raw_val, str) and raw_val.strip() == '?'
    
    if not found:
        if is_placeholder:
            # Column not in DataFrame and value is placeholder - skip this condition
            _log("_parse_where_item", f"Skipping condition: column '{field}' not in DataFrame")
            return None
        elif not allow_missing:
            # Column not in DataFrame and not a literal value - error
            available = ', '.join(sorted(df_cols))
            raise ValueError(
                f"WHERE clause column '{field}' not found in DataFrame. "
                f"Available columns: {available}"
            )
        # Column not in DataFrame but has literal value - allow it
        _log("_parse_where_item", f"Using literal value for column '{field}' not in DataFrame")
        aligned_field = field
    
    # Process value
    if isinstance(raw_val, str):
        value = '?' if is_placeholder else raw_val.strip().strip("'").strip('"')
    else:
        value = raw_val
    
    return {
        'field': aligned_field,
        'operator': op.upper() if isinstance(op, str) else op,
        'value': value
    }


def _row_conditions(row: pd.Series, where: List[Any], allow_missing: bool = False) -> List[Dict[str, Any]]:
    """
    Build conditions for a row, replacing placeholders with actual values.
    
    Args:
        row: DataFrame row (Series)
        where: List of WHERE clause items
        allow_missing: If True, allow columns not in DataFrame
    
    Returns:
        List of condition dictionaries
    """
    conds = []
    df_cols = set(row.index)
    
    for w in where:
        d = _parse_where_item(w, df_cols, allow_missing=allow_missing)
        if not d:
            continue
        
        # Replace placeholder with actual value from row
        if d['value'] == '?':
            if d['field'] not in row.index:
                raise ValueError(f"Placeholder column '{d['field']}' not found in row")
            d['value'] = row[d['field']]
        
        conds.append(d)
    
    return conds


# ═══════════════════════════════════════════════════════════════════════════
# Statement Builders
# ═══════════════════════════════════════════════════════════════════════════

def _select_stmt(
    row: pd.Series,
    table: str,
    where: List[Any],
    expr: str,
    dialect: str,
    columns: Optional[List[str]] = None,
    allow_missing_where_cols: bool = False
) -> Tuple[str, Dict[str, Any]]:
    """Build SELECT statement with WHERE clause."""
    sql_w, param_w = sql_where(
        _row_conditions(row, where, allow_missing=allow_missing_where_cols),
        expr,
        dialect
    )
    tbl_esc = ".".join(escape_identifier(p, dialect) for p in table.split('.'))
    cols_sql = '*'
    if columns:
        cols_sql = ", ".join(escape_identifier(c, dialect) for c in columns)
    return f"SELECT {cols_sql} FROM {tbl_esc} WHERE {sql_w}", param_w


def _update_stmt(
    row: pd.Series,
    table: str,
    where: List[Any],
    expr: str,
    dialect: str,
    update_cols: Optional[List[str]] = None,
    allow_missing_where_cols: bool = True
) -> Tuple[str, Dict[str, Any]]:
    """
    Build UPDATE statement with WHERE clause.
    
    Args:
        row: DataFrame row with values to update
        table: Table name
        where: WHERE clause conditions
        expr: Expression to combine WHERE conditions
        dialect: Database dialect
        update_cols: Columns to update (if None, use all row columns)
        allow_missing_where_cols: If True, allow WHERE columns not in row (for PKs)
    
    Returns:
        Tuple of (SQL string, parameters dict)
    """
    # Build WHERE clause (allow missing columns for PK-based updates)
    sql_w, param_w = sql_where(
        _row_conditions(row, where, allow_missing=allow_missing_where_cols),
        expr,
        dialect
    )
    
    tbl_esc = ".".join(escape_identifier(p, dialect) for p in table.split('.'))
    
    # Build SET clause
    cols_to_use = update_cols if update_cols else list(row.index)
    set_parts = []
    param_u = {}
    
    for c in cols_to_use:
        if c not in row.index:
            raise ValueError(f"Update column '{c}' not found in row. Available: {list(row.index)}")
        c_esc = escape_identifier(str(c), dialect)
        set_parts.append(f"{c_esc} = :u_{c}")
        param_u[f"u_{c}"] = row[c]
    
    # Combine parameters
    param_u.update(param_w)
    
    _log("_update_stmt", f"SET columns: {cols_to_use}, WHERE params: {list(param_w.keys())}")
    
    return f"UPDATE {tbl_esc} SET {', '.join(set_parts)} WHERE {sql_w}", param_u


def _insert_stmt(row: pd.Series, table: str, dialect: str = 'sqlite') -> Tuple[str, Dict[str, Any]]:
    """Build INSERT statement."""
    tbl_esc = ".".join(escape_identifier(p, dialect) for p in table.split('.'))
    cols = list(row.index)
    cols_esc = [escape_identifier(str(c), dialect) for c in cols]
    ph = [f":i_{c}" for c in cols]
    return f"INSERT INTO {tbl_esc} ({', '.join(cols_esc)}) VALUES ({', '.join(ph)})", {f"i_{c}": row[c] for c in cols}


# ═══════════════════════════════════════════════════════════════════════════
# High-Level API
# ═══════════════════════════════════════════════════════════════════════════

def build_update(
    data: Any,
    table: str,
    where: List[Any],
    dialect: str = 'sqlite',
    expression: Optional[str] = None,
    update_cols: Optional[List[str]] = None,
    allow_missing_where_cols: bool = True
) -> Tuple[str, Dict[str, Any]]:
    """
    Build UPDATE statement with aligned column names.
    
    Args:
        data: DataFrame, dict, or list with update values
        table: Table name
        where: WHERE clause conditions (list of tuples/strings/dicts)
        dialect: Database dialect
        expression: Optional expression to combine WHERE conditions
        update_cols: Columns to update (if None, use all data columns)
        allow_missing_where_cols: If True, allow WHERE columns not in data
    
    Returns:
        Tuple of (SQL string, parameters dict)
    
    Example:
        >>> df = pd.DataFrame([{'cost_code': 'NEW_VALUE'}])
        >>> sql, params = build_update(
        ...     df, 'test_quota', [('COST_ID', '=', 1001)], 'sqlite'
        ... )
    """
    df = _ensure_df(data)
    if len(df) != 1:
        raise ValueError(f"Expected 1 row, got {len(df)}")
    
    row = df.iloc[0]
    expr = expression or ' AND '.join(str(i) for i in range(1, len(where) + 1))
    
    _log("build_update", f"table={table}, row_cols={list(row.index)}, where={where}")
    
    return _update_stmt(row, table, where, expr, dialect, update_cols, allow_missing_where_cols)


def build_select(
    data: Any,
    table: str,
    where: List[Any],
    dialect: str = 'sqlite',
    expression: Optional[str] = None,
    columns: Optional[List[str]] = None,
    allow_missing_where_cols: bool = False
) -> Tuple[str, Dict[str, Any]]:
    """Build SELECT statement with aligned column names."""
    df = _ensure_df(data)
    if len(df) != 1:
        raise ValueError(f"Expected 1 row, got {len(df)}")
    
    row = df.iloc[0]
    expr = expression or ' AND '.join(str(i) for i in range(1, len(where) + 1))
    
    return _select_stmt(row, table, where, expr, dialect, columns, allow_missing_where_cols)


def build_insert(data: Any, table: str, dialect: str = 'sqlite') -> Tuple[str, Dict[str, Any]]:
    """Build INSERT statement."""
    df = _ensure_df(data)
    if len(df) != 1:
        raise ValueError(f"Expected 1 row, got {len(df)}")
    
    return _insert_stmt(df.iloc[0], table, dialect)


# ═══════════════════════════════════════════════════════════════════════════
# Testing and Validation
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Test 1: Basic WHERE clause
    print("Test 1: Basic WHERE clause")
    conds = [
        "cell_id = '00123'",
        ("vendor", "IN", ["GP", "BL"]),
        {"field": "tech", "operator": "LIKE", "value": "2G"},
        "dt BETWEEN '2023-01-01' and '2023-01-31'"
    ]
    sql, binds = sql_where(conds, "1 AND (2 OR 3) AND 4", dialect="oracle")
    print(f"SQL: {sql}")
    print(f"Params: {binds}\n")
    
    # Test 2: Column name alignment
    print("Test 2: Column name alignment (case mismatch)")
    df = pd.DataFrame([{"cost_code": "NEW_VALUE"}])  # lowercase
    sql, params = build_update(
        df,
        "test_quota",
        [("COST_ID", "=", 1001)],  # uppercase WHERE column
        dialect="sqlite",
        allow_missing_where_cols=True
    )
    print(f"SQL: {sql}")
    print(f"Params: {params}\n")
    
    # Test 3: Error handling - missing column
    print("Test 3: Error handling - missing column without allow_missing")
    try:
        df = pd.DataFrame([{"cost_code": "NEW_VALUE"}])
        sql, params = build_update(
            df,
            "test_quota",
            [("COST_ID", "=", 1001)],
            dialect="sqlite",
            allow_missing_where_cols=False  # Should raise error
        )
    except ValueError as e:
        print(f"Expected error: {e}\n")
