import re
import json
import os
from keyword import iskeyword
from typing import Optional, Union, List, Tuple, Dict, Sequence, Any, Mapping, cast

import pandas as pd
from logger import log_call, log_json, log_string

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import SQLAlchemyError
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False

from casting import cast_df
from data_profiler import get_pk, profile_dataframe

# --------------------------------------------------------------------
# Constants / configuration
# --------------------------------------------------------------------

RESERVED_WORDS = {
    'oracle': {
        'SELECT', 'INSERT', 'DELETE', 'UPDATE', 'WHERE', 'FROM', 'GROUP', 'ORDER',
        'BY', 'CREATE', 'TABLE', 'INDEX', 'VIEW', 'PRIMARY', 'KEY', 'FOREIGN',
        'CONSTRAINT', 'NUMBER', 'DATE', 'UNION', 'ALL', 'DISTINCT', 'JOIN',
        'INNER', 'OUTER', 'LEFT', 'RIGHT', 'ON', 'USING', 'HAVING', 'LIMIT',
        'OFFSET', 'ALTER', 'DROP', 'TRUNCATE', 'USER', 'LEVEL', 'ACCESS', 'MODE'
    },
    'postgresql': {
        'SELECT', 'INSERT', 'DELETE', 'UPDATE', 'WHERE', 'FROM', 'GROUP', 'ORDER',
        'BY', 'CREATE', 'TABLE', 'PRIMARY', 'KEY', 'FOREIGN', 'CONSTRAINT',
        'REFERENCES', 'BIGINT', 'INTEGER', 'SMALLINT', 'VARCHAR', 'TIMESTAMP',
        'BOOLEAN', 'NUMERIC', 'DECIMAL', 'REAL'
    },
    'mysql': {
        'SELECT', 'INSERT', 'DELETE', 'UPDATE', 'WHERE', 'FROM', 'TABLE',
        'CREATE', 'DROP', 'ALTER', 'PRIMARY', 'KEY', 'FOREIGN', 'CONSTRAINT',
        'INT', 'VARCHAR', 'DATETIME', 'BIGINT', 'SMALLINT', 'AUTO_INCREMENT',
        'UNIQUE', 'INDEX'
    },
    'mssql': {
        'SELECT', 'INSERT', 'DELETE', 'UPDATE', 'WHERE', 'FROM', 'TABLE',
        'CREATE', 'DROP', 'ALTER', 'PRIMARY', 'KEY', 'FOREIGN', 'CONSTRAINT',
        'IDENTITY', 'INT', 'VARCHAR', 'DATETIME', 'BIT', 'FLOAT', 'NUMERIC',
        'DECIMAL', 'BIGINT', 'SMALLINT'
    },
    'sqlite': {
        'SELECT', 'INSERT', 'DELETE', 'UPDATE', 'WHERE', 'FROM', 'TABLE',
        'CREATE', 'DROP', 'ALTER', 'CONSTRAINT', 'PRIMARY', 'KEY', 'FOREIGN',
        'UNIQUE', 'CHECK', 'DEFAULT'
    }
}

ORACLE_MAX_IDENTIFIER = 30
DEFAULT_VARCHAR_SIZE = 255

DTYPE_MAP = {
    'oracle': {
        'object': 'VARCHAR2(255)', 'string': 'VARCHAR2(255)', 'category': 'VARCHAR2(255)',
        'Int64': 'NUMBER', 'int64': 'NUMBER',
        'Float64': 'BINARY_DOUBLE', 'float64': 'BINARY_DOUBLE', 'float32': 'BINARY_FLOAT',
        'boolean': 'NUMBER(1,0)', 'bool': 'NUMBER(1,0)',
        'datetime64[ns]': 'TIMESTAMP', 'datetime64[us]': 'TIMESTAMP',
        'timedelta64[ns]': 'INTERVAL DAY TO SECOND', 'timedelta64[us]': 'INTERVAL DAY TO SECOND',
        'date': 'DATE', 'time': 'DATE', 'datetime': 'TIMESTAMP', 'timedelta': 'INTERVAL DAY TO SECOND'
    },
    'sqlite': {
        'object': 'TEXT', 'string': 'TEXT', 'category': 'TEXT',
        'Int64': 'INTEGER', 'int64': 'INTEGER',
        'Float64': 'REAL', 'float64': 'REAL', 'float32': 'REAL',
        'boolean': 'INTEGER', 'bool': 'INTEGER',
        'datetime64[ns]': 'DATETIME', 'datetime64[us]': 'DATETIME',
        'timedelta64[ns]': 'TEXT', 'timedelta64[us]': 'TEXT',
        'date': 'DATE', 'time': 'TIME', 'datetime': 'DATETIME', 'timedelta': 'TEXT'
    },
    'mssql': {
        'object': 'VARCHAR(255)', 'string': 'VARCHAR(255)', 'category': 'VARCHAR(255)',
        'Int64': 'BIGINT', 'int64': 'BIGINT',
        'Float64': 'FLOAT', 'float64': 'FLOAT', 'float32': 'REAL',
        'boolean': 'BIT', 'bool': 'BIT',
        'datetime64[ns]': 'DATETIME2', 'datetime64[us]': 'DATETIME2',
        'timedelta64[ns]': 'VARCHAR(255)', 'timedelta64[us]': 'VARCHAR(255)',
        'date': 'DATE', 'time': 'TIME', 'datetime': 'DATETIME2', 'timedelta': 'VARCHAR(255)'
    },
    'mysql': {
        'object': 'VARCHAR(255)', 'string': 'VARCHAR(255)', 'category': 'VARCHAR(255)',
        'Int64': 'BIGINT', 'int64': 'BIGINT',
        'Float64': 'DOUBLE', 'float64': 'DOUBLE', 'float32': 'FLOAT',
        'boolean': 'TINYINT', 'bool': 'TINYINT',
        'datetime64[ns]': 'DATETIME', 'datetime64[us]': 'DATETIME',
        'timedelta64[ns]': 'VARCHAR(255)', 'timedelta64[us]': 'VARCHAR(255)',
        'date': 'DATE', 'time': 'TIME', 'datetime': 'DATETIME', 'timedelta': 'VARCHAR(255)'
    },
    'postgresql': {
        'object': 'TEXT', 'string': 'TEXT', 'category': 'TEXT',
        'Int64': 'BIGINT', 'int64': 'BIGINT',
        'Float64': 'DOUBLE PRECISION', 'float64': 'DOUBLE PRECISION', 'float32': 'REAL',
        'boolean': 'BOOLEAN', 'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP', 'datetime64[us]': 'TIMESTAMP',
        'timedelta64[ns]': 'INTERVAL', 'timedelta64[us]': 'INTERVAL',
        'date': 'DATE', 'time': 'TIME', 'datetime': 'TIMESTAMP', 'timedelta': 'INTERVAL'
    }
}


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------

def sanitize_cols(
    obj,
    allow_space: bool = False,
    to_lower: bool = True,
    fallback: str = 'col_',
    dialect: str = 'postgresql'
):
    d_key = dialect.lower()
    if d_key == 'postgres':
        d_key = 'postgresql'

    sql_kw = {kw.upper() for kw in RESERVED_WORDS.get(d_key, set())}
    sql_kw.update({
        "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "LIMIT",
        "JOIN", "TABLE", "COLUMN", "INSERT", "UPDATE", "DELETE",
        "CREATE", "DROP", "ALTER", "SCHEMA", "INDEX", "VIEW",
        "TRIGGER", "PROCEDURE", "FUNCTION", "DATABASE", "USER",
        "ROLE", "GRANT", "REVOKE"
    })
    blank_count = iter(range(1, 1_000_000))

    def _c(s: Any):
        if not isinstance(s, str):
            s = str(s)
        s = s.strip()
        s = re.sub(r'[^\w\s]' if allow_space else r'[^\w]', '_', s)
        if allow_space:
            s = re.sub(r'\s+', ' ', s)
        if to_lower:
            s = s.lower()
        s = re.sub(r'__+', '_', s).strip('_')
        if re.match(r'^\d', s):
            s = '_' + s
        if not s:
            s = f'{fallback}{next(blank_count)}'
        if s.upper() in sql_kw or iskeyword(s):
            s += '_'
        return s

    if isinstance(obj, (list, tuple, pd.Index, Sequence)) and not isinstance(obj, str):
        seen: Dict[str, int] = {}
        clean = []
        for n in obj:
            c = _c(n)
            i = seen.get(c, 0)
            seen[c] = i + 1
            clean.append(c if i == 0 else f'{c}_{i}')
        return clean

    if isinstance(obj, dict):
        k = sanitize_cols(list(obj.keys()), allow_space, to_lower, fallback, d_key)
        return dict(zip(k, obj.values()))

    if isinstance(obj, pd.DataFrame):
        df = obj.copy()
        df.columns = sanitize_cols(df.columns.tolist(), allow_space, to_lower, fallback, d_key)
        return df

    if isinstance(obj, str):
        return _c(obj)
    # If we reach here, it's an unsupported type for column/dict/dataframe processing
    return obj


def _truncate_identifier(name: str, max_len: int = ORACLE_MAX_IDENTIFIER) -> str:
    return name[:max_len] if len(name) > max_len else name


def escape_identifier(name: str, server: str = 'oracle') -> str:
    d_key = server.lower()
    if d_key == 'postgres':
        d_key = 'postgresql'
    reserved = RESERVED_WORDS.get(d_key, RESERVED_WORDS['oracle'])
    name_upper = name.upper()

    if d_key in ('oracle', 'postgresql', 'sqlite'):
        if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_$#]*', name) and name_upper not in reserved:
            return name.upper() if d_key == 'oracle' else name
        return '"{}"'.format(name.replace('"', '""'))

    if d_key == 'mysql':
        if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_$]*', name) and name_upper not in reserved:
            return name
        return '`{}`'.format(name.replace('`', '``'))

    if d_key == 'mssql':
        return f'[{name}]'

    return name


def _normalize_dtype(dtype) -> str:
    dtype = str(dtype).lower()
    if dtype in ('int64', 'int32', 'int16', 'int8'):
        return 'Int64'
    if dtype in ('float64', 'float32'):
        return 'Float64'
    if dtype == 'bool':
        return 'boolean'
    return dtype


def _object_sql_type(
    server: str,
    semantic_type: Optional[str],
    varchar_sizes: Optional[Dict[str, int]],
    col_name: Optional[str]
) -> Optional[str]:
    d_key = server.lower()
    if d_key == 'postgres':
        d_key = 'postgresql'
    size = DEFAULT_VARCHAR_SIZE
    if varchar_sizes and col_name is not None:
        size = varchar_sizes.get(col_name, DEFAULT_VARCHAR_SIZE)
    if semantic_type == "STRING_OBJECT":
        if d_key == 'oracle':
            return f"VARCHAR2({size})"
        if d_key == 'sqlite':
            return "TEXT"
        if d_key == 'mssql':
            return f"VARCHAR({size})"
        if d_key == 'mysql':
            return f"VARCHAR({size})"
        if d_key == 'postgresql':
            return "TEXT"
    if semantic_type in ("STRUCTURED_OBJECT", "TRUE_OBJECT"):
        if d_key == 'oracle':
            return "BLOB"
        if d_key == 'sqlite':
            return "BLOB"
        if d_key == 'mssql':
            return "VARBINARY(MAX)"
        if d_key == 'mysql':
            return "JSON"
        if d_key == 'postgresql':
            return "JSONB"
    return None


def get_sql_type(
    col_dtype,
    server: str,
    varchar_sizes: Optional[Dict[str, int]] = None,
    col_name: Optional[str] = None,
    semantic_type: Optional[str] = None
) -> str:
    """
    Maps pandas dtype to SQL type for a given server.
    Respects per-column varchar_sizes for generic string/unknown types.
    """
    d_key = server.lower()
    if d_key == 'postgres':
        d_key = 'postgresql'

    dtype_map = DTYPE_MAP.get(d_key, DTYPE_MAP['oracle'])
    norm_dtype = _normalize_dtype(col_dtype)
    if norm_dtype == 'object' and semantic_type:
        obj_type = _object_sql_type(d_key, semantic_type, varchar_sizes, col_name)
        if obj_type:
            try:
                log_json(f"type_mapping_{col_name}", {"pandas_dtype": str(col_dtype), "sql_type": obj_type, "server": d_key, "semantic_type": semantic_type})
            except Exception:
                pass
            return obj_type
    sql_type = dtype_map.get(norm_dtype)

    if sql_type is None:
        cd = str(col_dtype).lower()
        if any(x in cd for x in ('datetime', 'date', 'time')):
            sql_type = dtype_map.get('datetime', 'VARCHAR(255)')
        elif 'timedelta' in cd:
            sql_type = dtype_map.get('timedelta', 'VARCHAR(255)')
        else:
            size = DEFAULT_VARCHAR_SIZE
            if varchar_sizes and col_name is not None:
                size = varchar_sizes.get(col_name, DEFAULT_VARCHAR_SIZE)
            sql_type = 'TEXT' if d_key in ('postgresql', 'sqlite') else f'VARCHAR({size})'
    try:
        log_json(f"type_mapping_{col_name}", {"pandas_dtype": str(col_dtype), "sql_type": sql_type, "server": d_key, "semantic_type": semantic_type})
    except Exception:
        pass
    return sql_type


def normalize_cols(cols: Optional[Union[str, Sequence[str]]]) -> List[str]:
    return [cols] if isinstance(cols, str) else (list(cols) if cols else [])


def build_pk_constraint(table: str, pk, server: str) -> str:
    pk_list = ", ".join(escape_identifier(x, server) for x in normalize_cols(pk))
    if server.lower() == 'sqlite':
        return f"PRIMARY KEY ({pk_list})"
    name = escape_identifier(_truncate_identifier(f"{table}_PK"), server)
    return f"CONSTRAINT {name} PRIMARY KEY ({pk_list})"


def build_fk_constraint(
    table: str,
    col: str,
    ref_tab: str,
    ref_col: str,
    idx: int,
    server: str
) -> str:
    col_esc = escape_identifier(col, server)
    ref_tab_esc = escape_identifier(ref_tab, server)
    ref_col_esc = escape_identifier(ref_col, server)
    if server.lower() == 'sqlite':
        return f"FOREIGN KEY ({col_esc}) REFERENCES {ref_tab_esc}({ref_col_esc})"
    name = escape_identifier(_truncate_identifier(f"{table}_fk{idx}"), server)
    return f"CONSTRAINT {name} FOREIGN KEY ({col_esc}) REFERENCES {ref_tab_esc}({ref_col_esc})"


def build_unique_constraint(table: str, cols, idx: int, server: str) -> str:
    col_list = ", ".join(escape_identifier(x, server) for x in normalize_cols(cols))
    if server.lower() == 'sqlite':
        return f"UNIQUE ({col_list})"
    name = escape_identifier(_truncate_identifier(f"{table}_uq{idx}"), server)
    return f"CONSTRAINT {name} UNIQUE ({col_list})"


def _build_autoincrement_clause(col: str, server: str, initial_value: int) -> str:
    d_key = server.lower()
    if d_key == 'postgres':
        d_key = 'postgresql'
    col_esc = escape_identifier(col, d_key)
    if d_key == 'sqlite':
        return f"{col_esc} INTEGER PRIMARY KEY AUTOINCREMENT"
    if d_key == 'mysql':
        return f"{col_esc} BIGINT AUTO_INCREMENT"
    if d_key == 'mssql':
        return f"{col_esc} BIGINT IDENTITY({initial_value},1)"
    if d_key == 'postgresql':
        return f"{col_esc} SERIAL"
    if d_key == 'oracle':
        return (f"{col_esc} NUMBER GENERATED ALWAYS AS IDENTITY "
                f"(START WITH {initial_value} INCREMENT BY 1)")
    return ""


def get_table_name(table: str, schema_name: Optional[str], server: str) -> str:
    tbl = escape_identifier(table, server)
    if not schema_name:
        return tbl
    sch = escape_identifier(schema_name, server)
    return f"{sch}.{tbl}"


# --------------------------------------------------------------------
# Core DDL building
# --------------------------------------------------------------------

def build_create_table_statement(
    df: pd.DataFrame,
    table: str,
    schema_name: Optional[str],
    pk,
    fk,
    unique,
    autoincrement,
    server: str,
    varchar_sizes: Optional[Dict[str, int]] = None,
    dtype_semantics: Optional[Dict[str, str]] = None
) -> str:
    cols: List[str] = []
    auto_col = autoincrement[0] if autoincrement else None
    pk_cols = normalize_cols(pk)
    escaped = {c: escape_identifier(c, server) for c in df.columns}
    d_key = server.lower()
    if d_key == 'postgres':
        d_key = 'postgresql'

    # 1. Column definitions
    for col in df.columns:
        col_dtype = str(df[col].dtype)
        semantic_type = (dtype_semantics or {}).get(col)
        sql_type = get_sql_type(col_dtype, d_key, varchar_sizes, col_name=col, semantic_type=semantic_type)
        col_esc = escaped[col]
        is_auto = (col == auto_col and autoincrement is not None)

        if is_auto:
            inline = _build_autoincrement_clause(col, d_key, autoincrement[1])
            if inline:
                cols.append(inline + " NOT NULL")
                continue

        nullable = 'NOT NULL' if not df[col].isna().any() else 'NULL'
        cols.append(f"{col_esc} {sql_type} {nullable}")

    # 2. Constraints
    add_pk = pk is not None
    if add_pk:
        is_single_auto_pk = (len(pk_cols) == 1 and pk_cols[0] == auto_col)
        if is_single_auto_pk and d_key in ('sqlite', 'mysql', 'mssql', 'postgresql', 'oracle'):
            add_pk = False
    if add_pk:
        cols.append(build_pk_constraint(table, pk, d_key))

    if fk:
        for i, (c, rt, rc) in enumerate(fk, 1):
            cols.append(build_fk_constraint(table, c, rt, rc, i, d_key))

    if unique:
        for i, ug in enumerate(unique, 1):
            cols.append(build_unique_constraint(table, ug, i, d_key))

    tbl_name = get_table_name(table, schema_name, d_key)
    ddl = f"CREATE TABLE {tbl_name} (\n    " + ",\n    ".join(cols) + "\n)"
    return ddl


def build_schema_json(
    df: pd.DataFrame,
    table: str,
    schema_name: Optional[str],
    pk,
    fk,
    unique,
    autoincrement,
    server: str,
    varchar_sizes: Optional[Dict[str, int]] = None,
    dtype_semantics: Optional[Dict[str, str]] = None,
    column_mapping: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    d_key = server.lower()
    if d_key == 'postgres':
        d_key = 'postgresql'

    columns = []
    for col in df.columns:
        col_dtype = str(df[col].dtype)
        semantic_type = (dtype_semantics or {}).get(col)
        sql_type = get_sql_type(col_dtype, d_key, varchar_sizes, col_name=col, semantic_type=semantic_type)
        nullable = df[col].isna().any()
        non_null = df[col].dropna()
        sample = str(non_null.iloc[0]) if not non_null.empty else None

        col_entry = {
            'name': col,
            'pandas_dtype': col_dtype,
            'sql_dtype': sql_type,
            'nullable': nullable,
            'sample_value': sample
        }
        if semantic_type:
            col_entry['semantic_type'] = semantic_type
        columns.append(col_entry)

    pk_cols = normalize_cols(pk)
    fk_list = [{'column': c, 'references_table': rt, 'references_column': rc}
               for c, rt, rc in (fk or [])]
    unique_list = [normalize_cols(u) for u in (unique or [])]

    return {
        'server': d_key,
        'schema': schema_name,
        'table': table,
        'columns': columns,
        'primary_key': pk_cols,
        'foreign_keys': fk_list,
        'unique_constraints': unique_list,
        'autoincrement': (
            {'column': autoincrement[0], 'initial_value': autoincrement[1]}
            if autoincrement else None
        ),
        'column_mapping': column_mapping,
        'row_count': len(df),
        'column_count': len(df.columns)
    }


# --------------------------------------------------------------------
# Public API: df_ddl
# --------------------------------------------------------------------

@log_call
def df_ddl(
    input_df_or_csv: Union[str, pd.DataFrame],
    table: str,
    server: str = 'oracle',
    schema_name: Optional[str] = None,
    pk: Optional[Union[str, List[str]]] = None,
    fk: Optional[List[Tuple[str, str, str]]] = None,
    unique: Optional[List[Union[str, List[str]]]] = None,
    autoincrement: Optional[Tuple[str, int]] = None,
    _default_varchar: int = DEFAULT_VARCHAR_SIZE,   # kept for backward compat, not used directly
    varchar_sizes: Optional[Dict[str, int]] = None,
    cast: bool = True,
    cast_kwargs: Optional[Dict[str, Any]] = None,
    sanitize: bool = False,
    rename_column: bool = False,
    return_dtype_meta: bool = False,
    **kwargs
) -> Union[Tuple[pd.DataFrame, str, Dict[str, Any]], Tuple[pd.DataFrame, str, Dict[str, Any], Dict[str, Any]]]:
    """
    Generate CREATE TABLE DDL and schema metadata from a DataFrame or CSV.
    """
    server = server.lower()
    if server == 'postgres':
        server = 'postgresql'

    if schema_name is None and "schema" in kwargs:
        schema_name = kwargs.pop("schema")

    if server not in DTYPE_MAP:
        raise ValueError(f"Unsupported server: {server}")

    if isinstance(input_df_or_csv, str):
        df = pd.read_csv(input_df_or_csv)
    elif isinstance(input_df_or_csv, pd.DataFrame):
        df = input_df_or_csv
    else:
        raise TypeError("First argument must be a pandas DataFrame or CSV file path")

    if df.empty:
        raise ValueError("DataFrame is empty")
    if not table or not isinstance(table, str):
        raise ValueError("Table name must be non-empty string")

    # 1. Validation and Sanitization
    reserved = RESERVED_WORDS.get(server, RESERVED_WORDS['oracle'])
    problem_cols = []
    
    for col in df.columns:
        col_name = str(col)
        # Check for reserved words or invalid characters (anything non-alphanumeric/underscore or leading digit)
        is_reserved = col_name.upper() in reserved
        is_invalid_chars = not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', col_name)
        
        if is_reserved or is_invalid_chars:
            problem_cols.append(col_name)

    if problem_cols and not (sanitize or rename_column):
        bold_msg = f"\033[1mColumn names {problem_cols} contain reserved words or invalid characters! Please remove or rename them manually, or set rename_column=True for auto-renaming.\033[0m"
        print(bold_msg)
        raise ValueError(bold_msg)

    mapping = None
    if sanitize or rename_column:
        orig_cols = df.columns.tolist()
        df = sanitize_cols(df, dialect=server)
        new_cols = df.columns.tolist()
        mapping = dict(zip(orig_cols, new_cols))

        if pk:
            if isinstance(pk, str):
                pk = mapping.get(pk, pk)
            else:
                pk = [mapping.get(c, c) for c in pk]
        if fk:
            fk = [(mapping.get(c, c), rt, rc) for c, rt, rc in fk]
        if unique:
            new_unique = []
            for u in unique:
                if isinstance(u, str):
                    new_unique.append(mapping.get(u, u))
                else:
                    new_unique.append([mapping.get(c, c) for c in u])
            unique = new_unique
        if autoincrement:
            col, val = autoincrement
            autoincrement = (mapping.get(col, col), val)

    cast_meta = None
    dtype_semantics: Dict[str, str] = {}

    # 2. Type casting
    if cast:
        cast_kwargs = cast_kwargs or {}
        cast_kwargs = {**cast_kwargs, "return_dtype_meta": True}
        df, cast_meta = cast_df(df, **cast_kwargs)
        if cast_meta:
            for col, meta in cast_meta.items():
                semantic = meta.get("object_semantic_type")
                if semantic:
                    dtype_semantics[col] = semantic

    dtype_meta = {}
    for col in df.columns:
        col_dtype = df[col].dtype
        semantic_type = dtype_semantics.get(col)
        output_type = get_sql_type(col_dtype, server, varchar_sizes, col_name=col, semantic_type=semantic_type)
        dtype_meta[col] = {"input_dtype": str(col_dtype), "output_dtype": output_type}
        if semantic_type:
            dtype_meta[col]["semantic_type"] = semantic_type

    # 3. Validation
    if autoincrement:
        col, val = autoincrement
        if col not in df.columns:
            raise ValueError(f"Autoincrement column '{col}' not in DataFrame")
        if not pd.api.types.is_integer_dtype(df[col].dtype):
            raise ValueError("Autoincrement column must be integer type")
        if not isinstance(val, int) or val < 1:
            raise ValueError("Initial value must be positive integer")

    if pk:
        pk_cols = normalize_cols(pk)
        for c in pk_cols:
            if c not in df.columns:
                raise ValueError(f"Primary key references non-existent column '{c}'")
        if len(set(pk_cols)) != len(pk_cols):
            raise ValueError(f"Duplicate column in primary key: {pk_cols}")

    if fk:
        for i, (c, rt, rc) in enumerate(fk, 1):
            if c not in df.columns:
                raise ValueError(f"Foreign key {i} references non-existent column '{c}'")

    if unique:
        for i, u in enumerate(unique, 1):
            cols = normalize_cols(u)
            for c in cols:
                if c not in df.columns:
                    raise ValueError(f"Unique constraint {i} references non-existent column '{c}'")
            if len(set(cols)) != len(cols):
                raise ValueError(f"Duplicate column in unique constraint {i}: {cols}")

    # 4. Generate DDL and metadata
    ddl = build_create_table_statement(
        df, table, schema_name, pk, fk, unique, autoincrement, server, varchar_sizes, dtype_semantics
    )
    meta = build_schema_json(
        df, table, schema_name, pk, fk, unique, autoincrement, server, varchar_sizes, dtype_semantics, column_mapping=mapping
    )
    try:
        ddl_log = ddl if len(ddl) < 5000 else ddl[:5000] + "\n... (truncated)"
        log_string(f"ddl_{server}_{table}", ddl_log)
        log_json(f"schema_meta_{server}_{table}", {"table": table, "server": server, "columns": len(meta.get('columns', [])), "pk": meta.get('primary_key')})
    except Exception:
        pass
    if return_dtype_meta:
        return df, ddl, meta, dtype_meta
    return df, ddl, meta


# --------------------------------------------------------------------
# SQLAlchemy Declarative Model Generator
# --------------------------------------------------------------------

def generate_sqlalchemy_model(
    df: pd.DataFrame,
    table_name: str,
    class_name: Optional[str] = None,
    sanitize: bool = True,
    cast: bool = True,
    pk: Optional[Union[str, List[str]]] = None,
    autoincrement: Optional[str] = None,
    cast_kwargs: Optional[Dict[str, Any]] = None
) -> str:
    if df.empty:
        raise ValueError("Input DataFrame is empty.")

    work_df = df.copy()

    if cast:
        cast_kwargs = cast_kwargs or {}
        work_df = cast_df(work_df, **cast_kwargs)

    if sanitize:
        orig_cols = work_df.columns.tolist()
        work_df = sanitize_cols(work_df)
        new_cols = work_df.columns.tolist()
        mapping = dict(zip(orig_cols, new_cols))
        if pk:
            if isinstance(pk, str):
                pk = mapping.get(pk, pk)
            else:
                pk = [mapping.get(c, c) for c in pk]
        if autoincrement:
            autoincrement = mapping.get(autoincrement, autoincrement)

    if not class_name:
        class_name = "".join(x.title() for x in table_name.split('_'))

    pk_cols = [pk] if isinstance(pk, str) else (pk or [])

    lines = [
        "from sqlalchemy import Column, BigInteger, Float, Boolean, String, DateTime, Date, Time, Interval",
        "from sqlalchemy.orm import declarative_base",
        "",
        "Base = declarative_base()",
        "",
        f"class {class_name}(Base):",
        f"    __tablename__ = '{table_name}'",
        ""
    ]

    for col in work_df.columns:
        dtype = str(work_df[col].dtype).lower()
        if 'int' in dtype:
            sa_type = "BigInteger"
        elif 'float' in dtype or 'double' in dtype:
            sa_type = "Float"
        elif 'bool' in dtype:
            sa_type = "Boolean"
        elif 'datetime' in dtype:
            sa_type = "DateTime"
        elif dtype == 'date':
            sa_type = "Date"
        elif dtype == 'time':
            sa_type = "Time"
        elif 'timedelta' in dtype or 'interval' in dtype:
            sa_type = "Interval"
        else:
            sa_type = "String(255)"

        attrs = []
        if col in pk_cols:
            attrs.append("primary_key=True")
        if col == autoincrement:
            attrs.append("autoincrement=True")
        if not work_df[col].isna().any():
            attrs.append("nullable=False")

        attr_str = ", ".join(attrs)
        if attr_str:
            lines.append(f"    {col} = Column({sa_type}, {attr_str})")
        else:
            lines.append(f"    {col} = Column({sa_type})")

    return "\n".join(lines)


# --------------------------------------------------------------------
# Create table + load data
# --------------------------------------------------------------------

@log_call
def df_ddl_create(
    conn_dict: Mapping[str, str],
    df: pd.DataFrame,
    table: str,
    schema_name: Optional[str] = None,
    pk: Optional[Union[str, List[str]]] = None,
    fk: Optional[List[Tuple[str, str, str]]] = None,
    unique: Optional[List[Union[str, List[str]]]] = None,
    autoincrement: Optional[Tuple[str, int]] = None,
    varchar_sizes: Optional[Dict[str, int]] = None,
    cast: bool = True,
    cast_kwargs: Optional[Dict[str, Any]] = None,
    sanitize: bool = False
) -> Dict[str, Any]:
    if not HAS_SQLALCHEMY:
        raise ImportError("SQLAlchemy is not installed")

    if df.empty:
        raise ValueError("DataFrame is empty")
    if not table or not isinstance(table, str):
        raise ValueError("Table name must be non-empty string")

    df_processed = df.copy()

    # Sanitization
    if sanitize:
        orig_cols = df_processed.columns.tolist()
        df_processed = sanitize_cols(df_processed)
        new_cols = df_processed.columns.tolist()
        mapping = dict(zip(orig_cols, new_cols))
        if pk:
            if isinstance(pk, str):
                pk = mapping.get(pk, pk)
            else:
                pk = [mapping.get(c, c) for c in pk]
        if fk:
            fk = [(mapping.get(c, c), rt, rc) for c, rt, rc in fk]
        if unique:
            unique = [[mapping.get(c, c) for c in (u if isinstance(u, (list, tuple)) else [u])]
                      for u in unique]
        if autoincrement:
            col, val = autoincrement
            autoincrement = (mapping.get(col, col), val)
    
    
    
    if cast:
        cast_kwargs = cast_kwargs or {}
        df_processed = cast_df(df_processed, **cast_kwargs)
    
    if pk is None:
        _, pk_name, _ = get_pk(df_processed, dfinfo=profile_dataframe(df_processed))
        pk = pk_name

    results: Dict[str, Any] = {"df_processed": df_processed, "servers": {}}

    os.makedirs("schema", exist_ok=True)

    for server_key, conn_str in conn_dict.items():
        d_key = server_key.lower()
        if d_key == 'postgres':
            d_key = 'postgresql'
        if d_key not in DTYPE_MAP:
            print(f"⚠️ ERROR: Skipping {d_key.upper()}. Unsupported server.")
            results["servers"][d_key] = {"status": "ERROR", "message": "Unsupported server"}
            continue

        effective_schema = schema_name if d_key != 'sqlite' else None
        print(f"\n--- Processing Database: {d_key.upper()} ---")

        ddl, meta = df_ddl(
            df_processed,
            table,
            server=d_key,
            schema_name=effective_schema,
            pk=pk,
            fk=fk,
            unique=unique,
            autoincrement=autoincrement,
            varchar_sizes=varchar_sizes,
            cast=False,
            sanitize=False
        )

        engine = create_engine(conn_str)
        server_status: Dict[str, Any] = {"ddl_success": False, "data_load_success": False}

        try:
            print("  ➡️ Attempting Table Creation (DDL)...", end=' ')
            with engine.connect() as conn:
                statements = [stmt.strip() for stmt in ddl.split(';') if stmt.strip()]
                for stmt in statements:
                    conn.execute(text(stmt))
                conn.commit()
            server_status["ddl_success"] = True
            print("✅ SUCCESS")

            print("  ➡️ Attempting Data Load...", end=' ')
            df_processed.to_sql(
                table,
                engine,
                schema=effective_schema,
                if_exists='append',
                index=False,
                method='multi'
            )
            server_status["data_load_success"] = True
            print("✅ SUCCESS")

            json_filename = f"schema/{d_key}_{table}_schema.json"
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(meta, f, indent=2, default=str)
            try:
                log_json(f"ddl_result_{d_key}_{table}", {"status": "SUCCESS", "rows": len(df_processed), "ddl_file": json_filename, "server": d_key, "table": table})
            except Exception:
                pass

            results["servers"][d_key] = cast(Dict[str, Any], {
                **server_status,
                "status": "COMPLETED",
                "ddl_file": json_filename,
                "schema_metadata": meta
            })

        except SQLAlchemyError as err:
            msg = f"SQLAlchemy Error: {err.__class__.__name__}. Details: {str(err).splitlines()[0]}"
            print(f"❌ FAIL ({msg})")
            if not server_status["ddl_success"]:
                overall_status = "DDL_FAILED"
            elif not server_status["data_load_success"]:
                overall_status = "DATA_LOAD_FAILED"
            else:
                overall_status = "ERROR"
            try:
                log_json(f"ddl_error_{d_key}_{table}", {"status": overall_status, "error": str(err), "error_type": err.__class__.__name__, "server": d_key, "table": table})
            except Exception:
                pass

            results["servers"][d_key] = cast(Dict[str, Any], {
                **server_status,
                "status": overall_status,
                "message": msg
            })

        except Exception as err:
            msg = f"Unexpected Error: {err.__class__.__name__}. Details: {str(err).splitlines()[0]}"
            print(f"❌ FAIL ({msg})")
            try:
                log_json(f"ddl_error_{d_key}_{table}", {"status": "UNEXPECTED_ERROR", "error": str(err), "error_type": err.__class__.__name__, "server": d_key, "table": table})
            except Exception:
                pass
            results["servers"][d_key] = cast(Dict[str, Any], {
                **server_status,
                "status": "UNEXPECTED_ERROR",
                "message": msg
            })

    return results


@log_call
def csv_ddl_create(
    conn_dict: Mapping[str, str],
    csv_path: str,
    table: str,
    schema_name: Optional[str] = None,
    pk: Optional[Union[str, List[str]]] = None,
    fk: Optional[List[Tuple[str, str, str]]] = None,
    unique: Optional[List[Union[str, List[str]]]] = None,
    autoincrement: Optional[Tuple[str, int]] = None,
    varchar_sizes: Optional[Dict[str, int]] = None,
    cast: bool = True,
    cast_kwargs: Optional[Dict[str, Any]] = None,
    csv_overwrite: bool = True
) -> Dict[str, Any]:
    if not isinstance(csv_path, str):
        raise ValueError("csv_path must be a string path to the CSV file")

    df = pd.read_csv(csv_path)

    results = df_ddl_create(
        conn_dict=conn_dict,
        df=df,
        table=table,
        schema_name=schema_name,
        pk=pk,
        fk=fk,
        unique=unique,
        autoincrement=autoincrement,
        varchar_sizes=varchar_sizes,
        cast=cast,
        cast_kwargs=cast_kwargs,
        sanitize=False
    )

    if csv_overwrite:
        processed_df = results.get("df_processed")
        if processed_df is not None:
            processed_df.to_csv(csv_path, index=False)
            print(f"Overwrote original CSV at {csv_path} with processed DataFrame.")

    return results


# --------------------------------------------------------------------
# Self-test / example
# --------------------------------------------------------------------

if __name__ == "__main__":
    df_test = pd.DataFrame({
        'user_id': [1, 2, 3],
        'user_name': ['Alice', 'Bob', 'Charlie'],
        'email': ['alice@test.com', 'bob@test.com', None],
        'salary': [50000.50, 60000.75, None],
        'is_active': [True, False, True],
        'hire_date': pd.to_datetime(['2020-01-15', '2019-06-20', '2021-03-10']),
        'comment': ['a', None, 'c']
    })
    servers = ['oracle', 'sqlite', 'mssql', 'mysql', 'postgres']

    print("--- DDL Generation Test ---")
    for srv in servers:
        print("\n" + "="*70)
        print(f"Database: {srv.upper()}")
        print("="*70)
        try:
            sql, schema_meta = df_ddl(
                df_test,
                'employees',
                server=srv,
                schema_name='hr' if srv != 'sqlite' else None,
                pk='user_id',
                unique=['email'],
                autoincrement=('user_id', 100),
                varchar_sizes={'user_name': 50}
            )
            print(sql)
            print("\nSchema Metadata:")
            print(json.dumps(schema_meta, indent=2, default=str))
        except Exception as err:
            print(f"Error generating DDL for {srv}: {err}")