from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
try:
    from dateutil import parser as _du
except ImportError:
    _du = None
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError
from sqlalchemy.sql import sqltypes as sat
from .logger import log_call, log_json, log_string

# Oracle-specific type imports (safe import with fallback)
try:
    from sqlalchemy.dialects.oracle import (
        VARCHAR2, NVARCHAR2, CHAR, NCHAR, CLOB, NCLOB,
        NUMBER, FLOAT as ORACLE_FLOAT, BINARY_DOUBLE, BINARY_FLOAT,
        DATE as ORACLE_DATE, TIMESTAMP as ORACLE_TIMESTAMP,
        RAW, BLOB as ORACLE_BLOB, LONG, ROWID, INTERVAL
    )
    ORACLE_TYPES_AVAILABLE = True
except ImportError:
    ORACLE_TYPES_AVAILABLE = False
    VARCHAR2: Any = None
    NVARCHAR2: Any = None
    CHAR: Any = None
    NCHAR: Any = None
    CLOB: Any = None
    NCLOB: Any = None
    NUMBER: Any = None
    ORACLE_FLOAT: Any = None
    BINARY_DOUBLE: Any = None
    BINARY_FLOAT: Any = None
    ORACLE_DATE: Any = None
    ORACLE_TIMESTAMP: Any = None
    RAW: Any = None
    ORACLE_BLOB: Any = None
    LONG: Any = None
    ROWID: Any = None
    INTERVAL: Any = None

# MySQL/MariaDB-specific type imports
try:
    from sqlalchemy.dialects.mysql import (
        TINYINT, SMALLINT, MEDIUMINT, INTEGER as MYSQL_INTEGER, BIGINT as MYSQL_BIGINT,
        FLOAT as MYSQL_FLOAT, DOUBLE as MYSQL_DOUBLE, DECIMAL as MYSQL_DECIMAL,
        VARCHAR as MYSQL_VARCHAR, CHAR as MYSQL_CHAR, TEXT as MYSQL_TEXT,
        TINYTEXT, MEDIUMTEXT, LONGTEXT,
        DATETIME as MYSQL_DATETIME, TIMESTAMP as MYSQL_TIMESTAMP, DATE as MYSQL_DATE, TIME as MYSQL_TIME,
        BLOB as MYSQL_BLOB, TINYBLOB, MEDIUMBLOB, LONGBLOB,
        JSON as MYSQL_JSON, ENUM as MYSQL_ENUM, SET as MYSQL_SET
    )
    MYSQL_TYPES_AVAILABLE = True
except ImportError:
    MYSQL_TYPES_AVAILABLE = False
    TINYINT: Any = None
    SMALLINT: Any = None
    MEDIUMINT: Any = None
    MYSQL_INTEGER: Any = None
    MYSQL_BIGINT: Any = None
    MYSQL_FLOAT: Any = None
    MYSQL_DOUBLE: Any = None
    MYSQL_DECIMAL: Any = None
    MYSQL_VARCHAR: Any = None
    MYSQL_CHAR: Any = None
    MYSQL_TEXT: Any = None
    TINYTEXT: Any = None
    MEDIUMTEXT: Any = None
    LONGTEXT: Any = None
    MYSQL_DATETIME: Any = None
    MYSQL_TIMESTAMP: Any = None
    MYSQL_DATE: Any = None
    MYSQL_TIME: Any = None
    MYSQL_BLOB: Any = None
    TINYBLOB: Any = None
    MEDIUMBLOB: Any = None
    LONGBLOB: Any = None
    MYSQL_JSON: Any = None
    MYSQL_ENUM: Any = None
    MYSQL_SET: Any = None

# MSSQL-specific type imports
try:
    from sqlalchemy.dialects.mssql import (
        TINYINT as MSSQL_TINYINT, SMALLINT as MSSQL_SMALLINT, 
        INTEGER as MSSQL_INTEGER, BIGINT as MSSQL_BIGINT,
        FLOAT as MSSQL_FLOAT, REAL as MSSQL_REAL, DECIMAL as MSSQL_DECIMAL, MONEY, SMALLMONEY,
        VARCHAR as MSSQL_VARCHAR, CHAR as MSSQL_CHAR, NVARCHAR, NCHAR as MSSQL_NCHAR, TEXT as MSSQL_TEXT, NTEXT,
        DATETIME as MSSQL_DATETIME, DATETIME2, SMALLDATETIME, DATE as MSSQL_DATE, TIME as MSSQL_TIME,
        DATETIMEOFFSET,
        BINARY as MSSQL_BINARY, VARBINARY as MSSQL_VARBINARY, IMAGE,
        BIT
    )
    MSSQL_TYPES_AVAILABLE = True
except ImportError:
    MSSQL_TYPES_AVAILABLE = False
    MSSQL_TINYINT: Any = None
    MSSQL_SMALLINT: Any = None
    MSSQL_INTEGER: Any = None
    MSSQL_BIGINT: Any = None
    MSSQL_FLOAT: Any = None
    MSSQL_REAL: Any = None
    MSSQL_DECIMAL: Any = None
    MONEY: Any = None
    SMALLMONEY: Any = None
    MSSQL_VARCHAR: Any = None
    MSSQL_CHAR: Any = None
    NVARCHAR: Any = None
    MSSQL_NCHAR: Any = None
    MSSQL_TEXT: Any = None
    NTEXT: Any = None
    MSSQL_DATETIME: Any = None
    DATETIME2: Any = None
    SMALLDATETIME: Any = None
    MSSQL_DATE: Any = None
    MSSQL_TIME: Any = None
    DATETIMEOFFSET: Any = None
    MSSQL_BINARY: Any = None
    MSSQL_VARBINARY: Any = None
    IMAGE: Any = None
    BIT: Any = None


from .schema_analyzer import SchemaAnalyzer, ColumnInfo, ConstraintInfo

# Optional sklearn
try:
    from sklearn.ensemble import IsolationForest
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# Configure logging
logger = logging.getLogger("StrictCorrector")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


class SchemaAligner:
    """
    Aligns DataFrame to SQL table schema with STRICT type enforcement and Outlier Correction.
    Uses SchemaAnalyzer for introspection.
    """

    DEFAULT_TRUE = {"1", "true", "yes", "y", "on"}
    DEFAULT_FALSE = {"0", "false", "no", "n", "off"}

    def __init__(self, conn: Connection | Engine, db_type: str | None = None,
                 *, on_error: str = 'coerce', failure_threshold: float = 3,
                 validate_fk: bool = False, add_missing_cols: bool = False,
                 col_map: Dict[str, str] | None = None):
        """Initialize SchemaAligner with a persistent connection and defaults."""
        if conn is None:
            raise ValueError("Connection/engine is required")
        self._conn = conn
        self.supported = {"oracle", "sqlite", "mssql", "postgres", "postgresql", "mysql", "mariadb"}
        self._db_type = db_type.lower() if db_type else None
        if self._db_type and self._db_type not in self.supported:
            raise ValueError(f"Unsupported db_type '{db_type}'")
        self._default_on_error = on_error
        self._default_failure_threshold = failure_threshold
        self._default_validate_fk = validate_fk
        self._default_add_missing_cols = add_missing_cols
        self._default_col_map = dict(col_map) if col_map else None
    
    def _detect_dialect(self, conn: Connection | Engine) -> str:
        """Extract dialect name from SQLAlchemy engine/connection."""
        if isinstance(conn, Connection):
            dialect_name = conn.engine.dialect.name.lower()
        else:
            dialect_name = conn.dialect.name.lower()
        
        # Normalize dialect names
        if dialect_name in ('postgresql', 'postgres'):
            return 'postgres'
        if dialect_name in ('mariadb',):
            return 'mysql'
        return dialect_name
    
    @property
    def db_type(self) -> str:
        """Current database type. Set after align() is called with auto-detection."""
        return self._db_type or 'unknown'

    @log_call
    def align(self, df: pd.DataFrame, table: str, 
              schema: str | None = None,
              *, conn: Connection | Engine | None = None,
              on_error: str | None = None,
              failure_threshold: float | None = None,
              validate_fk: bool | None = None,
              add_missing_cols: bool | None = None,
              col_map: Dict[str, str] | None = None,
              semantic_type_meta: Dict[str, str] | None = None) -> pd.DataFrame:
        """
        Main entry point.
        :param conn: Optional override for the engine/connection supplied at init.
        :param col_map: Optional mapping of {DataFrameColumn: SQLColumn} to handle aliasing.
        """
        actual_conn = conn or self._conn
        if actual_conn is None:
            raise ValueError("Connection/engine is required")
        on_error = on_error if on_error is not None else self._default_on_error
        failure_threshold = failure_threshold if failure_threshold is not None else self._default_failure_threshold
        validate_fk = validate_fk if validate_fk is not None else self._default_validate_fk
        add_missing_cols = add_missing_cols if add_missing_cols is not None else self._default_add_missing_cols
        if col_map is not None:
            effective_col_map = dict(col_map)
        elif self._default_col_map is not None:
            effective_col_map = dict(self._default_col_map)
        else:
            effective_col_map = None

        if self._db_type is None:
            self._db_type = self._detect_dialect(actual_conn)
            logger.info(f"Auto-detected database dialect: {self._db_type}")

        analyzer = SchemaAnalyzer(actual_conn)
        
        report = analyzer.analyze_table(schema=schema, table_name=table, df=None, run_fk_checks=False)
        
        if not report.table_exists:
            raise NoSuchTableError(f"Table '{table}' not found")
        
        meta = report.columns
        constraints = report.constraints

        if add_missing_cols:
            mapped_df = self._map_columns(df, meta, effective_col_map)
            
            cols_in_db = set(meta.keys())
            candidates = [c for c in mapped_df.columns if c not in cols_in_db]
            
            if candidates:
                logger.info(f"Schema Evolution: Adding {len(candidates)} columns: {candidates}")
                self._add_missing_columns(actual_conn, table, schema, mapped_df, candidates)
                
                analyzer = SchemaAnalyzer(actual_conn)
                report = analyzer.analyze_table(schema=schema, table_name=table, df=None, run_fk_checks=False)
                meta = report.columns
                constraints = report.constraints

        df_mapped = self._map_columns(df, meta, effective_col_map)
        try:
            if effective_col_map:
                log_json(f"column_mapping_{table}", effective_col_map)
        except Exception:
            pass
        
        cols_to_keep = [c for c in df_mapped.columns if c in meta]
        dropped_cols = set(df_mapped.columns) - set(cols_to_keep)
        if dropped_cols:
            logger.warning(f"Dropping extra columns not in target schema: {list(dropped_cols)}")
            try:
                log_json(f"dropped_columns_{table}", list(dropped_cols))
            except Exception:
                pass
        df_mapped = df_mapped[cols_to_keep]
        try:
            if semantic_type_meta:
                log_json(f"semantic_routing_{table}", semantic_type_meta)
        except Exception:
            pass

        aligned = self._coerce_types(df_mapped, meta, on_error, failure_threshold, semantic_type_meta)

        aligned = self._enforce_nullability(aligned, meta, on_error, failure_threshold)
        
        self._validate_constraints(actual_conn, aligned, meta, constraints, validate_fk)

        final_cols = [c for c in meta.keys() if c in aligned.columns]
        for c, info in meta.items():
            if c not in aligned.columns:
                if not info.nullable and info.default is None:
                     logger.warning(f"Missing required column '{c}'. Filling NULL.")
                aligned[c] = None
        
        # Final pass: Ensure native Python types for DB driver compatibility (e.g. Oracle DPY-3002)
        aligned = self._finalize_types(aligned)

        return aligned[final_cols]

    # -------------------------------------------------------------------------
    # Schema Evolution
    # -------------------------------------------------------------------------
    def _add_missing_columns(self, conn: Connection | Engine, table: str, schema: str | None, 
                             df: pd.DataFrame, new_cols: List[str]):
        """
        Generates and executes ALTER TABLE statements to add missing columns.
        Uses a simple type mapping strategy.
        """
        # Dialect-aware Type Mapping (Pandas -> SQL)
        def map_type(s: pd.Series) -> str:
            dialect = self.db_type
            
            # Integer mapping
            if pd.api.types.is_integer_dtype(s):
                if dialect == 'oracle':
                    return "NUMBER(19)"  # Oracle equivalent of BIGINT
                if dialect == 'sqlite':
                    return "INTEGER"  # SQLite uses dynamic typing
                if dialect == 'postgres':
                    return "BIGINT"
                return "BIGINT"  # Default
            
            # Float mapping
            if pd.api.types.is_float_dtype(s):
                if dialect == 'oracle':
                    return "NUMBER"  # Oracle's flexible numeric
                if dialect == 'postgres':
                    return "DOUBLE PRECISION"
                return "FLOAT"
            
            # Boolean mapping
            if pd.api.types.is_bool_dtype(s):
                if dialect == 'mssql':
                    return "BIT"
                if dialect == 'oracle':
                    return "NUMBER(1)"  # Oracle's boolean idiom
                if dialect == 'sqlite':
                    return "INTEGER"  # SQLite uses 0/1
                if dialect == 'postgres':
                    return "BOOLEAN"
                return "BOOLEAN"
            
            # Datetime mapping
            if pd.api.types.is_datetime64_any_dtype(s):
                if dialect == 'oracle':
                    return "TIMESTAMP"  # Oracle TIMESTAMP (DATE has issues)
                if dialect == 'postgres':
                    return "TIMESTAMP WITH TIME ZONE"
                if dialect == 'sqlite':
                    return "TEXT"  # SQLite stores as ISO string
                return "TIMESTAMP"
            
            # JSON check
            if s.apply(lambda x: isinstance(x, (dict, list))).any():
                if dialect == 'postgres':
                    return "JSONB"
                if dialect == 'oracle':
                    return "CLOB"  # Pre-21c Oracle stores JSON in CLOB
                if dialect == 'sqlite':
                    return "TEXT"  # SQLite uses TEXT for JSON
                if dialect == 'mysql':
                    return "JSON"
                return "TEXT"
            
            # String mapping
            # compute max length ignoring nulls (avoid 'nan' string inflating length)
            if len(s.dropna()) > 0:
                max_len_val = s.dropna().astype(str).str.len().max()
                try:
                    max_len = int(max_len_val) if not pd.isna(max_len_val) else 255
                except (TypeError, ValueError):
                    max_len = 255
            else:
                max_len = 255
            # Ensure max_len is a sane positive integer (prevent injection via malformed metadata)
            try:
                max_len = int(max_len)
            except (TypeError, ValueError):
                max_len = 255
            if max_len < 1:
                max_len = 1
            if max_len > 4000:
                if dialect == 'oracle':
                    return "CLOB"
                if dialect == 'postgres':
                    return "TEXT"
                if dialect == 'mssql':
                    return "VARCHAR(MAX)"
                return "TEXT"
            elif max_len > 255:
                if dialect == 'oracle':
                    return f"VARCHAR2({max_len})"
                return f"VARCHAR({max_len})"
            else:
                if dialect == 'oracle':
                    return "VARCHAR2(255)"
                return "VARCHAR(255)"

        # sanitize identifiers to avoid SQL injection via column/table/schema names
        def _safe_ident(name: Optional[str]) -> str:
            if name is None:
                raise ValueError("Identifier cannot be None")
            if not isinstance(name, str):
                raise ValueError(f"Identifier must be a string: {name!r}")
            if re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name):
                return name
            raise ValueError(f"Invalid identifier name: {name}")

        # Validate schema and table names strictly (no silent fallback to unsafe values)
        safe_table = _safe_ident(table)
        safe_schema = _safe_ident(schema) if schema else None
        full_table = f"{safe_schema}.{safe_table}" if safe_schema else safe_table
        # Function to execute SQL
        def run_sql(sql):
            if isinstance(conn, Engine):
                with conn.begin() as c:
                    c.execute(text(sql))
            else:
                conn.execute(text(sql))
        
        for col in new_cols:
            try:
                sql_type = map_type(df[col])
                # Validate and quote column name
                safe_col_name = _safe_ident(col)
                if self.db_type in ('mysql', 'mariadb'):
                    quoted_col = f'`{safe_col_name}`'
                elif self.db_type == 'mssql':
                    quoted_col = f'[{safe_col_name}]'
                else:
                    quoted_col = f'"{safe_col_name}"'

                alter_stmt = f"ALTER TABLE {full_table} ADD COLUMN {quoted_col} {sql_type}"

                # Dialect adjustments
                if self.db_type == 'mssql':
                    alter_stmt = f"ALTER TABLE {full_table} ADD {quoted_col} {sql_type}"
                elif self.db_type == 'oracle':
                    alter_stmt = f"ALTER TABLE {full_table} ADD {quoted_col} {sql_type}"
                elif self.db_type == 'sqlite':
                    # SQLite supports ADD COLUMN
                    pass
                    
                logger.info(f"Executing: {alter_stmt}")
                run_sql(alter_stmt)
                try:
                    log_string(f"schema_evolution_{table}", f"Added column: {col} ({sql_type})")
                except Exception:
                    pass
            except (ValueError, KeyError, SQLAlchemyError) as e:
                # Restrict to expected error classes to avoid masking unrelated bugs
                logger.error(f"Failed to add column '{col}': {e}")
                try:
                    log_json(f"schema_evolution_error_{table}", {"column": col, "error": str(e), "sql_type": sql_type})
                except Exception:
                    pass
                # Continue attempting remaining columns

    # -------------------------------------------------------------------------
    # Mapping - Key Normalization
    # -------------------------------------------------------------------------
    def _map_columns(self, df: pd.DataFrame, meta: Dict[str, ColumnInfo], 
                     col_map: Dict[str, str] | None = None) -> pd.DataFrame:
        """
        Maps DF columns to DB columns.
        Priority:
        1. Explicit `col_map` alias (InputCol -> DBCol)
        2. Exact/Case-Insensitive match (InputCol -> DBCol)
        """
        db_map = {k.lower(): k for k in meta.keys()}
        rename_map = {}
        
        # Normalize custom map keys for robust matching
        custom_map = {k.lower(): v for k, v in (col_map or {}).items()} if col_map else {}
        
        for col in df.columns:
            # 1. Check Custom Map (Case Insensitive Input match)
            if col.lower() in custom_map:
                target = custom_map[col.lower()]
                if target.lower() in db_map:
                    rename_map[col] = db_map[target.lower()]
                else:
                    rename_map[col] = target 
                continue
                
            # 2. Check DB Map
            c_low = col.lower()
            if c_low in db_map:
                rename_map[col] = db_map[c_low]
            
        return df.rename(columns=rename_map)

    # -------------------------------------------------------------------------
    # Strict Coercion
    # -------------------------------------------------------------------------
    def _coerce_types(self, df: pd.DataFrame, meta: Dict[str, ColumnInfo], 
                      on_error: str, threshold: float, semantic_type_meta: Dict[str, str] | None = None) -> pd.DataFrame:
        out = df.copy()
        sem_map = semantic_type_meta or {}
        
        for col_name, info in meta.items():
            if col_name not in out.columns: continue
            s = out[col_name]
            if s.empty: continue
            
            semantic = sem_map.get(col_name)
            if semantic and pd.api.types.is_object_dtype(s):
                try:
                    log_json(f"semantic_override_{col_name}", {"column": col_name, "semantic_type": semantic, "sql_type": getattr(info, 'type_str', None)})
                except Exception:
                    pass
                if semantic == "STRING_OBJECT":
                    out[col_name] = self._strict_string(s, info, on_error, threshold)
                    continue
                if semantic == "STRUCTURED_OBJECT":
                    out[col_name] = self._strict_json(s, info, on_error, threshold)
                    continue
                if semantic == "TRUE_OBJECT":
                    out[col_name] = self._strict_binary(s, info, on_error, threshold)
                    continue
            raw_t = info.raw_type
            
            if self._is_int_type(raw_t):
                out[col_name] = self._strict_int(s, info, on_error, threshold)
            elif self._is_string_type(raw_t):
                out[col_name] = self._strict_string(s, info, on_error, threshold)
            elif self._is_float_type(raw_t):
                out[col_name] = self._strict_float(s, info, on_error, threshold)
            elif self._is_bool_type(raw_t):
                out[col_name] = self._strict_bool(s, info, on_error, threshold)
            elif self._is_datetime_type(raw_t):
                out[col_name] = self._strict_datetime(s, info, on_error, threshold)
            elif self._is_json_type(raw_t):
                out[col_name] = self._strict_json(s, info, on_error, threshold)
            elif self._is_binary_type(raw_t):
                out[col_name] = self._strict_binary(s, info, on_error, threshold)

        return out

    def _strict_binary(self, s: pd.Series, info: ColumnInfo, on_error: str, threshold: float) -> pd.Series:
        # Validate bytes-like values (preserve bytes, nullify others)
        is_binary = s.map(lambda x: pd.isna(x) or isinstance(x, (bytes, bytearray, memoryview)))
        
        mask_fail = ~is_binary
        if mask_fail.any():
             self._validate_failure_rate(mask_fail, info.name, on_error, threshold, "binary content (bytes)")
             
        out = s.copy()
        out[mask_fail] = None
        return out

    def _strict_json(self, s: pd.Series, info: ColumnInfo, on_error: str, threshold: float) -> pd.Series:
        import json
        def validate_json(val):
            if pd.isna(val): return None
            # If already dict/list, dump it
            if isinstance(val, (dict, list)):
                return json.dumps(val)
            # If string, try load then dump to normalize (or just validate)
            try:
                if isinstance(val, str):
                    parsed = json.loads(val)
                    return json.dumps(parsed)
            except (ValueError, TypeError, json.JSONDecodeError):
                pass
            return None

        res = s.apply(validate_json)
        
        mask_fail = s.notna() & res.isna()
        if mask_fail.any():
             self._validate_failure_rate(mask_fail, info.name, on_error, threshold, "valid JSON")
             
        return res

    def _validate_failure_rate(self, failure_mask: pd.Series, col_name: str, 
                               on_error: str, threshold: float, msg: str,
                               original_series: Optional[pd.Series] = None):
        """
        Validates failure rate against threshold. Outlier detection is attempted if exceeded.
        """
        fail_count = int(failure_mask.sum())
        if fail_count == 0:
            return

        total = len(failure_mask)
        rate = fail_count / total if total > 0 else 0.0
        
        if rate <= threshold:
            logger.warning(f"[{col_name}] {fail_count}/{total} ({rate:.1%}) failures (<= {threshold:.1%}). Coercing to NULL.")
            return

        try:
            log_json(f"validation_failure_{col_name}", {"column": col_name, "failure_count": fail_count, "failure_rate": rate, "threshold": threshold, "message": msg})
        except Exception:
            pass

        # Attempt Outlier Correction
        if SKLEARN_AVAILABLE and original_series is not None and pd.api.types.is_numeric_dtype(original_series):
             logger.info(f"[{col_name}] Failure rate {rate:.1%} exceeds threshold. Attempting Outlier Detection.")
             is_outlier = self._detect_outliers(original_series)
             
             systematic_mask = failure_mask & (~is_outlier)
             sys_count = int(systematic_mask.sum())
             sys_rate = sys_count / total
             
             logger.info(f"[{col_name}] Outlier analysis: {fail_count} total fails. {sys_count} systematic ({fail_count-sys_count} outliers). New Rate: {sys_rate:.1%}")
             
             if sys_rate <= threshold:
                 logger.warning(f"[{col_name}] Systematic failures {sys_rate:.1%} <= threshold (outliers removed). Coercing to NULL.")
                 return
             
             # If still over threshold, use sys_rate for error message
             # Note: caller uses original mask, so this is just for validation logic.
             # Strict corrector will nullify failures later if we return? 
             # No, this function returns void. The caller (e.g. _strict_int) sets NaNs if we return.
             # If we raise, process aborts.
             rate = sys_rate
             fail_count = sys_count

        err_msg = f"[{col_name}] Critical: {fail_count}/{total} ({rate:.1%}) rows failed {msg}."
        
        # Audit Fix: Respect on_error='coerce' even if threshold exceeded
        if on_error == 'coerce':
             logger.error(f"{err_msg} Exceeds threshold {threshold:.1%} but on_error='coerce'. Coercing to NULL.")
             return

        if on_error == 'raise':
            raise ValueError(f"{err_msg} Aborting.")
        else:
            raise ValueError(f"{err_msg} Exceeds threshold {threshold:.1%}.")

    def _detect_outliers(self, s: pd.Series) -> pd.Series:
        s_clean = s.dropna()
        if s_clean.empty:
            return pd.Series(False, index=s.index)
        
        X = s_clean.to_numpy().reshape(-1, 1)
        try:
            clf = IsolationForest(random_state=42, contamination='auto')
            preds = clf.fit_predict(X)
        except (ValueError, RuntimeError) as e:
            logger.warning(f"Outlier detection failed: {e}")
            return pd.Series(False, index=s.index)

        outlier_mask = pd.Series(False, index=s.index)
        outlier_mask.loc[s_clean.index] = (preds == -1)
        return outlier_mask

    # --- Type Handlers ---

    def _strict_int(self, s: pd.Series, info: ColumnInfo, on_error: str, threshold: float) -> pd.Series:
        s_num = pd.to_numeric(s, errors='coerce')
        mask_invalid_str = s.notna() & s_num.isna()
        has_remainder = (s_num.fillna(0) % 1).abs() > 1e-9
        mask_float_loss = s_num.notna() & has_remainder
        total_failures = mask_invalid_str | mask_float_loss
        
        if total_failures.any():
            self._validate_failure_rate(total_failures, info.name, on_error, threshold, "strict integer check", original_series=s_num)
            s_num[total_failures] = np.nan
        return s_num.astype('Int64')

    def _strict_float(self, s: pd.Series, info: ColumnInfo, on_error: str, threshold: float) -> pd.Series:
        s_num = pd.to_numeric(s, errors='coerce')
        mask_fail = s.notna() & s_num.isna()
        if mask_fail.any():
            self._validate_failure_rate(mask_fail, info.name, on_error, threshold, "numeric conversion", original_series=s_num)
        return s_num

    def _strict_string(self, s: pd.Series, info: ColumnInfo, on_error: str, threshold: float) -> pd.Series:
        s_str = s.astype(str).where(s.notna())
        if info.length:
            out_of_bounds = s_str.str.len() > info.length
            if out_of_bounds.any():
                 self._validate_failure_rate(out_of_bounds, info.name, on_error, threshold, f"max length {info.length}", original_series=s_str.str.len())
                 s_str.loc[out_of_bounds] = None
        return s_str

    def _strict_bool(self, s: pd.Series, info: ColumnInfo, on_error: str, threshold: float) -> pd.Series:
        valid_true = self.DEFAULT_TRUE
        valid_false = self.DEFAULT_FALSE
        s_norm = s.astype(str).str.lower().str.strip()
        mask_na = s.isna()
        s_norm = s_norm.where(~mask_na, np.nan)
        mask_true = s_norm.isin(valid_true)
        mask_false = s_norm.isin(valid_false)
        mask_fail = s.notna() & (~mask_true) & (~mask_false)
        
        if mask_fail.any():
            self._validate_failure_rate(mask_fail, info.name, on_error, threshold, "boolean constraint")
            
        out = pd.Series(index=s.index, dtype='object')
        out[mask_true] = True
        out[mask_false] = False
        out[mask_fail] = None
        return out.astype('boolean')

    def _strict_datetime(self, s: pd.Series, info: ColumnInfo, on_error: str, threshold: float) -> pd.Series:
        # Flexible parsing: try vectorized parse, then fallback to per-value dateutil parsing
        try:
            # Avoid using pandas-only 'format' values that require pandas>=2.0.
            s_dt = pd.to_datetime(s, errors='coerce')
        except (TypeError, ValueError) as e:
            logger.debug(f"Vectorized datetime parse failed: {e}")
            s_dt = pd.Series(pd.NaT, index=s.index)

        # For any entries that failed vectorized parse, try dateutil per-value (more tolerant)
        if _du is not None:
            mask_try = s_dt.isna() & s.notna()
            if mask_try.any():
                def _try_parse(v):
                    try:
                        return _du.parse(str(v))
                    except Exception:
                        return pd.NaT
                parsed = s.loc[mask_try].map(_try_parse)
                # Assign back and coerce to pandas datetime
                s_dt.loc[mask_try] = pd.to_datetime(parsed, errors='coerce')
        
        # For databases that need timezone awareness, localize naive datetimes to UTC
        if self.db_type not in ('sqlite', 'mysql'):
            # Only localize if dtype is naive datetime64 (not timezone-aware)
            try:
                from pandas.api.types import is_datetime64tz_dtype
                if not is_datetime64tz_dtype(s_dt.dtype):
                    s_dt = s_dt.dt.tz_localize('UTC')  # type: ignore[attr-defined]
            except (AttributeError, TypeError, ValueError) as e:
                logger.debug(f"Timezone localization skipped: {e}")
        
        mask_fail = s.notna() & s_dt.isna()
        
        if mask_fail.any():
            self._validate_failure_rate(mask_fail, info.name, on_error, threshold, "datetime parsing")
            
        return s_dt
    
    # -------------------------------------------------------------------------
    # Validation & Nullability
    # -------------------------------------------------------------------------
    def _enforce_nullability(self, df: pd.DataFrame, meta: Dict[str, ColumnInfo], on_error: str, threshold: float) -> pd.DataFrame:
        for col_name, info in meta.items():
            if col_name not in df.columns: continue
            if not info.nullable:
                nulls = df[col_name].isna()
                if nulls.any():
                     self._validate_failure_rate(nulls, col_name, on_error, threshold, "NOT NULL constraint")
        return df

    def _validate_constraints(self, conn, df: pd.DataFrame, meta: Dict[str, ColumnInfo], 
                              constraints: ConstraintInfo, validate_fk: bool):
        """Pre-insertion validation of structural constraints using ConstraintInfo from Analyzer."""
        #pylint: disable=unused-argument
        
        # 1. PK Validation
        pk = constraints.primary_key
        if pk:
            pk_cols = pk.get('constrained_columns', [])
            # Filter cols present in DF
            pk_cols_present = [c for c in pk_cols if c in df.columns]
            if pk_cols_present and len(pk_cols_present) == len(pk_cols):
                if df.duplicated(subset=pk_cols_present).any():
                    dupes = df[df.duplicated(subset=pk_cols_present, keep=False)]
                    logger.error(f"Primary Key violation: Duplicates found in {pk_cols_present}. count={len(dupes)}")

        # 2. Unique Constraints
        for u in constraints.unique_constraints:
            u_cols = u.get('column_names', [])
            u_cols_present = [c for c in u_cols if c in df.columns]
            if u_cols_present and len(u_cols_present) == len(u_cols):
                if df.duplicated(subset=u_cols_present).any():
                     logger.error(f"Unique constraint violation: Duplicates in {u_cols_present}.")

        # 3. Foreign Keys (DB Check)
        if validate_fk:
             pass

    # -------------------------------------------------------------------------
    # Type Helpers (Oracle, MySQL, MSSQL support)
    # -------------------------------------------------------------------------
    def _is_int_type(self, t: Any) -> bool:
        # Standard integer types
        if isinstance(t, (sat.INTEGER, sat.BIGINT, sat.SmallInteger)):
            return True
        # Oracle NUMBER with scale=0 is integer
        if ORACLE_TYPES_AVAILABLE and isinstance(t, NUMBER):
            # Only treat as integer when scale is explicitly 0.
            if getattr(t, 'scale', None) == 0:
                return True
        # MySQL integer types
        if MYSQL_TYPES_AVAILABLE:
            if isinstance(t, (TINYINT, SMALLINT, MEDIUMINT, MYSQL_INTEGER, MYSQL_BIGINT)):
                return True
        # MSSQL integer types
        if MSSQL_TYPES_AVAILABLE:
            if isinstance(t, (MSSQL_TINYINT, MSSQL_SMALLINT, MSSQL_INTEGER, MSSQL_BIGINT)):
                return True
        return False

    def _is_float_type(self, t: Any) -> bool:
        # Standard float types
        if isinstance(t, (sat.Float, sat.REAL, sat.DOUBLE_PRECISION, sat.Numeric)):
            return True
        # Oracle-specific float types
        if ORACLE_TYPES_AVAILABLE:
            if isinstance(t, (ORACLE_FLOAT, BINARY_DOUBLE, BINARY_FLOAT)):
                return True
            if isinstance(t, NUMBER) and hasattr(t, 'scale') and t.scale and t.scale > 0:
                return True
        # MySQL float types
        if MYSQL_TYPES_AVAILABLE:
            if isinstance(t, (MYSQL_FLOAT, MYSQL_DOUBLE, MYSQL_DECIMAL)):
                return True
        # MSSQL float types
        if MSSQL_TYPES_AVAILABLE:
            if isinstance(t, (MSSQL_FLOAT, MSSQL_REAL, MSSQL_DECIMAL, MONEY, SMALLMONEY)):
                return True
        return False

    def _is_string_type(self, t: Any) -> bool:
        # Standard string types
        if isinstance(t, (sat.String, sat.Unicode, sat.Text, sat.VARCHAR, sat.CHAR)):
            return True
        # Oracle-specific string types
        if ORACLE_TYPES_AVAILABLE:
            if isinstance(t, (VARCHAR2, NVARCHAR2, CHAR, NCHAR, CLOB, NCLOB, LONG)):
                return True
        # MySQL string types
        if MYSQL_TYPES_AVAILABLE:
            if isinstance(t, (MYSQL_VARCHAR, MYSQL_CHAR, MYSQL_TEXT, TINYTEXT, MEDIUMTEXT, LONGTEXT, MYSQL_ENUM, MYSQL_SET)):
                return True
        # MSSQL string types
        if MSSQL_TYPES_AVAILABLE:
            if isinstance(t, (MSSQL_VARCHAR, MSSQL_CHAR, NVARCHAR, MSSQL_NCHAR, MSSQL_TEXT, NTEXT)):
                return True
        return False

    def _is_bool_type(self, t: Any) -> bool:
        if isinstance(t, sat.Boolean):
            return True
        type_name = str(t).upper()
        if type_name.startswith("BOOL"):
            return True
        # Oracle: NUMBER(1)
        if ORACLE_TYPES_AVAILABLE and isinstance(t, NUMBER):
            if hasattr(t, 'precision') and t.precision == 1 and (not hasattr(t, 'scale') or t.scale == 0):
                return True
        # MySQL: TINYINT(1)
        if MYSQL_TYPES_AVAILABLE and isinstance(t, TINYINT):
            if hasattr(t, 'display_width') and t.display_width == 1:
                return True
        # MSSQL: BIT
        if MSSQL_TYPES_AVAILABLE and isinstance(t, BIT):
            return True
        return False

    def _is_datetime_type(self, t: Any) -> bool:
        # Standard datetime types
        if isinstance(t, (sat.DateTime, sat.TIMESTAMP, sat.Date)):
            return True
        # Oracle-specific datetime types
        if ORACLE_TYPES_AVAILABLE:
            if isinstance(t, (ORACLE_DATE, ORACLE_TIMESTAMP, INTERVAL)):
                return True
        # MySQL datetime types
        if MYSQL_TYPES_AVAILABLE:
            if isinstance(t, (MYSQL_DATETIME, MYSQL_TIMESTAMP, MYSQL_DATE, MYSQL_TIME)):
                return True
        # MSSQL datetime types
        if MSSQL_TYPES_AVAILABLE:
            if isinstance(t, (MSSQL_DATETIME, DATETIME2, SMALLDATETIME, MSSQL_DATE, MSSQL_TIME, DATETIMEOFFSET)):
                return True
        return False
    
    def _is_json_type(self, t: Any) -> bool:
        if isinstance(t, (sat.JSON, sat.ARRAY)):
            return True
        type_name = type(t).__name__.upper()
        if type_name in ('JSON', 'JSONB'):
            return True
        # MySQL native JSON
        if MYSQL_TYPES_AVAILABLE and isinstance(t, MYSQL_JSON):
            return True
        return False

    def _is_binary_type(self, t: Any) -> bool:
        # Standard binary types
        if isinstance(t, (sat.LargeBinary, sat.BINARY, sat.VARBINARY, sat.BLOB)):
            return True
        # Oracle-specific binary types
        if ORACLE_TYPES_AVAILABLE:
            if isinstance(t, (RAW, ORACLE_BLOB)):
                return True
        # MySQL binary types
        if MYSQL_TYPES_AVAILABLE:
            if isinstance(t, (MYSQL_BLOB, TINYBLOB, MEDIUMBLOB, LONGBLOB)):
                return True
        # MSSQL binary types
        if MSSQL_TYPES_AVAILABLE:
            if isinstance(t, (MSSQL_BINARY, MSSQL_VARBINARY, IMAGE)):
                return True
        return False

    def _get_dialect_type_info(self, t: Any) -> Dict[str, Any]:
        """Extract type metadata (precision, scale, length) from dialect-specific types."""
        info = {'precision': None, 'scale': None, 'length': None}
        
        # Oracle NUMBER
        if ORACLE_TYPES_AVAILABLE and isinstance(t, NUMBER):
            info['precision'] = getattr(t, 'precision', None)
            info['scale'] = getattr(t, 'scale', None)
        
        # MySQL DECIMAL
        if MYSQL_TYPES_AVAILABLE and isinstance(t, MYSQL_DECIMAL):
            info['precision'] = getattr(t, 'precision', None)
            info['scale'] = getattr(t, 'scale', None)
        
        # MSSQL DECIMAL
        if MSSQL_TYPES_AVAILABLE and isinstance(t, MSSQL_DECIMAL):
            info['precision'] = getattr(t, 'precision', None)
            info['scale'] = getattr(t, 'scale', None)
        
        # String length
        if hasattr(t, 'length'):
            info['length'] = t.length
        
        return info

    def _finalize_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert pandas/numpy specific types (Int64, float64, etc.) to native Python types.
        This prevents errors in strict DB drivers like oracledb (DPY-3002).
        """
        out = df.copy()
        for col in out.columns:
            dtype = out[col].dtype
            
            # Convert Nullable Integers (Int64) and standard integers to Python int
            if pd.api.types.is_integer_dtype(dtype):
                # Convert to object series with Python ints and None
                out[col] = out[col].astype(object).where(out[col].notna(), None)
                
            # Convert Floats to Python float
            elif pd.api.types.is_float_dtype(dtype):
                out[col] = out[col].astype(object).where(out[col].notna(), None)
                
            # Convert Booleans
            elif pd.api.types.is_bool_dtype(dtype):
                out[col] = out[col].astype(object).where(out[col].notna(), None)
            
        return out
