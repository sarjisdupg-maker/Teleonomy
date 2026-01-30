# align_to_sql.py
# Robust merged version: Combines detailed metadata & safety from v1
# with concise structure & precise type handling from v2.
# Returns (aligned_df, report) for observability.

from __future__ import annotations

import pandas as pd
import numpy as np
# z-score computed without external dependencies
from sqlalchemy import MetaData, Table, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.sql.type_api import TypeEngine
from typing import Dict, Tuple, Optional

__all__ = [
    'normalize_sql_type',
    'coerce_series',
    'cast_qualify',
    'detect_outliers',
    'correct_outliers',
    'align_column',
    'detect_table_schema',
    'infer_sql_type',
    'add_column_to_table',
    'align_dataframe_to_schema',
    'save_aligned_dataframe',
    'generate_schema'
]

# ------------------------------------------------------------------
# Canonical type normalization (precise, from v2)
# ------------------------------------------------------------------
def normalize_sql_type(col_type: TypeEngine) -> str:
    """SQLAlchemy type → 'int'|'float'|'str'|'datetime'|'bool'."""
    from sqlalchemy import (
        Integer, SmallInteger, BigInteger,
        Numeric, Float, DECIMAL,
        String, Text, Unicode, Enum,
        DateTime, Date, Time,
        Boolean
    )
    if isinstance(col_type, (Integer, SmallInteger, BigInteger)):
        return 'int'
    if isinstance(col_type, (Numeric, Float, DECIMAL)):
        return 'float'
    if isinstance(col_type, (String, Text, Unicode, Enum)):
        return 'str'
    if isinstance(col_type, (DateTime, Date, Time)):
        return 'datetime'
    if isinstance(col_type, Boolean):
        return 'bool'
    return 'str'  # fallback

# ------------------------------------------------------------------
# Nullable dtype helpers
# ------------------------------------------------------------------
def _nullable_dtype(canonical: str) -> str:
    """Get pandas nullable dtype for canonical type."""
    mapping = {
        'int': 'Int64',
        'float': 'Float64',
        'str': 'string',
        'datetime': 'datetime64[ns]',
        'bool': 'boolean'
    }
    return mapping.get(canonical, 'object')

# ------------------------------------------------------------------
# Strict series coercion (merged, with fallbacks)
# ------------------------------------------------------------------
def coerce_series(series: pd.Series, canonical: str) -> pd.Series:
    """Strict cast Series to canonical dtype or raise."""
    try:
        if canonical == 'int':
            return pd.to_numeric(series, errors='raise').astype('Int64')
        if canonical == 'float':
            return pd.to_numeric(series, errors='raise').astype('Float64')
        if canonical == 'str':
            return series.astype('string')
        if canonical == 'datetime':
            return pd.to_datetime(series, errors='raise')
        if canonical == 'bool':
            return series.astype('boolean')
        return series.astype('object')  # fallback
    except (ValueError, TypeError) as e:
        raise ValueError(f"Cannot coerce to {canonical}: {str(e)}") from e

def cast_qualify(before: pd.Series, after: pd.Series, threshold: float = 10.0) -> bool:
    """True if null-percentage increase <= threshold."""
    return ((after.isna().mean() - before.isna().mean()) * 100.0) <= threshold

# ------------------------------------------------------------------
# Outlier detection / correction (from v1, with zscore & iqr)
# ------------------------------------------------------------------
def detect_outliers(
    series: pd.Series,
    method: str = 'iqr',
    iqr_scale: float = 1.5,
    z_threshold: float = 3.0
) -> pd.Series:
    """Boolean mask: True for VALID values (not outliers)."""
    if not pd.api.types.is_numeric_dtype(series) or len(series.dropna()) < 4:
        return pd.Series(True, index=series.index)

    v = series.astype(float)
    
    if method.lower() == 'iqr':
        q1, q3 = v.quantile(0.25), v.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - iqr_scale * iqr
        upper = q3 + iqr_scale * iqr
        return (v >= lower) & (v <= upper) | v.isna()

    if method.lower() in {'z', 'zscore'}:
        valid_mask = ~v.isna()
        if not valid_mask.any():
            return pd.Series(True, index=series.index)
        vals = v[valid_mask].to_numpy(dtype=float)
        mu = float(np.mean(vals))
        sigma = float(np.std(vals))
        if sigma == 0.0:
            return pd.Series(True, index=series.index)
        z_scores = (vals - mu) / sigma
        result = pd.Series(True, index=series.index)
        result[valid_mask] = np.abs(z_scores) <= z_threshold
        return result

    raise ValueError(f"Unsupported method: {method}")

def correct_outliers(series: pd.Series, valid_mask: pd.Series) -> pd.Series:
    """Replace outliers with NA, preserve dtype."""
    result = series.copy()
    result[~valid_mask] = pd.NA
    return result

# ------------------------------------------------------------------
# Single column alignment (merged, with metadata)
# ------------------------------------------------------------------
def align_column(
    series: pd.Series,
    sql_type: TypeEngine,
    threshold: float = 10.0,
    fix_outliers: bool = False,
    outlier_method: str = 'iqr',
    iqr_scale: float = 1.5,
    z_threshold: float = 3.0
) -> Tuple[pd.Series, Dict[str, any]]:
    """Align one column, return (aligned_series, col_report)."""
    canonical = normalize_sql_type(sql_type)
    report = {
        'original_null_count': int(series.isna().sum()),
        'original_null_pct': float(series.isna().mean() * 100),
        'outliers_detected': 0,
        'outliers_corrected': 0,
        'cast_reliable': True,
        'error': None,
        'final_null_count': 0,
        'final_null_pct': 0.0
    }

    try:
        coerced = coerce_series(series, canonical)
        reliable = cast_qualify(series, coerced, threshold)
        report['cast_reliable'] = reliable

        if not reliable and fix_outliers and canonical in {'int', 'float'}:
            valid_mask = detect_outliers(
                series if pd.api.types.is_numeric_dtype(series) else coerced,
                method=outlier_method,
                iqr_scale=iqr_scale,
                z_threshold=z_threshold
            )
            report['outliers_detected'] = int((~valid_mask).sum())
            
            if report['outliers_detected'] > 0:
                corrected = correct_outliers(series, valid_mask)
                coerced = coerce_series(corrected, canonical)
                report['outliers_corrected'] = report['outliers_detected']

        report['final_null_count'] = int(coerced.isna().sum())
        report['final_null_pct'] = float(coerced.isna().mean() * 100)

        return coerced, report

    except Exception as e:
        report['cast_reliable'] = False
        report['error'] = str(e)
        
        # Fallback to all-NA with proper dtype
        fallback_dtype = _nullable_dtype(canonical)
        fallback_series = pd.Series(pd.NA, index=series.index, dtype=fallback_dtype)
        report['final_null_count'] = int(fallback_series.isna().sum())
        report['final_null_pct'] = 100.0
        
        return fallback_series, report

# ------------------------------------------------------------------
# Schema helpers (from v2, with safety)
# ------------------------------------------------------------------
def detect_table_schema(engine: Engine, table: str) -> Optional[str]:
    """Return first schema containing table or None if default."""
    insp = inspect(engine)
    for sch in insp.get_schema_names():
        if table in insp.get_table_names(schema=sch):
            return sch
    return None

def _reflect_table(engine: Engine, table: str, schema: Optional[str]) -> Table:
    """Lightweight table reflection."""
    return Table(table, MetaData(), autoload_with=engine, schema=schema)

def _table_column_order(table: Table) -> list[str]:
    """Physical column order."""
    return [c.name for c in table.columns]

# ------------------------------------------------------------------
# Infer SQL type & add column (from v1, dialect-aware)
# ------------------------------------------------------------------
def infer_sql_type(series: pd.Series) -> TypeEngine:
    """Infer SQLAlchemy type from Series."""
    from sqlalchemy import (
        BigInteger, Float as SqlFloat, Boolean, DateTime, Text, String
    )
    if pd.api.types.is_integer_dtype(series):
        return BigInteger()
    if pd.api.types.is_float_dtype(series):
        return SqlFloat()
    if pd.api.types.is_bool_dtype(series):
        return Boolean()
    if pd.api.types.is_datetime64_any_dtype(series):
        return DateTime()
    if series.dtype == 'object' or pd.api.types.is_string_dtype(series):
        max_len = series.astype(str).str.len().max()
        if pd.isna(max_len) or max_len <= 255:
            return String(255)
        return Text()
    return Text()  # fallback

def add_column_to_table(
    engine: Engine,
    table: str,
    column_name: str,
    sql_type: TypeEngine,
    schema: Optional[str] = None
) -> None:
    """
    Add column to table (dialect-safe & idempotent).
    Checks if column exists (case-insensitive) before adding.
    """
    # 1. Check if exists (Case-Insensitive Normalization)
    insp = inspect(engine)
    # Handle schema defaults for inspection
    # Note: inspect.get_columns handles some case sensitivity, but we do manual lower check to be sure
    existing_cols = []
    try:
        existing_cols = [c['name'].lower() for c in insp.get_columns(table, schema=schema)]
    except Exception:
        # Fallback if table doesn't exist or inspection fails (though caller usually ensured table exists)
        pass
    
    if column_name.lower() in existing_cols:
        return # Already exists, skip

    table_ref = f"{schema}.{table}" if schema else table
    dialect = engine.dialect.name.lower()
    
    # 2. Compile type
    type_obj = sql_type
    type_str = str(type_obj.compile(dialect=engine.dialect))
    
    # 3. Dialect-specific ALTER
    # We rely on the engine to handle casing of 'table' and 'column_name' via standard SQL 
    # unless we force quoting. For now, we use raw strings which mimic standard SQL behavior (case insensitive often).
    if dialect in ('oracle', 'mysql', 'mariadb'):
        query = f"ALTER TABLE {table_ref} ADD {column_name} {type_str}"
    elif dialect == 'mssql':
        query = f"ALTER TABLE {table_ref} ADD {column_name} {type_str}"
    elif dialect == 'sqlite':
        query = f"ALTER TABLE {table_ref} ADD COLUMN {column_name} {type_str}"
    else:
        query = f"ALTER TABLE {table_ref} ADD COLUMN {column_name} {type_str}"
    
    with engine.begin() as conn:
        conn.execute(text(query))

# ------------------------------------------------------------------
# High-level DF aligner (merged core logic + report)
# ------------------------------------------------------------------
def align_dataframe_to_schema(
    df: pd.DataFrame,
    engine: Engine,
    table: str,
    schema: Optional[str] = None,
    threshold: float = 10.0,
    fix_outliers: bool = False,
    auto_alter: bool = False,
    outlier_method: str = 'iqr',
    iqr_scale: float = 1.5,
    z_threshold: float = 3.0
) -> Tuple[pd.DataFrame, Dict[str, any]]:
    """
    Align DF to schema, optional auto-alter & outlier fix.
    Returns (aligned_df, report) for observability.
    """
    if schema is None:
        schema = detect_table_schema(engine, table)
    
    insp = inspect(engine)
    if not insp.has_table(table, schema=schema):
        # Table doesn't exist, so we can't align or evolve. 
        # Return original DF (assumes caller will handle creation via to_sql).
        return df, {'status': 'new_table_skipped_alignment'}

    table_obj = _reflect_table(engine, table, schema)
    tab_cols = {c.name.lower(): c for c in table_obj.columns}
    
    df_cols_lower = {c.lower(): c for c in df.columns}
    
    report = {
        'columns': {},
        'missing_columns': [],
        'extra_columns': [],
        'altered_columns': [],
        'columns_failed': [],
        'outliers_removed': {},
        'nulls_introduced_pct': {}
    }

    aligned: Dict[str, pd.Series] = {}
    
    # Process expected columns
    for lower_name, col in tab_cols.items():
        if lower_name in df_cols_lower:
            orig_col = df_cols_lower[lower_name]
            aligned_series, col_report = align_column(
                df[orig_col],
                col.type,
                threshold=threshold,
                fix_outliers=fix_outliers,
                outlier_method=outlier_method,
                iqr_scale=iqr_scale,
                z_threshold=z_threshold
            )
            aligned[col.name] = aligned_series
            report['columns'][col.name] = col_report
            
            # Track for summary
            introduced_pct = col_report['final_null_pct'] - col_report['original_null_pct']
            report['nulls_introduced_pct'][col.name] = introduced_pct
            if introduced_pct > threshold:
                report['columns_failed'].append(col.name)
            if col_report['outliers_corrected'] > 0:
                report['outliers_removed'][col.name] = col_report['outliers_corrected']
        else:
            report['missing_columns'].append(col.name)
            canonical = normalize_sql_type(col.type)
            dtype = _nullable_dtype(canonical)
            aligned[col.name] = pd.Series(pd.NA, index=df.index, dtype=dtype)

    # Handle extra columns
    tab_lower_names = set(tab_cols.keys())
    for lower_name, orig_col in df_cols_lower.items():
        if lower_name not in tab_lower_names:
            report['extra_columns'].append(orig_col)
            
            if auto_alter and not df[orig_col].isna().all():
                try:
                    sql_type = infer_sql_type(df[orig_col])
                    add_column_to_table(engine, table, lower_name, sql_type, schema)
                    report['altered_columns'].append(lower_name)
                    
                    # Reset reflection (Project Requirement: ensure inspect is reset after adding column)
                    table_obj = _reflect_table(engine, table, schema)
                    new_col = table_obj.columns[lower_name]
                    aligned_series, col_report = align_column(
                        df[orig_col],
                        new_col.type,
                        threshold=threshold,
                        fix_outliers=fix_outliers,
                        outlier_method=outlier_method,
                        iqr_scale=iqr_scale,
                        z_threshold=z_threshold
                    )
                    aligned[new_col.name] = aligned_series
                    report['columns'][new_col.name] = col_report
                    
                    # Track summary
                    introduced_pct = col_report['final_null_pct'] - col_report['original_null_pct']
                    report['nulls_introduced_pct'][new_col.name] = introduced_pct
                    if introduced_pct > threshold:
                        report['columns_failed'].append(new_col.name)
                    if col_report['outliers_corrected'] > 0:
                        report['outliers_removed'][new_col.name] = col_report['outliers_corrected']
                    
                except Exception as e:
                    report['columns'].setdefault(orig_col, {})['error'] = str(e)

    # Final DF with correct order
    ordered_cols = _table_column_order(table_obj)
    final_df = pd.DataFrame({c: aligned.get(c, pd.Series(pd.NA, index=df.index)) for c in ordered_cols})
    
    return final_df, report

# ------------------------------------------------------------------
# Save helper (from v1)
# ------------------------------------------------------------------
def generate_schema(
    df: pd.DataFrame, 
    dialect: str = 'postgresql'
) -> Dict[str, TypeEngine]:
    """
    Generate SQLAlchemy schema (dtype dict) from DataFrame.
    
    Args:
        df: Input DataFrame
        dialect: Target definition for types (hints optimizer)
        
    Returns:
        Dictionary of {column_name: SQLAlchemyType}
    """
    schema = {}
    for col in df.columns:
        schema[col] = infer_sql_type(df[col])
    return schema

def save_aligned_dataframe(
    df: pd.DataFrame,
    engine: Engine,
    table: str,
    schema: Optional[str] = None,
    if_exists: str = 'append'
) -> None:
    """Save aligned DF (extract dtypes dynamically)."""
    table_obj = _reflect_table(engine, table, schema)
    sql_dtypes = {c.name.lower(): c.type for c in table_obj.columns}
    
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        dtype=sql_dtypes,
        method='multi'  # faster
    )

# ------------------------------------------------------------------
# Quick demo / test (merged example)
# ------------------------------------------------------------------
if __name__ == '__main__':
    from sqlalchemy import create_engine
    import json
    
    engine = create_engine('sqlite:///:memory:')
    
    # Create sample table
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE sample_table (
                id INTEGER,
                name TEXT,
                value REAL,
                created_at TIMESTAMP,
                active BOOLEAN
            )
        """))
    
    # Sample DF with mismatches
    df = pd.DataFrame({
        'ID': [1, 2, 3, 4, 5],
        'NAME': ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve'],
        'value': [10.5, 20.3, 1000.0, 30.7, 40.2],  # outlier
        'created_at': ['2023-01-01', '2023-01-02', 'invalid', '2023-01-04', '2023-01-05'],
        'active': [True, False, True, False, True],
        'extra_column': ['A', 'B', 'C', 'D', 'E']
    })
    
    aligned_df, report = align_dataframe_to_schema(
        df=df,
        engine=engine,
        table='sample_table',
        fix_outliers=True,
        auto_alter=True,
        outlier_method='iqr'
    )
    
    print("Aligned DataFrame:")
    print(aligned_df)
    
    print("\nReport:")
    print(json.dumps(report, indent=2, default=str))
    
    # Save example
    save_aligned_dataframe(aligned_df, engine, 'sample_table')
    
    # Verify
    saved_df = pd.read_sql_table('sample_table', engine)
    print("\nSaved Data:")
    print(saved_df)