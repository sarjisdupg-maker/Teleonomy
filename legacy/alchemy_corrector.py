import pandas as pd
import numpy as np
from sqlalchemy import Integer, Float, String, DateTime, Boolean, MetaData, Table, Column, text
from sqlalchemy.types import TypeDecorator, Numeric, Date, Time, Text, VARCHAR, CHAR


def normalize_sql_type(sql_type) -> str:
    """Map SQLAlchemy type to base type string."""
    if isinstance(sql_type, (Integer, TypeDecorator)) and isinstance(getattr(sql_type, 'impl', sql_type), Integer):
        return 'int'
    if isinstance(sql_type, (Float, Numeric, TypeDecorator)) and isinstance(getattr(sql_type, 'impl', sql_type), (Float, Numeric)):
        return 'float'
    if isinstance(sql_type, (String, Text, VARCHAR, CHAR, TypeDecorator)) and isinstance(getattr(sql_type, 'impl', sql_type), (String, Text, VARCHAR, CHAR)):
        return 'str'
    if isinstance(sql_type, (DateTime, Date, Time, TypeDecorator)) and isinstance(getattr(sql_type, 'impl', sql_type), (DateTime, Date, Time)):
        return 'datetime'
    if isinstance(sql_type, (Boolean, TypeDecorator)) and isinstance(getattr(sql_type, 'impl', sql_type), Boolean):
        return 'bool'
    return 'str'  # default fallback


def coerce_series(series: pd.Series, target_type: str) -> pd.Series:
    """Strictly coerce series to target type with nullable dtypes."""
    if target_type == 'int':
        return pd.to_numeric(series, errors='raise').astype('Int64')
    if target_type == 'float':
        return pd.to_numeric(series, errors='raise').astype('Float64')
    if target_type == 'str':
        return series.astype('string')
    if target_type == 'datetime':
        return pd.to_datetime(series, errors='raise')
    if target_type == 'bool':
        if series.dtype == 'boolean':
            return series
        return series.map(lambda x: pd.NA if pd.isna(x) else bool(x)).astype('boolean')
    raise ValueError(f"Unknown target type: {target_type}")


def cast_qualify(series_before: pd.Series, series_after: pd.Series, threshold: float = 10.0) -> bool:
    """Return True if null percentage increased by more than threshold (failure signal)."""
    null_pct_before = (series_before.isna().sum() / len(series_before)) * 100 if len(series_before) > 0 else 0
    null_pct_after = (series_after.isna().sum() / len(series_after)) * 100 if len(series_after) > 0 else 0
    return (null_pct_after - null_pct_before) > threshold


def detect_outliers(series: pd.Series, method: str = 'iqr') -> pd.Series:
    """Detect outliers in numeric series; return all True for non-numeric."""
    if series.dtype not in ['Int64', 'Float64', 'int64', 'float64']:
        return pd.Series([True] * len(series), index=series.index)
    
    numeric = pd.to_numeric(series, errors='coerce').astype('float64')
    valid = numeric[numeric.notna()]
    
    if len(valid) < 4:
        return pd.Series([True] * len(series), index=series.index)
    
    if method == 'iqr':
        q1 = valid.quantile(0.25)
        q3 = valid.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            return pd.Series([True] * len(series), index=series.index)
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        mask = (numeric >= lower) & (numeric <= upper) | numeric.isna()
    elif method == 'zscore':
        mean = valid.mean()
        std = valid.std()
        if std == 0:
            return pd.Series([True] * len(series), index=series.index)
        z = np.abs((numeric - mean) / std)
        mask = (z <= 3) | numeric.isna()
    else:
        raise ValueError(f"Unknown method: {method}")
    
    return mask


def correct_outliers(series: pd.Series, outlier_mask: pd.Series) -> pd.Series:
    """Replace outliers (where mask is False) with pd.NA."""
    corrected = series.copy()
    corrected[~outlier_mask] = pd.NA
    return corrected


def align_column(series: pd.Series, target_type: str, outlier_correction: bool = False, 
                 outlier_method: str = 'iqr', cast_threshold: float = 10.0) -> pd.Series:
    """Align series to target type with optional outlier correction on cast failure."""
    coerced = coerce_series(series, target_type)
    
    if cast_qualify(series, coerced, cast_threshold) and outlier_correction and target_type in ['int', 'float']:
        outlier_mask = detect_outliers(series, outlier_method)
        corrected = correct_outliers(series, outlier_mask)
        coerced = coerce_series(corrected, target_type)
    
    return coerced


def align_dataframe(df: pd.DataFrame, engine, table_name: str, schema: str = None,
                    auto_alter: bool = False, outlier_correction: bool = False,
                    outlier_method: str = 'iqr', cast_threshold: float = 10.0) -> pd.DataFrame:
    """Align DataFrame to table schema with type safety and optional schema evolution."""
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine, schema=schema)
    
    col_mapping = {col.name.lower(): col.name for col in table.columns}
    df_cols_lower = {col.lower(): col for col in df.columns}
    
    result_data = {}
    
    # Process existing columns
    for col_lower, col_name in col_mapping.items():
        sql_col = table.columns[col_name]
        target_type = normalize_sql_type(sql_col.type)
        
        if col_lower in df_cols_lower:
            df_col_name = df_cols_lower[col_lower]
            result_data[col_name] = align_column(
                df[df_col_name], target_type, outlier_correction, outlier_method, cast_threshold
            )
        else:
            if target_type == 'datetime':
                result_data[col_name] = pd.Series([pd.NaT] * len(df), dtype='datetime64[ns]')
            elif target_type == 'int':
                result_data[col_name] = pd.Series([pd.NA] * len(df), dtype='Int64')
            elif target_type == 'float':
                result_data[col_name] = pd.Series([pd.NA] * len(df), dtype='Float64')
            elif target_type == 'bool':
                result_data[col_name] = pd.Series([pd.NA] * len(df), dtype='boolean')
            else:
                result_data[col_name] = pd.Series([pd.NA] * len(df), dtype='string')
    
    # Handle extra columns
    existing_lower = set(col_mapping.keys())
    extra_cols = [col for col in df.columns if col.lower() not in existing_lower]
    
    if extra_cols and not auto_alter:
        raise ValueError(f"Extra columns not in table: {extra_cols}")
    
    if extra_cols and auto_alter:
        with engine.begin() as conn:
            for col in extra_cols:
                if df[col].notna().any():
                    if pd.api.types.is_integer_dtype(df[col]):
                        sql_type = 'INTEGER'
                    elif pd.api.types.is_float_dtype(df[col]):
                        sql_type = 'FLOAT'
                    elif pd.api.types.is_bool_dtype(df[col]):
                        sql_type = 'BOOLEAN'
                    elif pd.api.types.is_datetime64_any_dtype(df[col]):
                        sql_type = 'TIMESTAMP'
                    else:
                        max_len = df[col].astype(str).str.len().max()
                        sql_type = f'VARCHAR({min(max_len * 2, 4000)})'
                        if engine.dialect.name == 'oracle':
                            sql_type = f'VARCHAR2({min(max_len * 2, 4000)})'
                        elif engine.dialect.name == 'sqlite':
                            sql_type = 'TEXT'
                    
                    alter_sql = f"ALTER TABLE {table_name} ADD {col} {sql_type}"
                    conn.execute(text(alter_sql))
        
        # Re-reflect after ALTER
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine, schema=schema)
        
        for col in extra_cols:
            if col in [c.name for c in table.columns]:
                sql_col = table.columns[col]
                target_type = normalize_sql_type(sql_col.type)
                result_data[col] = align_column(
                    df[col], target_type, outlier_correction, outlier_method, cast_threshold
                )
    
    # Return in table column order
    ordered_cols = [col.name for col in table.columns if col.name in result_data]
    return pd.DataFrame(result_data)[ordered_cols]


if __name__ == "__main__":
    from sqlalchemy import create_engine
    
    # Create test database
    engine = create_engine("sqlite:///:memory:")
    engine.execute(text("""
        CREATE TABLE test_table (
            id INTEGER,
            value FLOAT,
            name TEXT,
            created TIMESTAMP,
            active BOOLEAN
        )
    """))
    
    # Test data with outliers and type mismatches
    df = pd.DataFrame({
        'id': ['1', '2', '999', '4', '5'],
        'value': [1.5, 2.3, 100.0, 3.1, pd.NA],
        'name': ['Alice', 'Bob', None, 'Charlie', 'Dave'],
        'created': ['2023-01-01', '2023-01-02', 'invalid', '2023-01-04', None],
        'active': [True, False, True, None, False],
        'extra_col': ['should', 'be', 'added', 'if', 'auto_alter']
    })
    
    # Test without auto_alter (will raise on extra column)
    try:
        aligned = align_dataframe(df, engine, 'test_table', outlier_correction=False)
    except ValueError as e:
        print(f"Expected error: {e}")
    
    # Test with auto_alter and outlier correction
    aligned = align_dataframe(
        df.drop('extra_col', axis=1), 
        engine, 
        'test_table',
        outlier_correction=True,
        outlier_method='iqr'
    )
    
    print("\nAligned DataFrame:")
    print(aligned)
    print("\nDtypes:")
    print(aligned.dtypes)
