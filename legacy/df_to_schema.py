"""
Direct pandas approach to generate SQL schema from DataFrame
Provides a simple way to generate CREATE TABLE statements without complex dependencies
"""
import pandas as pd
from typing import Optional, Dict, Any
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql, mysql, oracle, sqlite, mssql
from sqlalchemy.sql.ddl import CreateTable
from sqlalchemy import MetaData, Table, Column, Integer, BigInteger, String, Float, Boolean, DateTime, Date, Text


def df_to_sql_schema(
    df: pd.DataFrame, 
    table_name: str, 
    engine: Optional[create_engine] = None, 
    dialect: str = 'postgresql',
    varchar_length: int = 255,
    include_nullable: bool = True
) -> str:
    """
    Generate SQL CREATE TABLE statement from a pandas DataFrame.
    
    Args:
        df: The pandas DataFrame to generate schema for
        table_name: Name of the table to create
        engine: Optional SQLAlchemy engine to detect dialect automatically
        dialect: Database dialect ('postgresql', 'mysql', 'oracle', 'sqlite', 'mssql')
        varchar_length: Default length for VARCHAR columns
        include_nullable: Whether to include NULL/NOT NULL constraints
    
    Returns:
        SQL CREATE TABLE statement as string
    """
    if df.empty:
        raise ValueError("DataFrame is empty, cannot generate schema")
    
    if engine is not None:
        dialect = engine.dialect.name
    
    # Map pandas dtypes to SQLAlchemy types based on dialect
    dtype_mapping = _get_dtype_mapping(dialect, varchar_length)
    
    columns = []
    for col_name, dtype in df.dtypes.items():
        # Get the appropriate SQLAlchemy type
        dtype_str = str(dtype).lower()
        
        # Special handling for object types
        if dtype_str == 'object':
            # If it's an object column, check if it contains dates/datetimes
            non_null_series = df[col_name].dropna()
            if len(non_null_series) > 0:
                sample_val = non_null_series.iloc[0]
                if pd.api.types.is_datetime64_any_dtype(non_null_series):
                    sql_type = DateTime
                elif isinstance(sample_val, str) and _looks_like_datetime(sample_val):
                    sql_type = DateTime
                else:
                    # Default to String with specified length
                    sql_type = String(varchar_length)
            else:
                sql_type = String(varchar_length)
        else:
            # Map other dtypes directly
            sql_type = dtype_mapping.get(dtype_str, String(varchar_length))
        
        # Determine nullability
        nullable = None
        if include_nullable:
            nullable = df[col_name].isna().any()
        
        # Create the column
        if nullable is not None:
            col = Column(col_name, sql_type, nullable=nullable)
        else:
            col = Column(col_name, sql_type)
        
        columns.append(col)
    
    # Create table and metadata
    metadata = MetaData()
    table = Table(table_name, metadata, *columns)
    
    # Generate SQL based on the dialect
    try:
        if dialect == 'postgresql':
            compiled = CreateTable(table).compile(dialect=postgresql.dialect())
        elif dialect == 'mysql':
            compiled = CreateTable(table).compile(dialect=mysql.dialect())
        elif dialect == 'oracle':
            compiled = CreateTable(table).compile(dialect=oracle.dialect())
        elif dialect == 'mssql':
            compiled = CreateTable(table).compile(dialect=mssql.dialect())
        else:  # sqlite as default
            compiled = CreateTable(table).compile(dialect=sqlite.dialect())
        
        return str(compiled)
    except Exception as e:
        # Fixed: Don't let compilation errors pass silently
        print(f"WARNING: Error compiling SQL for dialect {dialect}: {e}")
        # Fall back to manual SQL generation
        return _manual_sql_generation(df, table_name, dtype_mapping, varchar_length, include_nullable)


def _get_dtype_mapping(dialect: str, varchar_length: int) -> Dict[str, Any]:
    """Get the appropriate dtype mapping for the specified dialect."""
    # Default mapping that works for most databases
    mapping = {
        'int64': BigInteger,
        'int32': Integer,
        'int16': Integer,
        'int8': Integer,
        'float64': Float,
        'float32': Float,
        'bool': Boolean,
        'boolean': Boolean,  # pandas nullable boolean
        'datetime64[ns]': DateTime,
        'datetime64[us]': DateTime,
        'datetime64[ms]': DateTime,
        'datetime64[s]': DateTime,
        'date': Date,
        'timedelta64[ns]': String(varchar_length),  # Most databases don't have timedelta
    }
    
    # Adjust for specific dialect requirements
    if dialect == 'oracle':
        # Oracle-specific adjustments
        mapping.update({
            'float64': 'BINARY_DOUBLE',
            'float32': 'BINARY_FLOAT',
            'object': String(varchar_length),
        })
    elif dialect == 'mysql':
        # MySQL-specific adjustments
        mapping.update({
            'bool': 'TINYINT(1)',
        })
    
    return mapping


def _looks_like_datetime(value: str) -> bool:
    """Check if a string value looks like a datetime."""
    import re
    
    # Common datetime patterns
    patterns = [
        r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # YYYY-MM-DDTHH:MM:SS
        r'\d{2}/\d{2}/\d{4}',                     # MM/DD/YYYY
        r'\d{4}-\d{2}-\d{2}',                     # YYYY-MM-DD
    ]
    
    return any(re.match(pattern, str(value)) for pattern in patterns)


def _manual_sql_generation(
    df: pd.DataFrame, 
    table_name: str, 
    dtype_mapping: Dict[str, Any], 
    varchar_length: int, 
    include_nullable: bool
) -> str:
    """Manually generate SQL as a fallback when SQLAlchemy compilation fails."""
    columns = []
    
    for col_name, dtype in df.dtypes.items():
        # Convert pandas dtype to SQL type string
        dtype_str = str(dtype).lower()
        
        if dtype_str == 'object':
            # For object columns, default to VARCHAR with specified length
            sql_type = f'VARCHAR({varchar_length})'
        elif 'int' in dtype_str:
            sql_type = 'INTEGER'
        elif 'float' in dtype_str:
            sql_type = 'REAL'
        elif 'bool' in dtype_str:
            sql_type = 'BOOLEAN'
        elif 'datetime' in dtype_str:
            sql_type = 'TIMESTAMP'
        elif 'date' in dtype_str:
            sql_type = 'DATE'
        else:
            sql_type = f'VARCHAR({varchar_length})'
        
        # Check nullability
        nullable_clause = ""
        if include_nullable:
            is_nullable = df[col_name].isna().any()
            nullable_clause = " NOT NULL" if not is_nullable else ""
        
        # Escape column name to handle special characters
        escaped_col_name = f'"{col_name}"'
        columns.append(f"    {escaped_col_name} {sql_type}{nullable_clause}")
    
    # Join all column definitions
    columns_def = ",\n".join(columns)
    
    # Create the full SQL statement
    sql_schema = f"CREATE TABLE {table_name} (\n{columns_def}\n);"
    
    return sql_schema


# Convenience functions for specific databases
def df_to_postgres_schema(df: pd.DataFrame, table_name: str, varchar_length: int = 255) -> str:
    """Generate PostgreSQL CREATE TABLE statement."""
    return df_to_sql_schema(df, table_name, dialect='postgresql', varchar_length=varchar_length)


def df_to_mysql_schema(df: pd.DataFrame, table_name: str, varchar_length: int = 255) -> str:
    """Generate MySQL CREATE TABLE statement."""
    return df_to_sql_schema(df, table_name, dialect='mysql', varchar_length=varchar_length)


def df_to_oracle_schema(df: pd.DataFrame, table_name: str, varchar_length: int = 255) -> str:
    """Generate Oracle CREATE TABLE statement."""
    return df_to_sql_schema(df, table_name, dialect='oracle', varchar_length=varchar_length)


def df_to_sqlite_schema(df: pd.DataFrame, table_name: str, varchar_length: int = 255) -> str:
    """Generate SQLite CREATE TABLE statement."""
    return df_to_sql_schema(df, table_name, dialect='sqlite', varchar_length=varchar_length)


def df_to_mssql_schema(df: pd.DataFrame, table_name: str, varchar_length: int = 255) -> str:
    """Generate MS SQL Server CREATE TABLE statement."""
    return df_to_sql_schema(df, table_name, dialect='mssql', varchar_length=varchar_length)


# Example usage
if __name__ == "__main__":
    # Example DataFrame
    import numpy as np
    df_example = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'score': [95.5, 87.2, 92.1],
        'active': [True, False, True],
        'created_at': pd.date_range('2023-01-01', periods=3)
    })
    
    # Generate schema for PostgreSQL
    schema = df_to_postgres_schema(df_example, 'users')
    print("PostgreSQL Schema:")
    print(schema)
    
    # Generate schema for MySQL
    schema = df_to_mysql_schema(df_example, 'users')
    print("\nMySQL Schema:")
    print(schema)