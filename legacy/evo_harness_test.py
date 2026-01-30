"""
Enhanced CRUD Test Harness for CSV Validation

This module provides comprehensive test data generation and validation for
INSERT, UPSERT, and UPDATE operations following ETL library patterns.

Key Features:
- Automatic test data generation with smart mutation
- NULL-safe value mutation
- PK/constraint column preservation
- Built-in validation with database queries
- Full cycle testing (INSERT → UPSERT → UPDATE)

Usage:
    from evocsv.test_harness import EnhancedCrudTestHarness
    
    # Create 10-row test DataFrame
    df = pd.DataFrame({...})  # 10 rows
    
    # Initialize harness
    harness = EnhancedCrudTestHarness(
        df_src=df,
        pk_cols=['id'],
        constraint_cols=['username'],
        table_name='users'
    )
    
    # Get test data
    insert_df = harness.insert_df  # Rows 0-5
    upsert_df = harness.upsert_df  # Rows 3-7 (3-6 mutated)
    update_df = harness.update_df  # Rows 1,7,8 (mutated)
    
    # Get validation queries
    sql, params = harness.get_insert_check_query()
    
    # Run full test cycle
    result = harness.run_full_cycle_test(engine)
"""
import pandas as pd
import numpy as np
from typing import List, Tuple, Dict, Any, Optional
from dataclasses import dataclass
import sqlalchemy as sa
from sqlalchemy.engine import Engine

from .sql_generator import lazy_insert_sql, lazy_upsert_sql, lazy_update_sql, lazy_select_sql
from utils.data_profiler import profile_dataframe, get_pk

__all__ = ['EnhancedCrudTestHarness', 'ValidationResult', 'TestResult']


@dataclass
class ValidationResult:
    """Result of validation comparison."""
    passed: bool
    expected_rows: int
    actual_rows: int
    differences: List[Dict[str, Any]]
    message: str
    
    def __str__(self):
        status = "✓ PASSED" if self.passed else "✗ FAILED"
        return f"{status}: {self.message} (expected={self.expected_rows}, actual={self.actual_rows})"


@dataclass
class TestResult:
    """Result of test execution."""
    operation: str
    sql_queries: List[Any]  # List of SQLQuery objects
    execution_stats: Dict[str, Any]
    validation: ValidationResult
    
    def __str__(self):
        return f"TestResult({self.operation}): {self.validation}"


class EnhancedCrudTestHarness:
    """
    Enhanced test harness for deterministic testing of INSERT/UPSERT/UPDATE SQL generators.
    
    Features:
    - Prepares mutated test DataFrames with preserved NULLs
    - Generates safe, NULL-aware SELECT queries for verification
    - Provides validation with floating-point and datetime tolerance
    - Supports composite PKs and constraint keys
    - Execute and validate in single method calls
    """

    def __init__(self, df_src: pd.DataFrame, pk_cols=None, constraint_cols=None, table_name: str = "t"):
        """
        Initialize test harness.
        
        Args:
            df_src: Source DataFrame with exactly 10 rows (index 0-9)
            pk_cols: Primary key column name(s) - string or list. If None, inferred automatically.
            constraint_cols: Constraint column name(s) - string or list  
            table_name: Table name for SQL generation
        """
        if not isinstance(df_src, pd.DataFrame) or len(df_src) != 10:
            raise ValueError("df_src must be DataFrame with exactly 10 rows")

        if not isinstance(df_src.index, pd.RangeIndex) or not df_src.index.equals(pd.RangeIndex(10)):
            raise ValueError("df_src index must be RangeIndex(0..9)")

        self.df = df_src.copy()
        
        # Infer PK if not provided
        if pk_cols is None:
            profile = profile_dataframe(self.df)
            _, _, pk_meta = get_pk(self.df, profile)
            pk_cols = pk_meta.get('components')
            if not pk_cols:
                raise ValueError("Could not infer primary key from DataFrame")
        
        self.pk = [pk_cols] if isinstance(pk_cols, str) else list(pk_cols)
        
        if constraint_cols is None:
            raise ValueError("constraint_cols must be provided")
            
        self.constraint = [constraint_cols] if isinstance(constraint_cols, str) else list(constraint_cols)
        self.table = table_name

        all_cols = set(self.df.columns)
        for group, name in [(self.pk, "pk"), (self.constraint, "constraint")]:
            if not group:
                raise ValueError(f"{name} columns must be non-empty")
            missing = set(group) - all_cols
            if missing:
                raise ValueError(f"Missing {name} columns: {missing}")

        if set(self.pk) & set(self.constraint):
            raise ValueError("pk and constraint columns must be disjoint")

        self.mutable = [c for c in self.df.columns if c not in set(self.pk + self.constraint)]
        if not self.mutable:
            raise ValueError("No mutable columns found")

        if self.df[self.pk].duplicated().any():
            raise ValueError("Source DataFrame contains duplicate PK values")

        if self.df[self.constraint].duplicated().any():
            raise ValueError("Source DataFrame contains duplicate constraint values")

    # =========================================================================
    # Deterministic mutation (NULL-safe)
    # =========================================================================

    @staticmethod
    def _mutate_value(v):
        """Mutate a single value preserving NULLs."""
        if pd.isna(v):
            return v
        if isinstance(v, bool):
            return not v
        if isinstance(v, (int, np.integer)):
            return v + 1
        if isinstance(v, (float, np.floating)):
            return v + 1.0
        if isinstance(v, (pd.Timestamp, pd.Period)):
            return v + pd.Timedelta(days=1)
        if isinstance(v, str):
            return f"{v}⟐m"  # recognizable mutation marker
        return v

    def _mutate_mutable_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Mutate only mutable columns, preserving PK and constraints."""
        df = df.copy()
        for col in self.mutable:
            df[col] = df[col].map(self._mutate_value, na_action='ignore')
        return df

    # =========================================================================
    # Test case DataFrames
    # =========================================================================

    @property
    def insert_df(self) -> pd.DataFrame:
        """
        INSERT test DataFrame: rows 0-5 (6 rows, unmodified).
        
        These rows will be inserted fresh into an empty table.
        """
        return self.df.iloc[0:6].copy()

    @property
    def upsert_df(self) -> pd.DataFrame:
        """
        UPSERT test DataFrame: rows 3-7 (5 rows).
        
        - Rows 3-6: Exist in INSERT set, mutable columns mutated (4 rows)
        - Row 7: New row, not mutated (1 row)
        
        Expected behavior:
        - Rows 3-6 should UPDATE existing records
        - Row 7 should INSERT as new record
        """
        slice_df = self.df.iloc[3:8]
        existing = self._mutate_mutable_columns(slice_df.iloc[0:4])  # 3,4,5,6
        new = slice_df.iloc[4:5].copy()                              # 7
        return pd.concat([existing, new], ignore_index=True)

    @property
    def update_df(self) -> pd.DataFrame:
        """
        UPDATE test DataFrame: rows 1, 7, 8 (3 rows, all mutated).
        
        - Row 1: Exists in INSERT set (should update)
        - Row 7: Exists after UPSERT (should update)
        - Row 8: Does NOT exist (UPDATE should have no effect)
        
        Expected behavior:
        - Rows 1 and 7 should be updated
        - Row 8 update should silently fail (no error, 0 rows affected)
        """
        return self._mutate_mutable_columns(self.df.iloc[[1, 7, 8]])

    # =========================================================================
    # Verification SELECT queries
    # =========================================================================

    def _build_select_query(self, indices: List[int], key_columns: List[str], 
                           null_safe: bool = False, param_prefix: str = "p") -> Tuple[str, Dict[str, Any]]:
        """Build SELECT query to retrieve specific rows by key columns."""
        clauses = []
        params = {}

        for i, idx in enumerate(indices):
            conds = []
            for col in key_columns:
                val = self.df.at[idx, col]
                param_name = f"{param_prefix}_{col}_{i}"

                if null_safe and pd.isna(val):
                    conds.append(f"{col} IS NULL")
                else:
                    conds.append(f"{col} = :{param_name}")
                    params[param_name] = val

            clauses.append(" AND ".join(conds))

        where = " OR ".join(f"({c})" for c in clauses) if clauses else "FALSE"
        order = ", ".join(key_columns)
        sql = f"SELECT * FROM {self.table} WHERE {where} ORDER BY {order}"

        return sql, params

    def get_insert_check_query(self) -> Tuple[str, Dict[str, Any]]:
        """Get SELECT query to verify INSERT operation (rows 0-5)."""
        return self._build_select_query(list(range(0, 6)), self.pk)

    def get_upsert_check_query(self) -> Tuple[str, Dict[str, Any]]:
        """Get SELECT query to verify UPSERT operation (rows 3-7)."""
        return self._build_select_query(list(range(3, 8)), self.pk)

    def get_update_check_query(self) -> Tuple[str, Dict[str, Any]]:
        """Get SELECT query to verify UPDATE operation (rows 1, 7, 8)."""
        return self._build_select_query([1, 7, 8], self.constraint, null_safe=True, param_prefix="upd")

    # =========================================================================
    # Validation with tolerance
    # =========================================================================

    def assert_same_pk_set(self, db_df: pd.DataFrame, expected_df: pd.DataFrame):
        """Assert that PK sets match."""
        db_keys = set(tuple(row) for row in db_df[self.pk].to_numpy())
        exp_keys = set(tuple(row) for row in expected_df[self.pk].to_numpy())
        if db_keys != exp_keys:
            raise AssertionError(
                f"PK set mismatch\n"
                f"Expected: {sorted(exp_keys)}\n"
                f"Actual:   {sorted(db_keys)}"
            )

    def assert_frames_close(
        self,
        actual: pd.DataFrame,
        expected: pd.DataFrame,
        rtol: float = 1e-5,
        atol: float = 1e-8,
        dt_tolerance: pd.Timedelta = pd.Timedelta("5s"),
        msg: str = ""
    ):
        """Assert DataFrames are close with tolerance."""
        a = actual.sort_values(self.pk, ignore_index=True)
        b = expected.sort_values(self.pk, ignore_index=True)

        if list(a.columns) != list(b.columns):
            raise AssertionError(f"Column mismatch {msg}")
        if len(a) != len(b):
            raise AssertionError(f"Row count mismatch {msg}: exp {len(b)}, got {len(a)}")

        for col in a.columns:
            s1 = a[col].to_numpy()
            s2 = b[col].to_numpy()

            if pd.api.types.is_datetime64_any_dtype(a[col]):
                diff = np.abs((s1 - s2).astype('timedelta64[ns]'))
                if not np.all(diff <= dt_tolerance.to_numpy()):
                    raise AssertionError(
                        f"Datetime difference too large in '{col}' {msg} "
                        f"(max allowed: {dt_tolerance})"
                    )
            elif np.issubdtype(s1.dtype, np.floating) or np.issubdtype(s2.dtype, np.floating):
                if not np.allclose(s1, s2, rtol=rtol, atol=atol, equal_nan=True):
                    raise AssertionError(
                        f"Floating point values differ in '{col}' {msg} "
                        f"(rtol={rtol}, atol={atol})"
                    )
            else:
                if not np.array_equal(s1, s2, equal_nan=True):
                    raise AssertionError(f"Values differ in column '{col}' {msg}")

    # =========================================================================
    # High-level validation methods
    # =========================================================================

    def validate_after_insert(self, db_df: pd.DataFrame) -> ValidationResult:
        """Validate database state after INSERT operation."""
        try:
            self.assert_same_pk_set(db_df, self.insert_df)
            self.assert_frames_close(db_df, self.insert_df, msg="(after INSERT)")
            return ValidationResult(
                passed=True,
                expected_rows=len(self.insert_df),
                actual_rows=len(db_df),
                differences=[],
                message="INSERT validation passed"
            )
        except AssertionError as e:
            return ValidationResult(
                passed=False,
                expected_rows=len(self.insert_df),
                actual_rows=len(db_df),
                differences=[{"error": str(e)}],
                message=f"INSERT validation failed: {e}"
            )

    def validate_after_upsert(self, db_df: pd.DataFrame) -> ValidationResult:
        """Validate database state after UPSERT operation."""
        try:
            base = self.insert_df.set_index(self.pk)
            changes = self.upsert_df.set_index(self.pk)

            base.update(changes[self.mutable])
            new_rows = changes[~changes.index.isin(base.index)]
            expected = pd.concat([base.reset_index(), new_rows.reset_index()], ignore_index=True)

            self.assert_frames_close(db_df, expected, msg="(after UPSERT)")
            return ValidationResult(
                passed=True,
                expected_rows=len(expected),
                actual_rows=len(db_df),
                differences=[],
                message="UPSERT validation passed"
            )
        except AssertionError as e:
            return ValidationResult(
                passed=False,
                expected_rows=len(self.insert_df) + 1,  # Approximate
                actual_rows=len(db_df),
                differences=[{"error": str(e)}],
                message=f"UPSERT validation failed: {e}"
            )

    def validate_after_full_cycle(self, db_df: pd.DataFrame) -> ValidationResult:
        """Validate database state after full INSERT → UPSERT → UPDATE cycle."""
        try:
            # After INSERT
            state = self.insert_df.set_index(self.pk)
            
            # Apply UPSERT
            up = self.upsert_df.set_index(self.pk)
            state.update(up[self.mutable])
            new_upsert = up[~up.index.isin(state.index)]
            state = pd.concat([state.reset_index(), new_upsert.reset_index()], ignore_index=True)

            # Apply UPDATE
            state = state.set_index(self.constraint)
            upd = self.update_df.set_index(self.constraint)
            state.update(upd[self.mutable])
            final_expected = state.reset_index()

            self.assert_frames_close(db_df, final_expected, msg="(after full cycle INSERT→UPSERT→UPDATE)")
            return ValidationResult(
                passed=True,
                expected_rows=len(final_expected),
                actual_rows=len(db_df),
                differences=[],
                message="Full cycle validation passed"
            )
        except AssertionError as e:
            return ValidationResult(
                passed=False,
                expected_rows=7,  # Approximate
                actual_rows=len(db_df),
                differences=[{"error": str(e)}],
                message=f"Full cycle validation failed: {e}"
            )

    # =========================================================================
    # Execute and validate in one step
    # =========================================================================

    def run_insert_test(self, engine: Engine, dialect: str = "sqlite") -> TestResult:
        """Execute INSERT operation and validate results."""
        # Generate SQL
        queries = lazy_insert_sql(self.insert_df, self.table, dialect)
        
        # Execute
        with engine.begin() as conn:
            for q in queries:
                conn.execute(sa.text(q.sql), q.params)
        
        # Validate
        sql, params = self.get_insert_check_query()
        db_df = pd.read_sql(sa.text(sql), engine, params=params)
        validation = self.validate_after_insert(db_df)
        
        return TestResult(
            operation="INSERT",
            sql_queries=queries,
            execution_stats={"rows_inserted": len(queries)},
            validation=validation
        )

    def run_upsert_test(self, engine: Engine, dialect: str = "sqlite") -> TestResult:
        """Execute UPSERT operation and validate results."""
        # Generate SQL
        queries = lazy_upsert_sql(self.upsert_df, self.table, self.pk, dialect)
        
        # Execute
        with engine.begin() as conn:
            for q in queries:
                conn.execute(sa.text(q.sql), q.params)
        
        # Validate
        sql, params = self.get_upsert_check_query()
        db_df = pd.read_sql(sa.text(sql), engine, params=params)
        validation = self.validate_after_upsert(db_df)
        
        return TestResult(
            operation="UPSERT",
            sql_queries=queries,
            execution_stats={"rows_upserted": len(queries)},
            validation=validation
        )

    def run_update_test(self, engine: Engine, dialect: str = "sqlite") -> TestResult:
        """Execute UPDATE operation and validate results."""
        # Generate SQL for each row
        queries = []
        for _, row in self.update_df.iterrows():
            where_conditions = []
            for col in self.constraint:
                val = row[col]
                op = 'IS' if pd.isna(val) else '='
                where_conditions.append((col, op, None if pd.isna(val) else val))
            q = lazy_update_sql([row.to_dict()], self.table, where_conditions, dialect)
            queries.append(q)
        
        # Execute
        with engine.begin() as conn:
            for q in queries:
                conn.execute(sa.text(q.sql), q.params)
        
        # Validate (just check row count, UPDATE validation is complex)
        sql, params = self.get_update_check_query()
        db_df = pd.read_sql(sa.text(sql), engine, params=params)
        
        validation = ValidationResult(
            passed=len(db_df) == 2,
            expected_rows=2,  # Only rows 1 and 7 should exist
            actual_rows=len(db_df),
            differences=[],
            message=f"UPDATE executed, {len(db_df)} rows found" if len(db_df) == 2 else f"UPDATE validation failed: expected 2 rows, found {len(db_df)}"
        )
        
        return TestResult(
            operation="UPDATE",
            sql_queries=queries,
            execution_stats={"rows_updated": len(queries)},
            validation=validation
        )

    def run_full_cycle_test(self, engine: Engine, dialect: str = "sqlite") -> TestResult:
        """Execute full INSERT → UPSERT → UPDATE cycle and validate."""
        # Run INSERT
        insert_result = self.run_insert_test(engine, dialect)
        if not insert_result.validation.passed:
            return insert_result
        
        # Run UPSERT
        upsert_result = self.run_upsert_test(engine, dialect)
        if not upsert_result.validation.passed:
            return upsert_result
        
        # Run UPDATE
        update_result = self.run_update_test(engine, dialect)
        
        # Final validation
        sql = f"SELECT * FROM {self.table} ORDER BY {', '.join(self.pk)}"
        db_df = pd.read_sql(sql, engine)
        validation = self.validate_after_full_cycle(db_df)
        
        return TestResult(
            operation="FULL_CYCLE",
            sql_queries=insert_result.sql_queries + upsert_result.sql_queries + update_result.sql_queries,
            execution_stats={
                "insert_rows": len(insert_result.sql_queries),
                "upsert_rows": len(upsert_result.sql_queries),
                "update_rows": len(update_result.sql_queries),
                "total_operations": len(insert_result.sql_queries) + len(upsert_result.sql_queries) + len(update_result.sql_queries)
            },
            validation=validation
        )
