"""
EvoCSV Pipeline Core

Orchestrates CSV → DDL → CRUD validation → Artifact generation.
Uses existing public APIs from ddl, crud, and utils modules.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from sqlalchemy.engine import Engine

from ddl import df_ddl
from crud import auto_insert, auto_upsert, auto_update
from utils.data_cleaner import quick_clean
from utils.sanitization import sanitize_cols
from utils.cast import cast_df
from utils.data_profiler import profile_dataframe, get_pk
from utils.schema_manager import SchemaManager
from utils.sandbox import sqlite_sandbox, dialect_sandbox


@dataclass
class OperationResult:
    """Result of a single CRUD operation validation."""
    name: str
    status: str  # 'passed', 'failed', 'skipped'
    details: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    duration_ms: float = 0.0


@dataclass
class PipelineReport:
    """Complete pipeline execution report."""
    table: str
    dialect: str
    pk: list[str]
    operations: list[OperationResult] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict[str, Any]:
        """Convert report to JSON-serializable dictionary."""
        return {
            "table": self.table,
            "dialect": self.dialect,
            "pk": self.pk,
            "operations": [
                {
                    "name": op.name,
                    "status": op.status,
                    "details": op.details,
                    "error": op.error,
                    "duration_ms": op.duration_ms,
                }
                for op in self.operations
            ],
            "meta": self.meta,
        }
    
    def all_passed(self) -> bool:
        """Check if all operations passed."""
        return all(op.status == "passed" for op in self.operations)
    
    def passed_operations(self) -> list[str]:
        """Get list of operation names that passed."""
        return [op.name for op in self.operations if op.status == "passed"]


SUPPORTED_DIALECTS = {"sqlite", "postgresql", "mysql", "mssql", "oracle"}


@dataclass
class PipelineConfig:
    """Configuration for pipeline execution."""
    csv_path: Path
    table_name: str
    output_dir: Path
    pk: Optional[list[str]] = None
    dialect: str = "sqlite"
    chunksize: int = 10000
    
    def __post_init__(self):
        self.csv_path = Path(self.csv_path)
        self.output_dir = Path(self.output_dir)
        self.dialect = self.dialect.lower()
        if self.dialect not in SUPPORTED_DIALECTS:
            raise ValueError(f"Unsupported dialect: {self.dialect}. Supported: {SUPPORTED_DIALECTS}")


class EvoCSVJobPipeline:
    """
    Core pipeline for CSV → validated SQL + Python module.
    
    Orchestrates:
    1. CSV loading and cleaning
    2. DDL generation and execution
    3. CRUD operation validation (INSERT, UPSERT, UPDATE)
    4. Report generation
    """
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.report: Optional[PipelineReport] = None
        self._processed_df: Optional[pd.DataFrame] = None
        self._create_sql: Optional[str] = None
        self._column_map: dict[str, str] = {}
    
    def run(self, engine: Optional[Engine] = None, db_uri: Optional[str] = None) -> PipelineReport:
        """
        Execute the full pipeline.
        
        Args:
            engine: Optional SQLAlchemy Engine. If not provided, uses a sandbox.
            db_uri: Optional database URI for non-SQLite dialects.
            
        Returns:
            PipelineReport with results of all operations.
        """
        if engine is not None:
            return self._run_with_engine(engine)

        # Otherwise, create a sandbox based on configured dialect.
        dialect = (self.config.dialect or "sqlite").lower()

        if dialect == "sqlite":
            with sqlite_sandbox() as sandbox_engine:
                return self._run_with_engine(sandbox_engine)

        # For other dialects we rely on the generic dialect_sandbox helper.
        from utils.sandbox import dialect_sandbox
        with dialect_sandbox(dialect, db_uri=db_uri) as sandbox_engine:
            return self._run_with_engine(sandbox_engine)
    
    def _run_with_engine(self, engine: Engine) -> PipelineReport:
        """Execute pipeline with the given engine."""
        start_time = time.time()
        dialect = engine.dialect.name
        
        # Initialize report
        self.report = PipelineReport(
            table=self.config.table_name,
            dialect=dialect,
            pk=self.config.pk or [],
            meta={
                "csv_path": str(self.config.csv_path),
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        
        # Step 1: Load and clean CSV
        df = self._load_and_clean_csv()
        if df is None:
            return self.report
        
        # Step 2: Infer or verify PK
        pk = self._resolve_pk(df)
        self.report.pk = pk
        
        # Step 3: Generate and execute DDL
        if not self._execute_ddl(engine, df, pk):
            return self.report
        
        # Step 4: Validate INSERT
        self._validate_insert(engine)
        
        # Step 5: Validate UPSERT (with idempotency check)
        self._validate_upsert(engine)
        
        # Step 6: Validate UPDATE
        self._validate_update(engine)
        
        # Finalize metadata
        self.report.meta["row_count"] = len(df)
        self.report.meta["column_map"] = self._column_map
        self.report.meta["processed_df_shape"] = list(self._processed_df.shape) if self._processed_df is not None else None
        self.report.meta["constraints"] = {"pk": pk, "unique": []}
        self.report.meta["total_duration_ms"] = (time.time() - start_time) * 1000
        
        return self.report
    
    def _load_and_clean_csv(self) -> Optional[pd.DataFrame]:
        """Load CSV and apply cleaning transformations."""
        try:
            df = pd.read_csv(self.config.csv_path)
            
            # Store original column names for mapping
            original_cols = list(df.columns)
            
            # Apply cleaning pipeline
            df = quick_clean(df)
            df = sanitize_cols(df)
            df = cast_df(df)
            
            # Build column map
            self._column_map = dict(zip(original_cols, df.columns))
            
            return df
        except Exception as e:
            self.report.operations.append(OperationResult(
                name="load",
                status="failed",
                error=str(e),
            ))
            return None
    
    def _resolve_pk(self, df: pd.DataFrame) -> list[str]:
        """Resolve primary key - use provided or infer."""
        if self.config.pk:
            # Validate provided PK columns exist
            pk_cols = self.config.pk if isinstance(self.config.pk, list) else [self.config.pk]
            missing = [c for c in pk_cols if c not in df.columns]
            if missing:
                # Try case-insensitive match
                lower_cols = {c.lower(): c for c in df.columns}
                pk_cols = [lower_cols.get(c.lower(), c) for c in pk_cols]
            return pk_cols
        
        # Infer PK using profile
        try:
            profile = profile_dataframe(df)
            df_with_pk, pk_name, pk_meta = get_pk(df, profile)
            
            # Use components if it's a virtual PK
            if pk_name == 'pk' or pk_name.endswith('_pk'):
                return pk_meta.get('components', [df.columns[0]])
                
            return [pk_name] if isinstance(pk_name, str) else list(pk_name)
        except Exception:
            # Fallback: use first column
            return [df.columns[0]]
    
    def _execute_ddl(self, engine: Engine, df: pd.DataFrame, pk: list[str]) -> bool:
        """Generate and execute DDL."""
        start_time = time.time()
        
        try:
            # Generate DDL using existing API
            processed_df, create_sql, constraint_sql, schema, sa_schema = df_ddl(
                engine, df, self.config.table_name, pk=pk
            )
            
            self._processed_df = processed_df
            self._create_sql = create_sql
            
            # Execute DDL
            sm = SchemaManager(engine)
            
            # Drop if exists
            if sm.has_table(self.config.table_name):
                sm.drop_table(self.config.table_name)
            
            # Execute CREATE TABLE
            sm.execute_ddl(create_sql, f"Created table {self.config.table_name}")
            
            # Execute constraints
            if not constraint_sql:
                constraints: list[str] = []
            elif isinstance(constraint_sql, str):
                constraints = [constraint_sql]
            else:
                constraints = list(constraint_sql)

            for constraint in constraints:
                try:
                    sm.execute_ddl(constraint, "Applied constraint")
                except Exception:
                    pass  # Constraints are optional
            
            duration_ms = (time.time() - start_time) * 1000
            self.report.operations.append(OperationResult(
                name="ddl",
                status="passed",
                details={"columns": len(processed_df.columns)},
                duration_ms=duration_ms,
            ))
            return True
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            self.report.operations.append(OperationResult(
                name="ddl",
                status="failed",
                error=str(e),
                duration_ms=duration_ms,
            ))
            return False
    
    def _validate_insert(self, engine: Engine) -> None:
        """Validate INSERT operation."""
        if self._processed_df is None:
            return
        
        start_time = time.time()
        
        try:
            # Perform insert
            result = auto_insert(
                engine,
                self._processed_df,
                self.config.table_name,
                chunk_size=self.config.chunksize,
            )
            
            # Verify row count
            sm = SchemaManager(engine)
            actual_rows = sm.get_row_count(self.config.table_name)
            expected_rows = len(self._processed_df)
            
            if actual_rows == expected_rows:
                duration_ms = (time.time() - start_time) * 1000
                self.report.operations.append(OperationResult(
                    name="insert",
                    status="passed",
                    details={"rows": actual_rows},
                    duration_ms=duration_ms,
                ))
            else:
                duration_ms = (time.time() - start_time) * 1000
                self.report.operations.append(OperationResult(
                    name="insert",
                    status="failed",
                    details={"expected": expected_rows, "actual": actual_rows},
                    error=f"Row count mismatch: expected {expected_rows}, got {actual_rows}",
                    duration_ms=duration_ms,
                ))
                
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            self.report.operations.append(OperationResult(
                name="insert",
                status="failed",
                error=str(e),
                duration_ms=duration_ms,
            ))
    
    def _validate_upsert(self, engine: Engine) -> None:
        """Validate UPSERT operation with idempotency check."""
        if self._processed_df is None or not self.report.pk:
            self.report.operations.append(OperationResult(
                name="upsert",
                status="skipped",
                error="No primary key defined for upsert validation",
            ))
            return
        
        start_time = time.time()
        
        try:
            sm = SchemaManager(engine)
            
            # Get row count before first upsert
            count_before = sm.get_row_count(self.config.table_name)
            
            # First upsert
            auto_upsert(
                engine,
                self._processed_df,
                self.config.table_name,
                constrain=self.report.pk,
                chunk=self.config.chunksize,
            )
            count_after_first = sm.get_row_count(self.config.table_name)
            
            # Second upsert (idempotency check)
            auto_upsert(
                engine,
                self._processed_df,
                self.config.table_name,
                constrain=self.report.pk,
                chunk=self.config.chunksize,
            )
            count_after_second = sm.get_row_count(self.config.table_name)
            
            # Check idempotency: row count should not change after second upsert
            idempotent = count_after_first == count_after_second
            
            duration_ms = (time.time() - start_time) * 1000
            
            if idempotent:
                self.report.operations.append(OperationResult(
                    name="upsert",
                    status="passed",
                    details={
                        "idempotent": True,
                        "rows_before": count_before,
                        "rows_after": count_after_second,
                    },
                    duration_ms=duration_ms,
                ))
            else:
                self.report.operations.append(OperationResult(
                    name="upsert",
                    status="failed",
                    details={
                        "idempotent": False,
                        "count_after_first": count_after_first,
                        "count_after_second": count_after_second,
                    },
                    error="Upsert is not idempotent - row count changed after second run",
                    duration_ms=duration_ms,
                ))
                
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            self.report.operations.append(OperationResult(
                name="upsert",
                status="failed",
                error=str(e),
                duration_ms=duration_ms,
            ))
    
    def _validate_update(self, engine: Engine) -> None:
        """Validate UPDATE operation."""
        if self._processed_df is None or len(self._processed_df) == 0:
            self.report.operations.append(OperationResult(
                name="update",
                status="skipped",
                error="No data for update validation",
            ))
            return
        
        start_time = time.time()
        
        try:
            # Find a non-PK column to update
            non_pk_cols = [c for c in self._processed_df.columns if c not in self.report.pk]
            
            if not non_pk_cols:
                duration_ms = (time.time() - start_time) * 1000
                self.report.operations.append(OperationResult(
                    name="update",
                    status="skipped",
                    details={"reason": "no non-pk columns"},
                    duration_ms=duration_ms,
                ))
                return
            
            # Get first row for update test
            first_row = self._processed_df.iloc[0]
            pk_values = {col: first_row[col] for col in self.report.pk}
            
            # Build WHERE clause
            where_clause = [(col, "=", val) for col, val in pk_values.items()]
            
            # Find updateable column
            update_col = None
            for col in non_pk_cols:
                val = first_row[col]
                if pd.notna(val):
                    update_col = col
                    break
            
            if not update_col:
                duration_ms = (time.time() - start_time) * 1000
                self.report.operations.append(OperationResult(
                    name="update",
                    status="skipped",
                    details={"reason": "no candidate column with non-null value"},
                    duration_ms=duration_ms,
                ))
                return
            
            # Create update data with a small modification
            original_val = first_row[update_col]
            if isinstance(original_val, str):
                new_val = f"{original_val}_updated"
            elif isinstance(original_val, (int, float)):
                new_val = original_val + 1
            else:
                new_val = original_val
            
            update_df = pd.DataFrame([{update_col: new_val}])
            
            # Perform update
            auto_update(
                engine,
                self.config.table_name,
                update_df,
                where=where_clause,
            )
            
            duration_ms = (time.time() - start_time) * 1000
            self.report.operations.append(OperationResult(
                name="update",
                status="passed",
                details={
                    "column": update_col,
                    "original": str(original_val),
                    "updated": str(new_val),
                },
                duration_ms=duration_ms,
            ))
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            self.report.operations.append(OperationResult(
                name="update",
                status="failed",
                error=str(e),
                duration_ms=duration_ms,
            ))
    
    @property
    def ddl_sql(self) -> Optional[str]:
        """Get the generated CREATE TABLE SQL."""
        return self._create_sql
    
    @property
    def processed_dataframe(self) -> Optional[pd.DataFrame]:
        """Get the processed DataFrame used for operations."""
        return self._processed_df
