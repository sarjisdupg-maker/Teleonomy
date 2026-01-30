"""ETL execution pipeline (compact)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Sequence

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None

from sqlalchemy.engine import Engine

from legacy import crud_v2 as legacy_crud
from .cleaner import clean_dataframe, sanitize_columns
from .caster import cast_dataframe
from .aligner import align_dataframe
from .config import AlignPolicy, CastPolicy


@dataclass
class ExecutionResult:
    """Return object for ETL execution."""
    data: "pd.DataFrame"
    reports: Dict[str, Any]
    stats: Any | None = None


def _ensure_df(data: Any) -> "pd.DataFrame":
    if pd is None:
        raise ImportError("pandas is required for ETL execution")
    if isinstance(data, pd.DataFrame):
        return data.copy()
    if isinstance(data, Mapping):
        return pd.DataFrame([data])
    if isinstance(data, Sequence):
        return pd.DataFrame(data)
    raise TypeError("data must be DataFrame, list[dict], or dict")


def execute_etl(
    data: Any,
    engine: Engine,
    table: str,
    *,
    mode: str = "insert",
    sanitize: bool = True,
    clean: bool = True,
    cast: bool = True,
    align: bool = True,
    align_policy: AlignPolicy | None = None,
    cast_policy: CastPolicy | None = None,
    dialect: str = "postgresql",
    constrain: Sequence[str] | None = None,
    where: Sequence[Any] | None = None,
    expression: str | None = None,
    trace_sql: bool = False,
) -> ExecutionResult:
    """Run ETL pipeline then persist via legacy CRUD (insert/upsert/update)."""
    df = _ensure_df(data)
    reports: Dict[str, Any] = {}

    if sanitize:
        df = sanitize_columns(df, dialect=dialect)
    if clean:
        df = clean_dataframe(df)
    if cast:
        df, cast_report = cast_dataframe(df, policy=cast_policy)
        reports["cast"] = cast_report
    if align:
        df, align_report = align_dataframe(df, engine, table, policy=align_policy)
        reports["align"] = align_report

    mode_key = mode.lower()
    if mode_key == "insert":
        stats = legacy_crud.auto_insert(engine, df, table, trace_sql=trace_sql)
    elif mode_key == "upsert":
        if not constrain:
            raise ValueError("constrain is required for upsert")
        stats = legacy_crud.auto_upsert(engine, df, table, list(constrain), trace_sql=trace_sql)
    elif mode_key == "update":
        if not where:
            raise ValueError("where is required for update")
        stats = legacy_crud.auto_update(engine, table, df, list(where), expression=expression, trace_sql=trace_sql)
    else:
        raise ValueError(f"Unsupported mode: {mode}")

    return ExecutionResult(data=df, reports=reports, stats=stats)
