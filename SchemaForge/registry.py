"""Central type/coercion registry."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict

import pandas as pd


@dataclass
class TypeRegistry:
    _logical_to_pandas: Dict[str, str] = field(default_factory=dict)
    _logical_to_sql: Dict[str, Dict[str, str]] = field(default_factory=dict)

    def register(self, logical_type: str, pandas_dtype: str, sql_types_by_dialect: Dict[str, str]) -> None:
        self._logical_to_pandas[logical_type] = pandas_dtype
        self._logical_to_sql[logical_type] = dict(sql_types_by_dialect)

    def resolve_pandas(self, logical_type: str) -> str:
        return self._logical_to_pandas[logical_type]

    def resolve_sql(self, logical_type: str, dialect: str) -> str:
        return self._logical_to_sql[logical_type][dialect]

    def infer_logical_type(self, series: pd.Series) -> str:
        if pd.api.types.is_bool_dtype(series):
            return "bool"
        if pd.api.types.is_integer_dtype(series):
            return "int"
        if pd.api.types.is_float_dtype(series):
            return "float"
        if pd.api.types.is_datetime64_any_dtype(series):
            return "datetime"
        return "string"

    @classmethod
    def default(cls) -> "TypeRegistry":
        reg = cls()
        reg.register("int", "Int64", {"postgresql": "BIGINT", "mysql": "BIGINT", "sqlite": "INTEGER", "oracle": "NUMBER"})
        reg.register("float", "Float64", {"postgresql": "DOUBLE PRECISION", "mysql": "DOUBLE", "sqlite": "REAL", "oracle": "BINARY_DOUBLE"})
        reg.register("bool", "boolean", {"postgresql": "BOOLEAN", "mysql": "TINYINT", "sqlite": "INTEGER", "oracle": "NUMBER(1)"})
        reg.register("datetime", "datetime64[ns]", {"postgresql": "TIMESTAMP", "mysql": "DATETIME", "sqlite": "DATETIME", "oracle": "TIMESTAMP"})
        reg.register("string", "string", {"postgresql": "TEXT", "mysql": "VARCHAR(255)", "sqlite": "TEXT", "oracle": "VARCHAR2(255)"})
        return reg
