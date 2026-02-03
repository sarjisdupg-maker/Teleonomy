from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from enum import Enum
import json
import logging
import re
from typing import Any

import numpy as np
import pandas as pd
import sqlalchemy as sa


log = logging.getLogger(__name__)


class LogicalType(str, Enum):
    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
    STRING = "STRING"
    BOOL = "BOOL"
    DATETIME = "DATETIME"
    JSON = "JSON"
    BINARY = "BINARY"
    OTHER = "OTHER"


class OnError(str, Enum):
    COERCE = "coerce"
    RAISE = "raise"
    REPORT = "report"


class LengthOverflow(str, Enum):
    RAISE = "raise"
    TRUNCATE = "truncate"
    NULL = "null"


class OutlierStrategy(str, Enum):
    NONE = "none"
    IQR = "iqr"


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    db_type_repr: str
    logical_type: LogicalType
    nullable: bool
    length: int | None = None
    precision: int | None = None
    scale: int | None = None
    default: str | None = None
    semantic_tag: str | None = None
    writeable: bool = True
    has_server_default: bool = False


@dataclass(frozen=True)
class ForeignKeySpec:
    local_columns: list[str]
    ref_table: str
    ref_schema: str | None
    ref_columns: list[str]


@dataclass(frozen=True)
class ConstraintSpec:
    primary_key: list[str] = field(default_factory=list)
    unique_sets: list[list[str]] = field(default_factory=list)
    foreign_keys: list[ForeignKeySpec] = field(default_factory=list)


@dataclass(frozen=True)
class TableSchema:
    schema_name: str | None
    name: str
    columns: dict[str, ColumnSpec]
    constraints: ConstraintSpec


@dataclass(frozen=True)
class ColumnOverride:
    logical_type: LogicalType | None = None
    semantic_tag: str | None = None
    on_error: OnError | None = None
    max_failure_rate: float | None = None
    length_overflow: LengthOverflow | None = None
    outlier_strategy: OutlierStrategy | None = None


@dataclass(frozen=True)
class CorrectionConfig:
    on_error: OnError = OnError.RAISE
    max_failure_rate: float = 0.0
    length_overflow: LengthOverflow = LengthOverflow.RAISE
    outlier_strategy: OutlierStrategy = OutlierStrategy.NONE
    validate_fk: bool = False
    validate_unique_against_db: bool = False
    evolve_schema: bool = False
    evolve_schema_execute: bool = False
    datetime_parse_utc: bool = True
    datetime_output_naive: bool = False
    column_map: dict[str, str] = field(default_factory=dict)
    semantic_tags: dict[str, str] = field(default_factory=dict)
    column_overrides: dict[str, ColumnOverride] = field(default_factory=dict)
    sample_bad_values: int = 5
    fk_chunk_size: int = 1000
    unique_chunk_size: int = 1000


@dataclass
class ColumnReport:
    column: str
    logical_type: str
    db_type: str
    failures: int = 0
    total: int = 0
    failure_rate: float = 0.0
    action: str = "none"
    bad_values_sample: list[str] = field(default_factory=list)


@dataclass
class MappingReport:
    mapping: dict[str, str] = field(default_factory=dict)
    dropped_df_columns: list[str] = field(default_factory=list)
    missing_db_columns: list[str] = field(default_factory=list)
    dropped_non_writeable: list[str] = field(default_factory=list)


@dataclass
class MigrationOp:
    op_type: str
    column_name: str
    sa_type: sa.types.TypeEngine
    nullable: bool


@dataclass
class MigrationPlan:
    ops: list[MigrationOp] = field(default_factory=list)


@dataclass
class CorrectionReport:
    table: str
    schema: str | None
    mapping: MappingReport = field(default_factory=MappingReport)
    columns: dict[str, ColumnReport] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    migration_plan: MigrationPlan | None = None


class CorrectionError(Exception):
    pass


class SchemaIntrospectionError(Exception):
    pass


class ConstraintViolationError(CorrectionError):
    pass


class TypeRegistry:
    def __init__(self) -> None:
        self._generic: list[tuple[type[Any], LogicalType]] = []
        self._dialect: dict[str, list[tuple[type[Any], LogicalType]]] = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        self.register_generic(sa.Integer, LogicalType.INTEGER)
        self.register_generic(sa.BigInteger, LogicalType.INTEGER)
        self.register_generic(sa.SmallInteger, LogicalType.INTEGER)
        self.register_generic(sa.Numeric, LogicalType.DECIMAL)
        self.register_generic(sa.DECIMAL, LogicalType.DECIMAL)
        self.register_generic(sa.Float, LogicalType.DECIMAL)
        self.register_generic(sa.String, LogicalType.STRING)
        self.register_generic(sa.Unicode, LogicalType.STRING)
        self.register_generic(sa.Text, LogicalType.STRING)
        self.register_generic(sa.UnicodeText, LogicalType.STRING)
        self.register_generic(sa.Boolean, LogicalType.BOOL)
        self.register_generic(sa.DateTime, LogicalType.DATETIME)
        self.register_generic(sa.Date, LogicalType.DATETIME)
        self.register_generic(sa.Time, LogicalType.DATETIME)
        self.register_generic(sa.JSON, LogicalType.JSON)
        self.register_generic(sa.LargeBinary, LogicalType.BINARY)

    def register_generic(self, sa_type: type[Any], logical_type: LogicalType) -> None:
        item = (sa_type, logical_type)
        self._generic.append(item)

    def register_for_dialect(self, dialect: str, sa_type: type[Any], logical_type: LogicalType) -> None:
        items = self._dialect.get(dialect, [])
        items.append((sa_type, logical_type))
        self._dialect[dialect] = items

    def resolve(self, dialect: str, sa_type: sa.types.TypeEngine) -> LogicalType:
        items = self._dialect.get(dialect, [])
        resolved = self._resolve_from(items, sa_type)
        if resolved is not None:
            return resolved
        resolved = self._resolve_from(self._generic, sa_type)
        if resolved is not None:
            return resolved
        return LogicalType.OTHER

    def _resolve_from(self, items: list[tuple[type[Any], LogicalType]], sa_type: sa.types.TypeEngine) -> LogicalType | None:
        for cls, logical in items:
            if isinstance(sa_type, cls):
                return logical
        return None


def normalize_identifier(name: str) -> str:
    value = name.strip().lower()
    value = re.sub(r"[\s\-]+", "_", value)
    value = re.sub(r"[^a-z0-9_]+", "", value)
    value = re.sub(r"_+", "_", value)
    return value


def build_column_mapping(df_columns: list[str], db_columns: list[str], explicit_map: dict[str, str]) -> MappingReport:
    report = MappingReport()
    norm_db = {normalize_identifier(c): c for c in db_columns}
    used_targets: set[str] = set()

    for src in df_columns:
        tgt = explicit_map.get(src)
        if tgt is not None:
            report.mapping[src] = tgt
            used_targets.add(tgt)

    for src in df_columns:
        if src in report.mapping:
            continue
        norm_src = normalize_identifier(src)
        tgt = norm_db.get(norm_src)
        if tgt is None:
            continue
        if tgt in used_targets:
            continue
        report.mapping[src] = tgt
        used_targets.add(tgt)

    for src in df_columns:
        if src not in report.mapping:
            report.dropped_df_columns.append(src)

    mapped_targets = set(report.mapping.values())
    for col in db_columns:
        if col not in mapped_targets:
            report.missing_db_columns.append(col)

    return report


def apply_column_mapping(df: pd.DataFrame, mapping: MappingReport) -> pd.DataFrame:
    rename_map = dict(mapping.mapping)
    mapped = df.rename(columns=rename_map)
    keep = list(rename_map.values())
    out = mapped.loc[:, keep].copy()
    return out


def _type_repr(sa_type: sa.types.TypeEngine) -> str:
    value = str(sa_type)
    value = value.replace("\n", " ")
    return value


def inspect_table_schema(conn: sa.Engine | sa.Connection, schema_name: str | None, table_name: str, registry: TypeRegistry) -> TableSchema:
    inspector = sa.inspect(conn)
    dialect = inspector.dialect.name
    try:
        cols = inspector.get_columns(table_name, schema=schema_name)
    except Exception as e:
        msg = f"Failed to introspect {schema_name}.{table_name}: {e}"
        raise SchemaIntrospectionError(msg) from e

    columns: dict[str, ColumnSpec] = {}
    for col in cols:
        name = col["name"]
        sa_type = col["type"]
        logical = registry.resolve(dialect, sa_type)
        length = getattr(sa_type, "length", None)
        precision = getattr(sa_type, "precision", None)
        scale = getattr(sa_type, "scale", None)
        default = col.get("default")
        nullable = bool(col.get("nullable", True))
        has_server_default = default is not None
        writeable = True
        spec = ColumnSpec(name=name, db_type_repr=_type_repr(sa_type), logical_type=logical, nullable=nullable, length=length, precision=precision, scale=scale, default=default, has_server_default=has_server_default, writeable=writeable)
        columns[name] = spec

    pk = inspector.get_pk_constraint(table_name, schema=schema_name) or {}
    pk_cols = list(pk.get("constrained_columns") or [])
    uniques = inspector.get_unique_constraints(table_name, schema=schema_name) or []
    unique_sets: list[list[str]] = []
    for uq in uniques:
        cols = list(uq.get("column_names") or [])
        if cols:
            unique_sets.append(cols)

    fks = inspector.get_foreign_keys(table_name, schema=schema_name) or []
    fk_specs: list[ForeignKeySpec] = []
    for fk in fks:
        local_cols = list(fk.get("constrained_columns") or [])
        ref_cols = list(fk.get("referred_columns") or [])
        ref_table = str(fk.get("referred_table") or "")
        ref_schema = fk.get("referred_schema")
        if local_cols and ref_cols and ref_table:
            fk_specs.append(ForeignKeySpec(local_columns=local_cols, ref_table=ref_table, ref_schema=ref_schema, ref_columns=ref_cols))

    constraints = ConstraintSpec(primary_key=pk_cols, unique_sets=unique_sets, foreign_keys=fk_specs)
    schema = TableSchema(schema_name=schema_name, name=table_name, columns=columns, constraints=constraints)
    return schema


def apply_semantic_tags(schema: TableSchema, semantic_tags: dict[str, str]) -> TableSchema:
    cols: dict[str, ColumnSpec] = {}
    for name, spec in schema.columns.items():
        tag = semantic_tags.get(name)
        if tag is None:
            cols[name] = spec
            continue
        cols[name] = ColumnSpec(name=spec.name, db_type_repr=spec.db_type_repr, logical_type=spec.logical_type, nullable=spec.nullable, length=spec.length, precision=spec.precision, scale=spec.scale, default=spec.default, semantic_tag=tag, writeable=spec.writeable, has_server_default=spec.has_server_default)
    out = TableSchema(schema_name=schema.schema_name, name=schema.name, columns=cols, constraints=schema.constraints)
    return out


@dataclass(frozen=True)
class FailurePolicy:
    on_error: OnError
    max_failure_rate: float
    length_overflow: LengthOverflow
    outlier_strategy: OutlierStrategy
    sample_bad_values: int
    datetime_parse_utc: bool
    datetime_output_naive: bool


def make_policy(config: CorrectionConfig, spec: ColumnSpec) -> FailurePolicy:
    override = config.column_overrides.get(spec.name)
    on_error = config.on_error if override is None or override.on_error is None else override.on_error
    max_rate = config.max_failure_rate if override is None or override.max_failure_rate is None else override.max_failure_rate
    overflow = config.length_overflow if override is None or override.length_overflow is None else override.length_overflow
    outliers = config.outlier_strategy if override is None or override.outlier_strategy is None else override.outlier_strategy
    policy = FailurePolicy(on_error=on_error, max_failure_rate=max_rate, length_overflow=overflow, outlier_strategy=outliers, sample_bad_values=config.sample_bad_values, datetime_parse_utc=config.datetime_parse_utc, datetime_output_naive=config.datetime_output_naive)
    return policy


def apply_overrides(spec: ColumnSpec, config: CorrectionConfig) -> ColumnSpec:
    override = config.column_overrides.get(spec.name)
    if override is None:
        return spec
    logical = spec.logical_type if override.logical_type is None else override.logical_type
    tag = spec.semantic_tag if override.semantic_tag is None else override.semantic_tag
    out = ColumnSpec(name=spec.name, db_type_repr=spec.db_type_repr, logical_type=logical, nullable=spec.nullable, length=spec.length, precision=spec.precision, scale=spec.scale, default=spec.default, semantic_tag=tag, writeable=spec.writeable, has_server_default=spec.has_server_default)
    return out


class OutlierDetector:
    def detect(self, series: pd.Series) -> pd.Series:
        raise NotImplementedError


class NoOutlierDetector(OutlierDetector):
    def detect(self, series: pd.Series) -> pd.Series:
        mask = pd.Series(False, index=series.index)
        return mask


class IqrOutlierDetector(OutlierDetector):
    def __init__(self, k: float = 3.0) -> None:
        self._k = float(k)

    def detect(self, series: pd.Series) -> pd.Series:
        values = pd.to_numeric(series, errors="coerce")
        clean = values.dropna()
        if len(clean) < 8:
            mask = pd.Series(False, index=series.index)
            return mask
        q1 = clean.quantile(0.25)
        q3 = clean.quantile(0.75)
        iqr = q3 - q1
        lo = q1 - self._k * iqr
        hi = q3 + self._k * iqr
        mask = (values < lo) | (values > hi)
        mask = mask.fillna(False)
        return mask


@dataclass
class CoercionResult:
    series: pd.Series
    failures: pd.Series
    action: str
    bad_values_sample: list[str]


def _sample_bad_values(series: pd.Series, mask: pd.Series, limit: int) -> list[str]:
    bad = series[mask].head(limit)
    out = [str(x) for x in bad.tolist()]
    return out


def _decide(series: pd.Series, failures: pd.Series, policy: FailurePolicy) -> tuple[pd.Series, str, list[str]]:
    total = int(len(series))
    bad = int(failures.sum())
    rate = 0.0
    if total > 0:
        rate = bad / total
    sample = _sample_bad_values(series, failures, policy.sample_bad_values)
    if bad == 0:
        return series, "none", sample
    if policy.on_error == OnError.RAISE and rate > policy.max_failure_rate:
        msg = f"Too many failures: {bad}/{total} ({rate:.2%})"
        raise CorrectionError(msg)
    if policy.on_error == OnError.REPORT:
        return series, "report", sample
    corrected = series.mask(failures, pd.NA)
    return corrected, "coerce_to_null", sample


def _choose_outlier_detector(policy: FailurePolicy) -> OutlierDetector:
    if policy.outlier_strategy == OutlierStrategy.IQR:
        detector = IqrOutlierDetector()
        return detector
    detector = NoOutlierDetector()
    return detector


def coerce_integer(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult:
    raw = series.copy()
    numeric = pd.to_numeric(raw, errors="coerce")
    failures = numeric.isna() & raw.notna()
    non_int = numeric.notna() & (numeric % 1 != 0)
    failures = failures | non_int
    detector = _choose_outlier_detector(policy)
    outliers = detector.detect(numeric)
    failures = failures | outliers
    coerced = numeric.round(0)
    coerced = coerced.astype("Int64")
    final, action, sample = _decide(coerced, failures, policy)
    return CoercionResult(series=final, failures=failures, action=action, bad_values_sample=sample)


def _to_decimal(value: Any) -> Decimal | None:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        out = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None
    return out


def _quantize_decimal(value: Decimal, scale: int) -> Decimal:
    q = Decimal(10) ** (-scale)
    out = value.quantize(q, rounding=ROUND_HALF_UP)
    return out


def _decimal_fits(value: Decimal, precision: int | None, scale: int | None) -> bool:
    sign, digits, exp = value.as_tuple()
    _ = sign
    scale_eff = -exp if exp < 0 else 0
    int_digits = len(digits) - scale_eff
    if int_digits < 0:
        int_digits = 0
    total_digits = int_digits + scale_eff
    if precision is not None and total_digits > precision:
        return False
    if scale is not None and scale_eff > scale:
        return False
    return True


def coerce_decimal(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult:
    raw = series.copy()
    dec = raw.map(_to_decimal)
    failures = raw.notna() & dec.isna()
    if spec.scale is not None:
        ok = dec.notna()
        dec.loc[ok] = dec.loc[ok].map(lambda x: _quantize_decimal(x, spec.scale))  # type: ignore[misc]
    fits = dec.dropna().map(lambda x: _decimal_fits(x, spec.precision, spec.scale))
    bad_index = fits.index[~fits]
    if len(bad_index) > 0:
        failures.loc[bad_index] = True
    detector = _choose_outlier_detector(policy)
    outliers = detector.detect(pd.to_numeric(raw, errors="coerce"))
    failures = failures | outliers
    final, action, sample = _decide(dec, failures, policy)
    return CoercionResult(series=final, failures=failures, action=action, bad_values_sample=sample)


def coerce_string(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult:
    raw = series.copy()
    text = raw.astype("string")
    failures = pd.Series(False, index=raw.index)
    if spec.length is None:
        final, action, sample = _decide(text, failures, policy)
        return CoercionResult(series=final, failures=failures, action=action, bad_values_sample=sample)
    too_long = text.str.len().fillna(0).astype(int) > int(spec.length)
    failures = failures | too_long
    if too_long.any() and policy.length_overflow == LengthOverflow.TRUNCATE:
        text = text.where(~too_long, text.str.slice(0, int(spec.length)))
        failures = pd.Series(False, index=raw.index)
    if too_long.any() and policy.length_overflow == LengthOverflow.NULL:
        text = text.where(~too_long, pd.NA)
        failures = pd.Series(False, index=raw.index)
    final, action, sample = _decide(text, failures, policy)
    return CoercionResult(series=final, failures=failures, action=action, bad_values_sample=sample)


_TRUE = {"true", "t", "1", "yes", "y", "on"}
_FALSE = {"false", "f", "0", "no", "n", "off"}


def coerce_bool(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult:
    _ = spec
    raw = series.copy()
    out = pd.Series(pd.NA, index=raw.index, dtype="boolean")
    failures = pd.Series(False, index=raw.index)
    as_str = raw.astype("string").str.strip().str.lower()
    is_null = raw.isna()
    out.loc[is_null] = pd.NA
    true_mask = as_str.isin(_TRUE)
    false_mask = as_str.isin(_FALSE)
    out.loc[true_mask] = True
    out.loc[false_mask] = False
    numeric = pd.to_numeric(raw, errors="coerce")
    out.loc[numeric == 1] = True
    out.loc[numeric == 0] = False
    recognized = is_null | true_mask | false_mask | (numeric == 1) | (numeric == 0)
    failures = failures | ~recognized
    final, action, sample = _decide(out, failures, policy)
    return CoercionResult(series=final, failures=failures, action=action, bad_values_sample=sample)


def coerce_datetime(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult:
    _ = spec
    raw = series.copy()
    parsed = pd.to_datetime(raw, errors="coerce", utc=policy.datetime_parse_utc)
    failures = parsed.isna() & raw.notna()
    if policy.datetime_output_naive and getattr(parsed.dtype, "tz", None) is not None:
        parsed = parsed.dt.tz_convert("UTC").dt.tz_localize(None)
    final, action, sample = _decide(parsed, failures, policy)
    return CoercionResult(series=final, failures=failures, action=action, bad_values_sample=sample)


def _to_json(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        return None
    if isinstance(value, str):
        txt = value.strip()
        if txt == "":
            return None
        try:
            return json.loads(txt)
        except json.JSONDecodeError:
            return None
    return None


def coerce_json(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult:
    _ = spec
    raw = series.copy()
    parsed = raw.map(_to_json)
    failures = raw.notna() & parsed.isna()
    final, action, sample = _decide(parsed, failures, policy)
    return CoercionResult(series=final, failures=failures, action=action, bad_values_sample=sample)


def coerce_binary(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult:
    _ = spec
    raw = series.copy()
    out = pd.Series(pd.NA, index=raw.index, dtype="object")
    failures = pd.Series(False, index=raw.index)
    is_bytes = raw.map(lambda x: isinstance(x, (bytes, bytearray)))
    is_null = raw.isna()
    out.loc[is_null] = None
    out.loc[is_bytes] = raw.loc[is_bytes].map(bytes)
    recognized = is_null | is_bytes
    failures = failures | ~recognized
    final, action, sample = _decide(out, failures, policy)
    return CoercionResult(series=final, failures=failures, action=action, bad_values_sample=sample)


def coerce_series(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult:
    if spec.logical_type == LogicalType.INTEGER:
        result = coerce_integer(series, spec, policy)
        return result
    if spec.logical_type == LogicalType.DECIMAL:
        result = coerce_decimal(series, spec, policy)
        return result
    if spec.logical_type == LogicalType.STRING:
        result = coerce_string(series, spec, policy)
        return result
    if spec.logical_type == LogicalType.BOOL:
        result = coerce_bool(series, spec, policy)
        return result
    if spec.logical_type == LogicalType.DATETIME:
        result = coerce_datetime(series, spec, policy)
        return result
    if spec.logical_type == LogicalType.JSON:
        result = coerce_json(series, spec, policy)
        return result
    if spec.logical_type == LogicalType.BINARY:
        result = coerce_binary(series, spec, policy)
        return result
    fallback = coerce_string(series, spec, policy)
    return fallback


def validate_required_columns_present(df: pd.DataFrame, schema: TableSchema) -> list[str]:
    missing: list[str] = []
    for name, spec in schema.columns.items():
        if not spec.writeable:
            continue
        if name in df.columns:
            continue
        if spec.nullable:
            continue
        if spec.has_server_default:
            continue
        missing.append(name)
    return missing


def validate_non_nullable(df: pd.DataFrame, schema: TableSchema) -> list[str]:
    errors: list[str] = []
    for name, spec in schema.columns.items():
        if not spec.writeable:
            continue
        if spec.nullable:
            continue
        if name not in df.columns:
            continue
        nulls = df[name].isna()
        if not nulls.any():
            continue
        count = int(nulls.sum())
        errors.append(f"Non-nullable column {name} has {count} NULL values")
    return errors


def validate_uniques_in_batch(df: pd.DataFrame, unique_sets: list[list[str]]) -> list[str]:
    errors: list[str] = []
    for cols in unique_sets:
        if any(c not in df.columns for c in cols):
            continue
        dup = df.duplicated(subset=cols, keep=False)
        if not dup.any():
            continue
        count = int(dup.sum())
        errors.append(f"Duplicate keys in batch for {cols}: {count} rows")
    return errors


def _quote(conn: sa.Engine | sa.Connection, name: str) -> str:
    dialect = conn.dialect if isinstance(conn, sa.engine.Connection) else conn.dialect
    preparer = dialect.identifier_preparer
    out = preparer.quote(name)
    return out


def _qualified_table(conn: sa.Engine | sa.Connection, schema: str | None, table: str) -> str:
    qt = _quote(conn, table)
    if schema is None:
        return qt
    qs = _quote(conn, schema)
    return f"{qs}.{qt}"


def _fetch_existing_keys(conn: sa.Engine | sa.Connection, schema: str | None, table: str, cols: list[str], keys: list[tuple[Any, ...]]) -> set[tuple[Any, ...]]:
    if not keys:
        out: set[tuple[Any, ...]] = set()
        return out
    quoted_cols = ", ".join(_quote(conn, c) for c in cols)
    qtable = _qualified_table(conn, schema, table)
    placeholders = ", ".join("(" + ", ".join([":p" + str(i) + "_" + str(j) for j in range(len(cols))]) + ")" for i in range(len(keys)))
    sql = f"SELECT {quoted_cols} FROM {qtable} WHERE ({quoted_cols}) IN ({placeholders})"
    params: dict[str, Any] = {}
    for i, row in enumerate(keys):
        for j, val in enumerate(row):
            params[f"p{i}_{j}"] = val
    rows = conn.execute(sa.text(sql), params).fetchall()
    out = {tuple(r) for r in rows}
    return out


def validate_unique_against_db(conn: sa.Engine | sa.Connection, df: pd.DataFrame, schema: TableSchema, chunk_size: int) -> list[str]:
    errors: list[str] = []
    key_sets = [schema.constraints.primary_key] + list(schema.constraints.unique_sets)
    for cols in key_sets:
        if not cols:
            continue
        if any(c not in df.columns for c in cols):
            continue
        subset = df.loc[:, cols].dropna()
        tuples = list({tuple(x) for x in subset.itertuples(index=False, name=None)})
        if not tuples:
            continue
        for i in range(0, len(tuples), chunk_size):
            chunk = tuples[i : i + chunk_size]
            existing = _fetch_existing_keys(conn, schema.schema_name, schema.name, cols, chunk)
            if not existing:
                continue
            errors.append(f"Existing rows found for key set {cols}: {len(existing)} keys (chunked)")
    return errors


def validate_foreign_keys(conn: sa.Engine | sa.Connection, df: pd.DataFrame, schema: TableSchema, chunk_size: int) -> list[str]:
    errors: list[str] = []
    for fk in schema.constraints.foreign_keys:
        cols = fk.local_columns
        if any(c not in df.columns for c in cols):
            continue
        subset = df.loc[:, cols].dropna()
        tuples = list({tuple(x) for x in subset.itertuples(index=False, name=None)})
        if not tuples:
            continue
        for i in range(0, len(tuples), chunk_size):
            chunk = tuples[i : i + chunk_size]
            existing = _fetch_existing_keys(conn, fk.ref_schema, fk.ref_table, fk.ref_columns, chunk)
            missing = set(chunk) - existing
            if not missing:
                continue
            errors.append(f"Missing FK targets for {fk.local_columns}->{fk.ref_table}.{fk.ref_columns}: {len(missing)} keys (chunked)")
    return errors


def infer_add_column_type(series: pd.Series) -> sa.types.TypeEngine:
    """Infers a conservative SQLAlchemy type for ADD COLUMN."""
    s = series.dropna()
    if len(s) == 0:
        out = sa.String(length=255)
        return out
    if pd.api.types.is_bool_dtype(s):
        out = sa.Boolean()
        return out
    if pd.api.types.is_datetime64_any_dtype(s):
        out = sa.DateTime(timezone=False)
        return out
    numeric = pd.to_numeric(s, errors="coerce")
    if numeric.notna().mean() > 0.98:
        as_int = (numeric.dropna() % 1 == 0).mean() > 0.98
        if as_int:
            out = sa.BigInteger()
            return out
        out = sa.Numeric(precision=38, scale=10)
        return out
    max_len = int(s.astype("string").str.len().max())
    if max_len <= 255:
        out = sa.String(length=max(1, max_len))
        return out
    out = sa.Text()
    return out


def plan_schema_evolution(schema: TableSchema, df: pd.DataFrame) -> MigrationPlan:
    ops: list[MigrationOp] = []
    for col in df.columns:
        if col in schema.columns:
            continue
        sa_type = infer_add_column_type(df[col])
        op = MigrationOp(op_type="ADD_COLUMN", column_name=col, sa_type=sa_type, nullable=True)
        ops.append(op)
    plan = MigrationPlan(ops=ops)
    return plan


def execute_migration(conn: sa.Engine | sa.Connection, schema: str | None, table: str, plan: MigrationPlan) -> None:
    if not plan.ops:
        return
    qtable = _qualified_table(conn, schema, table)
    inspector = sa.inspect(conn)
    existing = {c['name'].lower() for c in inspector.get_columns(table, schema=schema)}
    with conn.begin() as tx:
        for op in plan.ops:
            if op.op_type != "ADD_COLUMN":
                continue
            if op.column_name.lower() in existing:
                continue
            col = _quote(conn, op.column_name)
            type_sql = op.sa_type.compile(dialect=conn.dialect)
            nullable_sql = "NULL" if op.nullable else "NOT NULL"
            ddl = f"ALTER TABLE {qtable} ADD COLUMN {col} {type_sql} {nullable_sql}"
            tx.execute(sa.text(ddl))


def _drop_non_writeable(df: pd.DataFrame, schema: TableSchema, mapping: MappingReport) -> pd.DataFrame:
    drop_cols: list[str] = []
    for name, spec in schema.columns.items():
        if name not in df.columns:
            continue
        if spec.writeable:
            continue
        drop_cols.append(name)
        mapping.dropped_non_writeable.append(name)
    out = df.drop(columns=drop_cols, errors="ignore")
    return out


def _finalize_series(series: pd.Series, spec: ColumnSpec) -> pd.Series:
    if spec.logical_type == LogicalType.DATETIME:
        out = series
        return out
    if spec.logical_type == LogicalType.INTEGER:
        out = series.astype("object")
        out = out.where(pd.notna(out), None)
        return out
    if spec.logical_type == LogicalType.BOOL:
        out = series.astype("object")
        out = out.where(pd.notna(out), None)
        return out
    if spec.logical_type == LogicalType.DECIMAL:
        out = series.astype("object")
        out = out.where(pd.notna(out), None)
        return out
    if spec.logical_type == LogicalType.JSON:
        out = series.astype("object")
        out = out.where(pd.notna(out), None)
        return out
    if spec.logical_type == LogicalType.BINARY:
        out = series.astype("object")
        out = out.where(pd.notna(out), None)
        return out
    out = series.astype("object")
    out = out.where(pd.notna(out), None)
    return out


def finalize_dataframe(df: pd.DataFrame, schema: TableSchema) -> pd.DataFrame:
    out = df.copy()
    for name in out.columns:
        spec = schema.columns.get(name)
        if spec is None:
            continue
        out[name] = _finalize_series(out[name], spec)
    return out


class DataCorrector:
    def __init__(self, registry: TypeRegistry | None = None) -> None:
        self._registry = registry if registry is not None else TypeRegistry()

    def correct(self, df: pd.DataFrame, conn: sa.Engine | sa.Connection, schema_name: str | None, table_name: str, config: CorrectionConfig) -> tuple[pd.DataFrame, CorrectionReport]:
        report = CorrectionReport(table=table_name, schema=schema_name)
        schema = inspect_table_schema(conn, schema_name, table_name, self._registry)
        schema = apply_semantic_tags(schema, config.semantic_tags)

        if config.evolve_schema:
            plan = plan_schema_evolution(schema, df)
            report.migration_plan = plan
            if config.evolve_schema_execute and plan.ops:
                with conn.begin():
                    execute_migration(conn, schema_name, table_name, plan)
                schema = inspect_table_schema(conn, schema_name, table_name, self._registry)
                schema = apply_semantic_tags(schema, config.semantic_tags)

        mapping = build_column_mapping(list(df.columns), list(schema.columns.keys()), config.column_map)
        report.mapping = mapping
        mapped = apply_column_mapping(df, mapping)
        mapped = _drop_non_writeable(mapped, schema, mapping)

        missing_required = validate_required_columns_present(mapped, schema)
        if missing_required:
            msg = f"Missing required columns: {missing_required}"
            report.errors.append(msg)
            if config.on_error == OnError.RAISE:
                raise ConstraintViolationError(msg)

        corrected = pd.DataFrame(index=mapped.index)
        for name in mapped.columns:
            base_spec = schema.columns.get(name)
            if base_spec is None:
                continue
            spec = apply_overrides(base_spec, config)
            policy = make_policy(config, spec)
            try:
                result = coerce_series(mapped[name], spec, policy)
            except CorrectionError as e:
                msg = f"Column {name}: {e}"
                report.errors.append(msg)
                if config.on_error == OnError.RAISE:
                    raise
                result = CoercionResult(series=mapped[name], failures=pd.Series(False, index=mapped.index), action="report", bad_values_sample=[])

            corrected[name] = result.series
            col_report = ColumnReport(column=name, logical_type=spec.logical_type.value, db_type=spec.db_type_repr)
            col_report.failures = int(result.failures.sum())
            col_report.total = int(len(result.failures))
            col_report.failure_rate = 0.0 if col_report.total == 0 else col_report.failures / col_report.total
            col_report.action = result.action
            col_report.bad_values_sample = result.bad_values_sample
            report.columns[name] = col_report

        constraint_errors = validate_non_nullable(corrected, schema)
        constraint_errors += validate_uniques_in_batch(corrected, [schema.constraints.primary_key] + schema.constraints.unique_sets)
        for err in constraint_errors:
            report.errors.append(err)

        if config.validate_unique_against_db:
            db_errors = validate_unique_against_db(conn, corrected, schema, config.unique_chunk_size)
            for err in db_errors:
                report.errors.append(err)

        if config.validate_fk and schema.constraints.foreign_keys:
            fk_errors = validate_foreign_keys(conn, corrected, schema, config.fk_chunk_size)
            for err in fk_errors:
                report.errors.append(err)

        if report.errors and config.on_error == OnError.RAISE:
            raise ConstraintViolationError("\n".join(report.errors))

        finalized = finalize_dataframe(corrected, schema)
        return finalized, report