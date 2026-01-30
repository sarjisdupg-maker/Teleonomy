"""Dialect strategy helpers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from legacy import ddl_create as legacy_ddl
from .registry import TypeRegistry


def _normalize(name: str) -> str:
    return "postgresql" if name.lower() == "postgres" else name.lower()


@dataclass
class DialectStrategy:
    name: str
    registry: TypeRegistry

    def quote_identifier(self, name: str) -> str:
        return legacy_ddl.escape_identifier(name, _normalize(self.name))

    def render_type(self, logical_type: str) -> str:
        return self.registry.resolve_sql(logical_type, _normalize(self.name))

    def build_upsert(self, table: str, columns: Sequence[str], pk: Sequence[str]) -> str:
        cols = ", ".join(self.quote_identifier(c) for c in columns)
        values = ", ".join(f":{c}" for c in columns)
        if _normalize(self.name) in {"postgresql", "sqlite"}:
            pk_cols = ", ".join(self.quote_identifier(c) for c in pk)
            updates = ", ".join(f"{self.quote_identifier(c)}=excluded.{self.quote_identifier(c)}" for c in columns if c not in pk)
            return f"INSERT INTO {self.quote_identifier(table)} ({cols}) VALUES ({values}) ON CONFLICT ({pk_cols}) DO UPDATE SET {updates}"
        if _normalize(self.name) == "mysql":
            updates = ", ".join(f"{self.quote_identifier(c)}=VALUES({self.quote_identifier(c)})" for c in columns if c not in pk)
            return f"INSERT INTO {self.quote_identifier(table)} ({cols}) VALUES ({values}) ON DUPLICATE KEY UPDATE {updates}"
        raise NotImplementedError(f"Upsert not implemented for {self.name}")


def get_dialect(engine_or_name, registry: TypeRegistry | None = None) -> DialectStrategy:
    name = engine_or_name.dialect.name if hasattr(engine_or_name, "dialect") else str(engine_or_name)
    return DialectStrategy(name=name, registry=registry or TypeRegistry.default())
