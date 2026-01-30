"""Configuration policies for alignment and casting."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class AlignPolicy:
    """Alignment behavior controls."""
    on_error: str = "coerce"
    failure_threshold: float = 3.0
    enforce_nullability: bool = True
    allow_schema_evolution: bool = False
    validate_fk: bool = False
    col_map: Mapping[str, str] | None = None


@dataclass(frozen=True)
class CastPolicy:
    """Casting behavior controls."""
    infer_threshold: float = 0.9
    max_null_increase: float = 0.1
    max_sample_size: int = 1000
    validate_conversions: bool = True
    use_transform: bool = True
    use_ml: bool = False
