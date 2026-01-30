"""Diagnostics report containers."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence


@dataclass
class CastReport:
    summary: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class AlignReport:
    status: str = "ok"
    warnings: Sequence[str] = field(default_factory=list)
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class ColReport:
    column: str
    summary: Mapping[str, Any] = field(default_factory=dict)


def log_report(report: Any, logger=None) -> None:
    """Emit report to logger if provided."""
    if logger is None:
        return
    logger.info(report)
