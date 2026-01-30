"""Casting adapters using legacy casting engine."""
from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from legacy import casting as legacy_cast
from .config import CastPolicy
from .diagnostics import CastReport


def _to_config(policy: CastPolicy | None, **kwargs: Any) -> legacy_cast.CastConfig:
    policy = policy or CastPolicy()
    return legacy_cast.CastConfig(
        use_transform=policy.use_transform,
        use_ml=policy.use_ml,
        infer_threshold=policy.infer_threshold,
        max_null_increase=policy.max_null_increase,
        max_sample_size=policy.max_sample_size,
        validate_conversions=policy.validate_conversions,
        **kwargs,
    )


def cast_dataframe(
    df: pd.DataFrame,
    dtype: Mapping[str, str] | None = None,
    policy: CastPolicy | None = None,
    return_report: bool = True,
    **kwargs: Any,
):
    """Cast DataFrame using legacy engine; returns (df, report)."""
    config = _to_config(policy, **kwargs)
    result = legacy_cast.cast_df(df, dtype=dtype, config=config, return_dtype_meta=True)
    if isinstance(result, tuple) and len(result) == 2:
        cast_df, meta = result
    else:
        cast_df, meta = result, {}
    report = CastReport(summary={"dtype_meta": meta})
    return (cast_df, report) if return_report else cast_df


def cast_series(
    series: pd.Series,
    policy: CastPolicy | None = None,
    **kwargs: Any,
) -> tuple[pd.Series, CastReport]:
    """Cast a single Series using the DataFrame pipeline."""
    df = pd.DataFrame({series.name or "value": series})
    cast_df, report = cast_dataframe(df, policy=policy, **kwargs)
    return cast_df.iloc[:, 0], report
