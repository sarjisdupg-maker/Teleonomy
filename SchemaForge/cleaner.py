"""Cleaning and sanitization adapters."""
from __future__ import annotations

from typing import Iterable, Sequence

import pandas as pd

from legacy import data_cleaner as legacy_cleaner
from legacy import ddl_create as legacy_ddl


def clean_series(series: pd.Series, config=None) -> pd.Series:
    """Normalize string values in a Series using legacy cleaner."""
    return legacy_cleaner.clean_series(series, config)


def clean_dataframe(df: pd.DataFrame, config=None, columns: Sequence[str] | None = None) -> pd.DataFrame:
    """Clean headers and target columns using legacy cleaner."""
    return legacy_cleaner.robust_clean(df, config, list(columns) if columns else None)


def sanitize_columns(
    df: pd.DataFrame,
    dialect: str = "postgresql",
    allow_space: bool = False,
    to_lower: bool = True,
    fallback: str = "col_",
) -> pd.DataFrame:
    """Sanitize column names for a SQL dialect."""
    return legacy_ddl.sanitize_cols(
        df,
        allow_space=allow_space,
        to_lower=to_lower,
        fallback=fallback,
        dialect=dialect,
    )


def normalize_sentinels(df: pd.DataFrame, sentinels: Iterable[str] | None = None) -> pd.DataFrame:
    """Replace common sentinel values with NA."""
    default = ["", "na", "n/a", "null", "none", "nan"]
    sentinels = list(sentinels) if sentinels is not None else default
    replace_map = {s: pd.NA for s in sentinels}
    return df.replace(replace_map)
