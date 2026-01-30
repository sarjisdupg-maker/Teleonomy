import numpy as np
import pandas as pd
from typing import Tuple, Dict, Optional, List, Any
import logging
from .logger import log_call, log_dataframe, log_json

# Configure logging for ETL traceability (use in pipelines)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@log_call
def profile_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Generate column profiling info: types, uniqueness, nulls, etc."""
    if df.empty:
        return pd.DataFrame()

    total_rows = len(df)
    profile_data: List[Dict[str, Any]] = []

    for col in df.columns:
        col_data = df[col]
        dtype = col_data.dtype
        null_count = col_data.isnull().sum()
        non_null_count = total_rows - null_count
        unique_count = col_data.nunique()
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0

        # Heuristic: categorical if category dtype or low cardinality (<=10% uniques)
        is_categorical = dtype.name == 'category' or (non_null_count > 0 and unique_count / non_null_count <= 0.1)
        is_all_unique = unique_count == non_null_count and non_null_count > 0
        is_datetime = 'datetime' in dtype.name

        # Infer datetime from object if parseable
        if dtype == 'object' and not is_datetime:
            try:
                pd.to_datetime(col_data.dropna(), errors='raise')
                is_datetime = True
            except ValueError:
                pass

        profile_data.append({
            'Column': col,
            'Data Type': dtype,
            'Is Categorical': is_categorical,
            'All Unique': is_all_unique,
            'Is Datetime': is_datetime,
            'Count of None': null_count,
            'Data Length (Non-Null)': non_null_count,
            '% of None': round(null_pct, 2),
            'Total Unique Data Points': unique_count
        })

    result = pd.DataFrame(profile_data)
    try:
        high_null_cols = [row for row in profile_data if row['% of None'] > 20]
        if high_null_cols:
            log_json("high_null_columns", high_null_cols)
    except Exception:
        logger.exception("Failed to log high_null_columns", exc_info=True)
    return result


def _validate_inputs(df: pd.DataFrame, dfinfo: Optional[pd.DataFrame] = None) -> None:
    """Validate inputs; raise ValueError for ETL safety."""
    if df.empty:
        raise ValueError("Input DataFrame is empty.")
    if dfinfo is not None and dfinfo.empty:
        raise ValueError("dfinfo is empty.")
    if dfinfo is not None:
        required_cols = {'Column', 'All Unique', 'Is Datetime', 'Is Categorical', 'Total Unique Data Points', 'Data Length (Non-Null)', 'Count of None', '% of None'}
        if not required_cols.issubset(dfinfo.columns):
            raise ValueError(f"dfinfo missing required columns: {required_cols - set(dfinfo.columns)}")


def _compute_uniqueness_ratio(dfinfo: pd.DataFrame) -> pd.DataFrame:
    """Add uniqueness ratio to dfinfo (unique / non-null); handle div/0."""
    dfinfo = dfinfo.copy()
    dfinfo['Uniqueness Ratio'] = dfinfo.apply(
        lambda row: row['Total Unique Data Points'] / row['Data Length (Non-Null)'] if row['Data Length (Non-Null)'] > 0 else 0.0,
        axis=1
    )
    return dfinfo


@log_call
def sample_dispatcher(
    df: pd.DataFrame,
    percent: int = 5,
    sort: bool = True,
    filter_none: bool = True,
    sort_key: Optional[str] = None
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Sample head + tail rows post-sort/filter for ETL optimization.

    Returns: (sampled_df, metadata) where metadata includes stats/warnings.
    """
    _validate_inputs(df)
    if percent <= 0 or percent > 100:
        raise ValueError("percent must be 1-100.")

    df_work = df.copy(deep=False)  # Shallow copy for views
    metadata: Dict[str, Any] = {'original_rows': len(df), 'reduction_pct': 0.0, 'warnings': []}

    # Filter null rows if enabled (row-level; may bias if nulls >20%)
    if filter_none:
        initial_rows = len(df_work)
        df_work = df_work.dropna(how='any')
        filtered = initial_rows - len(df_work)
        metadata['filtered_rows'] = filtered
        if initial_rows > 0 and filtered / initial_rows > 0.2:
            metadata['warnings'].append("High null filtering (>20%); sample may be biased.")

    if len(df_work) == 0:
        metadata['sampled_rows'] = 0
        return df_work, metadata

    # Sort if enabled (by key or index; skip if already sorted for perf)
    if sort:
        if sort_key and sort_key in df_work.columns:
            was_sorted = df_work[sort_key].is_monotonic_increasing
            if not was_sorted:
                df_work = df_work.sort_values(sort_key).reset_index(drop=True)
            logger.info(f"Sorted by '{sort_key}' (was already sorted: {was_sorted}).")
        else:
            df_work = df_work.reset_index(drop=True)  # Ensure clean index order
            logger.info("Used index order for sampling.")

    # Balanced head/tail sample (min 1 per side; dedup overlap)
    total_rows = len(df_work)
    num_per_side = max(1, int(total_rows * percent / 200))  # /200 for split
    head = df_work.iloc[:num_per_side]
    tail = df_work.iloc[-num_per_side:]
    sampled_df = pd.concat([head, tail], ignore_index=True).drop_duplicates()

    metadata.update({
        'sampled_rows': len(sampled_df),
        'num_per_side': num_per_side,
        'reduction_pct': (1 - len(sampled_df) / total_rows) * 100
    })

    logger.info(f"Sampled {len(sampled_df)} rows ({metadata['reduction_pct']:.1f}% reduction).")
    if metadata['warnings']:
        logger.warning(f"Sampling warnings: {metadata['warnings']}")
    try:
        log_json("sampling_stats", metadata)
    except Exception:
        logger.exception("Failed to log sampling_stats", exc_info=True)

    return sampled_df, metadata


def _build_composite_pk(
    df: pd.DataFrame,
    pk_components: List[str],
    delimiter: str = '|'
) -> pd.Series:
    """Build composite PK: str concat with delimiter (collision-resistant via escaping)."""
    # Escape specials in strings to avoid false splits (e.g., escape delimiter in values)
    escape_char = '\\'
    df_escaped = df[pk_components].astype(str).apply(lambda s: s.str.replace(escape_char, escape_char + escape_char, regex=False).str.replace(delimiter, escape_char + delimiter, regex=False))
    composite = df_escaped.agg(delimiter.join, axis=1)
    return composite


@log_call
def get_pk(
    df: pd.DataFrame,
    dfinfo: pd.DataFrame,
    uniqueness_threshold: float = 0.8,
    max_composite_size: int = 5
) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    """
    Generate primary key: single if unique, else minimal composite.

    Returns: (df_with_pk, pk_name, metadata) e.g., pk_name='col_composite'.
    """
    _validate_inputs(df, dfinfo)
    if uniqueness_threshold < 0 or uniqueness_threshold > 1:
        raise ValueError("uniqueness_threshold must be 0-1.")

    df_work = df.copy(deep=False)
    dfinfo_work = _compute_uniqueness_ratio(dfinfo[dfinfo['Column'].isin(df.columns)])
    metadata: Dict[str, Any] = {'components': [], 'is_unique': False, 'non_null_pk_rows': 0}

    # Rule 1: Single unique column (first in order)
    unique_cols = dfinfo_work[dfinfo_work['All Unique']]['Column'].tolist()
    if unique_cols:
        pk_col = unique_cols[0]
        df_work['pk'] = df_work[pk_col].astype(str)
        metadata.update({'components': [pk_col], 'is_unique': True})
        logger.info(f"Single PK: '{pk_col}'.")
        try:
            log_json("pk_detection", {"pk_name": 'pk', "components": metadata['components'], "is_unique": metadata['is_unique'], "non_null_rows": metadata.get('non_null_pk_rows', 0), "warnings": metadata.get('warnings', [])})
        except Exception:
            logger.exception("Failed to log pk_detection (single)", exc_info=True)
        return df_work, 'pk', metadata

    # Rule 2/3: Candidates (high ratio, non-datetime first; add first datetime if exists)
    candidates = dfinfo_work[
        (dfinfo_work['Uniqueness Ratio'] >= uniqueness_threshold) &
        (dfinfo_work['Data Length (Non-Null)'] > 0)
    ].sort_values(['Uniqueness Ratio', 'Column'], ascending=[False, True])

    datetime_cols = dfinfo_work[dfinfo_work['Is Datetime']]['Column'].tolist()
    pk_components: List[str] = []
    metadata['warnings'] = []

    if not candidates.empty:
        pk_components = [candidates.iloc[0]['Column']]
    else:
        # Fallback: first available column
        pk_components = [df.columns[0]]
        metadata['warnings'].append("No high-uniqueness candidates; using fallback.")

    if datetime_cols and len(pk_components) < max_composite_size:
        dt_col = datetime_cols[0]
        if dt_col not in pk_components:
            pk_components.append(dt_col)
            logger.info(f"Added datetime: '{dt_col}'.")

    # Rule 4: Iterative composite until unique (check on non-null rows)
    while len(pk_components) < max_composite_size:
        temp_pk = _build_composite_pk(df_work, pk_components)
        df_temp = df_work.dropna(subset=pk_components)  # Ignore null-component rows
        if len(df_temp) == 0:
            if 'warnings' not in metadata:
                metadata['warnings'] = []
            metadata['warnings'].append("All rows null in components; PK invalid.")
            break

        temp_pk_nonnull = temp_pk[df_temp.index]
        is_unique = temp_pk_nonnull.nunique() == len(temp_pk_nonnull)

        if is_unique:
            df_work['pk'] = temp_pk
            metadata.update({
                'components': pk_components,
                'is_unique': True,
                'non_null_pk_rows': len(temp_pk_nonnull)
            })
            logger.info(f"Unique composite PK with {len(pk_components)} cols.")
            try:
                log_json("pk_detection", {"pk_name": '_'.join(pk_components) + '_pk', "components": metadata['components'], "is_unique": metadata['is_unique'], "non_null_rows": metadata.get('non_null_pk_rows', 0), "warnings": metadata.get('warnings', [])})
            except Exception:
                logger.exception("Failed to log pk_detection (composite)", exc_info=True)
            return df_work, '_'.join(pk_components) + '_pk', metadata

        # Add next candidate (exclude current)
        next_cands = candidates[~candidates['Column'].isin(pk_components)]
        if next_cands.empty:
            break
        next_col = next_cands.iloc[0]['Column']
        pk_components.append(next_col)
        logger.info(f"Added: '{next_col}' (ratio: {next_cands.iloc[0]['Uniqueness Ratio']:.2f}).")

    # Fallback: Use current (may not be unique)
    df_work['pk'] = _build_composite_pk(df_work, pk_components)
    pk_name = '_'.join(pk_components) + '_pk'
    metadata.update({
        'components': pk_components,
        'is_unique': False,
        'non_null_pk_rows': df_work['pk'].notna().sum()
    })
    if not metadata['is_unique']:
        metadata['warnings'] = ["Fallback PK not fully unique; consider data cleansing."]

    logger.info(f"Fallback composite PK: '{pk_name}' (unique: {metadata['is_unique']}).")
    if metadata.get('warnings'):
        logger.warning(f"PK warnings: {metadata['warnings']}")
    try:
        log_json("pk_detection", {"pk_name": pk_name, "components": metadata['components'], "is_unique": metadata['is_unique'], "non_null_rows": metadata.get('non_null_pk_rows', 0), "warnings": metadata.get('warnings', [])})
    except Exception:
        logger.exception("Failed to log pk_detection (fallback)", exc_info=True)

    return df_work, pk_name, metadata


# Example Usage (for testing/ETL pipeline)
if __name__ == "__main__":
    # Sample data
    data = {
        'name': ['Alice', 'Bob', None, 'Charlie'],
        'age': [25, None, 30, 35],
        'birth_date': pd.to_datetime(['1990-01-01', '1995-02-02', None, '1985-03-03']),
        'category': ['A', 'B', 'A', 'C'],
        'id': [1, 2, 3, 4],
        'salary': [50000, 60000, None, 70000]
    }
    df = pd.DataFrame(data)

    # Profile
    dfinfo = profile_dataframe(df)

    # Sample
    sampled_df, sample_meta = sample_dispatcher(df, percent=50, sort_key='birth_date')
    print(f"Sample shape: {sampled_df.shape}, Meta: {sample_meta}")

    # PK
    df_pk, pk_name, pk_meta = get_pk(df, dfinfo)
    print(f"PK name: {pk_name}, Meta: {pk_meta}")
    print(df_pk[['pk', 'id']].head())
