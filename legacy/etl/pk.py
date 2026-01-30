from __future__ import annotations

from typing import Union

import pandas as pd

from ..data_profiler import get_pk, profile_dataframe
from ..schema_manager import SchemaManager


def _process_pk(ctx, df: pd.DataFrame, table: str, pk_arg: Union[str, list]) -> list:
    """Processes and validates the primary key argument, prioritizing DB-side PKs."""
    if pk_arg == "verify":
        print(f"Verifying existing PK for table '{table}'...")
        return _derive_pk(ctx, df, table)

    if pk_arg == "derive":
        print(f"Deriving PK for '{table}' (checking DB first)...")
        return _derive_pk(ctx, df, table)

    if not pk_arg:
        return []

    if isinstance(pk_arg, str):
        pk_arg = [pk_arg]

    if not isinstance(pk_arg, list):
        raise ValueError("pk argument must be 'derive', 'verify', or a list of column names.")

    def _norm(name: str) -> str:
        import re
        return re.sub(r"[^a-zA-Z0-9_]", "_", str(name)).strip("_").lower()

    df_cols_map = {c.lower(): c for c in df.columns}
    df_norm_map = {_norm(c): c for c in df.columns}
    final_pk = []
    missing = []
    for c in pk_arg:
        low = c.lower()
        if low in df_cols_map:
            final_pk.append(df_cols_map[low])
            continue
        norm = _norm(c)
        if norm in df_norm_map:
            final_pk.append(df_norm_map[norm])
        else:
            missing.append(c)

    if missing:
        print(f"PK validation WARNING: Columns {missing} not found in DataFrame. Proceeding without PK.")
        return []

    pk_arg = final_pk

    if len(pk_arg) == 1:
        col = pk_arg[0]
        if df[col].duplicated().any():
            print(f"PK validation WARNING: Column '{col}' contains duplicates. Proceeding without PK.")
            return []
    else:
        combined = df[pk_arg].astype(str).agg("_".join, axis=1)
        if combined.duplicated().any():
            print(f"PK validation WARNING: Composite {pk_arg} contains duplicates. Proceeding without PK.")
            return []

    print(f"PK validation SUCCESS for {table}: {pk_arg}")
    return pk_arg


def _derive_pk(ctx, df: pd.DataFrame, table: str) -> list:
    """Attempts to find a PK from the database first, then falls back to profiling."""
    sm = SchemaManager(ctx.engine)
    if sm.has_table(table):
        db_pk = sm.get_primary_keys(table)
        if db_pk:
            print(f"Detected existing database PK for '{table}': {db_pk}")
            df_cols_map = {c.lower(): c for c in df.columns}
            final_pk = []
            missing = []
            for c in db_pk:
                if c.lower() in df_cols_map:
                    final_pk.append(df_cols_map[c.lower()])
                else:
                    missing.append(c)

            if not missing:
                print(f"Matching DB PK with data columns: {final_pk}")
                return final_pk
            print(f"Warning: Database PK columns {missing} missing in source data. Falling back to profiling.")

    print(f"Profiling DataFrame to find optimal PK for '{table}'...")
    pk_info = get_pk(df, profile_dataframe(df))
    derived = pk_info[2].get("components") if isinstance(pk_info, (list, tuple)) else []
    if derived:
        print(f"Derived PK from data: {derived}")
        return derived
    return []
