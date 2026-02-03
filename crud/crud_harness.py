import pandas as pd
import numpy as np


class CrudTestHarness:
    """
    Test harness for deterministic testing of INSERT / UPSERT / UPDATE SQL generators.

    Features:
    - Prepares mutated test DataFrames with preserved NULLs
    - Generates safe, NULL-aware SELECT queries for verification
    - Provides validation with floating-point and datetime tolerance
    - Supports composite PKs and constraint keys
    """

    def __init__(self, df_src, pk_cols, constraint_cols, table_name="t"):
        if not isinstance(df_src, pd.DataFrame) or len(df_src) != 10:
            raise ValueError("df_src must be DataFrame with exactly 10 rows")

        if not isinstance(df_src.index, pd.RangeIndex) or not df_src.index.equals(pd.RangeIndex(10)):
            raise ValueError("df_src index must be RangeIndex(0..9)")

        self.df = df_src.copy()
        self.pk = [pk_cols] if isinstance(pk_cols, str) else list(pk_cols)
        self.constraint = [constraint_cols] if isinstance(constraint_cols, str) else list(constraint_cols)
        self.table = table_name

        all_cols = set(self.df.columns)
        for group, name in [(self.pk, "pk"), (self.constraint, "constraint")]:
            if not group:
                raise ValueError(f"{name} columns must be non-empty")
            missing = set(group) - all_cols
            if missing:
                raise ValueError(f"Missing {name} columns: {missing}")

        if set(self.pk) & set(self.constraint):
            raise ValueError("pk and constraint columns must be disjoint")

        self.mutable = [c for c in self.df.columns if c not in set(self.pk + self.constraint)]
        if not self.mutable:
            raise ValueError("No mutable columns found")

        if self.df[self.pk].duplicated().any():
            raise ValueError("Source DataFrame contains duplicate PK values")

    # -------------------------------------------------------------------------
    # Deterministic mutation (NULL-safe)
    # -------------------------------------------------------------------------

    @staticmethod
    def _mutate_value(v):
        if pd.isna(v):
            return v
        if isinstance(v, bool):
            return not v
        if isinstance(v, (int, np.integer)):
            return v + 1
        if isinstance(v, (float, np.floating)):
            return v + 1.0
        if isinstance(v, (pd.Timestamp, pd.Period)):
            return v + pd.Timedelta(days=1)
        if isinstance(v, str):
            return f"{v}⟐m"          # recognizable mutation marker
        return v

    def _mutate_mutable_columns(self, df):
        df = df.copy()
        for col in self.mutable:
            df[col] = df[col].map(self._mutate_value, na_action='ignore')
        return df

    # -------------------------------------------------------------------------
    # Test case DataFrames
    # -------------------------------------------------------------------------

    @property
    def insert_df(self):
        return self._mutate_mutable_columns(self.df.iloc[0:6])

    @property
    def upsert_df(self):
        slice_df = self.df.iloc[3:8]
        existing = self._mutate_mutable_columns(slice_df.iloc[0:3])  # 3,4,5
        new = slice_df.iloc[3:5].copy()                              # 6,7
        return pd.concat([existing, new])

    @property
    def update_df(self):
        return self._mutate_mutable_columns(self.df.iloc[[1, 7, 8]])

    # -------------------------------------------------------------------------
    # Verification SELECT queries
    # -------------------------------------------------------------------------

    def _build_select_query(self, indices, key_columns, null_safe=False, param_prefix="p"):
        clauses = []
        params = {}

        for i, idx in enumerate(indices):
            conds = []
            for col in key_columns:
                val = self.df.at[idx, col]
                param_name = f"{param_prefix}_{col}_{i}"

                if null_safe and pd.isna(val):
                    conds.append(f"{col} IS NULL")
                else:
                    conds.append(f"{col} = :{param_name}")
                    params[param_name] = val

            clauses.append(" AND ".join(conds))

        where = " OR ".join(f"({c})" for c in clauses) if clauses else "FALSE"
        order = ", ".join(key_columns)
        sql = f"SELECT * FROM {self.table} WHERE {where} ORDER BY {order}"

        return sql, params

    def get_insert_check_query(self):
        return self._build_select_query(range(0, 6), self.pk)

    def get_upsert_check_query(self):
        return self._build_select_query(range(3, 8), self.pk)

    def get_update_check_query(self):
        return self._build_select_query([1, 7, 8], self.constraint, null_safe=True, param_prefix="upd")

    # -------------------------------------------------------------------------
    # Validation with tolerance
    # -------------------------------------------------------------------------

    def assert_same_pk_set(self, db_df, expected_df):
        db_keys = set(tuple(row) for row in db_df[self.pk].to_numpy())
        exp_keys = set(tuple(row) for row in expected_df[self.pk].to_numpy())
        if db_keys != exp_keys:
            raise AssertionError(
                f"PK set mismatch\n"
                f"Expected: {sorted(exp_keys)}\n"
                f"Actual:   {sorted(db_keys)}"
            )

    def assert_frames_close(
        self,
        actual,
        expected,
        rtol=1e-5,
        atol=1e-8,
        dt_tolerance=pd.Timedelta("5s"),
        msg=""
    ):
        a = actual.sort_values(self.pk, ignore_index=True)
        b = expected.sort_values(self.pk, ignore_index=True)

        if list(a.columns) != list(b.columns):
            raise AssertionError(f"Column mismatch {msg}")
        if len(a) != len(b):
            raise AssertionError(f"Row count mismatch {msg}: exp {len(b)}, got {len(a)}")

        for col in a.columns:
            s1 = a[col].to_numpy()
            s2 = b[col].to_numpy()

            if pd.api.types.is_datetime64_any_dtype(a[col]):
                diff = np.abs((s1 - s2).astype('timedelta64[ns]'))
                if not np.all(diff <= dt_tolerance.to_numpy()):
                    raise AssertionError(
                        f"Datetime difference too large in '{col}' {msg} "
                        f"(max allowed: {dt_tolerance})"
                    )
            elif np.issubdtype(s1.dtype, np.floating) or np.issubdtype(s2.dtype, np.floating):
                if not np.allclose(s1, s2, rtol=rtol, atol=atol, equal_nan=True):
                    raise AssertionError(
                        f"Floating point values differ in '{col}' {msg} "
                        f"(rtol={rtol}, atol={atol})"
                    )
            else:
                if not np.array_equal(s1, s2, equal_nan=True):
                    raise AssertionError(f"Values differ in column '{col}' {msg}")

    # -------------------------------------------------------------------------
    # High-level validation methods
    # -------------------------------------------------------------------------

    def validate_after_insert(self, db_df):
        self.assert_same_pk_set(db_df, self.insert_df)
        self.assert_frames_close(db_df, self.insert_df, msg="(after INSERT)")

    def validate_after_upsert(self, db_df):
        base = self.insert_df.set_index(self.pk)
        changes = self.upsert_df.set_index(self.pk)

        base.update(changes[self.mutable])
        new_rows = changes[~changes.index.isin(base.index)]
        expected = pd.concat([base.reset_index(), new_rows.reset_index()], ignore_index=True)

        self.assert_frames_close(db_df, expected, msg="(after UPSERT)")

    def validate_after_full_cycle(self, db_df):
        # After upsert
        state = self.insert_df.set_index(self.pk)
        up = self.upsert_df.set_index(self.pk)
        state.update(up[self.mutable])
        new_upsert = up[~up.index.isin(state.index)]
        state = pd.concat([state.reset_index(), new_upsert.reset_index()], ignore_index=True)

        # Apply UPDATE
        state = state.set_index(self.constraint)
        upd = self.update_df.set_index(self.constraint)
        state.update(upd[self.mutable])
        final_expected = state.reset_index()

        self.assert_frames_close(db_df, final_expected, msg="(after full cycle INSERT→UPSERT→UPDATE)")