from __future__ import annotations

from pathlib import Path
import logging
from typing import Union

import pandas as pd

logger = logging.getLogger(__name__)


def _to_dataframe(source: Union[Path, str, pd.DataFrame, list]) -> pd.DataFrame:
    """Converts various source types (CSV, Excel, DF, List) to a pandas DataFrame."""
    if isinstance(source, pd.DataFrame):
        df = source.copy()
    elif isinstance(source, list):
        df = pd.DataFrame(source)
    else:
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"Source file not found: {path}")

        suffix = path.suffix.lower()
        if suffix == ".csv":
            try:
                df = pd.read_csv(path, encoding="utf-8", on_bad_lines="warn", engine="python", compression="infer")
                if df.shape[1] == 1:
                    try:
                        df_retry = pd.read_csv(path, sep=None, engine="python", encoding="utf-8", on_bad_lines="warn")
                        if df_retry.shape[1] > 1:
                            df = df_retry
                    except Exception:
                        logger.exception("CSV sniff retry failed", exc_info=True)
            except Exception as e:
                try:
                    df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8", on_bad_lines="warn")
                except Exception as e2:
                    raise ValueError(f"Failed to read CSV {path}: {e}") from e2
        elif suffix in (".xlsx", ".xls"):
            try:
                engine = "openpyxl" if suffix == ".xlsx" else None
                df = pd.read_excel(path, engine=engine)
            except ImportError:
                raise ImportError("Missing optional dependency 'openpyxl'. Install it to read .xlsx files.")
            except Exception as e:
                raise ValueError(f"Failed to read Excel {path}: {e}")
        else:
            raise ValueError(f"Unsupported file format: {suffix}")

    if df.empty or df.columns.empty:
        source_desc = str(source) if isinstance(source, (str, Path)) else f"type {type(source).__name__}"
        raise ValueError(f"Empty dataset: {source_desc}")

    return df
