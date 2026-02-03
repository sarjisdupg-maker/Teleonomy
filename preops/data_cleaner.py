"""
Core Problem: Provide reusable, configurable cleaning utilities to normalize text-rich pandas DataFrames ahead of ETL tasks.

Execution Flow: Narrative: The module defines a dataclass-backed cleaning policy and applies it at three granularities: scalars, Series, and entire DataFrames, so one can normalize whitespace, canonicalize Unicode/homoglyphs, strip control characters, trim, and optionally strip special chars. Scalar cleaning short-circuits non-string/NAN inputs, while Series cleaning vectorizes replacements before applying regex or Unicode logic; header cleaning sanitizes column names to DB-safe identifiers and robust_clean stitches headers with selected object/string columns. quick_clean just invokes robust_clean with defaults tuned for BOM/trim/smart-character handling.

Steps:

Instantiate CleaningConfig defaults (strip BOM, normalize Unicode via NFKC, handle NA, etc.), using custom_map for common replacements/homoglyph fixes.
clean_string returns early for None/NaN or non-strings, else normalizes Unicode, applies custom replacements, removes control chars optionally, strips special characters optionally, then trims.
clean_series short-circuits non-string/object dtype, converts to str (handling ‘nan’/‘None’/‘NAT’ as NaN when handle_na), vectorizes the map replacements, optionally applies Unicode normalization and control-character filtering via .apply, strips special characters via regex, then trims.
clean_headers runs every column through clean_string, lowercases if configured, replaces non-alphanumeric characters with underscores, collapses duplicates, and assigns sanitized names back to the DataFrame.
robust_clean calls clean_headers, selects the requested columns or all object/string columns, and overwrites each with clean_series, returning the cleaned DataFrame; quick_clean is a thin wrapper using the default config.
Implicit Contracts: pandas DataFrames/Series present, object/string columns expected for cleaning, numpy imported for NaN handling, Unicode normalization available via unicodedata, and config defaults assume BOM and smart punctuation replacement is desirable for telecom datasets.

Failure Modes: Non-string columns are coerced to strings during clean_series, so dtype information is lost and numeric data may become 'nan'; handle_na default true masks NaNs as None silently; large Series cleaning with .apply per-row introduces performance degradation; aggressive remove_special_chars may strip legitimate DB characters unexpectedly.

Obscure Choices: custom_map aggressively replaces homoglyphs and Unicode punctuation before other rules, implying a priority on ASCII-normalized identifiers; clean_series always casts to string even when handle_na is False, which can still morph numeric nan-like tokens into 'nan'; header cleaning enforces DB-safe names by collapsing repeated underscores and stripping leading/trailing underscores.

"""


import re
import unicodedata
import pandas as pd
import numpy as np
from typing import Union, List, Any, Dict, Optional, Set
from dataclasses import dataclass, field

@dataclass
class CleaningConfig:
    """Configuration for data cleaning operations."""
    strip_bom: bool = True
    normalize_whitespace: bool = True
    standardize_smart_chars: bool = True
    remove_control_chars: bool = True
    normalize_unicode: Optional[str] = 'NFKC'  # NFKC, NFKD, NFC, NFD or None
    handle_na: bool = True
    trim: bool = True
    lowercase_headers: bool = False
    remove_special_chars: bool = False  # If True, removes everything except \w and \s
    
    # Custom replacement map for specific characters
    custom_map: Dict[str, str] = field(default_factory=lambda: {
        '\u00a0': ' ',   # Non-breaking space
        '\u200b': '',    # Zero-width space
        '\u200c': '',    # Zero-width non-joiner
        '\u200d': '',    # Zero-width joiner
        '\u2060': '',    # Word joiner
        '\ufeff': '',    # BOM
        '\u180e': '',    # Mongolian vowel separator (invisible)
        '\u202f': ' ',   # Narrow non-breaking space
        
        # Smart Punctuation
        '\u201c': '"',   # Smart Double Left
        '\u201d': '"',   # Smart Double Right
        '\u2018': "'",   # Smart Single Left
        '\u2019': "'",   # Smart Single Right
        '\u201b': "'",   # Single high-reversed-9 quote
        '\u201e': '"',   # Double low-9 quote
        '\u201f': '"',   # Double high-reversed-9 quote
        
        # Dashes
        '\u2013': '-',   # en dash
        '\u2014': '-',   # em dash
        '\u2015': '-',   # horizontal bar
        
        # Others
        '\u2026': '...', # ellipsis
        
        # Homoglyphs (Cyrillic to Latin)
        '\u0410': 'A', '\u0430': 'a', # A
        '\u0412': 'B',               # B
        '\u0415': 'E', '\u0435': 'e', # E
        '\u041a': 'K', '\u043a': 'k', # K
        '\u041c': 'M',               # M
        '\u041d': 'H',               # H
        '\u041e': 'O', '\u043e': 'o', # O
        '\u0420': 'P', '\u0440': 'p', # P
        '\u0421': 'C', '\u0441': 'c', # C
        '\u0422': 'T',               # T
        '\u0425': 'X', '\u0445': 'x', # X
        '\u0443': 'y',               # y
        
        # Homoglyphs (Greek to Latin)
        '\u0391': 'A', '\u03b1': 'a', # Alpha
        '\u0392': 'B', '\u03b2': 'b', # Beta
        '\u0395': 'E', '\u03b5': 'e', # Epsilon
        '\u0397': 'H',               # Eta
        '\u0399': 'I', '\u03b9': 'i', # Iota
        '\u039a': 'K', '\u03ba': 'k', # Kappa
        '\u039d': 'N', '\u03bd': 'v', # Nu
        '\u039f': 'O', '\u03bf': 'o', # Omicron
        '\u03a1': 'P', '\u03c1': 'p', # Rho
        '\u03a4': 'T', '\u03c4': 't', # Tau
        '\u03a5': 'Y', '\u03c5': 'y', # Upsilon
        '\u03a7': 'X', '\u03c7': 'x', # Chi
    })

def clean_string(text: Any, config: Optional[CleaningConfig] = None) -> Any:
    """Robustly cleans a single value."""
    if config is None: config = CleaningConfig()
    if text is None or (isinstance(text, float) and np.isnan(text)):
        return None if config.handle_na else text
    if not isinstance(text, str): return text
    if config.normalize_unicode:
        # type: ignore
        text = unicodedata.normalize(config.normalize_unicode, text)
    for old, new in config.custom_map.items():
        text = text.replace(old, new)
    if config.remove_control_chars:
        text = "".join(ch for ch in text if ch in "\n\r\t" or unicodedata.category(ch)[0] != "C")
    if config.remove_special_chars:
        text = re.sub(r'[^\w\s\n\r\t]', '', text)
    if config.trim: text = text.strip()
    return text

def clean_series(s: pd.Series, config: Optional[CleaningConfig] = None) -> pd.Series:
    """Robustly cleans a pandas Series using vectorized operations."""
    if config is None: config = CleaningConfig()
    if not pd.api.types.is_object_dtype(s.dtype) and not pd.api.types.is_string_dtype(s.dtype):
        return s
    
    # 1. NA Handling (avoid unconditional astype(str))
    working_s = s.copy()
    if config.handle_na:
        working_s = working_s.replace(['nan', 'None', 'NAT', 'NAN', 'null'], np.nan)
        
    # 2. Vectorized operations on string part
    str_part = working_s.astype(str).where(working_s.notna())
    
    # Custom Map Replacements
    for old, new in config.custom_map.items():
        str_part = str_part.str.replace(old, new, regex=False)
        
    # Unicode Normalization
    if config.normalize_unicode:
        str_part = str_part.str.normalize(config.normalize_unicode)
        
    # Control Characters (C0 and C1 control blocks, preserving \n\r\t)
    if config.remove_control_chars:
        str_part = str_part.str.replace(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', regex=True)

    if config.remove_special_chars:
        str_part = str_part.str.replace(r'[^\w\s\n\r\t]', '', regex=True)
        
    # Trim
    if config.trim:
        str_part = str_part.str.strip()
        
    # Put back into series (preserve NaNs)
    return str_part.where(working_s.notna(), np.nan if config.handle_na else working_s)

def clean_headers(df: pd.DataFrame, config: Optional[CleaningConfig] = None) -> pd.DataFrame:
    """Robustly cleans DataFrame headers to DB-safe identifiers."""
    if config is None: config = CleaningConfig()
    new_cols = []
    for col in df.columns:
        c = clean_string(str(col), config)
        if config.lowercase_headers: c = c.lower()
        c = re.sub(r'[^a-zA-Z0-9_]', '_', c)
        c = re.sub(r'__+', '_', c).strip('_')
        new_cols.append(c if c else f'col_{len(new_cols)+1}') # fallback for empty headers
        
    # Handle duplicates
    seen = {}
    final_cols = []
    for c in new_cols:
        if c in seen:
            seen[c] += 1
            final_cols.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            final_cols.append(c)
            
    df.columns = final_cols
    return df

def robust_clean(df: pd.DataFrame, config: Optional[CleaningConfig] = None, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Perform a robust clean of the entire DataFrame headers and data."""
    if config is None: config = CleaningConfig()
    df = clean_headers(df.copy(), config)
    target_cols = columns if columns else df.select_dtypes(include=['object', 'string']).columns
    for col in target_cols:
        # type: ignore
        df[col] = clean_series(df[col], config)
    return df

def quick_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Standard robust cleanup: BOM, Trim, Smart Chars, Controls."""
    return robust_clean(df, CleaningConfig(lowercase_headers=False))
