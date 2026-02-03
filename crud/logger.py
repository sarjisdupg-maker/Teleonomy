"""
Lightweight function call + snapshot logger with 10-min file bucketing.
"""
import os
from functools import wraps
from datetime import datetime
from pathlib import Path
import inspect
import json
import yaml
import sys

import pandas as pd

try:
    from sqlalchemy.engine import Engine
    HAS_SQLALCHEMY = True
except ImportError:
    Engine = None
    HAS_SQLALCHEMY = False

# ===================== CONFIG =====================

_warned_config = False
_warned_logfile = False


def _print_once(msg: str, flag: str) -> None:
    """Print a warning to stderr once per flag type (config/logfile)."""
    global _warned_config, _warned_logfile
    if flag == "config" and not _warned_config:
        print(msg, file=sys.stderr)
        _warned_config = True
    elif flag == "logfile" and not _warned_logfile:
        print(msg, file=sys.stderr)
        _warned_logfile = True


def load_config():
    """Load logging config from config.yml (CWD or project root), with safe defaults.

    Resolution order:
    - CWD/config.yml
    - project_root/config.yml (one level above this file)
    """
    # Resolve config from CWD/config.yml or project root config.yml
    cwd_path = Path.cwd() / "config.yml"
    try:
        module_root = Path(__file__).resolve().parents[1]
    except Exception:
        module_root = Path.cwd()
    alt_path = module_root / "config.yml"
    config_path = cwd_path if cwd_path.exists() else (alt_path if alt_path.exists() else None)

    defaults = {
        "logging": {
            "enabled": False,
            "dir": "log",
            "max_repr_len": 2000,
            "bucket_minutes": 10,
        }
    }

    if config_path:
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                user_config = yaml.safe_load(f)
                if user_config and "logging" in user_config:
                    # Merge user config with defaults
                    defaults["logging"].update(user_config["logging"])
        except Exception as e:
            _print_once(f"[logger] failed to load {config_path}: {e} (using defaults)", "config")
    else:
        _print_once("[logger] config.yml not found; using defaults", "config")

    return defaults["logging"]


_LOG_CONFIG = load_config()

LOG_DIR = Path.cwd() / _LOG_CONFIG.get("dir", "log")
LOG_DIR.mkdir(parents=True, exist_ok=True)
SEP = "-" * 42
MAX_REPR_LEN = _LOG_CONFIG.get("max_repr_len", 2000)
BUCKET_MINUTES = _LOG_CONFIG.get("bucket_minutes", 10)

# Environment variable override
ENV_LOGGING = os.getenv("ETL_LOGGING")
if ENV_LOGGING is not None:
    ENABLE_LOGGING = ENV_LOGGING.lower() == "true"
else:
    ENABLE_LOGGING = _LOG_CONFIG.get("enabled", False)

# ===================== CORE =====================


def _bucketed_filename():
    now = datetime.now()
    minute = (now.minute // BUCKET_MINUTES) * BUCKET_MINUTES
    ts = now.replace(minute=minute, second=0).strftime("%d%m%Y-%H%M")
    return LOG_DIR / f"log_{ts}.txt"


def _safe_str(obj, maxlen=MAX_REPR_LEN):
    if obj is None:
        return "None"

    if isinstance(obj, (str, int, float, bool)):
        return str(obj)

    if isinstance(obj, bytes):
        s = obj[:maxlen].hex()
        return s + ("..." if len(obj) > maxlen else "")

    if isinstance(obj, pd.DataFrame):
        return f"DataFrame(shape={obj.shape}, columns={list(obj.columns)})"

    if isinstance(obj, pd.Series):
        return f"Series(len={len(obj)}, name={obj.name})"

    if HAS_SQLALCHEMY and Engine is not None and isinstance(obj, Engine):
        return f"SQLAlchemyEngine(url={obj.url})"

    try:
        s = repr(obj)
        return s[:maxlen] + ("..." if len(s) > maxlen else "")
    except Exception:
        return f"<unserializable {type(obj).__name__}>"


# ===================== DECORATOR =====================


def log_call(func):
    """Decorator that logs function call inputs and outputs/errors if ETL_LOGGING is enabled."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        if not ENABLE_LOGGING:
            return func(*args, **kwargs)

        mod = func.__module__
        name = func.__qualname__
        fname = _bucketed_filename()

        # Log inputs
        try:
            with fname.open("a", encoding="utf-8") as f:
                f.write(f"# {mod} - {name} (START)\n")
                f.write("- inputs:\n")
                try:
                    sig = inspect.signature(func)
                    bound = sig.bind_partial(*args, **kwargs)
                    for k, v in bound.arguments.items():
                        f.write(f"  {k}: {_safe_str(v)}\n")
                except Exception as e:
                    f.write(f"  <failed to parse signature: {e}>\n")
                f.write("\n")
        except Exception as e:
            _print_once(f"[logger] failed to write to {fname}: {e}", "logfile")

        try:
            out = func(*args, **kwargs)
            # Log success
            try:
                with fname.open("a", encoding="utf-8") as f:
                    f.write(f"# {mod} - {name} (SUCCESS)\n")
                    f.write("- outputs:\n")
                    f.write(f"  {_safe_str(out)}\n")
                    f.write(f"{SEP}\n")
            except Exception as e2:
                _print_once(f"[logger] failed to write to {fname}: {e2}", "logfile")
            return out
        except Exception as e:
            # Log failure
            try:
                with fname.open("a", encoding="utf-8") as f:
                    f.write(f"# {mod} - {name} (FAILURE)\n")
                    f.write(f"- error: {type(e).__name__}: {str(e)}\n")
                    f.write(f"{SEP}\n")
            except Exception as e3:
                _print_once(f"[logger] failed to write to {fname}: {e3}", "logfile")
            raise e

    return wrapper


# ===================== SNAPSHOT HELPERS =====================


def log_string(label: str, value: str):
    """Append labeled string snapshot."""
    if not ENABLE_LOGGING:
        return
    if not isinstance(value, str):
        raise TypeError("log_string expects str")

    fname = _bucketed_filename()
    try:
        with fname.open("a", encoding="utf-8") as f:
            f.write(f"# STRING - {label}\n\n")
            f.write(value + "\n")
            f.write(f"{SEP}\n")
    except Exception as e:
        _print_once(f"[logger] failed to write to {fname}: {e}", "logfile")


def log_json(label: str, obj):
    """Append JSON snapshot (best-effort serialization)."""
    if not ENABLE_LOGGING:
        return
    try:
        payload = json.dumps(obj, indent=2, default=str)
    except Exception as e:
        payload = f"<json serialization failed: {e}>"

    fname = _bucketed_filename()
    try:
        with fname.open("a", encoding="utf-8") as f:
            f.write(f"# JSON - {label}\n\n")
            f.write(payload + "\n")
            f.write(f"{SEP}\n")
    except Exception as e2:
        _print_once(f"[logger] failed to write to {fname}: {e2}", "logfile")


def log_dataframe(label: str, df: pd.DataFrame, max_rows=20):
    """Append dataframe snapshot (head only)."""
    if not ENABLE_LOGGING:
        return
    if not isinstance(df, pd.DataFrame):
        raise TypeError("log_dataframe expects pandas.DataFrame")

    fname = _bucketed_filename()
    try:
        with fname.open("a", encoding="utf-8") as f:
            f.write(f"# DATAFRAME - {label}\n\n")
            f.write(f"shape={df.shape}\n\n")
            f.write(df.head(max_rows).to_string(index=False))
            if len(df) > max_rows:
                f.write(f"\n... ({len(df) - max_rows} more rows)")
            f.write(f"\n{SEP}\n")
    except Exception as e:
        _print_once(f"[logger] failed to write to {fname}: {e}", "logfile")


def conditional_log_call(func):
    if ENABLE_LOGGING:
        return log_call(func)
    return func
