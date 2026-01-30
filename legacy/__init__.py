"""Legacy module package with import compatibility."""
from __future__ import annotations

import sys
from pathlib import Path

_legacy_dir = Path(__file__).resolve().parent
if str(_legacy_dir) not in sys.path:
    sys.path.insert(0, str(_legacy_dir))
