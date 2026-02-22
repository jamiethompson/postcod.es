from __future__ import annotations

import sys
from pathlib import Path


SRC_PATH = Path(__file__).resolve().parents[1] / "src"
SRC_PATH_STR = str(SRC_PATH)

if SRC_PATH_STR not in sys.path:
    sys.path.insert(0, SRC_PATH_STR)
