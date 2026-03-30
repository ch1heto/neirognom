"""Deprecated compatibility shim for the legacy bridge control runtime."""

from __future__ import annotations

import warnings

warnings.warn(
    "control_runtime.py moved to legacy.control_runtime and is not part of the active backend runtime.",
    DeprecationWarning,
    stacklevel=2,
)

from legacy.control_runtime import *  # noqa: F401,F403
