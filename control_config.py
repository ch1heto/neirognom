"""Deprecated compatibility shim for legacy bridge runtime configuration."""

from __future__ import annotations

import warnings

warnings.warn(
    "control_config.py moved to legacy.control_config and is retained only for bridge-era compatibility.",
    DeprecationWarning,
    stacklevel=2,
)

from legacy.control_config import *  # noqa: F401,F403
