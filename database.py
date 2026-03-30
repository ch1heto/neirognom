"""Deprecated compatibility shim for the legacy bridge event store."""

from __future__ import annotations

import warnings

warnings.warn(
    "database.py moved to legacy.database. Active backend state lives in backend.state.store.",
    DeprecationWarning,
    stacklevel=2,
)

from legacy.database import *  # noqa: F401,F403
