"""Deprecated compatibility shim for the legacy operator CLI."""

from __future__ import annotations

import warnings

warnings.warn(
    "operator_cli.py moved to legacy.operator_cli. Use the backend operator UI/API for active manual control.",
    DeprecationWarning,
    stacklevel=2,
)

from legacy.operator_cli import *  # noqa: F401,F403
