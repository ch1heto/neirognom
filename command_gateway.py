"""Deprecated compatibility shim for the legacy bridge command gateway."""

from __future__ import annotations

import warnings

warnings.warn(
    "command_gateway.py moved to legacy.command_gateway and is not part of the active backend runtime.",
    DeprecationWarning,
    stacklevel=2,
)

from legacy.command_gateway import *  # noqa: F401,F403
