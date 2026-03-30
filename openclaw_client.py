"""Deprecated compatibility shim for the legacy OpenClaw transport helper."""

from __future__ import annotations

import warnings

warnings.warn(
    "openclaw_client.py moved to legacy.openclaw_client and is not part of the active control runtime.",
    DeprecationWarning,
    stacklevel=2,
)

from legacy.openclaw_client import *  # noqa: F401,F403
