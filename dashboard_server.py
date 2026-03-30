"""Deprecated compatibility shim for the legacy diagnostic dashboard."""

from __future__ import annotations

import warnings

warnings.warn(
    "dashboard_server.py moved to legacy.dashboard_server. Use backend_server.py and operator_ui.html for the active surfaces.",
    DeprecationWarning,
    stacklevel=2,
)

from legacy.dashboard_server import *  # noqa: F401,F403
