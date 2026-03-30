"""Deprecated compatibility shim for the legacy Influx retry writer."""

from __future__ import annotations

import warnings

warnings.warn(
    "influx_writer.py moved to legacy.influx_writer. Active telemetry history integration lives in backend.state.influx.",
    DeprecationWarning,
    stacklevel=2,
)

from legacy.influx_writer import *  # noqa: F401,F403
