from __future__ import annotations

import logging
import warnings

from backend.runtime import BackendRuntime


log = logging.getLogger("smart_bridge")


def main() -> int:
    warnings.warn(
        "smart_bridge.py is deprecated. The runtime is now backend-centric; use backend_server.py for the canonical entrypoint.",
        DeprecationWarning,
        stacklevel=2,
    )
    log.warning("smart_bridge compatibility entrypoint delegates to backend.runtime.BackendRuntime")
    BackendRuntime().run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
