from __future__ import annotations

from backend.runtime import BackendRuntime


def main() -> int:
    BackendRuntime().run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
