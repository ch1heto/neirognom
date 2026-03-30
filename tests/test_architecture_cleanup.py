from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parent.parent
ACTIVE_CODE_ROOTS = [
    REPO_ROOT / "backend",
    REPO_ROOT / "integrations",
    REPO_ROOT / "mqtt",
    REPO_ROOT / "shared",
]
LEGACY_SHIMS = [
    "command_gateway.py",
    "control_runtime.py",
    "control_config.py",
    "database.py",
    "influx_writer.py",
    "openclaw_client.py",
    "operator_cli.py",
    "dashboard_server.py",
]
BANNED_IMPORT_SNIPPETS = [
    "import database",
    "from database",
    "import openclaw_client",
    "from openclaw_client",
    "import command_gateway",
    "from command_gateway",
    "import control_runtime",
    "from control_runtime",
    "import influx_writer",
    "from influx_writer",
]


class ArchitectureCleanupTests(unittest.TestCase):
    def test_legacy_modules_are_isolated_under_legacy_directory(self) -> None:
        legacy_root = REPO_ROOT / "legacy"
        self.assertTrue((legacy_root / "__init__.py").exists())
        for filename in LEGACY_SHIMS:
            self.assertTrue((REPO_ROOT / filename).exists(), filename)
            self.assertTrue((legacy_root / filename).exists(), filename)

    def test_active_runtime_code_does_not_import_legacy_root_modules(self) -> None:
        offenders: list[str] = []
        for root in ACTIVE_CODE_ROOTS:
            for path in root.rglob("*.py"):
                text = path.read_text(encoding="utf-8")
                for snippet in BANNED_IMPORT_SNIPPETS:
                    if snippet in text:
                        offenders.append(f"{path.relative_to(REPO_ROOT)} -> {snippet}")
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
