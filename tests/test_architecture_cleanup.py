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
REMOVED_LEGACY_FILES = [
    "command_gateway.py",
    "control_runtime.py",
    "control_config.py",
    "database.py",
    "influx_writer.py",
    "openclaw_client.py",
    "operator_cli.py",
    "dashboard_server.py",
    "smart_bridge.py",
    "runtime_profiles.json",
    "openclaw_policy_schema.json",
    "system_prompt_core.txt",
    "system_prompt_schema.txt",
    "docs/legacy_compatibility.md",
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
    "import smart_bridge",
    "from smart_bridge",
]


class ArchitectureCleanupTests(unittest.TestCase):
    def test_removed_legacy_files_are_absent(self) -> None:
        for filename in REMOVED_LEGACY_FILES:
            self.assertFalse((REPO_ROOT / filename).exists(), filename)
        self.assertFalse((REPO_ROOT / "legacy").exists())

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
