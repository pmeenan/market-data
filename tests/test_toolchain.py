import subprocess
import sys
from pathlib import Path

CHECKER = Path(__file__).resolve().parent.parent / "tools" / "check_licenses.py"


def _run_check(tmp_path, lock: str, inventory: str) -> subprocess.CompletedProcess:
    lock_path = tmp_path / "uv.lock"
    project_path = tmp_path / "pyproject.toml"
    inventory_path = tmp_path / "dependency-licenses.toml"
    lock_path.write_text(lock)
    project_path.write_text("")
    inventory_path.write_text(inventory)
    return subprocess.run(
        [
            sys.executable,
            CHECKER,
            "--lock",
            lock_path,
            "--project",
            project_path,
            "--inventory",
            inventory_path,
        ],
        capture_output=True,
        text=True,
        check=False,
    )


def test_license_policy_covers_platform_marked_lock_packages(tmp_path):
    lock = """
[[package]]
name = "certifi"
version = "1"
source = { registry = "https://pypi.org/simple" }
[[package]]
name = "base"
version = "1"
source = { registry = "https://pypi.org/simple" }
[[package]]
name = "windows-only"
version = "1"
source = { registry = "https://pypi.org/simple" }
"""
    inventory = """
schema-version = 1
[[package]]
name = "certifi"
version = "1"
license = "MPL-2.0"
source = "test metadata"
[[package]]
name = "base"
version = "1"
license = "MIT"
source = "test metadata"
[[package]]
name = "windows-only"
version = "1"
license = "BSD-3-Clause"
source = "test metadata"
"""

    result = _run_check(tmp_path, lock, inventory)

    assert result.returncode == 0, result.stderr


def test_license_policy_rejects_absent_named_exception(tmp_path):
    lock = """
[[package]]
name = "base"
version = "1"
source = { registry = "https://pypi.org/simple" }
"""
    inventory = """
schema-version = 1
[[package]]
name = "base"
version = "1"
license = "MIT"
source = "test metadata"
"""

    result = _run_check(tmp_path, lock, inventory)

    assert result.returncode == 1
    assert "required license exception is absent or changed" in result.stderr
