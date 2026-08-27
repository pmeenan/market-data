"""Verify that every locked dependency has a reviewed SPDX license."""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

PERMISSIVE_SPDX_IDS = frozenset(
    {
        "0BSD",
        "Apache-2.0",
        "BSD-2-Clause",
        "BSD-3-Clause",
        "CC0-1.0",
        "ISC",
        "MIT",
        "PSF-2.0",
        "Zlib",
    }
)
REQUIRED_EXCEPTIONS = {("certifi", "MPL-2.0"): "D-018"}
_NAME_NORMALIZER = re.compile(r"[-_.]+")
_SPDX_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9.-]*")
_EXACT_REQUIREMENT = re.compile(r"^([A-Za-z0-9_.-]+)==([^;\s]+)$")


def _canonical_name(name: str) -> str:
    return _NAME_NORMALIZER.sub("-", name).lower()


def _locked_packages(lock: dict[str, Any], errors: list[str]) -> set[tuple[str, str]]:
    packages: set[tuple[str, str]] = set()
    for package in lock.get("package", []):
        source = package.get("source", {})
        if "registry" in source:
            packages.add((_canonical_name(package["name"]), package["version"]))
        elif source.get("editable") != ".":
            errors.append(
                f"locked dependency has an unsupported source: {package['name']}"
            )
    return packages


def _build_packages(project: dict[str, Any], errors: list[str]) -> set[tuple[str, str]]:
    packages: set[tuple[str, str]] = set()
    for requirement in project.get("build-system", {}).get("requires", []):
        match = _EXACT_REQUIREMENT.fullmatch(requirement)
        if match is None:
            errors.append(f"build requirement is not exactly pinned: {requirement}")
        else:
            packages.add((_canonical_name(match.group(1)), match.group(2)))
    return packages


def _is_permissive(expression: str) -> bool:
    identifiers = {
        token
        for token in _SPDX_TOKEN.findall(expression)
        if token not in {"AND", "OR", "WITH"}
    }
    return bool(identifiers) and identifiers <= PERMISSIVE_SPDX_IDS


def validate(
    lock: dict[str, Any],
    project: dict[str, Any],
    inventory: dict[str, Any],
    *,
    required_exceptions: dict[tuple[str, str], str] = REQUIRED_EXCEPTIONS,
) -> list[str]:
    """Return policy violations for parsed lock, project, and inventory data."""
    errors: list[str] = []
    if inventory.get("schema-version") != 1:
        errors.append("dependency license inventory has an unsupported schema-version")

    locked = _locked_packages(lock, errors) | _build_packages(project, errors)
    reviewed: dict[tuple[str, str], dict[str, str]] = {}
    for package in inventory.get("package", []):
        key = (_canonical_name(package["name"]), package["version"])
        if key in reviewed:
            errors.append(f"duplicate license inventory entry: {key[0]}=={key[1]}")
        reviewed[key] = package

    for name, version in sorted(locked - reviewed.keys()):
        errors.append(f"locked dependency has no license review: {name}=={version}")
    for name, version in sorted(reviewed.keys() - locked):
        errors.append(
            f"stale license review is not in the lock/build: {name}=={version}"
        )

    observed_exceptions: set[tuple[str, str]] = set()
    for (name, version), package in sorted(reviewed.items()):
        license_expression = package.get("license", "")
        exception_key = (name, license_expression)
        if exception_key in required_exceptions:
            observed_exceptions.add(exception_key)
        elif not _is_permissive(license_expression):
            errors.append(
                f"dependency is not permissively licensed: "
                f"{name}=={version} ({license_expression or 'missing SPDX expression'})"
            )
        if not package.get("source"):
            errors.append(f"license review has no metadata source: {name}=={version}")

    for exception, decision in required_exceptions.items():
        if exception not in observed_exceptions:
            errors.append(
                f"required license exception is absent or changed: "
                f"{exception[0]} ({exception[1]}, {decision})"
            )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", type=Path, default=Path("uv.lock"))
    parser.add_argument("--project", type=Path, default=Path("pyproject.toml"))
    parser.add_argument(
        "--inventory", type=Path, default=Path("dependency-licenses.toml")
    )
    args = parser.parse_args()

    lock = tomllib.loads(args.lock.read_text())
    project = tomllib.loads(args.project.read_text())
    inventory = tomllib.loads(args.inventory.read_text())
    errors = validate(lock, project, inventory)
    if errors:
        for error in errors:
            print(f"license check: {error}", file=sys.stderr)
        return 1
    print(
        f"License policy covers {len(inventory['package'])} locked/build dependencies."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
