#!/usr/bin/env python3
"""Capture code, package, API, and container provenance for the run."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from experiment_lib import EXPERIMENT_DIR, MANIFESTS_DIR, sha256_file, write_json


def command(*args: str) -> str | None:
    try:
        return subprocess.run(
            args,
            cwd=EXPERIMENT_DIR.parents[1],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def package_versions(names: list[str]) -> dict[str, str | None]:
    versions: dict[str, str | None] = {}
    for name in names:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = None
    return versions


def main() -> None:
    tracked_patterns = [
        "README.md",
        "requirements.txt",
        "docker-compose.yml",
        "config/*.json",
        "scripts/*.py",
        "scripts/*.sh",
        "source-db/**/*.sql",
        "source-db/**/Dockerfile",
    ]
    files: dict[str, str] = {}
    for pattern in tracked_patterns:
        for path in sorted(EXPERIMENT_DIR.glob(pattern)):
            if path.is_file():
                files[str(path.relative_to(EXPERIMENT_DIR))] = sha256_file(path)

    openapi = requests.get("http://localhost:8004/openapi.json", timeout=30)
    openapi.raise_for_status()
    canonical_openapi = json.dumps(
        openapi.json(), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")

    containers_raw = command(
        "docker",
        "ps",
        "--format",
        "{{json .}}",
    )
    containers: list[dict[str, Any]] = []
    if containers_raw:
        for line in containers_raw.splitlines():
            item = json.loads(line)
            if "datafabric" in item.get("Names", "") or "dermexp" in item.get("Names", ""):
                containers.append(
                    {
                        key: item[key]
                        for key in (
                            "Names",
                            "Image",
                            "State",
                            "Status",
                            "HealthStatus",
                            "Ports",
                            "Networks",
                        )
                        if key in item
                    }
                )

    git_status = command("git", "status", "--short") or ""
    manifest = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "python": {
            "version": sys.version,
            "platform": platform.platform(),
            "packages": package_versions(
                ["pandas", "matplotlib", "seaborn", "requests"]
            ),
        },
        "repository": {
            "commit": command("git", "rev-parse", "HEAD"),
            "dirty": bool(git_status),
            "status_short": git_status.splitlines(),
            "note": "Pre-existing non-experiment changes were not modified by this run.",
        },
        "experiment_file_sha256": files,
        "backend_openapi_sha256": hashlib.sha256(canonical_openapi).hexdigest(),
        "containers": containers,
    }
    write_json(MANIFESTS_DIR / "environment_manifest.json", manifest)
    print(
        f"Captured {len(files)} experiment file hashes and "
        f"{len(containers)} running containers"
    )


if __name__ == "__main__":
    main()
