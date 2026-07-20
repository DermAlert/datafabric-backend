#!/usr/bin/env python3
"""Fail closed if the governed experiment package is incomplete or inconsistent."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from experiment_lib import EXPERIMENT_DIR, FIGURES_DIR, MANIFESTS_DIR, RESULTS_DIR, sha256_file, write_json


CONDITIONS = {
    "G2_DERM_CANCER_HISTO": 2689,
    "G3_CLINICAL_CANCER_PHOTOTYPE": 1299,
    "G4_CC_BY_POLICY": 3914,
}
FIGURES = [
    "governed_fig1_cohort_views",
    "governed_fig2_split_leakage",
    "governed_fig3_phototype_sufficiency",
    "governed_fig4_semantic_evolution",
    "governed_fig5_assurance_matrix",
]
PUBLIC_RESULTS = [
    "governed_article_metrics.json",
    "governed_assurance_checks.csv",
    "governed_backend_oracle_agreement.csv",
    "governed_backend_versions.csv",
    "governed_cohort_funnel.csv",
    "governed_phototype_sufficiency.csv",
    "governed_semantic_version_impact.csv",
    "governed_split_leakage.csv",
]


def load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def assert_true(checks: dict[str, bool], name: str, value: bool) -> None:
    checks[name] = bool(value)
    if not value:
        raise AssertionError(name)


def forbidden_persistence_scan() -> list[str]:
    findings = []
    patterns = ["X-Amz-Credential", "X-Amz-Signature", "AWSAccessKeyId=", '"bearer_token"', '"token":']
    paths = [
        MANIFESTS_DIR / "governed_datafabric_run.json",
        RESULTS_DIR / "governed_sharing_validation.json",
        EXPERIMENT_DIR / "logs" / "governed_datafabric_api_events.json",
    ]
    for path in paths:
        text = path.read_text(encoding="utf-8")
        for pattern in patterns:
            if pattern in text:
                findings.append(f"{path.relative_to(EXPERIMENT_DIR)}:{pattern}")
    return findings


def main() -> None:
    checks: dict[str, bool] = {}
    backend = pd.read_csv(RESULTS_DIR / "governed_backend_versions.csv")
    oracle = pd.read_csv(RESULTS_DIR / "governed_backend_oracle_agreement.csv")
    compliance = pd.read_csv(RESULTS_DIR / "governed_rule_compliance.csv")
    sharing = load(RESULTS_DIR / "governed_sharing_validation.json")
    manifest = load(MANIFESTS_DIR / "governed_datafabric_run.json")
    metrics = load(RESULTS_DIR / "governed_article_metrics.json")

    assert_true(checks, "protocol_frozen", manifest["protocol_version"] == "derm-governed-cohorts-v1.0.0")
    assert_true(checks, "input_count", metrics["input_images"] == 15634)
    assert_true(checks, "condition_set", set(backend["condition"]) == set(CONDITIONS))
    for condition, expected_rows in CONDITIONS.items():
        rows = backend[backend["condition"] == condition]
        assert_true(checks, f"{condition}_versions_0_1_2", set(rows["delta_version"]) == {0, 1, 2})
        assert_true(checks, f"{condition}_row_count", set(rows["rows"]) == {expected_rows})
        assert_true(checks, f"{condition}_stable_hash", rows["semantic_sha256"].nunique() == 1)
        assert_true(checks, f"{condition}_oracle", bool(rows["exact_oracle_agreement"].all()))
    assert_true(checks, "oracle_zero_missing", int(oracle["missing_expected_ids"].sum()) == 0)
    assert_true(checks, "oracle_zero_unexpected", int(oracle["unexpected_ids"].sum()) == 0)
    assert_true(
        checks,
        "rule_compliance_100_percent",
        bool((compliance["output_compliance_percent"] == 100.0).all()),
    )
    assert_true(checks, "sharing_delivery", sharing["delivery_validated"] is True)
    assert_true(checks, "sharing_access_control", sharing["access_control_enforced"] is True)
    assert_true(
        checks,
        "sharing_pin_independent",
        sharing["pinned_release_independent_of_producer_head"] is True,
    )
    assert_true(
        checks,
        "all_shared_files_are_parquet",
        all(item["sample_has_parquet_magic"] for item in sharing["tables"].values()),
    )
    assert_true(checks, "no_persisted_credentials_or_urls", not forbidden_persistence_scan())

    artifact_hashes: dict[str, str] = {}
    for stem in FIGURES:
        png = FIGURES_DIR / f"{stem}.png"
        assert_true(checks, f"{stem}_png", png.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n")
        artifact_hashes[str(png.relative_to(EXPERIMENT_DIR))] = sha256_file(png)
    for name in PUBLIC_RESULTS:
        path = RESULTS_DIR / name
        assert_true(checks, f"{name}_nonempty", path.stat().st_size > 100)
        artifact_hashes[str(path.relative_to(EXPERIMENT_DIR))] = sha256_file(path)
    for name in ["README.md", "requirements.txt"]:
        path = EXPERIMENT_DIR / name
        assert_true(checks, f"{name}_nonempty", path.stat().st_size > 50)
        artifact_hashes[str(path.relative_to(EXPERIMENT_DIR))] = sha256_file(path)

    report = {
        "status": "PASS",
        "checks_passed": sum(checks.values()),
        "checks_total": len(checks),
        "checks": checks,
        "artifact_sha256": artifact_hashes,
        "forbidden_persistence_findings": [],
    }
    write_json(RESULTS_DIR / "governed_artifact_validation.json", report)
    print(f"Governed artifact package: PASS ({len(checks)}/{len(checks)} checks)")


if __name__ == "__main__":
    main()
