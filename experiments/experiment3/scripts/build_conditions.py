#!/usr/bin/env python3
"""Build the C0 raw federation and C1 harmonized reference materializations."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

from experiment_lib import (
    CONFIG_DIR,
    DERIVED_DIR,
    EXPERIMENT_DIR,
    MANIFESTS_DIR,
    ensure_output_dirs,
    mapping_config,
    present_mask,
    read_source,
    sha256_file,
    source_config,
    stable_csv,
    write_json,
)


def text_column(df: pd.DataFrame, name: str) -> pd.Series:
    if name not in df.columns:
        return pd.Series(pd.NA, index=df.index, dtype="string")
    output = df[name].astype("string").str.strip()
    return output.mask(~present_mask(df[name]), pd.NA)


def age_group(value: Any, groups: list[dict[str, Any]]) -> str:
    if pd.isna(value):
        return "MISSING"
    numeric = float(value)
    for group in groups:
        if group["minimum"] <= numeric < group["maximum_exclusive"]:
            return str(group["label"])
    return "UNRESOLVED"


def normalize_from_map(
    values: pd.Series,
    mapping: dict[str, str],
    *,
    missing: str = "MISSING",
    unresolved: str = "UNRESOLVED",
) -> pd.Series:
    normalized = values.map(mapping)
    normalized = normalized.mask(values.isna(), missing)
    return normalized.fillna(unresolved).astype("string")


def build_silver(
    raw: pd.DataFrame,
    mappings: dict[str, Any],
    source_hashes: dict[str, str],
) -> pd.DataFrame:
    silver = pd.DataFrame(index=raw.index)
    silver["isic_id"] = text_column(raw, "isic_id")
    silver["source_collection_id"] = raw["source_collection_id"].astype("int64")
    silver["source_collection_name"] = text_column(raw, "source_collection_name")
    silver["source_key"] = text_column(raw, "source_key")
    silver["source_record_version"] = silver["source_key"].map(
        {key: checksum[:16] for key, checksum in source_hashes.items()}
    )

    silver["patient_id"] = text_column(raw, "patient_id")
    silver["patient_uid"] = (
        silver["source_key"] + ":" + silver["patient_id"]
    ).mask(silver["patient_id"].isna(), pd.NA)
    silver["lesion_id"] = text_column(raw, "lesion_id")
    silver["lesion_uid"] = (
        silver["source_key"] + ":" + silver["lesion_id"]
    ).mask(silver["lesion_id"].isna(), pd.NA)

    silver["image_type_original"] = text_column(raw, "image_type")
    silver["image_type_harmonized"] = normalize_from_map(
        silver["image_type_original"], mappings["image_type"]
    )

    for level in range(1, 6):
        silver[f"diagnosis_original_level_{level}"] = text_column(
            raw, f"diagnosis_{level}"
        )
    diagnosis_map = mappings["diagnosis"]

    def diagnosis_value(value: Any, field: str, missing_value: str) -> str:
        if pd.isna(value):
            return missing_value
        entry = diagnosis_map.get(str(value))
        return str(entry.get(field, "")) if entry and entry.get(field) else "UNRESOLVED"

    original_diagnosis = silver["diagnosis_original_level_3"]
    silver["diagnosis_harmonized"] = original_diagnosis.map(
        lambda value: diagnosis_value(value, "code", "MISSING")
    )
    silver["diagnosis_candidate"] = original_diagnosis.map(
        lambda value: diagnosis_value(value, "candidate_code", "MISSING")
    )

    def mapping_status(value: Any) -> str:
        if pd.isna(value):
            return "SOURCE_MISSING"
        entry = diagnosis_map.get(str(value))
        return str(entry["status"]) if entry else "UNMAPPED"

    silver["diagnosis_mapping_status"] = original_diagnosis.map(mapping_status)
    silver["diagnosis_confirm_type"] = text_column(raw, "diagnosis_confirm_type")

    silver["age_approx"] = pd.to_numeric(raw.get("age_approx"), errors="coerce")
    silver["age_group"] = silver["age_approx"].map(
        lambda value: age_group(value, mappings["age_groups"])
    )
    silver["recorded_sex_original"] = text_column(raw, "sex")
    silver["recorded_sex"] = normalize_from_map(
        silver["recorded_sex_original"], mappings["recorded_sex"]
    )

    for level in range(1, 6):
        silver[f"anatom_site_original_{level}"] = text_column(
            raw, f"anatom_site_{level}"
        )
    silver["anatom_site_special_original"] = text_column(raw, "anatom_site_special")
    # ISIC anatom_site_1 is already the common broad taxonomy. Harmonization
    # therefore selects that broad level while preserving all more-specific
    # original levels; it does not invent a second spelling convention.
    silver["anatom_site_harmonized"] = silver[
        "anatom_site_original_1"
    ].fillna("MISSING")

    fitzpatrick = text_column(raw, "fitzpatrick_skin_type")
    allowed = set(mappings["fitzpatrick_allowed"])
    silver["fitzpatrick_skin_type"] = fitzpatrick.map(
        lambda value: "MISSING"
        if pd.isna(value)
        else (str(value) if str(value) in allowed else "UNRESOLVED")
    )

    silver["attribution"] = text_column(raw, "attribution")
    silver["copyright_license"] = text_column(raw, "copyright_license")
    silver["bronze_version"] = silver["source_record_version"]
    silver["semantic_catalog_version"] = mappings["catalog_version"]
    silver["silver_rule_version"] = mappings["silver_rule_version"]

    known_diagnosis = ~silver["diagnosis_harmonized"].isin(
        ["MISSING", "UNRESOLVED"]
    )
    silver["included_in_audit_cohort"] = True
    silver["included_in_dermoscopy_cohort"] = (
        silver["image_type_harmonized"].eq("DERMOSCOPIC") & known_diagnosis
    )
    silver["included_in_clinical_phototype_cohort"] = (
        silver["image_type_harmonized"].isin(
            ["CLINICAL_OVERVIEW", "CLINICAL_CLOSE_UP"]
        )
        & silver["fitzpatrick_skin_type"].isin(allowed)
    )

    reasons: list[str] = []
    for _, row in silver.iterrows():
        row_reasons: list[str] = []
        if row["image_type_harmonized"] == "MISSING":
            row_reasons.append("IMAGE_TYPE_MISSING")
        if row["diagnosis_harmonized"] == "MISSING":
            row_reasons.append("DIAGNOSIS_MISSING")
        elif row["diagnosis_harmonized"] == "UNRESOLVED":
            row_reasons.append("DIAGNOSIS_REVIEW_REQUIRED")
        if row["fitzpatrick_skin_type"] == "MISSING":
            row_reasons.append("PHOTOTYPE_MISSING_OR_NOT_PROVIDED")
        reasons.append(";".join(row_reasons))
    silver["audit_flags"] = reasons
    return silver


def build(output_dir: Path) -> dict[str, Any]:
    ensure_output_dirs()
    output_dir.mkdir(parents=True, exist_ok=True)
    sources = source_config()
    mappings = mapping_config()

    frames: list[pd.DataFrame] = []
    source_hashes: dict[str, str] = {}
    source_rows: dict[str, int] = {}
    for source in sources["sources"]:
        frame = read_source(source)
        source_path = MANIFESTS_DIR.parent / "raw" / source["local_filename"]
        source_hashes[source["key"]] = sha256_file(source_path)
        source_rows[source["key"]] = len(frame)
        frame.insert(0, "source_key", source["key"])
        frame.insert(1, "source_collection_id", source["collection_id"])
        frame.insert(2, "source_collection_name", source["collection_name"])
        frames.append(frame)

    raw_federated = pd.concat(frames, ignore_index=True, sort=False)
    c0_path = output_dir / "c0_federated.csv"
    stable_csv(raw_federated, c0_path, ["source_collection_id", "isic_id"])

    silver = build_silver(raw_federated, mappings, source_hashes)
    c1_path = output_dir / "c1_silver.csv"
    stable_csv(silver, c1_path, ["source_collection_id", "isic_id"])

    manifest = {
        "condition_c0": {
            "path": str(c0_path.relative_to(EXPERIMENT_DIR)),
            "records": len(raw_federated),
            "columns": len(raw_federated.columns),
            "sha256": sha256_file(c0_path),
        },
        "condition_c1": {
            "path": str(c1_path.relative_to(EXPERIMENT_DIR)),
            "records": len(silver),
            "columns": len(silver.columns),
            "sha256": sha256_file(c1_path),
        },
        "source_rows": source_rows,
        "source_sha256": source_hashes,
        "source_config_sha256": sha256_file(CONFIG_DIR / "sources.json"),
        "mapping_config_sha256": sha256_file(CONFIG_DIR / "mappings.json"),
        "semantic_catalog_version": mappings["catalog_version"],
        "silver_rule_version": mappings["silver_rule_version"],
    }
    manifest_path = output_dir / "build_manifest.json"
    write_json(manifest_path, manifest)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DERIVED_DIR,
        help="output directory (defaults to the experiment derived directory)",
    )
    args = parser.parse_args()
    manifest = build(args.output_dir.resolve())
    print(
        f"C0: {manifest['condition_c0']['records']} rows, "
        f"sha256={manifest['condition_c0']['sha256']}"
    )
    print(
        f"C1: {manifest['condition_c1']['records']} rows, "
        f"sha256={manifest['condition_c1']['sha256']}"
    )


if __name__ == "__main__":
    main()
