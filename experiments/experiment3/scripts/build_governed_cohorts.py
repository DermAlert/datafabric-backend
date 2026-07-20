#!/usr/bin/env python3
"""Build and evaluate the governed dermatology cohort conditions locally."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from build_conditions import build_silver
from experiment_lib import (
    CONFIG_DIR,
    DERIVED_DIR,
    RESULTS_DIR,
    ensure_output_dirs,
    sha256_file,
    stable_csv,
    write_json,
)


GOVERNED_DIR = DERIVED_DIR / "governed"
MATERIALIZATIONS_DIR = GOVERNED_DIR / "materializations"
GOVERNED_CONFIG_PATH = CONFIG_DIR / "governed-cohorts.json"
OLD_MAPPING_PATH = CONFIG_DIR / "mappings-v1.0.0.json"


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def markdown_table(frame: pd.DataFrame) -> str:
    display = frame.fillna("N/A").astype(str)
    header = "| " + " | ".join(display.columns) + " |"
    separator = "| " + " | ".join(["---"] * len(display.columns)) + " |"
    rows = ["| " + " | ".join(row) + " |" for row in display.to_numpy().tolist()]
    return "\n".join([header, separator, *rows]) + "\n"


def summarize(condition_id: str, name: str, frame: pd.DataFrame, raw_count: int) -> dict[str, Any]:
    return {
        "condition": condition_id,
        "name": name,
        "images": len(frame),
        "lesions": int(frame["lesion_uid"].nunique()),
        "patients": int(frame["patient_uid"].nunique()),
        "sources": int(frame["source_collection_name"].nunique()),
        "retention_percent": round(100 * len(frame) / raw_count, 3),
        "source_provenance_coverage": round(float(frame["source_collection_id"].notna().mean()), 6),
        "lesion_id_coverage": round(float(frame["lesion_uid"].notna().mean()), 6),
        "patient_id_coverage": round(float(frame["patient_uid"].notna().mean()), 6),
        "license_coverage": round(float(frame["copyright_license"].notna().mean()), 6),
    }


def partition_for(value: Any, split: dict[str, Any]) -> str:
    digest = hashlib.sha256(str(value).encode("utf-8")).hexdigest()
    bucket = int(digest[:16], 16) % 10000
    if bucket < int(split["train_upper_exclusive"]):
        return "train"
    if bucket < int(split["validation_upper_exclusive"]):
        return "validation"
    return "test"


def leakage_row(
    frame: pd.DataFrame,
    *,
    condition: str,
    strategy: str,
    partition_key: pd.Series,
    target_group: str,
    split: dict[str, Any],
) -> dict[str, Any]:
    evaluated = frame.loc[frame[target_group].notna(), ["isic_id", target_group]].copy()
    evaluated["partition_key"] = partition_key.loc[evaluated.index].astype("string")
    evaluated["partition"] = evaluated["partition_key"].map(lambda value: partition_for(value, split))
    partitions_per_group = evaluated.groupby(target_group)["partition"].nunique()
    leaky_groups = set(partitions_per_group[partitions_per_group > 1].index)
    affected = int(evaluated[target_group].isin(leaky_groups).sum())
    return {
        "condition": condition,
        "split_strategy": strategy,
        "protected_unit": target_group.replace("_uid", ""),
        "evaluated_images": len(evaluated),
        "evaluated_groups": len(partitions_per_group),
        "groups_crossing_partitions": len(leaky_groups),
        "group_leakage_percent": round(
            100 * len(leaky_groups) / len(partitions_per_group), 3
        ) if len(partitions_per_group) else 0.0,
        "images_in_leaky_groups": affected,
        "affected_image_percent": round(100 * affected / len(evaluated), 3)
        if len(evaluated)
        else 0.0,
    }


def policy_rule_rows(
    baseline: pd.DataFrame,
    output: pd.DataFrame,
    condition: str,
    rules: list[tuple[str, Callable[[pd.DataFrame], pd.Series]]],
) -> list[dict[str, Any]]:
    rows = []
    for name, predicate in rules:
        baseline_pass = int(predicate(baseline).sum())
        output_pass = int(predicate(output).sum())
        rows.append(
            {
                "condition": condition,
                "rule": name,
                "baseline_records": len(baseline),
                "baseline_passing": baseline_pass,
                "baseline_pass_percent": round(100 * baseline_pass / len(baseline), 3),
                "output_records": len(output),
                "output_passing": output_pass,
                "output_compliance_percent": round(100 * output_pass / len(output), 3)
                if len(output)
                else 0.0,
            }
        )
    combined_baseline = pd.Series(True, index=baseline.index)
    combined_output = pd.Series(True, index=output.index)
    for _, predicate in rules:
        combined_baseline &= predicate(baseline)
        combined_output &= predicate(output)
    rows.append(
        {
            "condition": condition,
            "rule": "ALL_PREDECLARED_RULES",
            "baseline_records": len(baseline),
            "baseline_passing": int(combined_baseline.sum()),
            "baseline_pass_percent": round(100 * combined_baseline.mean(), 3),
            "output_records": len(output),
            "output_passing": int(combined_output.sum()),
            "output_compliance_percent": round(100 * combined_output.mean(), 3),
        }
    )
    return rows


def main() -> None:
    ensure_output_dirs()
    GOVERNED_DIR.mkdir(parents=True, exist_ok=True)
    MATERIALIZATIONS_DIR.mkdir(parents=True, exist_ok=True)

    protocol = read_json(GOVERNED_CONFIG_PATH)
    old_mappings = read_json(OLD_MAPPING_PATH)
    build_manifest = read_json(DERIVED_DIR / "build_manifest.json")
    c0 = pd.read_csv(DERIVED_DIR / "c0_federated.csv", low_memory=False)
    c1 = pd.read_csv(DERIVED_DIR / "c1_silver.csv", low_memory=False)
    raw_count = len(c1)

    malignant = set(protocol["malignant_codes"])
    phototypes = set(protocol["known_phototypes"])
    histopathology = str(protocol["histopathology_value"])
    clinical_modalities = {"CLINICAL_OVERVIEW", "CLINICAL_CLOSE_UP"}

    known_diagnosis = ~c1["diagnosis_harmonized"].isin(["MISSING", "UNRESOLVED"]) & c1[
        "diagnosis_harmonized"
    ].notna()
    dermoscopic = c1["image_type_harmonized"].eq("DERMOSCOPIC")
    clinical = c1["image_type_harmonized"].isin(clinical_modalities)
    cancer = c1["diagnosis_harmonized"].isin(malignant)
    histo = c1["diagnosis_confirm_type"].eq(histopathology)
    known_photo = c1["fitzpatrick_skin_type"].isin(phototypes)
    cc_by = c1["copyright_license"].eq("CC-BY")

    cohorts: dict[str, pd.DataFrame] = {
        "G0_RAW": c1.copy(),
        "G1_SEMANTIC": c1.copy(),
        "G1_KNOWN_DIAGNOSIS": c1.loc[known_diagnosis].copy(),
        "G1_DERM_KNOWN_HISTO": c1.loc[dermoscopic & known_diagnosis & histo].copy(),
        "G2_DERM_CANCER_HISTO": c1.loc[dermoscopic & cancer & histo].copy(),
        "G3_CLINICAL_CANCER_PHOTOTYPE": c1.loc[
            clinical & cancer & histo & known_photo
        ].copy(),
        "G4_CC_BY_POLICY": c1.loc[cc_by].copy(),
    }

    output_names = {
        "G0_RAW": "Raw integrated image union",
        "G1_SEMANTIC": "Full harmonized audit",
        "G1_KNOWN_DIAGNOSIS": "Known harmonized diagnosis",
        "G1_DERM_KNOWN_HISTO": "Dermoscopy, known diagnosis, histopathology",
        "G2_DERM_CANCER_HISTO": "Dermoscopic cancer, histopathology",
        "G3_CLINICAL_CANCER_PHOTOTYPE": "Clinical cancer with phototype",
        "G4_CC_BY_POLICY": "Exact CC-BY allowlist view",
    }

    for cohort_id in ["G2_DERM_CANCER_HISTO", "G3_CLINICAL_CANCER_PHOTOTYPE", "G4_CC_BY_POLICY"]:
        stable_csv(
            cohorts[cohort_id],
            GOVERNED_DIR / f"{cohort_id.lower()}.csv",
            ["source_collection_id", "isic_id"],
        )

    funnel = pd.DataFrame(
        [summarize(key, output_names[key], value, raw_count) for key, value in cohorts.items()]
    )
    funnel.to_csv(RESULTS_DIR / "governed_cohort_funnel.csv", index=False)

    granularity_rows = []
    for source, frame in list(c1.groupby("source_collection_name")) + [("ALL_SOURCES", c1)]:
        lesion_sizes = frame.dropna(subset=["lesion_uid"]).groupby("lesion_uid").size()
        patient_sizes = frame.dropna(subset=["patient_uid"]).groupby("patient_uid").size()
        lesions = len(lesion_sizes)
        patients = len(patient_sizes)
        granularity_rows.append(
            {
                "source": source,
                "images": len(frame),
                "lesions": lesions,
                "patients": patients,
                "extra_images_vs_lesions": len(frame) - lesions,
                "image_to_lesion_inflation_percent": round(100 * (len(frame) / lesions - 1), 3),
                "multi_image_lesions": int((lesion_sizes > 1).sum()),
                "multi_image_patients": int((patient_sizes > 1).sum()),
                "max_images_per_lesion": int(lesion_sizes.max()),
                "max_images_per_patient": int(patient_sizes.max()) if patients else None,
            }
        )
    granularity = pd.DataFrame(granularity_rows)
    granularity.to_csv(RESULTS_DIR / "governed_granularity_metrics.csv", index=False)

    leakage_rows = []
    split = protocol["split"]
    for cohort_id in ["G2_DERM_CANCER_HISTO", "G3_CLINICAL_CANCER_PHOTOTYPE"]:
        frame = cohorts[cohort_id]
        strategies = {
            "naive_image_hash": frame["isic_id"],
            "lesion_grouped_hash": frame["lesion_uid"],
            "patient_or_lesion_grouped_hash": frame["patient_uid"].fillna(frame["lesion_uid"]),
        }
        for strategy, keys in strategies.items():
            for target in ["lesion_uid", "patient_uid"]:
                if frame[target].notna().sum() == 0:
                    continue
                leakage_rows.append(
                    leakage_row(
                        frame,
                        condition=cohort_id,
                        strategy=strategy,
                        partition_key=keys,
                        target_group=target,
                        split=split,
                    )
                )
    leakage = pd.DataFrame(leakage_rows)
    leakage.to_csv(RESULTS_DIR / "governed_split_leakage.csv", index=False)

    rules_by_condition: dict[str, list[tuple[str, Callable[[pd.DataFrame], pd.Series]]]] = {
        "G2_DERM_CANCER_HISTO": [
            ("DERMOSCOPIC", lambda df: df["image_type_harmonized"].eq("DERMOSCOPIC")),
            ("MALIGNANT_CODE", lambda df: df["diagnosis_harmonized"].isin(malignant)),
            ("HISTOPATHOLOGY", lambda df: df["diagnosis_confirm_type"].eq(histopathology)),
        ],
        "G3_CLINICAL_CANCER_PHOTOTYPE": [
            ("CLINICAL_MODALITY", lambda df: df["image_type_harmonized"].isin(clinical_modalities)),
            ("MALIGNANT_CODE", lambda df: df["diagnosis_harmonized"].isin(malignant)),
            ("HISTOPATHOLOGY", lambda df: df["diagnosis_confirm_type"].eq(histopathology)),
            ("KNOWN_PHOTOTYPE", lambda df: df["fitzpatrick_skin_type"].isin(phototypes)),
            ("PATIENT_ID_PRESENT", lambda df: df["patient_uid"].notna()),
        ],
        "G4_CC_BY_POLICY": [
            ("EXACT_CC_BY_LABEL", lambda df: df["copyright_license"].eq("CC-BY")),
        ],
    }
    policy_rows = []
    for cohort_id, rules in rules_by_condition.items():
        policy_rows.extend(policy_rule_rows(c1, cohorts[cohort_id], cohort_id, rules))
    policy = pd.DataFrame(policy_rows)
    policy.to_csv(RESULTS_DIR / "governed_rule_compliance.csv", index=False)

    g3_patients = cohorts["G3_CLINICAL_CANCER_PHOTOTYPE"].drop_duplicates("patient_uid")
    sufficiency_rows = []
    threshold = int(protocol["subgroup_minimum_patients"])
    for source, frame in list(g3_patients.groupby("source_collection_name")) + [
        ("ALL_SOURCES", g3_patients)
    ]:
        counts = frame["fitzpatrick_skin_type"].value_counts()
        for phototype in protocol["known_phototypes"]:
            count = int(counts.get(phototype, 0))
            sufficiency_rows.append(
                {
                    "source": source,
                    "phototype": phototype,
                    "patients": count,
                    "minimum_patients": threshold,
                    "eligible_for_descriptive_reporting": count >= threshold,
                    "shortfall": max(0, threshold - count),
                }
            )
    sufficiency = pd.DataFrame(sufficiency_rows)
    sufficiency.to_csv(RESULTS_DIR / "governed_phototype_sufficiency.csv", index=False)

    silver_v1 = build_silver(c0, old_mappings, build_manifest["source_sha256"])
    stable_csv(
        silver_v1,
        GOVERNED_DIR / "c1_catalog_v1_0.csv",
        ["source_collection_id", "isic_id"],
    )
    old = silver_v1.set_index("isic_id", drop=False)
    new = c1.set_index("isic_id", drop=False)
    shared = sorted(set(old.index) & set(new.index))
    changed = old.loc[shared, "diagnosis_harmonized"].fillna("MISSING") != new.loc[
        shared, "diagnosis_harmonized"
    ].fillna("MISSING")
    original_fields = [f"diagnosis_original_level_{level}" for level in range(1, 6)]
    provenance_fields = ["source_collection_id", "source_collection_name", "source_key"]
    originals_preserved = all(
        old.loc[shared, field].fillna("MISSING").astype(str).equals(
            new.loc[shared, field].fillna("MISSING").astype(str)
        )
        for field in original_fields + provenance_fields
    )
    old_derm = silver_v1["included_in_dermoscopy_cohort"].astype(str).str.lower().eq("true")
    new_derm = c1["included_in_dermoscopy_cohort"].astype(str).str.lower().eq("true")
    semantic_impact = pd.DataFrame(
        [
            {
                "metric": "total_records",
                "catalog_v1_0": len(silver_v1),
                "catalog_v1_1": len(c1),
                "difference": len(c1) - len(silver_v1),
            },
            {
                "metric": "unresolved_diagnosis_records",
                "catalog_v1_0": int(silver_v1["diagnosis_harmonized"].eq("UNRESOLVED").sum()),
                "catalog_v1_1": int(c1["diagnosis_harmonized"].eq("UNRESOLVED").sum()),
                "difference": int(c1["diagnosis_harmonized"].eq("UNRESOLVED").sum())
                - int(silver_v1["diagnosis_harmonized"].eq("UNRESOLVED").sum()),
            },
            {
                "metric": "BKL_records",
                "catalog_v1_0": int(silver_v1["diagnosis_harmonized"].eq("BKL").sum()),
                "catalog_v1_1": int(c1["diagnosis_harmonized"].eq("BKL").sum()),
                "difference": int(c1["diagnosis_harmonized"].eq("BKL").sum())
                - int(silver_v1["diagnosis_harmonized"].eq("BKL").sum()),
            },
            {
                "metric": "approved_dermoscopy_images",
                "catalog_v1_0": int(old_derm.sum()),
                "catalog_v1_1": int(new_derm.sum()),
                "difference": int(new_derm.sum() - old_derm.sum()),
            },
            {
                "metric": "approved_dermoscopy_lesions",
                "catalog_v1_0": int(silver_v1.loc[old_derm, "lesion_uid"].nunique()),
                "catalog_v1_1": int(c1.loc[new_derm, "lesion_uid"].nunique()),
                "difference": int(c1.loc[new_derm, "lesion_uid"].nunique())
                - int(silver_v1.loc[old_derm, "lesion_uid"].nunique()),
            },
            {
                "metric": "changed_harmonized_diagnoses",
                "catalog_v1_0": 0,
                "catalog_v1_1": int(changed.sum()),
                "difference": int(changed.sum()),
            },
            {
                "metric": "original_and_provenance_fields_preserved",
                "catalog_v1_0": True,
                "catalog_v1_1": originals_preserved,
                "difference": 0 if originals_preserved else 1,
            },
        ]
    )
    semantic_impact.to_csv(RESULTS_DIR / "governed_semantic_version_impact.csv", index=False)

    changed_ids = set(pd.Index(shared)[changed.to_numpy()])
    changed_rows = c1[c1["isic_id"].isin(changed_ids)].groupby(
        ["source_collection_name", "diagnosis_original_level_3", "diagnosis_harmonized"],
        dropna=False,
    ).size().reset_index(name="records")
    changed_rows.to_csv(RESULTS_DIR / "governed_semantic_changes_by_source.csv", index=False)

    reproducibility_runs = []
    for run_number in range(1, 4):
        run_dir = MATERIALIZATIONS_DIR / f"run-{run_number}"
        run_dir.mkdir(parents=True, exist_ok=True)
        for cohort_id in ["G2_DERM_CANCER_HISTO", "G3_CLINICAL_CANCER_PHOTOTYPE", "G4_CC_BY_POLICY"]:
            path = run_dir / f"{cohort_id.lower()}.csv"
            stable_csv(cohorts[cohort_id], path, ["source_collection_id", "isic_id"])
            reproducibility_runs.append(
                {
                    "run": run_number,
                    "condition": cohort_id,
                    "records": len(cohorts[cohort_id]),
                    "sha256": sha256_file(path),
                }
            )
    reproducibility = pd.DataFrame(reproducibility_runs)
    reproducibility.to_csv(RESULTS_DIR / "governed_local_reproducibility.csv", index=False)
    local_reproducible = all(
        group["sha256"].nunique() == 1 and group["records"].nunique() == 1
        for _, group in reproducibility.groupby("condition")
    )

    article_funnel = funnel[
        funnel["condition"].isin(
            [
                "G0_RAW",
                "G1_KNOWN_DIAGNOSIS",
                "G1_DERM_KNOWN_HISTO",
                "G2_DERM_CANCER_HISTO",
                "G3_CLINICAL_CANCER_PHOTOTYPE",
                "G4_CC_BY_POLICY",
            ]
        )
    ][["condition", "name", "images", "lesions", "patients", "sources", "retention_percent"]]
    (RESULTS_DIR / "table_governed_cohort_funnel.md").write_text(
        markdown_table(article_funnel), encoding="utf-8"
    )
    (RESULTS_DIR / "table_governed_granularity.md").write_text(
        markdown_table(granularity), encoding="utf-8"
    )
    (RESULTS_DIR / "table_governed_leakage.md").write_text(
        markdown_table(leakage), encoding="utf-8"
    )
    (RESULTS_DIR / "table_governed_semantic_impact.md").write_text(
        markdown_table(semantic_impact), encoding="utf-8"
    )
    (RESULTS_DIR / "table_governed_phototype_gate.md").write_text(
        markdown_table(sufficiency), encoding="utf-8"
    )

    report = {
        "protocol_version": protocol["protocol_version"],
        "frozen_on": protocol["frozen_on"],
        "source_records": raw_count,
        "conditions": funnel.to_dict(orient="records"),
        "local_reproducible": local_reproducible,
        "subgroup_minimum_patients": threshold,
        "semantic_change": {
            "changed_records": int(changed.sum()),
            "original_and_provenance_fields_preserved": originals_preserved,
        },
        "artifacts": {
            "funnel": "results/governed_cohort_funnel.csv",
            "granularity": "results/governed_granularity_metrics.csv",
            "leakage": "results/governed_split_leakage.csv",
            "rule_compliance": "results/governed_rule_compliance.csv",
            "phototype_sufficiency": "results/governed_phototype_sufficiency.csv",
            "semantic_impact": "results/governed_semantic_version_impact.csv",
        },
        "interpretation_constraints": protocol["interpretation_constraints"],
    }
    write_json(RESULTS_DIR / "governed_evaluation_report.json", report)

    if not local_reproducible:
        raise RuntimeError("Governed local outputs were not byte-identical")
    if not originals_preserved:
        raise RuntimeError("Catalog evolution changed original/provenance fields")
    if not all(
        policy.loc[policy["rule"].eq("ALL_PREDECLARED_RULES"), "output_compliance_percent"].eq(100)
    ):
        raise RuntimeError("A governed output violates a predeclared rule")

    print(f"Built governed conditions from {raw_count} records")
    for cohort_id in ["G2_DERM_CANCER_HISTO", "G3_CLINICAL_CANCER_PHOTOTYPE", "G4_CC_BY_POLICY"]:
        print(f"{cohort_id}: {len(cohorts[cohort_id])} images")
    print(f"Catalog v1.0→v1.1 changed {int(changed.sum())} harmonized diagnoses")
    print("Three local governed materializations are byte-identical")


if __name__ == "__main__":
    main()
