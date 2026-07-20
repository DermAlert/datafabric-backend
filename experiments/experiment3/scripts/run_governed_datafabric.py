#!/usr/bin/env python3
"""Materialize and validate the governed dermatology cohorts in DataFabric.

This runner is deliberately scoped to resources prefixed with ``dermexp3``.
It never serializes bearer tokens or Delta Sharing presigned URLs.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from experiment_lib import (
    CONFIG_DIR,
    DERIVED_DIR,
    LOGS_DIR,
    MANIFESTS_DIR,
    RESULTS_DIR,
    ensure_output_dirs,
    write_json,
)
from run_datafabric import (
    APIError,
    BRONZE_NAME,
    DataFabricClient,
    execute_and_wait,
    semantic_hash,
)


DEFAULT_BASE_URL = "http://localhost:8004"
EXPERIMENT_TAG = "dermatology_exp3"
CATALOG_VERSION = "derm-semantic-v1.1.0"
SHARE_NAME = "dermexp3_governed_release_v1"
SCHEMA_NAME = "cohorts"
AUTHORIZED_RECIPIENT = "dermexp3_external_reviewer_v1"
CONTROL_RECIPIENT = "dermexp3_unassigned_control_v1"
TARGET_RUNS = 3

TABLE_NAMES = {
    "G2_DERM_CANCER_HISTO": "derm_cancer_histo",
    "G3_CLINICAL_CANCER_PHOTOTYPE": "clinical_cancer_phototype",
    "G4_CC_BY_POLICY": "cc_by_policy",
}


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def condition_specs() -> list[dict[str, Any]]:
    protocol = read_json(CONFIG_DIR / "governed-cohorts.json")
    return [item for item in protocol["conditions"] if item.get("backend_name")]


def expected_identifiers(condition_id: str) -> set[str]:
    path = DERIVED_DIR / "governed" / f"{condition_id.lower()}.csv"
    frame = pd.read_csv(path, usecols=["isic_id"], dtype="string")
    return set(frame["isic_id"].dropna().astype(str))


def compact_json(value: Any) -> Any:
    """Remove response-only null fields before frozen-config comparison."""
    if isinstance(value, dict):
        return {key: compact_json(item) for key, item in value.items() if item is not None}
    if isinstance(value, list):
        return [compact_json(item) for item in value]
    return value


def find_bronze(client: DataFabricClient) -> dict[str, Any]:
    matches = [
        item
        for item in client.get("/api/bronze/configs/persistent")
        if item["name"] == BRONZE_NAME
    ]
    if len(matches) != 1:
        raise APIError(f"Expected exactly one Bronze config named {BRONZE_NAME}; found {len(matches)}")
    executions = client.get(f"/api/bronze/configs/persistent/{matches[0]['id']}/executions")
    successful = [
        item for item in executions if str(item.get("status", "")).lower() == "success"
    ]
    if not successful:
        raise APIError(f"Bronze config {matches[0]['id']} has no successful execution")
    successful.sort(key=lambda item: (int(item.get("delta_version") or 0), int(item["id"])))
    return {"config": matches[0], "execution": successful[-1]}


def find_catalog_groups(client: DataFabricClient) -> list[dict[str, Any]]:
    groups = [
        item
        for item in client.get("/api/equivalence/column-groups")
        if item.get("properties", {}).get("experiment") == EXPERIMENT_TAG
        and item.get("properties", {}).get("catalog_version") == CATALOG_VERSION
    ]
    expected_names = {
        "diagnosis_harmonized",
        "recorded_sex",
        "image_type_harmonized",
        "fitzpatrick_skin_type_harmonized",
        "anatom_site_harmonized",
    }
    names = {item["name"] for item in groups}
    if names != expected_names:
        raise APIError(f"Unexpected v1.1 semantic groups: {sorted(names)}")
    return sorted(groups, key=lambda item: int(item["id"]))


def ensure_silver_config(
    client: DataFabricClient,
    spec: dict[str, Any],
    bronze_id: int,
    bronze_version: int,
    group_ids: list[int],
) -> dict[str, Any]:
    existing = [
        item
        for item in client.get("/api/silver/persistent/configs")
        if item["name"] == spec["backend_name"]
    ]
    expected_filter = spec["backend_filter"]
    if existing:
        config = existing[0]
        checks = {
            "source_bronze_config_id": config.get("source_bronze_config_id") == bronze_id,
            "source_bronze_version": config.get("source_bronze_version") == bronze_version,
            "column_group_ids": set(config.get("column_group_ids") or []) == set(group_ids),
            "filters": compact_json(config.get("filters")) == compact_json(expected_filter),
            "exclude_unified_source_columns": not config.get("exclude_unified_source_columns"),
        }
        if not all(checks.values()):
            raise APIError(
                f"Existing config {config['id']} differs from frozen protocol: {checks}"
            )
        return config

    return client.post(
        "/api/silver/persistent/configs",
        {
            "name": spec["backend_name"],
            "description": (
                f"Governed dermatology condition {spec['id']}: {spec['name']}. "
                "Frozen protocol derm-governed-cohorts-v1.0.0."
            ),
            "source_bronze_config_id": bronze_id,
            "source_bronze_version": bronze_version,
            "column_group_ids": group_ids,
            "filters": expected_filter,
            "column_transformations": [],
            "exclude_unified_source_columns": False,
        },
        expected=(201,),
    )


def successful_executions(client: DataFabricClient, config_id: int) -> list[dict[str, Any]]:
    executions = client.get(f"/api/silver/persistent/configs/{config_id}/executions")
    result = [
        item for item in executions if str(item.get("status", "")).lower() == "success"
    ]
    result.sort(key=lambda item: (int(item.get("delta_version") or 0), int(item["id"])))
    return result


def ensure_runs(
    client: DataFabricClient,
    config: dict[str, Any],
    target: int,
    timeout_seconds: int,
    poll_seconds: int,
) -> list[dict[str, Any]]:
    executions = successful_executions(client, config["id"])
    previous_id = executions[-1]["id"] if executions else None
    while len(executions) < target:
        result = execute_and_wait(
            client,
            "silver",
            config["id"],
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )
        if result.get("id") == previous_id:
            raise APIError(f"Execution history for Silver config {config['id']} did not advance")
        previous_id = result.get("id")
        executions = successful_executions(client, config["id"])
        print(
            f"{config['name']}: successful version {result.get('delta_version')} "
            f"({len(executions)}/{target})"
        )
    return executions[:target]


def find_or_create(
    client: DataFabricClient,
    search_path: str,
    create_path: str,
    name: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    items = client.post(search_path, {"page": 1, "size": 100, "search": name})["items"]
    exact = [item for item in items if item["name"] == name]
    if exact:
        return exact[0]
    return client.post(create_path, payload, expected=(201,))


def ensure_sharing_resources(
    client: DataFabricClient,
    configs: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]], dict[str, Any], str, dict[str, Any], str]:
    share = find_or_create(
        client,
        "/api/delta-sharing/shares/search",
        "/api/delta-sharing/shares",
        SHARE_NAME,
        {
            "name": SHARE_NAME,
            "description": "Pinned governed dermatology cohorts for reproducibility review",
            "owner_email": "artifact-review@example.invalid",
            "terms_of_use": "Research artifact; source license labels and original provenance are retained.",
        },
    )
    schema = find_or_create(
        client,
        f"/api/delta-sharing/shares/{share['id']}/schemas/search",
        f"/api/delta-sharing/shares/{share['id']}/schemas",
        SCHEMA_NAME,
        {"name": SCHEMA_NAME, "description": "Governed cohort conditions G2-G4"},
    )

    tables: dict[str, dict[str, Any]] = {}
    existing = client.post(
        f"/api/delta-sharing/shares/{share['id']}/schemas/{schema['id']}/tables/search",
        {"page": 1, "size": 100},
    )["items"]
    for condition_id, config in configs.items():
        match = next(
            (
                item
                for item in existing
                if item.get("silver_persistent_config_id") == config["id"]
            ),
            None,
        )
        if match is None:
            client.post(
                f"/api/delta-sharing/shares/{share['id']}/schemas/{schema['id']}/tables/from-silver",
                {
                    "silver_config_id": config["id"],
                    "name": TABLE_NAMES[condition_id],
                    "description": f"Frozen governed condition {condition_id}, pinned to Delta version 0",
                    "share_mode": "full",
                    "pinned_delta_version": 0,
                },
                expected=(201,),
            )
            existing = client.post(
                f"/api/delta-sharing/shares/{share['id']}/schemas/{schema['id']}/tables/search",
                {"page": 1, "size": 100},
            )["items"]
            match = next(
                item
                for item in existing
                if item.get("silver_persistent_config_id") == config["id"]
            )
        if match.get("pinned_delta_version") != 0:
            match = client.request(
                "PUT",
                f"/api/delta-sharing/shares/{share['id']}/schemas/{schema['id']}/tables/{match['id']}/pin-version",
                json={"delta_version": 0},
            ).json()
        tables[condition_id] = match

    authorized = find_or_create(
        client,
        "/api/delta-sharing/recipients/search",
        "/api/delta-sharing/recipients",
        AUTHORIZED_RECIPIENT,
        {
            "identifier": AUTHORIZED_RECIPIENT,
            "name": AUTHORIZED_RECIPIENT,
            "organization_name": "ICSE artifact review simulation",
            "max_requests_per_hour": 1000,
            "max_downloads_per_day": 1000,
            "notes": "Experiment-scoped authenticated recipient",
        },
    )
    authorized_token = authorized.get("bearer_token")
    if not authorized_token:
        authorized_token = client.post(
            f"/api/delta-sharing/recipients/{authorized['id']}/regenerate-token", {}
        )["token"]
    client.request(
        "POST",
        f"/api/delta-sharing/recipients/{authorized['id']}/shares",
        expected=(204,),
        json={"share_ids": [share["id"]]},
    )

    control = find_or_create(
        client,
        "/api/delta-sharing/recipients/search",
        "/api/delta-sharing/recipients",
        CONTROL_RECIPIENT,
        {
            "identifier": CONTROL_RECIPIENT,
            "name": CONTROL_RECIPIENT,
            "organization_name": "Access-control negative test",
            "max_requests_per_hour": 1000,
            "max_downloads_per_day": 1000,
            "notes": "Intentionally unassigned experiment control",
        },
    )
    control_token = control.get("bearer_token")
    if not control_token:
        control_token = client.post(
            f"/api/delta-sharing/recipients/{control['id']}/regenerate-token", {}
        )["token"]
    # Explicitly preserve the negative control's empty assignment.
    client.request(
        "POST",
        f"/api/delta-sharing/recipients/{control['id']}/shares",
        expected=(204,),
        json={"share_ids": []},
    )
    return share, schema, tables, authorized, authorized_token, control, control_token


def parse_ndjson(response: requests.Response) -> list[dict[str, Any]]:
    return [json.loads(line) for line in response.text.splitlines() if line.strip()]


def validate_sharing(
    base_url: str,
    share: dict[str, Any],
    schema: dict[str, Any],
    tables: dict[str, dict[str, Any]],
    authorized_token: str,
    control_token: str,
) -> dict[str, Any]:
    root = f"{base_url.rstrip('/')}/api/delta-sharing"
    headers = {"Authorization": f"Bearer {authorized_token}", "Accept": "application/json"}
    shares_response = requests.get(f"{root}/shares", headers=headers, timeout=120)
    shares_response.raise_for_status()
    table_list_response = requests.get(
        f"{root}/shares/{share['name']}/schemas/{schema['name']}/tables",
        headers=headers,
        timeout=120,
    )
    table_list_response.raise_for_status()

    table_results: dict[str, Any] = {}
    for condition_id, table in tables.items():
        protocol_name = table.get("protocol_table_name") or TABLE_NAMES[condition_id]
        path = f"{root}/shares/{share['name']}/schemas/{schema['name']}/tables/{protocol_name}"
        version_response = requests.get(f"{path}/version", headers=headers, timeout=120)
        version_response.raise_for_status()
        metadata_response = requests.get(f"{path}/metadata", headers=headers, timeout=120)
        metadata_response.raise_for_status()
        query_response = requests.post(f"{path}/query", headers=headers, json={}, timeout=300)
        query_response.raise_for_status()

        metadata_actions = parse_ndjson(metadata_response)
        query_actions = parse_ndjson(query_response)
        files = [action["file"] for action in query_actions if "file" in action]
        metadata_items = [
            value
            for action in metadata_actions + query_actions
            for key, value in action.items()
            if key.lower() in {"metadata", "metaData".lower()}
        ]
        schema_fields = 0
        for item in metadata_items:
            schema_string = item.get("schemaString")
            if schema_string:
                schema_fields = max(schema_fields, len(json.loads(schema_string).get("fields", [])))

        parquet_magic = False
        download_status: int | None = None
        if files:
            with requests.get(
                files[0]["url"],
                headers={"Range": "bytes=0-3"},
                stream=True,
                timeout=120,
            ) as download:
                download_status = download.status_code
                download.raise_for_status()
                parquet_magic = download.raw.read(4) == b"PAR1"

        version = int(version_response.json()["version"])
        table_results[condition_id] = {
            "protocol_table_name": protocol_name,
            "version_status": version_response.status_code,
            "reported_version": version,
            "delta_version_header": int(version_response.headers["Delta-Table-Version"]),
            "metadata_status": metadata_response.status_code,
            "schema_field_count": schema_fields,
            "query_status": query_response.status_code,
            "file_action_count": len(files),
            "declared_bytes": sum(int(item.get("size") or 0) for item in files),
            "sample_download_status": download_status,
            "sample_has_parquet_magic": parquet_magic,
            "pinned_version_served": version == 0,
        }

    invalid = requests.get(
        f"{root}/shares", headers={"Authorization": "Bearer deliberately-invalid"}, timeout=120
    )
    unassigned = requests.get(
        # Use an unambiguous protocol route. The shorter /shares/{share}
        # collides with the management route /shares/{share_id} and yields a
        # FastAPI integer-validation 422 before authorization is evaluated.
        f"{root}/shares/{share['name']}/schemas",
        headers={"Authorization": f"Bearer {control_token}"},
        timeout=120,
    )
    result = {
        "authorized_share_listing_status": shares_response.status_code,
        "authorized_share_visible": any(
            item["name"] == share["name"] for item in shares_response.json()["items"]
        ),
        "authorized_table_listing_status": table_list_response.status_code,
        "authorized_table_count": len(table_list_response.json()["items"]),
        "tables": table_results,
        "invalid_token_status": invalid.status_code,
        "unassigned_recipient_status": unassigned.status_code,
        "access_control_enforced": invalid.status_code == 401 and unassigned.status_code == 404,
        "sensitive_values_persisted": False,
    }
    result["delivery_validated"] = (
        result["authorized_share_visible"]
        and result["authorized_table_count"] == len(tables)
        and result["access_control_enforced"]
        and all(
            item["pinned_version_served"]
            and item["schema_field_count"] > 0
            and item["file_action_count"] > 0
            and item["sample_has_parquet_magic"]
            for item in table_results.values()
        )
    )
    return result


def validate_backend_versions(
    client: DataFabricClient,
    specs: list[dict[str, Any]],
    configs: dict[str, dict[str, Any]],
    executions: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    version_rows: list[dict[str, Any]] = []
    oracle_rows: list[dict[str, Any]] = []
    report: dict[str, Any] = {"conditions": {}}
    for spec in specs:
        condition_id = spec["id"]
        expected = expected_identifiers(condition_id)
        validations: list[dict[str, Any]] = []
        for execution in executions[condition_id]:
            version = int(execution["delta_version"])
            response = client.get(
                f"/api/silver/persistent/configs/{configs[condition_id]['id']}/data",
                params={"version": version, "limit": 100000, "offset": 0},
                timeout=300,
                attempts=2,
            )
            rows = response["data"]
            identifiers = {str(row["isic_id"]) for row in rows}
            digest, excluded = semantic_hash(rows)
            missing = expected - identifiers
            unexpected = identifiers - expected
            source_counts: dict[str, int] = {}
            for row in rows:
                source = str(row.get("source_collection_name"))
                source_counts[source] = source_counts.get(source, 0) + 1
            validation = {
                "condition": condition_id,
                "config_id": configs[condition_id]["id"],
                "execution_id": execution["id"],
                "delta_version": version,
                "rows": len(rows),
                "reported_total_rows": response.get("total_rows"),
                "expected_rows": len(expected),
                "semantic_sha256": digest,
                "dynamic_columns_excluded": excluded,
                "source_counts": source_counts,
                "missing_expected_ids": len(missing),
                "unexpected_ids": len(unexpected),
                "exact_oracle_agreement": not missing and not unexpected and len(rows) == len(expected),
                "query_time_seconds": response.get("execution_time_seconds"),
            }
            validations.append(validation)
            version_rows.append(
                {key: value for key, value in validation.items() if key not in {"source_counts", "dynamic_columns_excluded"}}
            )
            oracle_rows.append(
                {
                    "condition": condition_id,
                    "delta_version": version,
                    "expected_ids": len(expected),
                    "observed_ids": len(identifiers),
                    "missing_expected_ids": len(missing),
                    "unexpected_ids": len(unexpected),
                    "exact_agreement": validation["exact_oracle_agreement"],
                }
            )

        hashes = {item["semantic_sha256"] for item in validations}
        counts = {item["rows"] for item in validations}
        exact = all(item["exact_oracle_agreement"] for item in validations)
        report["conditions"][condition_id] = {
            "versions": validations,
            "three_versions": len(validations) == TARGET_RUNS,
            "semantic_hashes_identical": len(hashes) == 1,
            "row_counts_identical": len(counts) == 1,
            "all_versions_match_independent_oracle": exact,
            "reproducible": len(validations) == TARGET_RUNS and len(hashes) == 1 and exact,
        }
    report["all_conditions_reproducible"] = all(
        item["reproducible"] for item in report["conditions"].values()
    )
    return report, pd.DataFrame(version_rows), pd.DataFrame(oracle_rows)


def public_resource(value: dict[str, Any], fields: list[str]) -> dict[str, Any]:
    return {field: value.get(field) for field in fields}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--poll-seconds", type=int, default=5)
    args = parser.parse_args()

    ensure_output_dirs()
    client = DataFabricClient(args.base_url)
    specs = condition_specs()
    manifest: dict[str, Any] = {
        "protocol_version": "derm-governed-cohorts-v1.0.0",
        "base_url": args.base_url,
        "catalog_version": CATALOG_VERSION,
        "target_runs_per_condition": TARGET_RUNS,
        "conditions": {},
        "sharing": {},
    }
    error: Exception | None = None
    try:
        bronze = find_bronze(client)
        bronze_version = int(bronze["execution"]["delta_version"])
        groups = find_catalog_groups(client)
        group_ids = [int(item["id"]) for item in groups]
        manifest["bronze"] = {
            "config_id": bronze["config"]["id"],
            "name": bronze["config"]["name"],
            "delta_version": bronze_version,
        }
        manifest["semantic_groups"] = [
            {"id": item["id"], "name": item["name"]} for item in groups
        ]

        configs: dict[str, dict[str, Any]] = {}
        executions: dict[str, list[dict[str, Any]]] = {}
        for spec in specs:
            config = ensure_silver_config(
                client, spec, bronze["config"]["id"], bronze_version, group_ids
            )
            configs[spec["id"]] = config
            executions[spec["id"]] = ensure_runs(
                client, config, 1, args.timeout_seconds, args.poll_seconds
            )

        share, schema, tables, authorized, auth_token, control, control_token = (
            ensure_sharing_resources(client, configs)
        )
        print("Created/reused pinned Delta Sharing release after version 0")

        for spec in specs:
            condition_id = spec["id"]
            executions[condition_id] = ensure_runs(
                client,
                configs[condition_id],
                TARGET_RUNS,
                args.timeout_seconds,
                args.poll_seconds,
            )

        backend_report, version_frame, oracle_frame = validate_backend_versions(
            client, specs, configs, executions
        )
        write_json(RESULTS_DIR / "governed_backend_validation.json", backend_report)
        version_frame.to_csv(RESULTS_DIR / "governed_backend_versions.csv", index=False)
        oracle_frame.to_csv(RESULTS_DIR / "governed_backend_oracle_agreement.csv", index=False)

        sharing_report = validate_sharing(
            args.base_url, share, schema, tables, auth_token, control_token
        )
        write_json(RESULTS_DIR / "governed_sharing_validation.json", sharing_report)

        producer_latest = {
            condition_id: max(int(item["delta_version"]) for item in runs)
            for condition_id, runs in executions.items()
        }
        pinned_independent = all(
            producer_latest[condition_id] > 0
            and sharing_report["tables"][condition_id]["reported_version"] == 0
            for condition_id in producer_latest
        )
        sharing_report["producer_latest_versions"] = producer_latest
        sharing_report["pinned_release_independent_of_producer_head"] = pinned_independent
        sharing_report["delivery_validated"] = (
            sharing_report["delivery_validated"] and pinned_independent
        )
        write_json(RESULTS_DIR / "governed_sharing_validation.json", sharing_report)

        for spec in specs:
            condition_id = spec["id"]
            manifest["conditions"][condition_id] = {
                "config": public_resource(
                    configs[condition_id],
                    ["id", "name", "source_bronze_config_id", "source_bronze_version", "column_group_ids", "filters"],
                ),
                "executions": [
                    public_resource(
                        item,
                        ["id", "status", "rows_processed", "rows_output", "delta_version", "started_at", "finished_at"],
                    )
                    for item in executions[condition_id]
                ],
                "backend_reproducible": backend_report["conditions"][condition_id]["reproducible"],
            }
        manifest["sharing"] = {
            "share": public_resource(share, ["id", "name", "status"]),
            "schema": public_resource(schema, ["id", "name"]),
            "tables": {
                condition_id: public_resource(
                    table,
                    ["id", "protocol_table_name", "silver_persistent_config_id", "pinned_delta_version"],
                )
                for condition_id, table in tables.items()
            },
            "authorized_recipient": public_resource(authorized, ["id", "identifier", "name"]),
            "unassigned_control": public_resource(control, ["id", "identifier", "name"]),
            "validation": sharing_report,
        }
        manifest["all_backend_validations_passed"] = backend_report["all_conditions_reproducible"]
        manifest["sharing_validation_passed"] = sharing_report["delivery_validated"]
        if not manifest["all_backend_validations_passed"]:
            raise APIError("One or more backend reproducibility validations failed")
        if not manifest["sharing_validation_passed"]:
            raise APIError("Authenticated/pinned Delta Sharing validation failed")
        print("All backend oracle, reproducibility, access-control, pinning, and delivery checks passed")
        return 0
    except Exception as exc:
        error = exc
        manifest["error"] = {"type": type(exc).__name__, "message": str(exc)[:2000]}
        raise
    finally:
        manifest["api_event_count"] = len(client.events)
        write_json(MANIFESTS_DIR / "governed_datafabric_run.json", manifest)
        write_json(LOGS_DIR / "governed_datafabric_api_events.json", client.events)
        if error:
            print(f"Governed DataFabric run failed: {type(error).__name__}")


if __name__ == "__main__":
    raise SystemExit(main())
