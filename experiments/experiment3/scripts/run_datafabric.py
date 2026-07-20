#!/usr/bin/env python3
"""Execute Dermatology Experiment 3 through the real DataFabric API.

The script is intentionally scoped to resources whose names begin with
``dermexp3``. It creates no relationships between sources: the collections do
not share patient or lesion identity keys, so Silver combines their Bronze
groups by union.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from experiment_lib import (
    CONFIG_DIR,
    LOGS_DIR,
    MANIFESTS_DIR,
    RESULTS_DIR,
    ensure_output_dirs,
    mapping_config,
    source_config,
    write_json,
)


DEFAULT_BASE_URL = "http://localhost:8004"
EXPERIMENT_TAG = "dermatology_exp3"
BRONZE_NAME = "dermexp3_bronze_union_v2"
SILVER_NAME = "dermexp3_silver_harmonized_v3"


@dataclass(frozen=True)
class BackendSource:
    key: str
    connection_name: str
    connection_type: str
    host: str
    port: int
    database: str
    username: str
    password: str
    table_name: str


SOURCES = (
    BackendSource(
        key="ham10000",
        connection_name="dermexp3 v2 HAM10000 collection 212",
        connection_type="postgresql",
        host="dermexp_ham_postgres",
        port=5432,
        database="ham10000",
        username="ham_user",
        password="ham_pass",
        table_name="metadata",
    ),
    BackendSource(
        key="hiba",
        connection_name="dermexp3 v2 HIBA collection 251",
        connection_type="postgresql",
        host="dermexp_hiba_postgres",
        port=5432,
        database="hiba",
        username="hiba_user",
        password="hiba_pass",
        table_name="metadata",
    ),
    BackendSource(
        key="pad_ufes_20",
        connection_name="dermexp3 v2 PAD-UFES-20 collection 406",
        connection_type="mysql",
        host="dermexp_pad_mysql",
        port=3306,
        database="isic",
        username="pad_user",
        password="pad_pass",
        table_name="metadata",
    ),
)


class APIError(RuntimeError):
    pass


class DataFabricClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.events: list[dict[str, Any]] = []

    def request(
        self,
        method: str,
        path: str,
        *,
        expected: tuple[int, ...] = (200,),
        attempts: int = 4,
        timeout: int = 120,
        **kwargs: Any,
    ) -> requests.Response:
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            started = time.perf_counter()
            try:
                response = self.session.request(
                    method,
                    f"{self.base_url}{path}",
                    timeout=timeout,
                    **kwargs,
                )
                duration = time.perf_counter() - started
                self.events.append(
                    {
                        "method": method,
                        "path": path,
                        "status_code": response.status_code,
                        "duration_seconds": round(duration, 6),
                        "attempt": attempt,
                    }
                )
                if response.status_code in expected:
                    return response
                if response.status_code >= 500 and attempt < attempts:
                    time.sleep(min(2 * attempt, 8))
                    continue
                raise APIError(
                    f"{method} {path} failed: {response.status_code} {response.text[:2000]}"
                )
            except (requests.ConnectionError, requests.ReadTimeout) as exc:
                duration = time.perf_counter() - started
                self.events.append(
                    {
                        "method": method,
                        "path": path,
                        "status_code": None,
                        "duration_seconds": round(duration, 6),
                        "attempt": attempt,
                        "error": type(exc).__name__,
                    }
                )
                last_error = exc
                if attempt < attempts:
                    time.sleep(min(2 * attempt, 8))
                    continue
        raise APIError(f"{method} {path} failed after retries: {last_error}")

    def get(self, path: str, **kwargs: Any) -> Any:
        return self.request("GET", path, **kwargs).json()

    def post(self, path: str, payload: Any | None = None, **kwargs: Any) -> Any:
        return self.request("POST", path, json=payload, **kwargs).json()

    def connection_types(self) -> list[dict[str, Any]]:
        return self.post("/api/connection/search", {"page": 1, "page_size": 100})["items"]

    def connections(self, name: str | None = None) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {"page": 1, "page_size": 200}
        if name:
            payload["name"] = name
        return self.post("/api/data-connections/search", payload)["items"]


def wait_until(
    description: str,
    getter: Any,
    terminal: Any,
    *,
    timeout_seconds: int,
    poll_seconds: int,
) -> Any:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        value = getter()
        done, failed = terminal(value)
        if done:
            if failed:
                raise APIError(f"{description} failed: {value}")
            return value
        time.sleep(poll_seconds)
    raise TimeoutError(f"Timed out waiting for {description}")


def ensure_connection(
    client: DataFabricClient,
    source: BackendSource,
    connection_type_ids: dict[str, int],
    *,
    organization_id: int,
    timeout_seconds: int,
    poll_seconds: int,
) -> dict[str, Any]:
    connection = next(
        (item for item in client.connections(source.connection_name) if item["name"] == source.connection_name),
        None,
    )
    if connection is None:
        connection_params: dict[str, Any] = {
            "host": source.host,
            "port": source.port,
            "database": source.database,
            "username": source.username,
            "password": source.password,
        }
        if source.connection_type == "postgresql":
            connection_params["sslmode"] = "disable"
        connection = client.post(
            "/api/data-connections/",
            {
                "name": source.connection_name,
                "description": f"Isolated {source.key} source for Dermatology Experiment 3",
                "connection_type_id": connection_type_ids[source.connection_type],
                "organization_id": organization_id,
                "content_type": "metadata",
                "connection_params": connection_params,
                "sync_settings": {"experiment": EXPERIMENT_TAG},
            },
            expected=(201,),
        )

    if connection.get("sync_status") != "success":
        try:
            client.post(
                "/api/internal/process-sync",
                {
                    "connection_id": connection["id"],
                    "job_id": f"dermexp3_sync_{connection['id']}_{int(time.time())}",
                    "priority": 1,
                },
                expected=(200,),
                timeout=300,
                attempts=2,
            )
        except APIError as exc:
            if "409" not in str(exc):
                raise

    def get_connection() -> dict[str, Any]:
        items = client.connections(source.connection_name)
        return next(item for item in items if item["name"] == source.connection_name)

    return wait_until(
        f"metadata sync for {source.connection_name}",
        get_connection,
        lambda value: (
            value.get("sync_status") in {"success", "error"},
            value.get("sync_status") == "error",
        ),
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
    )


def resolve_table(
    client: DataFabricClient, connection_id: int, table_name: str
) -> dict[str, Any]:
    for schema in client.get(f"/api/metadata/connections/{connection_id}/schemas"):
        for table in client.get(f"/api/metadata/schemas/{schema['id']}/tables"):
            if table["table_name"].lower() != table_name.lower():
                continue
            columns = client.get(f"/api/metadata/tables/{table['id']}/columns")
            return {
                "schema": schema,
                "table": table,
                "columns": columns,
                "columns_by_name": {
                    column["column_name"].lower(): column for column in columns
                },
            }
    raise APIError(f"Table {table_name} not found for connection {connection_id}")


def find_or_create(
    items: list[dict[str, Any]],
    predicate: Any,
    creator: Any,
) -> dict[str, Any]:
    existing = next((item for item in items if predicate(item)), None)
    return existing if existing else creator()


def ensure_semantic_domain(client: DataFabricClient) -> dict[str, Any]:
    name = "dermexp3_dermatology"
    return find_or_create(
        client.get("/api/equivalence/semantic-domains"),
        lambda item: item["name"] == name,
        lambda: client.post(
            "/api/equivalence/semantic-domains",
            {
                "name": name,
                "description": "Versioned dermatology concepts for Experiment 3",
                "color": "#4477AA",
                "domain_rules": {"experiment": EXPERIMENT_TAG, "version": "1.0.0"},
            },
        ),
    )


def ensure_dictionary_term(
    client: DataFabricClient,
    domain_id: int,
    *,
    name: str,
    display_name: str,
    standard_values: list[str],
    synonyms: list[str],
) -> dict[str, Any]:
    existing = next(
        (
            item
            for item in client.get("/api/equivalence/data-dictionary")
            if item["name"] == name
        ),
        None,
    )
    payload = {
        "display_name": display_name,
        "description": f"Dermatology Experiment 3 canonical field: {display_name}",
        "semantic_domain_id": domain_id,
        "data_type": "ENUM",
        "standard_values": standard_values,
        "validation_rules": {"experiment": EXPERIMENT_TAG},
        "example_values": {"values": standard_values[:3]},
        "synonyms": synonyms,
    }
    if existing is None:
        return client.post(
            "/api/equivalence/data-dictionary",
            {
                "name": name,
                **payload,
            },
        )
    if (
        list(existing.get("standard_values") or []) != standard_values
        or list(existing.get("synonyms") or []) != synonyms
    ):
        return client.request(
            "PUT",
            f"/api/equivalence/data-dictionary/{existing['id']}",
            json=payload,
        ).json()
    return existing


def ensure_column_group(
    client: DataFabricClient,
    domain_id: int,
    term_id: int,
    *,
    output_name: str,
    catalog_version: str,
) -> dict[str, Any]:
    groups = client.get("/api/equivalence/column-groups")
    return find_or_create(
        groups,
        lambda item: item["name"] == output_name
        and item.get("properties", {}).get("experiment") == EXPERIMENT_TAG
        and item.get("properties", {}).get("catalog_version") == catalog_version,
        lambda: client.post(
            "/api/equivalence/column-groups",
            {
                "name": output_name,
                "description": f"Cross-source {output_name} for Dermatology Experiment 3",
                "semantic_domain_id": domain_id,
                "data_dictionary_term_id": term_id,
                "properties": {
                    "experiment": EXPERIMENT_TAG,
                    "catalog_version": catalog_version,
                },
            },
        ),
    )


def ensure_group_mappings(
    client: DataFabricClient,
    group: dict[str, Any],
    column_ids: list[int],
    value_map: dict[str, str],
) -> dict[str, Any]:
    existing_columns = {
        int(item["column_id"]): item
        for item in client.get(
            f"/api/equivalence/column-groups/{group['id']}/column-mappings"
        )
    }
    for column_id in column_ids:
        if column_id not in existing_columns:
            client.post(
                "/api/equivalence/column-mappings",
                {
                    "group_id": group["id"],
                    "column_id": column_id,
                    "confidence_score": 1.0,
                    "notes": "Predefined Experiment 3 mapping",
                },
            )

    existing_values = {
        (int(item["source_column_id"]), str(item["source_value"])): item
        for item in client.get(
            f"/api/equivalence/column-groups/{group['id']}/value-mappings"
        )
    }
    for column_id in column_ids:
        for source_value, standard_value in value_map.items():
            existing = existing_values.get((column_id, source_value))
            description = "Versioned Experiment 3 value mapping"
            if existing is None:
                client.post(
                    "/api/equivalence/value-mappings",
                    {
                        "group_id": group["id"],
                        "source_column_id": column_id,
                        "source_value": source_value,
                        "standard_value": standard_value,
                        "description": description,
                    },
                )
            elif str(existing.get("standard_value")) != standard_value:
                client.request(
                    "PUT",
                    f"/api/equivalence/value-mappings/{existing['id']}",
                    json={
                        "standard_value": standard_value,
                        "description": description,
                    },
                )
    return client.get(f"/api/equivalence/column-groups/{group['id']}")


def ensure_equivalence_catalog(
    client: DataFabricClient,
    metadata: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    mappings = mapping_config()
    domain = ensure_semantic_domain(client)
    source_keys = [source.key for source in SOURCES]

    diagnosis_values = {
        raw: data["code"] for raw, data in mappings["diagnosis"].items()
    }
    concepts = [
        {
            "output": "diagnosis_harmonized",
            "term": "dermexp3_diagnosis",
            "display": "Harmonized diagnosis",
            "raw_column": "diagnosis_3",
            "sources": source_keys,
            "standard_values": ["MEL", "NV", "BCC", "SCC", "AK", "BKL", "DF", "UNRESOLVED"],
            "synonyms": ["diagnosis_3", "diagnostic", "dx"],
            "value_map": diagnosis_values,
        },
        {
            "output": "recorded_sex",
            "term": "dermexp3_recorded_sex",
            "display": "Recorded sex",
            "raw_column": "sex",
            "sources": source_keys,
            "standard_values": ["FEMALE", "MALE", "UNRESOLVED"],
            "synonyms": ["sex", "sexo"],
            "value_map": mappings["recorded_sex"],
        },
        {
            "output": "image_type_harmonized",
            "term": "dermexp3_image_type",
            "display": "Image modality",
            "raw_column": "image_type",
            "sources": source_keys,
            "standard_values": ["DERMOSCOPIC", "CLINICAL_OVERVIEW", "CLINICAL_CLOSE_UP"],
            "synonyms": ["image_type", "modality"],
            "value_map": mappings["image_type"],
        },
        {
            "output": "fitzpatrick_skin_type_harmonized",
            "term": "dermexp3_fitzpatrick",
            "display": "Recorded Fitzpatrick phototype",
            "raw_column": "fitzpatrick_skin_type",
            "sources": ["hiba", "pad_ufes_20"],
            "standard_values": mappings["fitzpatrick_allowed"],
            "synonyms": ["fitzpatrick_skin_type", "phototype"],
            "value_map": {value: value for value in mappings["fitzpatrick_allowed"]},
        },
        {
            "output": "anatom_site_harmonized",
            "term": "dermexp3_anatom_site",
            "display": "Broad anatomical site",
            "raw_column": "anatom_site_1",
            "sources": source_keys,
            "standard_values": [
                "Anogenital region",
                "Head and neck",
                "Lower extremity",
                "Trunk",
                "Upper extremity",
            ],
            "synonyms": ["anatom_site_1", "localization", "anatomical site"],
            "value_map": {},
        },
    ]

    catalog: dict[str, Any] = {
        "domain": domain,
        "catalog_version": mappings["catalog_version"],
        "groups": {},
    }
    for concept in concepts:
        term = ensure_dictionary_term(
            client,
            domain["id"],
            name=concept["term"],
            display_name=concept["display"],
            standard_values=concept["standard_values"],
            synonyms=concept["synonyms"],
        )
        group = ensure_column_group(
            client,
            domain["id"],
            term["id"],
            output_name=concept["output"],
            catalog_version=mappings["catalog_version"],
        )
        column_ids = [
            metadata[key]["columns_by_name"][concept["raw_column"]]["id"]
            for key in concept["sources"]
        ]
        full_group = ensure_group_mappings(
            client, group, column_ids, concept["value_map"]
        )
        catalog["groups"][concept["output"]] = full_group
    return catalog


def ensure_bronze(
    client: DataFabricClient, table_ids: list[int]
) -> dict[str, Any]:
    existing = next(
        (item for item in client.get("/api/bronze/configs/persistent") if item["name"] == BRONZE_NAME),
        None,
    )
    if existing:
        return existing
    return client.post(
        "/api/bronze/configs/persistent",
        {
            "name": BRONZE_NAME,
            "description": (
                "Raw versioned snapshots for HAM10000, HIBA, and PAD-UFES-20; "
                "no inter-source joins"
            ),
            "tables": [{"table_id": table_id, "select_all": True} for table_id in table_ids],
            "output_format": "delta",
            "enable_federated_joins": False,
            "properties": {"experiment": EXPERIMENT_TAG, "combination": "union_in_silver"},
        },
        expected=(201,),
    )


def latest_execution(client: DataFabricClient, layer: str, config_id: int) -> dict[str, Any] | None:
    path = (
        f"/api/bronze/configs/persistent/{config_id}/executions"
        if layer == "bronze"
        else f"/api/silver/persistent/configs/{config_id}/executions"
    )
    executions = client.get(path)
    return executions[0] if executions else None


def execute_and_wait(
    client: DataFabricClient,
    layer: str,
    config_id: int,
    *,
    timeout_seconds: int,
    poll_seconds: int,
) -> dict[str, Any]:
    if layer == "bronze":
        execute_path = f"/api/bronze/configs/persistent/{config_id}/execute"
    else:
        execute_path = f"/api/silver/persistent/configs/{config_id}/execute"
    # Execution is a mutating operation. Never automatically replay it after
    # an HTTP 5xx; a failed request may already have created an execution row.
    trigger = client.post(execute_path, timeout=300, attempts=1)

    def getter() -> dict[str, Any]:
        execution = latest_execution(client, layer, config_id)
        return execution or {"status": "pending", "trigger": trigger}

    terminal_statuses = {"success", "failed", "partial"} if layer == "bronze" else {"success", "failed"}
    return wait_until(
        f"{layer} execution for config {config_id}",
        getter,
        lambda value: (
            str(value.get("status", "")).lower() in terminal_statuses,
            str(value.get("status", "")).lower() != "success"
            if str(value.get("status", "")).lower() in terminal_statuses
            else False,
        ),
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
    )


def ensure_silver(
    client: DataFabricClient,
    bronze_config_id: int,
    bronze_version: int | None,
    group_ids: list[int],
) -> dict[str, Any]:
    existing = next(
        (item for item in client.get("/api/silver/persistent/configs") if item["name"] == SILVER_NAME),
        None,
    )
    if existing:
        return existing
    return client.post(
        "/api/silver/persistent/configs",
        {
            "name": SILVER_NAME,
            "description": (
                "Union-based dermatology Silver cohort with versioned semantic mappings "
                "and original fields preserved"
            ),
            "source_bronze_config_id": bronze_config_id,
            "source_bronze_version": bronze_version,
            "column_group_ids": group_ids,
            "filters": None,
            "column_transformations": [],
            "exclude_unified_source_columns": False,
        },
        expected=(201,),
    )


def semantic_hash(rows: list[dict[str, Any]]) -> tuple[str, list[str]]:
    dynamic = ["_silver_timestamp", "_transform_config_id"]
    cleaned = [
        {key: value for key, value in row.items() if key not in dynamic}
        for row in rows
    ]
    cleaned.sort(
        key=lambda row: (
            str(row.get("source_collection_id", "")),
            str(row.get("isic_id", "")),
        )
    )
    encoded = json.dumps(
        cleaned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest(), dynamic


def validate_silver_versions(
    client: DataFabricClient,
    config_id: int,
    executions: list[dict[str, Any]],
) -> dict[str, Any]:
    validations = []
    for execution in executions:
        version = execution.get("delta_version")
        query = client.get(
            f"/api/silver/persistent/configs/{config_id}/data",
            params={"version": version, "limit": 100000, "offset": 0},
            timeout=300,
            attempts=2,
        )
        rows = query["data"]
        digest, excluded = semantic_hash(rows)
        source_counts: dict[str, int] = {}
        diagnosis_counts: dict[str, int] = {}
        for row in rows:
            source = str(row.get("source_collection_name", "MISSING"))
            diagnosis = str(row.get("diagnosis_harmonized", "MISSING"))
            source_counts[source] = source_counts.get(source, 0) + 1
            diagnosis_counts[diagnosis] = diagnosis_counts.get(diagnosis, 0) + 1
        validations.append(
            {
                "delta_version": version,
                "execution_id": execution.get("id"),
                "queried_records": len(rows),
                "reported_total_rows": query.get("total_rows"),
                "semantic_sha256": digest,
                "dynamic_columns_excluded": excluded,
                "source_counts": source_counts,
                "diagnosis_counts": diagnosis_counts,
                "query_time_seconds": query.get("execution_time_seconds"),
            }
        )
    hashes = {item["semantic_sha256"] for item in validations}
    counts = {item["queried_records"] for item in validations}
    return {
        "versions": validations,
        "semantic_hashes_identical": len(hashes) == 1,
        "record_counts_identical": len(counts) == 1,
        "expected_records": 15634,
        "expected_record_count_met": counts == {15634},
        "reproducible": len(hashes) == 1 and counts == {15634},
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--organization-id", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument("--silver-runs", type=int, default=3)
    parser.add_argument("--reexecute", action="store_true")
    args = parser.parse_args()

    ensure_output_dirs()
    client = DataFabricClient(args.base_url)
    run: dict[str, Any] = {
        "base_url": args.base_url,
        "experiment": EXPERIMENT_TAG,
        "source_config": source_config(),
        "mapping_versions": {
            "catalog": mapping_config()["catalog_version"],
            "silver_rules": mapping_config()["silver_rule_version"],
        },
        "connections": {},
        "metadata": {},
        "equivalence": {},
        "bronze": {},
        "silver": {},
    }
    run_path = MANIFESTS_DIR / "datafabric_run.json"

    try:
        type_ids = {
            item["name"].lower(): item["id"] for item in client.connection_types()
        }
        for required in {"postgresql", "mysql"}:
            if required not in type_ids:
                raise APIError(f"Connection type not found: {required}")

        metadata: dict[str, dict[str, Any]] = {}
        for source in SOURCES:
            connection = ensure_connection(
                client,
                source,
                type_ids,
                organization_id=args.organization_id,
                timeout_seconds=args.timeout_seconds,
                poll_seconds=args.poll_seconds,
            )
            resolved = resolve_table(client, connection["id"], source.table_name)
            metadata[source.key] = resolved
            run["connections"][source.key] = {
                key: value
                for key, value in connection.items()
                if key not in {"connection_params"}
            }
            run["metadata"][source.key] = {
                "schema_id": resolved["schema"]["id"],
                "schema_name": resolved["schema"]["schema_name"],
                "table_id": resolved["table"]["id"],
                "table_name": resolved["table"]["table_name"],
                "column_ids": {
                    column["column_name"]: column["id"] for column in resolved["columns"]
                },
            }
            print(
                f"Synced {source.key}: table_id={resolved['table']['id']}, "
                f"columns={len(resolved['columns'])}"
            )

        catalog = ensure_equivalence_catalog(client, metadata)
        run["equivalence"] = catalog
        group_ids = [group["id"] for group in catalog["groups"].values()]
        print(f"Configured {len(group_ids)} semantic groups: {group_ids}")

        table_ids = [metadata[source.key]["table"]["id"] for source in SOURCES]
        bronze = ensure_bronze(client, table_ids)
        bronze_execution = latest_execution(client, "bronze", bronze["id"])
        if (
            args.reexecute
            or bronze_execution is None
            or str(bronze_execution.get("status", "")).lower() != "success"
        ):
            bronze_execution = execute_and_wait(
                client,
                "bronze",
                bronze["id"],
                timeout_seconds=args.timeout_seconds,
                poll_seconds=args.poll_seconds,
            )
        run["bronze"] = {"config": bronze, "execution": bronze_execution}
        print(
            f"Bronze success: config_id={bronze['id']}, "
            f"version={bronze_execution.get('delta_version')}"
        )

        silver = ensure_silver(
            client,
            bronze["id"],
            bronze_execution.get("delta_version"),
            group_ids,
        )
        previous_successes = [
            execution
            for execution in client.get(
                f"/api/silver/persistent/configs/{silver['id']}/executions"
            )
            if str(execution.get("status", "")).lower() == "success"
        ]
        previous_successes.sort(
            key=lambda execution: (
                int(execution.get("delta_version") or 0),
                int(execution.get("id") or 0),
            )
        )
        executions = [] if args.reexecute else previous_successes[-args.silver_runs :]
        previous_execution_id: Any = executions[-1].get("id") if executions else None
        for run_number in range(len(executions) + 1, args.silver_runs + 1):
            execution = execute_and_wait(
                client,
                "silver",
                silver["id"],
                timeout_seconds=args.timeout_seconds,
                poll_seconds=args.poll_seconds,
            )
            if execution.get("id") == previous_execution_id:
                raise APIError("Silver execution history did not advance after trigger")
            previous_execution_id = execution.get("id")
            executions.append(execution)
            print(
                f"Silver run {run_number}/{args.silver_runs}: "
                f"execution_id={execution.get('id')}, version={execution.get('delta_version')}"
            )

        if executions:
            print(
                f"Validating {len(executions)} successful Silver versions: "
                f"{[execution.get('delta_version') for execution in executions]}"
            )

        validation = validate_silver_versions(client, silver["id"], executions)
        run["silver"] = {
            "config": silver,
            "executions": executions,
            "validation": validation,
        }
        write_json(RESULTS_DIR / "backend_reproducibility_report.json", validation)
        pd.DataFrame(validation["versions"]).to_csv(
            RESULTS_DIR / "backend_silver_versions.csv", index=False
        )
        if not validation["reproducible"]:
            raise APIError(f"Backend Silver validation failed: {validation}")
        print("Backend Silver versions have identical semantic hashes and 15,634 rows")
        return 0
    except Exception as exc:
        run["error"] = {"type": type(exc).__name__, "message": str(exc)}
        raise
    finally:
        run["api_event_count"] = len(client.events)
        write_json(run_path, run)
        write_json(LOGS_DIR / "datafabric_api_events.json", client.events)
        print(f"Wrote run manifest to {run_path}")


if __name__ == "__main__":
    raise SystemExit(main())
