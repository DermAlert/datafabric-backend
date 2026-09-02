#!/usr/bin/env python3
"""Seed and execute the local, synthetic DermAlert federation demonstration.

The script deliberately uses only local Docker services and the real FastAPI
routes. It never reads the legacy remote connection already present in some
developer databases.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = [
    "docker",
    "compose",
    "-f",
    str(ROOT / "docker-compose.yml"),
    "-f",
    str(ROOT / "docker-compose.demo.yml"),
]
API_BASE = "http://127.0.0.1:8004"

IDA_CONNECTION = "IDA Synthetic Demo"
CMPD_CONNECTION = "CMPD Synthetic Demo"
BRONZE_CONFIG = "dermalert_demo_bronze"
SILVER_CONFIG = "dermalert_demo_silver"
SEMANTIC_DOMAIN = "dermalert_demo_demographics"
DICTIONARY_TERM = "dermalert_demo_biological_sex"
COLUMN_GROUP = "sexo_biologico_padronizado"
CATALOG_VERSION = "dermalert-demo-semantic-v1"


class DemoError(RuntimeError):
    pass


def run(
    command: list[str],
    *,
    input_bytes: bytes | None = None,
    timeout: int = 600,
) -> str:
    result = subprocess.run(
        command,
        cwd=ROOT,
        input=input_bytes,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        raise DemoError(
            f"Command failed ({result.returncode}): {' '.join(command)}\n"
            f"{stderr or stdout}"
        )
    return result.stdout.decode("utf-8", errors="replace").strip()


def compose_exec(service: str, command: list[str], *, sql_file: Path | None = None) -> str:
    payload = sql_file.read_bytes() if sql_file else None
    return run(COMPOSE + ["exec", "-T", service, *command], input_bytes=payload)


def api(
    method: str,
    path: str,
    payload: dict[str, Any] | list[Any] | None = None,
    *,
    timeout: int = 600,
) -> Any:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{API_BASE}{path}",
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise DemoError(f"{method} {path} returned HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise DemoError(f"{method} {path} failed: {exc}") from exc


def wait_for_api() -> None:
    deadline = time.monotonic() + 120
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if api("GET", "/api/internal/health", timeout=5).get("status") == "healthy":
                return
        except Exception as exc:  # service startup is intentionally retried
            last_error = exc
        time.sleep(2)
    raise DemoError(f"Backend did not become healthy: {last_error}")


def search(path: str, **filters: Any) -> list[dict[str, Any]]:
    payload = {
        "pagination": {"skip": 0, "limit": 500, "query_total": True},
        **filters,
    }
    response = api("POST", path, payload)
    return response.get("items", [])


def by_name(items: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    return next((item for item in items if item.get("name") == name), None)


def connection_type_id(name: str) -> int:
    matches = search("/api/connection/search", name=name)
    exact = by_name(matches, name)
    if not exact:
        raise DemoError(f"Required connection type {name!r} is not installed")
    return int(exact["id"])


def upsert_connection(
    *,
    name: str,
    connection_type: str,
    params: dict[str, Any],
) -> int:
    matches = search("/api/data-connections/search", organization_id=1, name=name)
    exact = by_name(matches, name)
    common = {
        "name": name,
        "description": "Deterministic local-only synthetic DermAlert demonstration source",
        "connection_params": params,
        "content_type": "metadata",
        "status": "active",
        "sync_settings": {"synthetic": True, "seed": CATALOG_VERSION},
    }
    if exact:
        connection = api("PUT", f"/api/data-connections/{exact['id']}", common)
    else:
        connection = api(
            "POST",
            "/api/data-connections/",
            {
                **common,
                "organization_id": 1,
                "connection_type_id": connection_type_id(connection_type),
            },
        )
    connection_id = int(connection["id"])
    tested = api("POST", f"/api/data-connections/{connection_id}/test", timeout=120)
    if not tested.get("success"):
        raise DemoError(f"Connection test failed for {name}: {tested}")
    synced = api(
        "POST",
        "/api/internal/process-sync",
        {
            "connection_id": connection_id,
            "job_id": f"{CATALOG_VERSION}-{connection_id}",
            "priority": 1,
        },
        timeout=600,
    )
    if synced.get("status") != "completed":
        raise DemoError(f"Metadata sync failed for {name}: {synced}")
    return connection_id


def metadata_table(
    connection_id: int,
    schema_name: str,
    table_name: str,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    schemas = api("GET", f"/api/metadata/connections/{connection_id}/schemas")
    schema = next((item for item in schemas if item["schema_name"] == schema_name), None)
    if not schema:
        raise DemoError(f"Schema {schema_name!r} not found for connection {connection_id}")
    tables = api("GET", f"/api/metadata/schemas/{schema['id']}/tables")
    table = next((item for item in tables if item["table_name"] == table_name), None)
    if not table:
        raise DemoError(f"Table {schema_name}.{table_name} not found")
    columns = api("GET", f"/api/metadata/tables/{table['id']}/columns")
    return table, {column["column_name"]: column for column in columns}


def upsert_relationship(
    ficha_table: dict[str, Any],
    ficha_columns: dict[str, dict[str, Any]],
    perfil_table: dict[str, Any],
    perfil_columns: dict[str, dict[str, Any]],
) -> int:
    left_column_id = int(ficha_columns["cpf_titular"]["id"])
    right_column_id = int(perfil_columns["numero_cpf"]["id"])
    relationships = api("GET", f"/api/relationships/table/{ficha_table['id']}?include_inactive=true")
    existing = next(
        (
            item
            for item in relationships
            if item["left_column"]["column_id"] == left_column_id
            and item["right_column"]["column_id"] == right_column_id
        ),
        None,
    )
    update = {
        "name": "dermalert_demo_cpf_join",
        "description": "Synthetic one-to-one relationship; values are DEV identifiers, not CPFs",
        "cardinality": "one_to_one",
        "default_join_type": "inner",
        "is_verified": True,
        "is_active": True,
        "properties": {"synthetic": True, "seed": CATALOG_VERSION},
    }
    if existing:
        relationship = api("PUT", f"/api/relationships/{existing['id']}", update)
    else:
        relationship = api(
            "POST",
            "/api/relationships/",
            {
                "left_table_id": ficha_table["id"],
                "left_column_id": left_column_id,
                "right_table_id": perfil_table["id"],
                "right_column_id": right_column_id,
                "name": update["name"],
                "description": update["description"],
                "cardinality": update["cardinality"],
                "default_join_type": update["default_join_type"],
                "properties": update["properties"],
            },
        )
    return int(relationship["id"])


def upsert_named(
    list_path: str,
    create_path: str,
    update_path: str,
    name: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    items = api("GET", list_path)
    existing = by_name(items, name)
    if existing:
        return api("PUT", update_path.format(id=existing["id"]), payload)
    return api("POST", create_path, {"name": name, **payload})


def upsert_semantic_catalog(
    ficha_columns: dict[str, dict[str, Any]],
    perfil_columns: dict[str, dict[str, Any]],
) -> int:
    domain = upsert_named(
        "/api/equivalence/semantic-domains",
        "/api/equivalence/semantic-domains",
        "/api/equivalence/semantic-domains/{id}",
        SEMANTIC_DOMAIN,
        {
            "description": "Synthetic demonstration demographics",
            "color": "#2563eb",
            "domain_rules": {"synthetic": True},
        },
    )
    term = upsert_named(
        "/api/equivalence/data-dictionary",
        "/api/equivalence/data-dictionary",
        "/api/equivalence/data-dictionary/{id}",
        DICTIONARY_TERM,
        {
            "display_name": "Biological sex (synthetic demo)",
            "description": "Two-value standard used only by the synthetic local demo",
            "semantic_domain_id": domain["id"],
            "data_type": "ENUM",
            "standard_values": ["F", "M"],
            "validation_rules": {"nullable": True},
            "example_values": {"synthetic": ["F", "M"]},
            "synonyms": ["sexo_biologico", "genero_identidade"],
        },
    )
    group = upsert_named(
        "/api/equivalence/column-groups",
        "/api/equivalence/column-groups",
        "/api/equivalence/column-groups/{id}",
        COLUMN_GROUP,
        {
            "description": "Synthetic equivalence requested for the local DermAlert demonstration",
            "semantic_domain_id": domain["id"],
            "data_dictionary_term_id": term["id"],
            "properties": {
                "synthetic": True,
                "semantic_catalog_version": CATALOG_VERSION,
                "native_catalog_versioning": False,
            },
        },
    )
    group_id = int(group["id"])

    desired_columns = [
        (int(ficha_columns["sexo_biologico"]["id"]), "Verified against IDA seed values"),
        (int(perfil_columns["genero_identidade"]["id"]), "Verified against CMPD seed values"),
    ]
    current_column_mappings = api(
        "GET", f"/api/equivalence/column-groups/{group_id}/column-mappings"
    )
    for column_id, notes in desired_columns:
        existing = next(
            (item for item in current_column_mappings if item["column_id"] == column_id),
            None,
        )
        payload = {
            "transformation_rule": "value_mapping_then_coalesce",
            "confidence_score": 1.0,
            "notes": notes,
        }
        if existing:
            api("PUT", f"/api/equivalence/column-mappings/{existing['id']}", payload)
        else:
            api(
                "POST",
                "/api/equivalence/column-mappings",
                {"group_id": group_id, "column_id": column_id, **payload},
            )

    expected_value_mappings = [
        (int(ficha_columns["sexo_biologico"]["id"]), "feminino", "F", 709),
        (int(ficha_columns["sexo_biologico"]["id"]), "masculino", "M", 710),
        (int(perfil_columns["genero_identidade"]["id"]), "feminino", "F", 721),
        (int(perfil_columns["genero_identidade"]["id"]), "masculino", "M", 721),
    ]
    current_value_mappings = api(
        "GET", f"/api/equivalence/column-groups/{group_id}/value-mappings"
    )
    for column_id, source_value, standard_value, record_count in expected_value_mappings:
        existing = next(
            (
                item
                for item in current_value_mappings
                if item["source_column_id"] == column_id
                and item["source_value"] == source_value
            ),
            None,
        )
        description = f"Verified deterministic synthetic mapping ({CATALOG_VERSION})"
        if existing:
            api(
                "PUT",
                f"/api/equivalence/value-mappings/{existing['id']}",
                {
                    "source_value": source_value,
                    "standard_value": standard_value,
                    "description": description,
                    "record_count": record_count,
                },
            )
        else:
            created = api(
                "POST",
                "/api/equivalence/value-mappings",
                {
                    "group_id": group_id,
                    "source_column_id": column_id,
                    "source_value": source_value,
                    "standard_value": standard_value,
                    "description": description,
                },
            )
            api(
                "PUT",
                f"/api/equivalence/value-mappings/{created['id']}",
                {"record_count": record_count},
            )
    return group_id


def upsert_bronze_config(table_ids: list[int], relationship_id: int) -> int:
    payload = {
        "description": "Persistent federated snapshot of the two local synthetic sources",
        "tables": [{"table_id": table_id, "select_all": True} for table_id in table_ids],
        "relationship_ids": [relationship_id],
        "enable_federated_joins": True,
        "output_format": "delta",
        "write_mode": "overwrite",
        "merge_keys": None,
        "output_bucket": "datafabric-bronze",
        "output_path_prefix": "dermalert-demo",
        "partition_columns": None,
        "properties": {"synthetic": True, "seed": CATALOG_VERSION},
        "is_active": True,
    }
    configs = api("GET", "/api/bronze/configs/persistent?include_inactive=true")
    existing = by_name(configs, BRONZE_CONFIG)
    if existing:
        config = api("PUT", f"/api/bronze/configs/persistent/{existing['id']}", payload)
    else:
        create_payload = {key: value for key, value in payload.items() if key != "is_active"}
        config = api(
            "POST",
            "/api/bronze/configs/persistent",
            {"name": BRONZE_CONFIG, **create_payload},
        )
    return int(config["id"])


def upsert_silver_config(
    bronze_config_id: int,
    group_id: int,
    ficha_columns: dict[str, dict[str, Any]],
    perfil_columns: dict[str, dict[str, Any]],
) -> int:
    payload = {
        "description": "Latest Bronze to Silver with semantic and text normalization",
        "source_bronze_config_id": bronze_config_id,
        "source_bronze_version": None,
        "silver_bucket": "datafabric-silver",
        "silver_path_prefix": "dermalert-demo",
        "column_group_ids": [group_id],
        "filters": None,
        "column_transformations": [
            {
                "column_id": int(ficha_columns["cor_pele_fitzpatrick"]["id"]),
                "type": "remove_accents",
            },
            {
                "column_id": int(perfil_columns["fumante_status"]["id"]),
                "type": "uppercase",
            },
        ],
        "image_labeling_config": None,
        "llm_extractions": None,
        "exclude_unified_source_columns": False,
        "is_active": True,
    }
    configs = api("GET", "/api/silver/persistent/configs?include_inactive=true")
    existing = by_name(configs, SILVER_CONFIG)
    if existing:
        config = api("PUT", f"/api/silver/persistent/configs/{existing['id']}", payload)
    else:
        create_payload = {key: value for key, value in payload.items() if key != "is_active"}
        config = api(
            "POST",
            "/api/silver/persistent/configs",
            {"name": SILVER_CONFIG, **create_payload},
        )
    return int(config["id"])


def seed_sources() -> None:
    compose_exec(
        "mysql-ida",
        [
            "mysql",
            "--default-character-set=utf8mb4",
            "-u",
            "dermalert_dev",
            "-pdermalert_dev",
            "ida",
        ],
        sql_file=ROOT / "scripts/sql/seed_ida_mysql.sql",
    )
    compose_exec(
        "postgres-cmpd",
        ["psql", "-v", "ON_ERROR_STOP=1", "-U", "dermalert_dev", "-d", "cmpd"],
        sql_file=ROOT / "scripts/sql/seed_cmpd_postgres.sql",
    )
    ida_rows = compose_exec(
        "mysql-ida",
        [
            "mysql",
            "-N",
            "-B",
            "-u",
            "dermalert_dev",
            "-pdermalert_dev",
            "ida",
            "-e",
            "SELECT COUNT(*) FROM ficha_dermato",
        ],
    )
    cmpd_rows = compose_exec(
        "postgres-cmpd",
        [
            "psql",
            "-At",
            "-U",
            "dermalert_dev",
            "-d",
            "cmpd",
            "-c",
            "SELECT COUNT(*) FROM perfil_saude",
        ],
    )
    if ida_rows != "1507" or cmpd_rows != "1507":
        raise DemoError(f"Unexpected seeded row counts: IDA={ida_rows}, CMPD={cmpd_rows}")


def configure() -> dict[str, int]:
    ida_id = upsert_connection(
        name=IDA_CONNECTION,
        connection_type="mysql",
        params={
            "host": "mysql-ida",
            "port": 3306,
            "database": "ida",
            "username": "dermalert_dev",
            "password": "dermalert_dev",
            "sslmode": "disable",
        },
    )
    cmpd_id = upsert_connection(
        name=CMPD_CONNECTION,
        connection_type="postgresql",
        params={
            "host": "postgres-cmpd",
            "port": 5432,
            "database": "cmpd",
            "username": "dermalert_dev",
            "password": "dermalert_dev",
            "sslmode": "disable",
        },
    )
    ficha_table, ficha_columns = metadata_table(ida_id, "ida", "ficha_dermato")
    perfil_table, perfil_columns = metadata_table(cmpd_id, "public", "perfil_saude")
    relationship_id = upsert_relationship(
        ficha_table, ficha_columns, perfil_table, perfil_columns
    )
    group_id = upsert_semantic_catalog(ficha_columns, perfil_columns)
    bronze_config_id = upsert_bronze_config(
        [int(ficha_table["id"]), int(perfil_table["id"])], relationship_id
    )
    return {
        "ida_connection_id": ida_id,
        "cmpd_connection_id": cmpd_id,
        "ficha_table_id": int(ficha_table["id"]),
        "perfil_table_id": int(perfil_table["id"]),
        "relationship_id": relationship_id,
        "column_group_id": group_id,
        "bronze_config_id": bronze_config_id,
    }


def seed() -> dict[str, int]:
    wait_for_api()
    seed_sources()
    state = configure()
    print(json.dumps({"status": "seeded", **state}, indent=2, sort_keys=True))
    return state


def build() -> dict[str, Any]:
    wait_for_api()
    state = configure()
    bronze = api(
        "POST",
        f"/api/bronze/configs/persistent/{state['bronze_config_id']}/execute",
        timeout=1200,
    )
    if bronze.get("status") != "success" or bronze.get("total_rows_ingested") != 1507:
        raise DemoError(f"Bronze execution did not produce 1,507 rows: {bronze}")
    _, ficha_columns = metadata_table(
        state["ida_connection_id"], "ida", "ficha_dermato"
    )
    _, perfil_columns = metadata_table(
        state["cmpd_connection_id"], "public", "perfil_saude"
    )
    silver_config_id = upsert_silver_config(
        state["bronze_config_id"],
        state["column_group_id"],
        ficha_columns,
        perfil_columns,
    )
    state["silver_config_id"] = silver_config_id
    silver = api(
        "POST",
        f"/api/silver/persistent/configs/{silver_config_id}/execute",
        timeout=1200,
    )
    if silver.get("status") != "success" or silver.get("rows_output") != 1507:
        raise DemoError(f"Silver execution did not produce 1,507 rows: {silver}")
    result = {
        "status": "built",
        **state,
        "bronze_execution_id": bronze["execution_id"],
        "bronze_version": bronze.get("delta_version"),
        "bronze_rows": bronze["total_rows_ingested"],
        "silver_execution_id": silver["execution_id"],
        "silver_version": silver.get("delta_version"),
        "silver_rows": silver["rows_output"],
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("seed", "build", "all"))
    args = parser.parse_args()
    if args.command == "seed":
        seed()
    elif args.command == "build":
        build()
    else:
        seed()
        build()


if __name__ == "__main__":
    try:
        main()
    except (DemoError, subprocess.TimeoutExpired) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
