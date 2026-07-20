#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import os
import re
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
import psycopg2
import requests


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[2]
API_BASE = os.environ.get("DATAFABRIC_API_BASE", "http://localhost:8004/api").rstrip("/")
RESULTS_ROOT = SCRIPT_DIR / "results"
_DEFAULT_RUN_ID = datetime.now().strftime("exp1_sqlquick_%Y%m%d_%H%M%S")
_RUN_SUFFIX = os.environ.get("DATAFABRIC_EXP1_RUN_SUFFIX", "").strip()
RUN_ID = os.environ.get("DATAFABRIC_EXP1_RUN_ID", _DEFAULT_RUN_ID).strip() or _DEFAULT_RUN_ID
if _RUN_SUFFIX:
    RUN_ID = f"{RUN_ID}_{_RUN_SUFFIX}"
RUN_DIR = RESULTS_ROOT / RUN_ID
ARTIFACTS_DIR = RUN_DIR / "artifacts"
RAW_METRICS_CSV = RUN_DIR / "run_metrics.csv"
SUMMARY_JSON = RUN_DIR / "summary.json"
REPORT_MD = RUN_DIR / "final_report.md"
LOG_FILE = RUN_DIR / "run.log"

WARMUPS = 5
TOTAL_RUNS = 35
MEASURED_RUNS = TOTAL_RUNS - WARMUPS

TIMEOUT = 60
SYNC_TIMEOUT = 900
STACK_TIMEOUT = 1800
EXECUTION_TIMEOUT = 1800
SILVER_QUEUE_GRACE_SECONDS = 60

DATAFABRIC_COMPOSE_DIR = ROOT
SOURCE_SQL_COMPOSE_DIR = SCRIPT_DIR / "sql"

DF_SERVICE_LIMITS = {
    "dermalert-backend": {"cpus": "1.5", "memory": "3g"},
    "postgres-backend": {"cpus": "1.0", "memory": "2g"},
    "postgres-airflow": {"cpus": "0.75", "memory": "1536m"},
    "redis": {"cpus": "0.25", "memory": "256m"},
    "minio": {"cpus": "0.5", "memory": "768m"},
    "trino": {"cpus": "1.5", "memory": "3g"},
    "spark-master": {"cpus": "1.0", "memory": "1536m"},
    "spark-worker": {"cpus": "2.0", "memory": "4g"},
    "airflow-webserver": {"cpus": "0.75", "memory": "1024m"},
    "airflow-scheduler": {"cpus": "0.75", "memory": "1024m"},
    "airflow-worker": {"cpus": "1.0", "memory": "1536m"},
    "airflow-triggerer": {"cpus": "0.5", "memory": "768m"},
}

SOURCE_LIMITS = {
    "exp1sql_sus_pg": {"cpus": "0.5", "memory": "1024m"},
    "exp1sql_sus_mysql": {"cpus": "0.5", "memory": "1024m"},
    "exp1sql_lab_pg": {"cpus": "0.5", "memory": "1024m"},
    "exp1sql_img_pg": {"cpus": "0.5", "memory": "1024m"},
    "exp1sql_img_mysql": {"cpus": "0.5", "memory": "1024m"},
    "exp1sql_tele_pg": {"cpus": "0.5", "memory": "1024m"},
    "exp1sql_tele_mysql": {"cpus": "0.5", "memory": "1024m"},
    "exp1sql_far_pg": {"cpus": "0.5", "memory": "1024m"},
}

PARAM_SCHEMA = {
    "type": "object",
    "required": ["host", "port", "database", "username", "password"],
    "properties": {
        "host": {"type": "string"},
        "port": {"type": "integer"},
        "database": {"type": "string"},
        "username": {"type": "string"},
        "password": {"type": "string"},
        "ssl": {"type": ["boolean", "string"]},
        "sslmode": {"type": "string"},
        "sslMode": {"type": "string"},
        "ssl_mode": {"type": "string"},
    },
}


@dataclass(frozen=True)
class ConnectionSpec:
    key: str
    display_name: str
    connection_type: str
    host: str
    port: int
    database: str
    username: str
    password: str
    container_name: str
    extra_params: Dict[str, Any]
    primary_table: str
    primary_column: str
    pair_tables: Tuple[str, str]

DEFAULT_SCENARIOS = ("bronze_pushdown", "bronze_federated", "silver_persistent")
CONNECTION_SPECS = {
    "sus_pg": ConnectionSpec(
        key="sus_pg",
        display_name="exp1sql_sus_pg",
        connection_type="postgresql",
        host="exp1sql_sus_pg",
        port=5432,
        database="db_sus_pg",
        username="suspg_user",
        password="suspg_pass",
        container_name="exp1sql_sus_pg",
        extra_params={},
        primary_table="atendimentos",
        primary_column="cpf",
        pair_tables=("atendimentos", "procedimentos_atendimento"),
    ),
    "lab_pg": ConnectionSpec(
        key="lab_pg",
        display_name="exp1sql_lab_pg",
        connection_type="postgresql",
        host="exp1sql_lab_pg",
        port=5432,
        database="db_laboratorio",
        username="labpg_user",
        password="labpg_pass",
        container_name="exp1sql_lab_pg",
        extra_params={},
        primary_table="exames_solicitados",
        primary_column="cpf",
        pair_tables=("exames_solicitados", "resultados_exame"),
    ),
    "img_mysql": ConnectionSpec(
        key="img_mysql",
        display_name="exp1sql_img_mysql",
        connection_type="mysql",
        host="exp1sql_img_mysql",
        port=3306,
        database="db_imagem_mysql",
        username="imgmy_user",
        password="imgmy_pass",
        container_name="exp1sql_img_mysql",
        extra_params={},
        primary_table="capturas_imagem",
        primary_column="cpf",
        pair_tables=("capturas_imagem", "laudos_imagem"),
    ),
    "tele_mysql": ConnectionSpec(
        key="tele_mysql",
        display_name="exp1sql_tele_mysql",
        connection_type="mysql",
        host="exp1sql_tele_mysql",
        port=3306,
        database="db_telemedicina_mysql",
        username="telemy_user",
        password="telemy_pass",
        container_name="exp1sql_tele_mysql",
        extra_params={},
        primary_table="consultas",
        primary_column="cpf",
        pair_tables=("consultas", "prescricoes_consulta"),
    ),
    "sus_mysql": ConnectionSpec(
        key="sus_mysql",
        display_name="exp1sql_sus_mysql",
        connection_type="mysql",
        host="exp1sql_sus_mysql",
        port=3306,
        database="db_sus_mysql",
        username="susmy_user",
        password="susmy_pass",
        container_name="exp1sql_sus_mysql",
        extra_params={},
        primary_table="atendimentos",
        primary_column="cpf",
        pair_tables=("atendimentos", "procedimentos_atendimento"),
    ),
    "img_pg": ConnectionSpec(
        key="img_pg",
        display_name="exp1sql_img_pg",
        connection_type="postgresql",
        host="exp1sql_img_pg",
        port=5432,
        database="db_imagem_pg",
        username="imgpg_user",
        password="imgpg_pass",
        container_name="exp1sql_img_pg",
        extra_params={},
        primary_table="capturas_imagem",
        primary_column="cpf",
        pair_tables=("capturas_imagem", "laudos_imagem"),
    ),
    "tele_pg": ConnectionSpec(
        key="tele_pg",
        display_name="exp1sql_tele_pg",
        connection_type="postgresql",
        host="exp1sql_tele_pg",
        port=5432,
        database="db_telemedicina_pg",
        username="telepg_user",
        password="telepg_pass",
        container_name="exp1sql_tele_pg",
        extra_params={},
        primary_table="consultas",
        primary_column="cpf",
        pair_tables=("consultas", "prescricoes_consulta"),
    ),
    "far_pg": ConnectionSpec(
        key="far_pg",
        display_name="exp1sql_far_pg",
        connection_type="postgresql",
        host="exp1sql_far_pg",
        port=5432,
        database="db_farmacia",
        username="farpg_user",
        password="farpg_pass",
        container_name="exp1sql_far_pg",
        extra_params={},
        primary_table="dispensacoes",
        primary_column="cpf",
        pair_tables=("dispensacoes", "itens_dispensacao"),
    ),
}

PRIMARY_CHAIN = [
    "sus_pg",
    "lab_pg",
    "img_mysql",
    "tele_mysql",
    "sus_mysql",
    "img_pg",
    "tele_pg",
    "far_pg",
]
TOPOLOGIES = {
    n: PRIMARY_CHAIN[:n]
    for n in range(1, len(PRIMARY_CHAIN) + 1)
}


def _parse_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise SystemExit(f"{name} invalido: {raw}") from exc


def _parse_csv_env(name: str) -> List[str]:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _merge_limit_overrides(
    base_limits: Dict[str, Dict[str, str]],
    overrides: Dict[str, Dict[str, str]],
) -> Dict[str, Dict[str, str]]:
    merged = {key: value.copy() for key, value in base_limits.items()}
    for key, value in overrides.items():
        current = merged.get(key, {}).copy()
        current.update(value)
        merged[key] = current
    return merged


_total_runs = _parse_int_env("DATAFABRIC_EXP1_TOTAL_RUNS", TOTAL_RUNS)
_warmups = _parse_int_env("DATAFABRIC_EXP1_WARMUPS", WARMUPS)
if _warmups < 0 or _total_runs <= 0 or _warmups >= _total_runs:
    raise SystemExit("Warmups/total runs invalidos para DATAFABRIC_EXP1_*")
TOTAL_RUNS = _total_runs
WARMUPS = _warmups
MEASURED_RUNS = TOTAL_RUNS - WARMUPS

_topology_filter = _parse_csv_env("DATAFABRIC_EXP1_TOPOLOGIES")
if _topology_filter:
    _selected_topologies = []
    for item in _topology_filter:
        try:
            _selected_topologies.append(int(item))
        except ValueError as exc:
            raise SystemExit(f"DATAFABRIC_EXP1_TOPOLOGIES invalido: {item}") from exc
    TOPOLOGIES = {n: TOPOLOGIES[n] for n in _selected_topologies if n in TOPOLOGIES}
    if not TOPOLOGIES:
        raise SystemExit("Nenhuma topologia valida selecionada em DATAFABRIC_EXP1_TOPOLOGIES")

ACTIVE_CONNECTION_KEYS = [
    key
    for key in PRIMARY_CHAIN
    if any(key in topology_keys for topology_keys in TOPOLOGIES.values())
]

_scenario_filter = _parse_csv_env("DATAFABRIC_EXP1_SCENARIOS")
if _scenario_filter:
    invalid = [item for item in _scenario_filter if item not in DEFAULT_SCENARIOS]
    if invalid:
        raise SystemExit(f"DATAFABRIC_EXP1_SCENARIOS invalido(s): {', '.join(invalid)}")
    SCENARIOS = tuple(_scenario_filter)
else:
    SCENARIOS = DEFAULT_SCENARIOS

_limit_overrides_raw = os.environ.get("DATAFABRIC_EXP1_LIMIT_OVERRIDES", "").strip()
if _limit_overrides_raw:
    try:
        _limit_overrides = json.loads(_limit_overrides_raw)
    except json.JSONDecodeError as exc:
        raise SystemExit("DATAFABRIC_EXP1_LIMIT_OVERRIDES precisa ser JSON valido") from exc
    DF_SERVICE_LIMITS = _merge_limit_overrides(
        DF_SERVICE_LIMITS,
        _limit_overrides.get("datafabric", {}),
    )
    SOURCE_LIMITS = _merge_limit_overrides(
        SOURCE_LIMITS,
        _limit_overrides.get("sources", {}),
    )


class StatsSampler:
    def __init__(self, container_names: List[str], interval_seconds: float = 1.0):
        self.container_names = container_names
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.samples: List[Dict[str, float]] = []

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> Dict[str, Optional[float]]:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
        if not self.samples:
            return {
                "cpu_avg_percent": None,
                "cpu_max_percent": None,
                "mem_avg_bytes": None,
                "mem_max_bytes": None,
                "sample_count": 0,
            }
        cpu_values = [sample["cpu_percent"] for sample in self.samples]
        mem_values = [sample["mem_bytes"] for sample in self.samples]
        return {
            "cpu_avg_percent": statistics.mean(cpu_values),
            "cpu_max_percent": max(cpu_values),
            "mem_avg_bytes": statistics.mean(mem_values),
            "mem_max_bytes": max(mem_values),
            "sample_count": len(self.samples),
        }

    def _run(self) -> None:
        while not self._stop.is_set():
            snapshot = collect_docker_stats(self.container_names)
            if snapshot:
                total_cpu = sum(item["cpu_percent"] for item in snapshot.values())
                total_mem = sum(item["mem_bytes"] for item in snapshot.values())
                self.samples.append(
                    {
                        "cpu_percent": total_cpu,
                        "mem_bytes": total_mem,
                    }
                )
            self._stop.wait(self.interval_seconds)


class ApiClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def request(
        self,
        method: str,
        path: str,
        *,
        expected: Iterable[int] = (200,),
        json_body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = TIMEOUT,
    ) -> Any:
        url = f"{self.base_url}{path}"
        upper_method = method.upper()
        attempts = 5 if upper_method == "GET" else 1
        backoff = 2.0
        response = None
        last_error: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            try:
                response = self.session.request(
                    upper_method,
                    url,
                    json=json_body,
                    params=params,
                    timeout=timeout,
                )
                last_error = None
                break
            except requests.RequestException as exc:
                last_error = exc
                if attempt == attempts:
                    raise
                time.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
        if response is None:
            raise RuntimeError(f"{upper_method} {url} failed before receiving a response: {last_error}")
        if response.status_code not in set(expected):
            body = response.text
            raise RuntimeError(f"{upper_method} {url} failed: {response.status_code} {body}")
        if response.content:
            return response.json()
        return None


def ensure_dirs() -> None:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line, flush=True)
    with LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def run(
    args: List[str],
    *,
    cwd: Optional[Path] = None,
    check: bool = True,
    capture_output: bool = True,
    env: Optional[Dict[str, str]] = None,
) -> subprocess.CompletedProcess[str]:
    log(f"$ {' '.join(args)}")
    result = subprocess.run(
        args,
        cwd=str(cwd) if cwd else None,
        check=False,
        text=True,
        capture_output=capture_output,
        env=env,
    )
    if result.stdout:
        with LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(result.stdout)
            if not result.stdout.endswith("\n"):
                fh.write("\n")
    if result.stderr:
        with LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(result.stderr)
            if not result.stderr.endswith("\n"):
                fh.write("\n")
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(args)}\n{result.stderr}"
        )
    return result


def docker_compose_up(compose_dir: Path, build: bool = False) -> None:
    cmd = ["docker", "compose", "up", "-d"]
    if build:
        cmd.append("--build")
    run(cmd, cwd=compose_dir)


def docker_compose_down(compose_dir: Path, volumes: bool = False) -> None:
    cmd = ["docker", "compose", "down"]
    if volumes:
        cmd.append("-v")
    run(cmd, cwd=compose_dir, check=False)


def service_container_name(compose_dir: Path, service: str) -> Optional[str]:
    result = run(
        ["docker", "compose", "ps", "-q", service],
        cwd=compose_dir,
        check=False,
    )
    container_id = result.stdout.strip()
    if not container_id:
        return None
    inspect = run(
        ["docker", "inspect", "-f", "{{.Name}}", container_id],
        check=False,
    )
    if inspect.returncode != 0:
        return None
    return inspect.stdout.strip().lstrip("/")


def container_running(name: str) -> bool:
    result = run(
        ["docker", "inspect", "-f", "{{.State.Running}}", name],
        check=False,
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


def container_exists(name: str) -> bool:
    result = run(
        ["docker", "inspect", name],
        check=False,
    )
    return result.returncode == 0


def container_networks(name: str) -> Dict[str, Any]:
    result = run(
        ["docker", "inspect", "-f", "{{json .NetworkSettings.Networks}}", name],
        check=False,
    )
    if result.returncode != 0:
        return {}
    raw = result.stdout.strip() or "{}"
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def ensure_container_network(name: str, network_name: str = "shared-network") -> None:
    networks = container_networks(name)
    if network_name in networks:
        return
    result = run(
        ["docker", "network", "connect", network_name, name],
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Falha ao conectar {name} na rede {network_name}: {result.stderr}")


def ensure_container_started(name: str, stack: Path) -> None:
    if container_running(name):
        ensure_container_network(name)
        return
    if container_exists(name):
        result = run(["docker", "start", name], check=False)
        if result.returncode == 0 and container_running(name):
            ensure_container_network(name)
            return
        log(f"Falha ao iniciar container existente {name}; tentando docker compose up")
    docker_compose_up(stack, build=True)
    ensure_container_network(name)


def wait_for_container_ready(name: str, timeout_seconds: int = 180) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        result = run(
            [
                "docker",
                "inspect",
                "-f",
                "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{if .State.Running}}running{{else}}stopped{{end}}{{end}}",
                name,
            ],
            check=False,
        )
        status = result.stdout.strip() if result.returncode == 0 else ""
        if status in {"healthy", "running"}:
            return
        time.sleep(3)
    raise TimeoutError(f"Timed out waiting container {name} to become ready")


def ensure_source_stacks() -> None:
    log("Recriando bancos locais do exp1 a partir dos SQLs")
    docker_compose_down(SOURCE_SQL_COMPOSE_DIR, volumes=True)
    docker_compose_up(SOURCE_SQL_COMPOSE_DIR, build=False)
    for spec in CONNECTION_SPECS.values():
        ensure_container_network(spec.container_name)
        wait_for_container_ready(spec.container_name, timeout_seconds=600)


def wait_for_http(url: str, timeout_seconds: int, success_codes: Tuple[int, ...] = (200,)) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code in success_codes:
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    raise TimeoutError(f"Timed out waiting for {url}")


def wait_for_datafabric_stack() -> None:
    stack_ready = False
    try:
        wait_for_http("http://localhost:9000/minio/health/ready", 5)
        wait_for_http("http://localhost:8089/v1/info", 5)
        wait_for_http("http://localhost:8004/docs", 5)
        stack_ready = True
    except Exception:
        stack_ready = False
    if stack_ready:
        log("Stack principal do datafabric ja esta saudavel; reutilizando ambiente")
    else:
        log("Subindo stack principal do datafabric")
        docker_compose_up(DATAFABRIC_COMPOSE_DIR, build=True)
    wait_for_http("http://localhost:9000/minio/health/ready", 600)
    wait_for_http("http://localhost:8089/v1/info", 600)
    wait_for_http("http://localhost:8082", 600)
    wait_for_http("http://localhost:8080/health", 600)
    wait_for_http("http://localhost:8004/docs", STACK_TIMEOUT)


def apply_container_limits() -> Dict[str, Dict[str, str]]:
    applied: Dict[str, Dict[str, str]] = {}
    for service, limits in DF_SERVICE_LIMITS.items():
        name = service_container_name(DATAFABRIC_COMPOSE_DIR, service)
        if not name:
            continue
        run(
            [
                "docker",
                "update",
                "--cpus",
                limits["cpus"],
                "--memory",
                limits["memory"],
                "--memory-swap",
                limits["memory"],
                name,
            ],
            check=False,
        )
        applied[name] = limits
    for name, limits in SOURCE_LIMITS.items():
        if container_running(name):
            run(
                [
                    "docker",
                    "update",
                    "--cpus",
                    limits["cpus"],
                    "--memory",
                    limits["memory"],
                    "--memory-swap",
                    limits["memory"],
                    name,
                ],
                check=False,
            )
            applied[name] = limits
    return applied


def bootstrap_organization() -> None:
    log("Garantindo organizacao base no banco interno do datafabric")
    conn = psycopg2.connect(
        host="localhost",
        port=5434,
        user="postgres",
        password="postgres",
        dbname="dermalert_backend",
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS core")
                cur.execute("SELECT id FROM core.organizacoes WHERE id = 1")
                row = cur.fetchone()
                if not row:
                    cur.execute(
                        "INSERT INTO core.organizacoes (id, nome) VALUES (1, %s)",
                        ("Experimento 1",),
                    )
    finally:
        conn.close()


def bytes_from_human(value: str) -> int:
    value = value.strip()
    match = re.match(r"([0-9.]+)\s*([A-Za-z]+)", value)
    if not match:
        return 0
    number = float(match.group(1))
    unit = match.group(2).lower()
    units = {
        "b": 1,
        "kb": 1000,
        "kib": 1024,
        "mb": 1000**2,
        "mib": 1024**2,
        "gb": 1000**3,
        "gib": 1024**3,
        "tb": 1000**4,
        "tib": 1024**4,
    }
    return int(number * units.get(unit, 1))


def collect_docker_stats(container_names: List[str]) -> Dict[str, Dict[str, float]]:
    existing = []
    for name in container_names:
        if container_running(name):
            existing.append(name)
    if not existing:
        return {}
    result = run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", *existing],
        check=False,
    )
    if result.returncode != 0:
        return {}
    parsed: Dict[str, Dict[str, float]] = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        item = json.loads(line)
        cpu = float(item["CPUPerc"].replace("%", "").strip() or "0")
        mem_usage = item["MemUsage"].split("/")[0].strip()
        parsed[item["Name"]] = {
            "cpu_percent": cpu,
            "mem_bytes": bytes_from_human(mem_usage),
        }
    return parsed


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def ensure_connection_types(api: ApiClient) -> Dict[str, int]:
    log("Garantindo connection types necessarios")
    existing = api.request(
        "POST",
        "/connection/search",
        expected=(200,),
        json_body={"page": 1, "size": 100},
    )
    by_name = {item["name"].lower(): item for item in existing["items"]}
    ids: Dict[str, int] = {}
    desired = [
        {
            "name": "postgresql",
            "description": "PostgreSQL via direct query",
            "icon": "database",
            "color_hex": "#336791",
        },
        {
            "name": "mysql",
            "description": "MySQL via direct query",
            "icon": "database",
            "color_hex": "#4479A1",
        },
    ]
    for item in desired:
        current = by_name.get(item["name"])
        payload = {
            "name": item["name"],
            "description": item["description"],
            "icon": item["icon"],
            "color_hex": item["color_hex"],
            "connection_params_schema": PARAM_SCHEMA,
            "metadata_extraction_method": "direct_query",
        }
        if current:
            ids[item["name"]] = current["id"]
            needs_update = any(current.get(k) != payload[k] for k in ("description", "icon", "color_hex", "metadata_extraction_method"))
            if needs_update or current.get("connection_params_schema") != PARAM_SCHEMA:
                updated = api.request(
                    "PUT",
                    f"/connection/{current['id']}",
                    expected=(200,),
                    json_body=payload,
                )
                ids[item["name"]] = updated["id"]
        else:
            created = api.request(
                "POST",
                "/connection/",
                expected=(201,),
                json_body=payload,
            )
            ids[item["name"]] = created["id"]
    save_json(ARTIFACTS_DIR / "connection_types.json", ids)
    return ids


def run_connection_test_with_retries(
    api: ApiClient,
    path: str,
    payload: Optional[Dict[str, Any]] = None,
    *,
    attempts: int = 3,
    sleep_seconds: int = 10,
) -> Dict[str, Any]:
    last_error: Optional[str] = None
    for attempt in range(1, attempts + 1):
        try:
            result = api.request(
                "POST",
                path,
                expected=(200,),
                json_body=payload,
                timeout=120,
            )
        except Exception as exc:
            last_error = str(exc)
            result = None
        else:
            if result.get("success", True):
                return result
            last_error = str(result)
        if attempt < attempts:
            log(f"Aviso: teste de conexao falhou em {path} tentativa {attempt}/{attempts}: {last_error}")
            time.sleep(sleep_seconds)
    raise RuntimeError(f"Connection test failed after {attempts} attempts for {path}: {last_error}")


def ensure_data_connections(api: ApiClient, type_ids: Dict[str, int]) -> Dict[str, Dict[str, Any]]:
    log("Criando ou atualizando data connections")
    connections: Dict[str, Dict[str, Any]] = {}
    for key in ACTIVE_CONNECTION_KEYS:
        spec = CONNECTION_SPECS[key]
        connection_params = {
            "host": spec.host,
            "port": spec.port,
            "database": spec.database,
            "username": spec.username,
            "password": spec.password,
            **spec.extra_params,
        }
        payload = {
            "organization_id": 1,
            "name": spec.display_name,
            "description": f"Experimento 1 remoto - {spec.key}",
            "connection_type_id": type_ids[spec.connection_type],
            "content_type": "metadata",
            "connection_params": connection_params,
            "sync_settings": {"mode": "full"},
            "cron_expression": None,
        }
        current = find_data_connection_by_name(api, spec.display_name)
        run_connection_test_with_retries(
            api,
            "/data-connections/test",
            {
                "connection_type_id": payload["connection_type_id"],
                "connection_params": payload["connection_params"],
            },
        )
        if current:
            changed = (
                current["connection_type_id"] != payload["connection_type_id"]
                or current["connection_params"] != payload["connection_params"]
                or current["description"] != payload["description"]
            )
            if changed:
                current = api.request(
                    "PUT",
                    f"/data-connections/{current['id']}",
                    expected=(200,),
                    json_body={
                        "name": payload["name"],
                        "description": payload["description"],
                        "connection_params": payload["connection_params"],
                        "sync_settings": payload["sync_settings"],
                        "content_type": payload["content_type"],
                    },
                )
        else:
            try:
                current = api.request(
                    "POST",
                    "/data-connections/",
                    expected=(201,),
                    json_body=payload,
                )
            except RuntimeError as exc:
                if "409" not in str(exc):
                    raise
                current = find_data_connection_by_name(api, spec.display_name)
                if not current:
                    raise
        run_connection_test_with_retries(
            api,
            f"/data-connections/{current['id']}/test",
        )
        details = current.get("sync_progress_details") or {}
        if not (current.get("sync_progress") == 100 and details.get("phase") == "complete"):
            sync_connection(api, current["id"])
        current = api.request("GET", f"/data-connections/{current['id']}", expected=(200,))
        connections[key] = current
    save_json(ARTIFACTS_DIR / "data_connections.json", connections)
    return connections


def find_data_connection_by_name(api: ApiClient, name: str) -> Optional[Dict[str, Any]]:
    result = api.request(
        "POST",
        "/data-connections/search",
        expected=(200,),
        json_body={"page": 1, "size": 100, "organization_id": 1, "name": name},
    )
    for item in result["items"]:
        if item["name"] == name:
            return item
    return None


def sync_connection(api: ApiClient, connection_id: int) -> None:
    job_id = f"exp1_direct_sync_{connection_id}_{int(time.time())}"
    api.request(
        "POST",
        "/internal/process-sync",
        expected=(200,),
        json_body={
            "connection_id": connection_id,
            "job_id": job_id,
            "priority": 1,
        },
        timeout=EXECUTION_TIMEOUT,
    )
    deadline = time.time() + SYNC_TIMEOUT
    while time.time() < deadline:
        current = api.request("GET", f"/data-connections/{connection_id}", expected=(200,))
        status = current.get("sync_status")
        details = current.get("sync_progress_details") or {}
        progress = current.get("sync_progress")
        if status in {"success", "partial"}:
            return
        if progress == 100 and details.get("phase") == "complete":
            log(
                f"Aviso: connection {connection_id} marcou sync_progress=100/complete "
                f"mas manteve sync_status={status}; tratando como concluido"
            )
            return
        if status == "failed":
            raise RuntimeError(f"Metadata sync failed for connection {connection_id}: {current}")
        time.sleep(5)
    raise TimeoutError(f"Timed out waiting metadata sync for connection {connection_id}")


def list_connection_tables(api: ApiClient, connection_id: int) -> Dict[str, Dict[str, Any]]:
    schemas = api.request(
        "GET",
        f"/metadata/connections/{connection_id}/schemas",
        expected=(200,),
        params={"is_system_schema": "false"},
    )
    tables_by_name: Dict[str, Dict[str, Any]] = {}
    for schema in schemas:
        tables = api.request(
            "GET",
            f"/metadata/schemas/{schema['id']}/tables",
            expected=(200,),
        )
        for table in tables:
            table["_schema"] = schema
            tables_by_name[table["table_name"]] = table
    return tables_by_name


def list_columns(api: ApiClient, table_id: int) -> Dict[str, Dict[str, Any]]:
    columns = api.request(
        "GET",
        f"/metadata/tables/{table_id}/columns",
        expected=(200,),
    )
    return {column["column_name"]: column for column in columns}


def build_metadata_index(
    api: ApiClient,
    connections: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    log("Indexando tabelas e colunas")
    metadata_index: Dict[str, Dict[str, Any]] = {}
    for key, connection in connections.items():
        spec = CONNECTION_SPECS[key]
        tables = list_connection_tables(api, connection["id"])
        required_names = set(spec.pair_tables) | {spec.primary_table}
        missing = required_names - set(tables.keys())
        if missing:
            raise RuntimeError(f"Missing tables for {spec.display_name}: {sorted(missing)}")
        table_index: Dict[str, Any] = {
            "connection": connection,
            "tables": {},
        }
        for table_name in required_names:
            table = tables[table_name]
            table_index["tables"][table_name] = {
                "table": table,
                "columns": list_columns(api, table["id"]),
            }
        metadata_index[key] = table_index
    save_json(ARTIFACTS_DIR / "metadata_index.json", metadata_index)
    return metadata_index


def extract_constraints_and_accept_intra(
    api: ApiClient,
    connections: Dict[str, Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    log("Extraindo constraints e aceitando relacionamentos intra-DB")
    accepted: Dict[str, List[Dict[str, Any]]] = {}
    for key, connection in connections.items():
        try:
            api.request(
                "POST",
                f"/metadata/connections/{connection['id']}/extract-constraints",
                expected=(200,),
                json_body={"schemas": []},
                timeout=180,
            )
        except RuntimeError as exc:
            log(f"Aviso: extract-constraints falhou para {key}: {exc}")
        api.request(
            "POST",
            "/relationships/discover",
            expected=(200,),
            json_body={"connection_ids": [connection["id"]], "auto_accept": False},
            timeout=180,
        )
        pending = api.request(
            "GET",
            "/relationships/suggestions/pending",
            expected=(200,),
            params={"connection_id": connection["id"], "size": 100, "page": 1},
        )
        accepted[key] = []
        for suggestion in pending["items"]:
            rel = api.request(
                "POST",
                f"/relationships/suggestions/{suggestion['id']}/accept",
                expected=(200,),
                params={"default_join_type": "full"},
            )
            accepted[key].append(rel)
    save_json(ARTIFACTS_DIR / "accepted_intra_relationships.json", accepted)
    return accepted


def existing_relationships_for_connection(api: ApiClient, connection_id: int, scope: str) -> List[Dict[str, Any]]:
    return api.request(
        "GET",
        f"/relationships/connection/{connection_id}",
        expected=(200,),
        params={"scope": scope},
    )


def relationship_matches(
    rel: Dict[str, Any],
    left_table_id: int,
    left_column_id: int,
    right_table_id: int,
    right_column_id: int,
) -> bool:
    pairs = [
        (
            rel["left_column"]["table_id"],
            rel["left_column"]["column_id"],
            rel["right_column"]["table_id"],
            rel["right_column"]["column_id"],
        ),
        (
            rel["right_column"]["table_id"],
            rel["right_column"]["column_id"],
            rel["left_column"]["table_id"],
            rel["left_column"]["column_id"],
        ),
    ]
    target = (left_table_id, left_column_id, right_table_id, right_column_id)
    return target in pairs


def ensure_manual_inter_relationships(
    api: ApiClient,
    metadata_index: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    log("Criando relacionamentos inter-DB por CPF")
    created: Dict[str, Dict[str, Any]] = {}
    for left_key, right_key in zip(PRIMARY_CHAIN, PRIMARY_CHAIN[1:]):
        if left_key not in metadata_index or right_key not in metadata_index:
            continue
        left_spec = CONNECTION_SPECS[left_key]
        right_spec = CONNECTION_SPECS[right_key]
        left_table = metadata_index[left_key]["tables"][left_spec.primary_table]["table"]
        right_table = metadata_index[right_key]["tables"][right_spec.primary_table]["table"]
        left_col = metadata_index[left_key]["tables"][left_spec.primary_table]["columns"][left_spec.primary_column]
        right_col = metadata_index[right_key]["tables"][right_spec.primary_table]["columns"][right_spec.primary_column]
        existing = existing_relationships_for_connection(
            api,
            metadata_index[left_key]["connection"]["id"],
            "inter_connection",
        )
        found = None
        for rel in existing:
            if relationship_matches(rel, left_table["id"], left_col["id"], right_table["id"], right_col["id"]):
                found = rel
                break
        if not found:
            found = api.request(
                "POST",
                "/relationships/",
                expected=(201,),
                json_body={
                    "left_table_id": left_table["id"],
                    "left_column_id": left_col["id"],
                    "right_table_id": right_table["id"],
                    "right_column_id": right_col["id"],
                    "name": f"{left_key}_to_{right_key}_cpf",
                    "description": f"Ligacao CPF entre {left_key} e {right_key}",
                    "cardinality": "many_to_many",
                    "default_join_type": "full",
                    "properties": {"experiment": "exp1", "link_key": "cpf"},
                },
            )
        created[f"{left_key}->{right_key}"] = found
    save_json(ARTIFACTS_DIR / "manual_inter_relationships.json", created)
    return created


def create_federation(api: ApiClient, topology_n: int, connection_ids: List[int], table_ids: List[int]) -> Dict[str, Any]:
    federation = api.request(
        "POST",
        "/federations",
        expected=(201,),
        json_body={
            "name": f"{RUN_ID}_topology_n{topology_n}",
            "description": f"Experimento 1 - topologia N={topology_n}",
        },
    )
    api.request(
        "POST",
        f"/federations/{federation['id']}/connections",
        expected=(200,),
        json_body={"connection_ids": connection_ids},
    )
    unique_table_ids = sorted(set(table_ids))
    if unique_table_ids:
        api.request(
            "POST",
            f"/federations/{federation['id']}/tables",
            expected=(200,),
            json_body={"table_ids": unique_table_ids},
        )
    return federation


def collect_relationship_ids(
    topology_keys: List[str],
    accepted_intra: Dict[str, List[Dict[str, Any]]],
    inter_relationships: Dict[str, Dict[str, Any]],
) -> Tuple[List[int], List[int]]:
    intra_ids: List[int] = []
    inter_ids: List[int] = []
    topology_set = set(topology_keys)
    for key in topology_keys:
        intra_ids.extend(rel["id"] for rel in accepted_intra.get(key, []))
    for chain_key, rel in inter_relationships.items():
        left_key, right_key = chain_key.split("->")
        if left_key in topology_set and right_key in topology_set:
            inter_ids.append(rel["id"])
    return sorted(set(intra_ids)), sorted(set(inter_ids))


def topology_table_ids(topology_keys: List[str], metadata_index: Dict[str, Dict[str, Any]]) -> List[int]:
    table_ids: List[int] = []
    for key in topology_keys:
        spec = CONNECTION_SPECS[key]
        table_ids.append(metadata_index[key]["tables"][spec.primary_table]["table"]["id"])
        for name in spec.pair_tables:
            table_ids.append(metadata_index[key]["tables"][name]["table"]["id"])
    return sorted(set(table_ids))


def table_payloads(table_names: List[str], metadata_for_connection: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        {
            "table_id": metadata_for_connection["tables"][name]["table"]["id"],
            "select_all": True,
        }
        for name in table_names
    ]


def create_bronze_config(
    api: ApiClient,
    name: str,
    description: str,
    tables: List[Dict[str, Any]],
    relationship_ids: List[int],
    enable_federated_joins: bool,
) -> Dict[str, Any]:
    config = api.request(
        "POST",
        "/bronze/configs/persistent",
        expected=(201,),
        json_body={
            "name": name,
            "description": description,
            "tables": tables,
            "relationship_ids": relationship_ids,
            "enable_federated_joins": enable_federated_joins,
            "output_format": "delta",
            "write_mode": "overwrite",
        },
    )
    preview = api.request(
        "POST",
        f"/bronze/configs/persistent/{config['id']}/preview",
        expected=(200,),
    )
    save_json(ARTIFACTS_DIR / "previews" / f"{name}.json", preview)
    return config


def create_silver_config(
    api: ApiClient,
    name: str,
    description: str,
    bronze_config_id: int,
) -> Dict[str, Any]:
    config = api.request(
        "POST",
        "/silver/persistent/configs",
        expected=(200, 201),
        json_body={
            "name": name,
            "description": description,
            "source_bronze_config_id": bronze_config_id,
        },
    )
    preview = api.request(
        "POST",
        f"/silver/persistent/configs/{config['id']}/preview",
        expected=(200,),
    )
    save_json(ARTIFACTS_DIR / "previews" / f"{name}.json", preview)
    return config


def bootstrap_bronze_source(api: ApiClient, config_id: int) -> Dict[str, Any]:
    log(f"Executando Bronze bootstrap para Silver (config_id={config_id})")
    return api.request(
        "POST",
        f"/bronze/configs/persistent/{config_id}/execute",
        expected=(200,),
        timeout=EXECUTION_TIMEOUT,
    )


def latest_version_metrics(api: ApiClient, scenario: str, config_id: int) -> Dict[str, Any]:
    if scenario.startswith("bronze"):
        history = api.request(
            "GET",
            f"/bronze/configs/persistent/{config_id}/versions",
            expected=(200,),
            params={"limit": 1},
        )
    else:
        history = api.request(
            "GET",
            f"/silver/persistent/configs/{config_id}/versions",
            expected=(200,),
            params={"limit": 1},
        )
    version = history["versions"][0] if history.get("versions") else {}
    return {
        "delta_current_version": history.get("current_version"),
        "delta_version": version.get("version"),
        "size_bytes": version.get("size_bytes"),
        "num_files": version.get("num_files"),
        "version_total_rows": version.get("total_rows"),
        "operation": version.get("operation"),
    }


def latest_execution_status(api: ApiClient, scenario: str, config_id: int) -> Dict[str, Any]:
    if scenario.startswith("bronze"):
        history = api.request(
            "GET",
            f"/bronze/configs/persistent/{config_id}/executions",
            expected=(200,),
            params={"limit": 1},
        )
    else:
        history = api.request(
            "GET",
            f"/silver/persistent/configs/{config_id}/executions",
            expected=(200,),
            params={"limit": 1},
        )
    return history[0] if history else {}


def dispatch_silver_internal(api: ApiClient, config_id: int, execution_id: int) -> Dict[str, Any]:
    return api.request(
        "POST",
        "/internal/spark/execute",
        expected=(200,),
        json_body={
            "job_type": "silver",
            "config_id": config_id,
            "execution_id": execution_id,
            "priority": 1,
        },
        timeout=EXECUTION_TIMEOUT,
    )


def execute_workload(
    api: ApiClient,
    scenario: str,
    config_id: int,
    monitor_containers: List[str],
) -> Dict[str, Any]:
    sampler = StatsSampler(monitor_containers)
    sampler.start()
    started = time.perf_counter()
    response_payload: Dict[str, Any] = {}
    failure = None
    status = "unknown"
    latest_exec: Dict[str, Any] = {}
    try:
        if scenario.startswith("bronze"):
            response_payload = api.request(
                "POST",
                f"/bronze/configs/persistent/{config_id}/execute",
                expected=(200,),
                timeout=EXECUTION_TIMEOUT,
            )
        else:
            response_payload = api.request(
                "POST",
                f"/silver/persistent/configs/{config_id}/execute",
                expected=(200, 202),
                timeout=EXECUTION_TIMEOUT,
            )
            if response_payload.get("status") == "queued":
                execution_id = response_payload["execution_id"]
                deadline = time.time() + EXECUTION_TIMEOUT
                fallback_triggered = False
                while time.time() < deadline:
                    latest = latest_execution_status(api, scenario, config_id)
                    if latest.get("id") == execution_id and latest.get("status") not in {"queued", "pending", "running"}:
                        response_payload = latest
                        break
                    if (
                        not fallback_triggered
                        and latest.get("id") == execution_id
                        and latest.get("status") in {"queued", "pending"}
                        and (deadline - time.time()) <= (EXECUTION_TIMEOUT - SILVER_QUEUE_GRACE_SECONDS)
                    ):
                        log(
                            f"Silver execution {execution_id} remained {latest.get('status')} "
                            f"for over {SILVER_QUEUE_GRACE_SECONDS}s; dispatching internal fallback"
                        )
                        dispatch_silver_internal(api, config_id, execution_id)
                        fallback_triggered = True
                    time.sleep(5)
                else:
                    raise TimeoutError(f"Timed out waiting queued silver execution {execution_id}")
        status = response_payload.get("status", "success")
    except Exception as exc:
        failure = str(exc)
    wall_latency = time.perf_counter() - started
    resource_metrics = sampler.stop()
    try:
        latest_exec = latest_execution_status(api, scenario, config_id)
    except Exception as exc:
        if not failure:
            failure = f"Could not read latest execution status: {exc}"
    latest_version = {}
    try:
        latest_version = latest_version_metrics(api, scenario, config_id)
    except Exception as exc:
        if not failure:
            log(f"Aviso: nao foi possivel obter metrics de versao para {scenario}/{config_id}: {exc}")
    if failure:
        status = latest_exec.get("status", "failed")
    success = failure is None and status in {"success", "SUCCESS"}
    rows_processed = None
    rows_output = None
    execution_time_seconds = response_payload.get("execution_time_seconds")
    if scenario.startswith("bronze"):
        rows_processed = response_payload.get("total_rows_ingested")
        rows_output = latest_version.get("version_total_rows")
    else:
        rows_processed = response_payload.get("rows_processed", latest_exec.get("rows_processed"))
        rows_output = response_payload.get("rows_output", latest_exec.get("rows_output"))
    return {
        "success": success,
        "status": status,
        "error_message": failure or latest_exec.get("error_message"),
        "latency_seconds_wall": wall_latency,
        "execution_time_seconds_api": execution_time_seconds,
        "rows_processed": rows_processed,
        "rows_output": rows_output,
        "size_bytes": latest_version.get("size_bytes"),
        "num_files": latest_version.get("num_files"),
        "delta_version": latest_version.get("delta_version"),
        "delta_current_version": latest_version.get("delta_current_version"),
        "delta_total_rows": latest_version.get("version_total_rows"),
        "resource_metrics": resource_metrics,
        "raw_response": response_payload,
        "raw_execution": latest_exec,
        "raw_version": latest_version,
    }


def mean_or_none(values: List[float]) -> Optional[float]:
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return statistics.mean(clean)


def median_or_none(values: List[float]) -> Optional[float]:
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return statistics.median(clean)


def p95_or_none(values: List[float]) -> Optional[float]:
    clean = sorted(v for v in values if v is not None)
    if not clean:
        return None
    index = math.ceil(0.95 * len(clean)) - 1
    return clean[max(0, min(index, len(clean) - 1))]


def stdev_or_none(values: List[float]) -> Optional[float]:
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    return statistics.stdev(clean)


def build_summary(rows: List[Dict[str, Any]], applied_limits: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "run_id": RUN_ID,
        "generated_at": datetime.now().isoformat(),
        "warmups_discarded": WARMUPS,
        "total_runs_per_scenario": TOTAL_RUNS,
        "measured_runs_per_scenario": MEASURED_RUNS,
        "api_base": API_BASE,
        "sql_source_order": PRIMARY_CHAIN,
        "sql_sources": {
            key: {
                "display_name": spec.display_name,
                "connection_type": spec.connection_type,
                "host": spec.host,
                "port": spec.port,
                "database": spec.database,
                "primary_table": spec.primary_table,
                "pair_tables": list(spec.pair_tables),
                "extra_params_keys": sorted(spec.extra_params.keys()),
            }
            for key, spec in CONNECTION_SPECS.items()
            if key in ACTIVE_CONNECTION_KEYS
        },
        "resource_limits": applied_limits,
        "scenarios": {},
        "notes": [
            "Bronze rows_processed = total_rows_ingested do execute.",
            "Bronze rows_output = latest versions[0].total_rows.",
            "Bronze persistent currently persists with overwrite regardless of requested write_mode.",
            "Relationship discovery via API discovered only intra-DB FK relations; inter-DB CPF links were created manually.",
            "CPU/RAM aggregate local DataFabric containers and the SQL source containers created for this quick run.",
        ],
    }
    for topology in sorted({row["topology_n"] for row in rows}):
        summary["scenarios"][str(topology)] = {}
        for scenario in sorted({row["scenario"] for row in rows if row["topology_n"] == topology}):
            scoped = [
                row for row in rows
                if row["topology_n"] == topology and row["scenario"] == scenario and not row["is_warmup"]
            ]
            successes = [row for row in scoped if row["success"]]
            latency_values = [row["latency_seconds_wall"] for row in successes]
            rows_processed_values = [row["rows_processed"] for row in successes]
            rows_output_values = [row["rows_output"] for row in successes]
            size_values = [row["size_bytes"] for row in successes]
            num_files_values = [row["num_files"] for row in successes]
            cpu_values = [row["cpu_avg_percent"] for row in successes]
            mem_values = [row["mem_avg_bytes"] for row in successes]
            failure_rate = 1 - (len(successes) / len(scoped) if scoped else 0)
            summary["scenarios"][str(topology)][scenario] = {
                "runs_measured": len(scoped),
                "successes": len(successes),
                "failures": len(scoped) - len(successes),
                "failure_rate": failure_rate,
                "latency_mean_s": mean_or_none(latency_values),
                "latency_median_s": median_or_none(latency_values),
                "latency_p95_s": p95_or_none(latency_values),
                "latency_stdev_s": stdev_or_none(latency_values),
                "rows_processed_mean": mean_or_none(rows_processed_values),
                "rows_output_mean": mean_or_none(rows_output_values),
                "size_bytes_mean": mean_or_none(size_values),
                "num_files_mean": mean_or_none(num_files_values),
                "cpu_avg_percent_mean": mean_or_none(cpu_values),
                "mem_avg_bytes_mean": mean_or_none(mem_values),
                "errors": sorted(set(row["error_message"] for row in scoped if row["error_message"])),
            }
    return summary


def render_report(summary: Dict[str, Any], rows: List[Dict[str, Any]]) -> str:
    lines = [
        f"# Relatorio {RUN_ID}",
        "",
        "## Escopo",
        "",
        f"- Fontes SQL locais: {', '.join(PRIMARY_CHAIN)}",
        f"- Topologias avaliadas: N={','.join(str(n) for n in TOPOLOGIES.keys())}",
        f"- Workloads: {', '.join(SCENARIOS)}",
        f"- Rodadas por cenario: {TOTAL_RUNS}",
        f"- Warm-ups descartados: {WARMUPS}",
        f"- Rodadas medidas por cenario: {MEASURED_RUNS}",
        "",
        "## Limites fixados de CPU/RAM locais",
        "",
    ]
    for container, limits in sorted(summary["resource_limits"].items()):
        lines.append(f"- `{container}`: cpu={limits['cpus']} mem={limits['memory']}")
    lines.extend(
        [
            "",
            "## Resultados consolidados",
            "",
            "| Topologia | Cenario | Sucesso | Falha | Latencia media (s) | P95 (s) | Rows processed | Rows output | Size medio (bytes) | Num files medio | CPU medio (%) | RAM media (bytes) |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for topology, scenarios in summary["scenarios"].items():
        for scenario, stats in scenarios.items():
            lines.append(
                "| {topology} | {scenario} | {successes} | {failures} | {latency_mean:.4f} | {latency_p95:.4f} | {rows_processed:.2f} | {rows_output:.2f} | {size_bytes:.2f} | {num_files:.2f} | {cpu_avg:.2f} | {mem_avg:.2f} |".format(
                    topology=topology,
                    scenario=scenario,
                    successes=stats["successes"],
                    failures=stats["failures"],
                    latency_mean=stats["latency_mean_s"] or 0.0,
                    latency_p95=stats["latency_p95_s"] or 0.0,
                    rows_processed=stats["rows_processed_mean"] or 0.0,
                    rows_output=stats["rows_output_mean"] or 0.0,
                    size_bytes=stats["size_bytes_mean"] or 0.0,
                    num_files=stats["num_files_mean"] or 0.0,
                    cpu_avg=stats["cpu_avg_percent_mean"] or 0.0,
                    mem_avg=stats["mem_avg_bytes_mean"] or 0.0,
                )
            )
    lines.extend(
        [
            "",
            "## Observacoes",
            "",
            "- Este benchmark usa somente bancos remotos como fontes; nenhum banco local participa das topologias testadas.",
            "- CPU/RAM no relatorio refletem apenas os containers locais do DataFabric. Os recursos internos dos bancos remotos gerenciados nao sao observaveis daqui.",
            "- `bronze_federated` com N=1 funciona como baseline de uma unica conexao, nao como federacao real.",
            "- `rows_processed`/`rows_output` no Bronze sao metricas derivadas, conforme mapeamento descrito no README.",
            "- A descoberta automatica de relacionamentos ficou restrita a FK intra-DB; os links cross-database foram criados manualmente por CPF via API.",
            "- O workload Bronze persistente no backend atual escreve em `overwrite` mesmo quando o schema da API menciona outros modos.",
            "",
            "## Falhas observadas",
            "",
        ]
    )
    errors = sorted(set(row["error_message"] for row in rows if row["error_message"]))
    if errors:
        for err in errors:
            lines.append(f"- {err}")
    else:
        lines.append("- Nenhuma falha registrada nas rodadas medidas.")
    return "\n".join(lines) + "\n"


def write_metrics_csv(rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "run_id",
        "timestamp",
        "topology_n",
        "scenario",
        "iteration",
        "is_warmup",
        "success",
        "status",
        "latency_seconds_wall",
        "execution_time_seconds_api",
        "rows_processed",
        "rows_output",
        "size_bytes",
        "num_files",
        "delta_version",
        "delta_current_version",
        "cpu_avg_percent",
        "cpu_max_percent",
        "mem_avg_bytes",
        "mem_max_bytes",
        "sample_count",
        "error_message",
        "config_id",
    ]
    with RAW_METRICS_CSV.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def build_workloads(
    api: ApiClient,
    topology_n: int,
    topology_keys: List[str],
    metadata_index: Dict[str, Dict[str, Any]],
    accepted_intra: Dict[str, List[Dict[str, Any]]],
    inter_relationships: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    intra_ids, inter_ids = collect_relationship_ids(topology_keys, accepted_intra, inter_relationships)
    bronze_pushdown_tables: List[Dict[str, Any]] = []
    bronze_source_tables: List[Dict[str, Any]] = []
    connection_ids = [metadata_index[key]["connection"]["id"] for key in topology_keys]
    for key in topology_keys:
        bronze_pushdown_tables.extend(
            table_payloads(list(CONNECTION_SPECS[key].pair_tables), metadata_index[key])
        )
        bronze_source_tables.append(
            {
                "table_id": metadata_index[key]["tables"][CONNECTION_SPECS[key].primary_table]["table"]["id"],
                "select_all": True,
            }
        )
    federation = create_federation(
        api,
        topology_n,
        connection_ids=connection_ids,
        table_ids=topology_table_ids(topology_keys, metadata_index),
    )
    workloads: Dict[str, Dict[str, Any]] = {"federation": federation}
    workloads["bronze_pushdown"] = create_bronze_config(
        api,
        name=f"{RUN_ID}_n{topology_n}_bronze_pushdown",
        description=f"Exp1 N={topology_n} bronze pushdown",
        tables=bronze_pushdown_tables,
        relationship_ids=intra_ids,
        enable_federated_joins=False,
    )
    workloads["bronze_federated"] = create_bronze_config(
        api,
        name=f"{RUN_ID}_n{topology_n}_bronze_federated",
        description=f"Exp1 N={topology_n} bronze federado",
        tables=bronze_source_tables,
        relationship_ids=inter_ids,
        enable_federated_joins=True,
    )
    bronze_source = create_bronze_config(
        api,
        name=f"{RUN_ID}_n{topology_n}_bronze_source_for_silver",
        description=f"Exp1 N={topology_n} bronze source for silver",
        tables=bronze_source_tables,
        relationship_ids=inter_ids,
        enable_federated_joins=False,
    )
    workloads["bronze_source_for_silver"] = bronze_source
    bootstrap_bronze_source(api, bronze_source["id"])
    workloads["silver_persistent"] = create_silver_config(
        api,
        name=f"{RUN_ID}_n{topology_n}_silver_persistent",
        description=f"Exp1 N={topology_n} silver persistente",
        bronze_config_id=bronze_source["id"],
    )
    return workloads


def monitor_container_names(topology_keys: List[str]) -> List[str]:
    names: List[str] = []
    for service in DF_SERVICE_LIMITS:
        name = service_container_name(DATAFABRIC_COMPOSE_DIR, service)
        if name:
            names.append(name)
    for key in topology_keys:
        names.append(CONNECTION_SPECS[key].container_name)
    return sorted(set(names))


def main() -> int:
    ensure_dirs()
    log(f"Iniciando {RUN_ID}")
    log(f"Fontes SQL configuradas: {PRIMARY_CHAIN}")
    log(f"Topologias selecionadas: {list(TOPOLOGIES.keys())}")
    log(f"Cenarios selecionados: {list(SCENARIOS)}")
    log(f"Rodadas por cenario: total={TOTAL_RUNS} warmups={WARMUPS} medidas={MEASURED_RUNS}")
    ensure_source_stacks()
    wait_for_datafabric_stack()
    applied_limits = apply_container_limits()
    bootstrap_organization()
    api = ApiClient(API_BASE)
    type_ids = ensure_connection_types(api)
    connections = ensure_data_connections(api, type_ids)
    metadata_index = build_metadata_index(api, connections)
    accepted_intra = extract_constraints_and_accept_intra(api, connections)
    inter_relationships = ensure_manual_inter_relationships(api, metadata_index)

    save_json(ARTIFACTS_DIR / "topologies.json", TOPOLOGIES)
    save_json(
        ARTIFACTS_DIR / "sql_sources.json",
        {
            key: {
                "display_name": spec.display_name,
                "connection_type": spec.connection_type,
                "host": spec.host,
                "port": spec.port,
                "database": spec.database,
                "primary_table": spec.primary_table,
                "pair_tables": list(spec.pair_tables),
                "extra_params_keys": sorted(spec.extra_params.keys()),
            }
            for key, spec in CONNECTION_SPECS.items()
            if key in ACTIVE_CONNECTION_KEYS
        },
    )
    run_rows: List[Dict[str, Any]] = []
    workload_index: Dict[str, Any] = {}

    for topology_n, topology_keys in TOPOLOGIES.items():
        log(f"Preparando topologia N={topology_n}: {topology_keys}")
        workloads = build_workloads(
            api,
            topology_n,
            topology_keys,
            metadata_index,
            accepted_intra,
            inter_relationships,
        )
        workload_index[str(topology_n)] = workloads
        monitor_names = monitor_container_names(topology_keys)
        for scenario in SCENARIOS:
            config_id = workloads[scenario]["id"]
            log(f"Executando cenario {scenario} para N={topology_n} com {TOTAL_RUNS} rodadas")
            for iteration in range(1, TOTAL_RUNS + 1):
                is_warmup = iteration <= WARMUPS
                result = execute_workload(api, scenario, config_id, monitor_names)
                row = {
                    "run_id": RUN_ID,
                    "timestamp": datetime.now().isoformat(),
                    "topology_n": topology_n,
                    "scenario": scenario,
                    "iteration": iteration,
                    "is_warmup": is_warmup,
                    "config_id": config_id,
                    "success": result["success"],
                    "status": result["status"],
                    "latency_seconds_wall": result["latency_seconds_wall"],
                    "execution_time_seconds_api": result["execution_time_seconds_api"],
                    "rows_processed": result["rows_processed"],
                    "rows_output": result["rows_output"],
                    "size_bytes": result["size_bytes"],
                    "num_files": result["num_files"],
                    "delta_version": result["delta_version"],
                    "delta_current_version": result["delta_current_version"],
                    "cpu_avg_percent": result["resource_metrics"]["cpu_avg_percent"],
                    "cpu_max_percent": result["resource_metrics"]["cpu_max_percent"],
                    "mem_avg_bytes": result["resource_metrics"]["mem_avg_bytes"],
                    "mem_max_bytes": result["resource_metrics"]["mem_max_bytes"],
                    "sample_count": result["resource_metrics"]["sample_count"],
                    "error_message": result["error_message"],
                }
                run_rows.append(row)
                save_json(
                    ARTIFACTS_DIR / "executions" / f"n{topology_n}_{scenario}_{iteration:02d}.json",
                    {
                        "row": row,
                        "raw_response": result["raw_response"],
                        "raw_execution": result["raw_execution"],
                        "raw_version": result["raw_version"],
                    },
                )
                status_label = "warmup" if is_warmup else "measured"
                log(
                    f"N={topology_n} {scenario} iter={iteration}/{TOTAL_RUNS} "
                    f"[{status_label}] success={row['success']} latency={row['latency_seconds_wall']:.3f}s"
                )

    save_json(ARTIFACTS_DIR / "workloads.json", workload_index)
    write_metrics_csv(run_rows)
    summary = build_summary(run_rows, applied_limits)
    save_json(SUMMARY_JSON, summary)
    REPORT_MD.write_text(render_report(summary, run_rows), encoding="utf-8")
    log(f"Experimento concluido. Resultados em {RUN_DIR}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        ensure_dirs()
        log(f"Falha fatal: {exc}")
        raise
