"""
AGHUse Ambulatorio Scraper DAG
================================
Extrai consultas ambulatoriais do prontuário online (POL) do AGHUse
e persiste no Delta Lake (MinIO) com **upsert/merge** por chave natural.

Fluxo (só requests, sem Selenium):
  1. Login via JSF form (credenciais em Airflow Variables)
  2. Seta contexto do paciente via addPOLProntuario no casca
  3. GET direto em consultaAmbulatorio.xhtml (iframe do POL)
  4. Pesquisa para ativar datatable, exporta XLS (Tudo) para pegar todos os registros
  5. Coleta Agenda+Descrição via rowSelect para os registros da primeira página
  6. Merge no Delta Lake (insert novos, update alterados, mantém existentes)

Deduplicação: cada registro recebe um `record_id` (SHA-256) baseado na
chave natural (prontuario + data_consulta + especialidade + profissional).
"""
from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import json
import logging
import re
from typing import Any, Dict, List
from urllib.parse import urljoin

import pandas as pd
import pyarrow as pa
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from deltalake import DeltaTable, write_deltalake
from minio import Minio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Airflow Variables (senha nunca no código)
# ---------------------------------------------------------------------------
def _var(name: str, default: str = "") -> str:
    return Variable.get(name, default_var=default)

AGHUSE_BASE      = "https://aghuse.saude.unicamp.br"
AGHUSE_LOGIN_URL = _var("AGHUSE_LOGIN_URL", f"{AGHUSE_BASE}/aghu/login.xhtml")
AGHUSE_USERNAME  = _var("AGHUSE_USERNAME")
AGHUSE_PASSWORD  = _var("AGHUSE_PASSWORD")
CASCA_URL        = f"{AGHUSE_BASE}/aghu/pages/casca/casca.xhtml"
AMB_URL          = f"{AGHUSE_BASE}/aghu/pages/paciente/prontuarioonline/consultaAmbulatorio.xhtml"

MINIO_ENDPOINT   = _var("AGHUSE_MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = _var("AGHUSE_MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = _var("AGHUSE_MINIO_SECRET_KEY", "minio123")
MINIO_SECURE     = _var("AGHUSE_MINIO_SECURE", "false").lower() == "true"
DELTA_BUCKET     = _var("AGHUSE_DELTA_BUCKET", "aghu-delta")
DELTA_PATH       = _var("AGHUSE_DELTA_TABLE_PATH", "aghu/consultas_ambulatoriais")

# ---------------------------------------------------------------------------
# Lista de prontuários a extrair (hardcoded — adicione novos aqui)
# ---------------------------------------------------------------------------
PRONTUARIOS_DEFAULT = [
    "9812909",   
    "12964234",
    "6227838",
    "3972664",
    "15272761",
]

# ---------------------------------------------------------------------------
# Helpers: MinIO / Delta
# ---------------------------------------------------------------------------
def _storage_options() -> Dict[str, str]:
    proto = "https" if MINIO_SECURE else "http"
    return {
        "AWS_ENDPOINT_URL": f"{proto}://{MINIO_ENDPOINT}",
        "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_ALLOW_HTTP": "true" if not MINIO_SECURE else "false",
    }

def _ensure_bucket(name: str) -> None:
    c = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=MINIO_SECURE)
    if not c.bucket_exists(name):
        c.make_bucket(name)

# ---------------------------------------------------------------------------
# Helpers: AGHUse session
# ---------------------------------------------------------------------------
REQUEST_TIMEOUT = 120  # AGHUse pode ser lento

def _new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 AGHUse-Airflow-Scraper",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return s

def _login(s: requests.Session) -> None:
    if not AGHUSE_USERNAME or not AGHUSE_PASSWORD:
        raise ValueError("Defina AGHUSE_USERNAME e AGHUSE_PASSWORD nas Airflow Variables.")
    html = s.get(AGHUSE_LOGIN_URL, timeout=REQUEST_TIMEOUT).text
    soup = BeautifulSoup(html, "html.parser")
    form = next(
        (f for f in soup.find_all("form") if "formlogin" in (f.get("id") or "").lower()),
        soup.find("form"),
    )
    if not form:
        raise RuntimeError("Form de login não encontrado.")
    payload = {i["name"]: i.get("value", "") for i in form.find_all("input") if i.get("name")}
    payload["username:username:inputId"] = AGHUSE_USERNAME
    payload["inputId"] = AGHUSE_PASSWORD
    payload["loginBtn:button"] = "Entrar"
    action = form.get("action") or AGHUSE_LOGIN_URL
    r = s.post(urljoin(AGHUSE_LOGIN_URL, action), data=payload, timeout=REQUEST_TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    if "login" in r.url.lower() and "casca" not in r.url.lower():
        raise RuntimeError("Login falhou. Verifique usuario/senha.")
    logger.info("Login OK.")

def _set_patient_context(s: requests.Session, prontuario: str) -> None:
    """Seta o prontuário ativo no POL via addPOLProntuario (JSF remoteCommand)."""
    html = s.get(CASCA_URL, timeout=REQUEST_TIMEOUT).text
    soup = BeautifulSoup(html, "html.parser")
    menu = soup.find("form", {"id": "casca_menu_form"})
    if not menu:
        raise RuntimeError("casca_menu_form não encontrado.")
    payload = {i["name"]: i.get("value", "") for i in menu.find_all("input") if i.get("name")}
    m = re.search(r'<script[^>]+id="([^"]+)"[^>]*>\s*addPOLProntuario\s*=', html, re.S)
    src = m.group(1) if m else "j_idt34"
    payload.update({
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": src,
        "javax.faces.partial.execute": src,
        "javax.faces.partial.render": src,
        src: src,
        f"{src}_param1": prontuario,
        f"{src}_param2": "true",
    })
    s.post(CASCA_URL, data=payload, headers={"Faces-Request": "partial/ajax"}, timeout=REQUEST_TIMEOUT)
    logger.info("Contexto do paciente %s definido.", prontuario)

# ---------------------------------------------------------------------------
# Core: parsing helpers
# ---------------------------------------------------------------------------
COLUMNS = ["Data", "Especialidade", "Profissional", "Cod_CMCE", "Unidade_Saude"]


def _full_form_state(soup: BeautifulSoup, form_id: str = "formConsulta") -> Dict[str, str]:
    """Captura TODOS os campos do formulário: inputs, selects e textareas."""
    form = soup.find("form", {"id": form_id})
    if not form:
        return {}
    state: Dict[str, str] = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if name:
            state[name] = inp.get("value", "")
    for sel in form.find_all("select"):
        name = sel.get("name")
        if name:
            opt = sel.find("option", selected=True) or sel.find("option")
            state[name] = opt.get("value", "") if opt else ""
    for ta in form.find_all("textarea"):
        name = ta.get("name")
        if name:
            state[name] = ta.get_text()
    state[form_id] = form_id
    return state

def _parse_rows(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """Extrai rows da tabela de consultas ambulatoriais."""
    date_re = re.compile(r"^\d{2}/\d{2}/\d{4}$")
    rows: List[Dict[str, str]] = []
    for tr in soup.find_all("tr", {"data-ri": True}):
        tds = tr.find_all("td")
        texts = [td.get_text(" ", strip=True) for td in tds]
        date_idx = next((i for i, t in enumerate(texts) if date_re.match(t)), None)
        if date_idx is not None:
            data = texts[date_idx:]
            padded = data + [""] * max(0, len(COLUMNS) - len(data))
            rows.append(dict(zip(COLUMNS, padded[: len(COLUMNS)])))
    return rows

def _parse_total(soup: BeautifulSoup) -> int:
    m = re.search(r"de\s+(\d+)\s+registros", soup.get_text(" ", strip=True))
    return int(m.group(1)) if m else 0

def _parse_agenda_descricao(soup: BeautifulSoup) -> Dict[str, str]:
    """Extrai Agenda e Descrição do painel linhaDescritiva."""
    result = {"agenda": "", "descricao": ""}
    block = soup.find(id="linhaDescritiva")
    if not block:
        block = soup

    # Agenda: busca por id contendo "agenda"
    for el in block.find_all(["div", "fieldset", "span", "input", "textarea"]):
        el_id = el.get("id", "").lower()
        if "agenda" in el_id:
            result["agenda"] = el.get("value", "") or el.get_text(" ", strip=True)
            break

    # Descrição: busca por textarea ou div com id contendo "descri"
    for el in block.find_all(["textarea", "div", "span"]):
        el_id = el.get("id", "").lower()
        if "descri" in el_id:
            txt = el.get("value", "") or el.get_text("\n", strip=True)
            if txt and len(txt) > 5:
                result["descricao"] = txt
                break

    # Fallback: todo o bloco
    if not result["descricao"] and block and block != soup:
        full_text = block.get_text("\n", strip=True)
        if full_text and len(full_text) > 10:
            result["descricao"] = full_text

    return result

# ---------------------------------------------------------------------------
# Task 1: scrape
# ---------------------------------------------------------------------------
def _scrape_one_prontuario(
    s: requests.Session, prontuario: str,
) -> List[Dict[str, Any]]:
    """Extrai todas as consultas ambulatoriais de UM prontuário.

    Recebe uma sessão já logada. Retorna lista de dicts prontos para Delta.
    """
    logger.info("--- Prontuário: %s ---", prontuario)
    _set_patient_context(s, prontuario)

    # Acessa a página de consultas ambulatoriais
    page = s.get(AMB_URL, timeout=REQUEST_TIMEOUT)
    page.raise_for_status()
    soup = BeautifulSoup(page.text, "html.parser")
    total = _parse_total(soup)
    logger.info("Total de consultas ambulatoriais: %d", total)

    if total == 0:
        return []

    all_rows = _parse_rows(soup)
    form = soup.find("form", {"id": "formConsulta"})
    if not form:
        logger.warning("formConsulta não encontrado.")
        return []

    base = {i["name"]: i.get("value", "") for i in form.find_all("input") if i.get("name")}
    table_id = "tabelaAmb:resultList"

    # Pesquisar (POST normal — ativa datatable)
    search_pay = dict(base)
    search_pay["bt_pesquisar:button"] = "bt_pesquisar:button"
    rp = s.post(AMB_URL, data=search_pay, timeout=REQUEST_TIMEOUT)
    sp = BeautifulSoup(rp.text, "html.parser")
    all_rows = _parse_rows(sp)
    total = _parse_total(sp)
    vs = sp.find("input", {"name": "javax.faces.ViewState"})
    if vs:
        base["javax.faces.ViewState"] = vs.get("value", "")
    logger.info("Após pesquisar: %d rows visíveis, total %d", len(all_rows), total)

    # Extrair URL com CID (conversa JSF)
    form2 = sp.find("form", {"id": "formConsulta"})
    form_action = form2.get("action", "") if form2 else ""
    post_url = urljoin(AMB_URL, form_action) if form_action else AMB_URL
    if form2:
        base = {i["name"]: i.get("value", "") for i in form2.find_all("input") if i.get("name")}

    # ---- XLS (Tudo) export — todos os registros de uma vez ----
    if len(all_rows) < total:
        xls_param = None
        for tag in sp.find_all("a"):
            text = tag.get_text(" ", strip=True).lower()
            onclick = tag.get("onclick", "")
            if "xls" in text and "tudo" in text:
                m = re.search(r"\{'([^']+)':'[^']+'\}", onclick)
                if m:
                    xls_param = m.group(1)
                    break

        if xls_param:
            logger.info("Exportando XLS (Tudo) via '%s'...", xls_param)
            xls_pay = dict(base)
            xls_pay[xls_param] = xls_param
            rp_xls = s.post(post_url, data=xls_pay, timeout=REQUEST_TIMEOUT)
            ct = rp_xls.headers.get("Content-Type", "")

            if "spreadsheet" in ct or "excel" in ct or "octet" in ct:
                import io
                xls_df = pd.read_excel(io.BytesIO(rp_xls.content))
                logger.info("XLS: %d rows, cols=%s", len(xls_df), list(xls_df.columns))

                col_map: Dict[str, str] = {}
                for c in xls_df.columns:
                    cl = str(c).strip().lower()
                    if cl == "data":
                        col_map["Data"] = c
                    elif "especialidade" in cl:
                        col_map["Especialidade"] = c
                    elif "profissional" in cl:
                        col_map["Profissional"] = c
                    elif "cmce" in cl or "cód" in cl:
                        col_map["Cod_CMCE"] = c
                    elif "unidade" in cl:
                        col_map["Unidade_Saude"] = c

                xls_rows: List[Dict[str, str]] = []
                for _, r in xls_df.iterrows():
                    row: Dict[str, str] = {}
                    for our_col, xls_col in col_map.items():
                        val = r.get(xls_col, "")
                        if pd.notna(val):
                            row[our_col] = val.strftime("%d/%m/%Y") if hasattr(val, "strftime") else str(val).strip()
                        else:
                            row[our_col] = ""
                    xls_rows.append(row)

                if xls_rows:
                    all_rows = xls_rows
                    total = len(xls_rows)
                    logger.info("XLS exportou %d registros.", total)
            else:
                logger.warning("XLS retornou content-type inesperado: %s", ct)
        else:
            logger.warning("Botão XLS (Tudo) não encontrado.")

    logger.info("Total rows coletadas: %d", len(all_rows))

    # ---- Coleta Agenda + Descrição via rowSelect (todas as páginas) ----
    # Re-pesquisa para ter datatable ativo no servidor
    search_pay2 = dict(base)
    search_pay2["bt_pesquisar:button"] = "bt_pesquisar:button"
    rp_desc = s.post(AMB_URL, data=search_pay2, timeout=REQUEST_TIMEOUT)
    sp_desc = BeautifulSoup(rp_desc.text, "html.parser")
    form_desc = sp_desc.find("form", {"id": "formConsulta"})
    fa = form_desc.get("action", "") if form_desc else ""
    desc_url = urljoin(AMB_URL, fa) if fa else AMB_URL

    # Captura o estado COMPLETO do form (inputs + selects + textareas)
    fstate = _full_form_state(sp_desc)
    page_rows = _parse_rows(sp_desc)
    rows_per_page = len(page_rows) or 10
    desc_total = _parse_total(sp_desc)

    # Extrai column order e toggler state do HTML (campos ocultos do PrimeFaces)
    col_order = fstate.get(f"{table_id}_columnOrder", "")
    col_toggler = fstate.get(f"{table_id}_columnTogglerState", "")

    logger.info("Coleta descrições: %d rows/page, total=%d, url=%s", rows_per_page, desc_total, desc_url)

    desc_map: Dict[str, Dict[str, str]] = {}
    current_selection = ""
    page_offset = 0

    while page_offset < desc_total:
        # Na primeira iteração, usamos a página já carregada; nas demais, paginamos
        if page_offset > 0:
            pag_pay = dict(fstate)
            pag_pay.update({
                "javax.faces.partial.ajax": "true",
                "javax.faces.source": table_id,
                "javax.faces.partial.execute": table_id,
                "javax.faces.partial.render": table_id,
                "javax.faces.behavior.event": "page",
                "javax.faces.partial.event": "page",
                f"{table_id}_pagination": "true",
                f"{table_id}_first": str(page_offset),
                f"{table_id}_rows": str(rows_per_page),
                f"{table_id}_skipChildren": "true",
                f"{table_id}_encodeFeature": "true",
            })
            if current_selection:
                pag_pay[f"{table_id}_selection"] = current_selection
            rp_pag = s.post(desc_url, data=pag_pay,
                            headers={"Faces-Request": "partial/ajax"}, timeout=REQUEST_TIMEOUT)
            vm = re.search(r'<update id="[^"]*ViewState[^"]*">\s*<!\[CDATA\[(.*?)\]\]>', rp_pag.text, re.S)
            if vm:
                fstate["javax.faces.ViewState"] = vm.group(1)
            cdata = "\n".join(re.findall(r"<!\[CDATA\[(.*?)\]\]>", rp_pag.text, re.S))
            sp_page = BeautifulSoup(cdata, "html.parser")
        else:
            sp_page = sp_desc

        # Extrai row keys e rows desta página
        page_rks = [tr["data-rk"] for tr in sp_page.find_all("tr", {"data-rk": True})]
        pg_rows = _parse_rows(sp_page)
        if not page_rks:
            logger.warning("Paginação offset=%d: 0 row_keys. Parando.", page_offset)
            break

        # rowSelect para cada row desta página
        page_descs = 0
        for i, rk in enumerate(page_rks):
            rs_pay = dict(fstate)
            rs_pay.update({
                "javax.faces.partial.ajax": "true",
                "javax.faces.source": table_id,
                "javax.faces.partial.execute": table_id,
                "javax.faces.partial.render": "linhaDescritiva botoes_impressao",
                "javax.faces.behavior.event": "rowSelect",
                "javax.faces.partial.event": "rowSelect",
                f"{table_id}_instantSelectedRowKey": rk,
                f"{table_id}_selection": rk,
                table_id: table_id,
            })
            r_rs = s.post(desc_url, data=rs_pay,
                          headers={"Faces-Request": "partial/ajax"}, timeout=60)
            m_ld = re.search(r'<update id="linhaDescritiva">\s*<!\[CDATA\[(.*?)\]\]>', r_rs.text, re.S)
            ld_content = m_ld.group(1) if m_ld else ""
            vm = re.search(r'<update id="[^"]*ViewState[^"]*">\s*<!\[CDATA\[(.*?)\]\]>', r_rs.text, re.S)
            if vm:
                fstate["javax.faces.ViewState"] = vm.group(1)

            soup_ld = BeautifulSoup(ld_content, "html.parser")
            detail = _parse_agenda_descricao(soup_ld)

            # Atualiza descConsulta no form state (como o browser faz)
            if detail["descricao"]:
                fstate["descConsulta"] = detail["descricao"]

            current_selection = rk

            if i < len(pg_rows) and detail["descricao"]:
                pr = pg_rows[i]
                key = f"{pr.get('Data','')}|{pr.get('Especialidade','')}|{pr.get('Profissional','')}"
                desc_map[key] = detail
                page_descs += 1

        page_num = page_offset // rows_per_page + 1
        logger.info("Página %d (offset=%d): %d/%d rows com descrição (total map=%d)",
                    page_num, page_offset, page_descs, len(page_rks), len(desc_map))
        page_offset += rows_per_page

    logger.info("Descrições coletadas: %d de %d total", len(desc_map), desc_total)

    # Associa descrições aos all_rows por chave
    for row in all_rows:
        key = f"{row.get('Data','')}|{row.get('Especialidade','')}|{row.get('Profissional','')}"
        if key in desc_map:
            row["agenda"] = desc_map[key].get("agenda", "")
            row["descricao"] = desc_map[key].get("descricao", "")

    rows_with_desc = sum(1 for r in all_rows if r.get("descricao"))
    logger.info("Rows com descrição: %d/%d", rows_with_desc, len(all_rows))

    # Normaliza output com record_id
    now = datetime.utcnow().isoformat()
    output: List[Dict[str, Any]] = []
    for row in all_rows:
        rec = {
            "prontuario": prontuario,
            "data_consulta": row.get("Data", ""),
            "especialidade": row.get("Especialidade", ""),
            "profissional": row.get("Profissional", ""),
            "cod_cmce": row.get("Cod_CMCE", ""),
            "unidade_saude": row.get("Unidade_Saude", ""),
            "agenda": row.get("agenda", ""),
            "descricao": row.get("descricao", ""),
            "raw_json": json.dumps(row, ensure_ascii=False),
        }
        key = f"{rec['prontuario']}|{rec['data_consulta']}|{rec['especialidade']}|{rec['profissional']}"
        rec["record_id"] = hashlib.sha256(key.encode()).hexdigest()
        rec["captured_at"] = now
        rec["updated_at"] = now
        output.append(rec)

    # Deduplicar por record_id
    seen: Dict[str, Dict[str, Any]] = {}
    for rec in output:
        rid = rec["record_id"]
        if rid not in seen or (rec["descricao"] and not seen[rid].get("descricao")):
            seen[rid] = rec
    deduped = list(seen.values())
    logger.info("Prontuário %s: %d brutos, %d únicos (total reportado: %d)",
                prontuario, len(output), len(deduped), total)
    return deduped


def scrape_consultas_ambulatoriais(**context: Any) -> List[Dict[str, Any]]:
    """Task principal: extrai consultas de um ou mais prontuários.

    Modos de uso:
      1. Lista hardcoded: PRONTUARIOS_DEFAULT (executado quando não há conf)
      2. API com um prontuário:
           curl -X POST .../dagRuns -d '{"conf": {"prontuario": "9812909"}}'
      3. API com lista:
           curl -X POST .../dagRuns -d '{"conf": {"prontuarios": ["9812909", "1234567"]}}'
    """
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}

    # Determina lista de prontuários
    if "prontuarios" in conf:
        prontuarios = [str(p).strip() for p in conf["prontuarios"]]
    elif "prontuario" in conf:
        prontuarios = [str(conf["prontuario"]).strip()]
    else:
        prontuarios = list(PRONTUARIOS_DEFAULT)

    logger.info("Prontuários a extrair: %s (%d)", prontuarios, len(prontuarios))

    s = _new_session()
    _login(s)

    all_results: List[Dict[str, Any]] = []
    for pront in prontuarios:
        try:
            rows = _scrape_one_prontuario(s, pront)
            all_results.extend(rows)
            logger.info("Prontuário %s: %d registros coletados", pront, len(rows))
        except Exception as e:
            logger.error("Erro no prontuário %s: %s", pront, e, exc_info=True)

    logger.info("Total geral: %d registros de %d prontuários", len(all_results), len(prontuarios))
    return all_results


# ---------------------------------------------------------------------------
# Task 2: write to Delta Lake
# ---------------------------------------------------------------------------
MERGE_KEY = "record_id"

def _delta_table_exists(uri: str, opts: Dict[str, str]) -> bool:
    try:
        DeltaTable(uri, storage_options=opts)
        return True
    except Exception:
        return False

def write_to_delta(**context: Any) -> str:
    rows = context["ti"].xcom_pull(task_ids="scrape_consultas_ambulatoriais")
    if not rows:
        logger.info("Sem dados para gravar.")
        return "no_data"

    opts = _storage_options()
    _ensure_bucket(DELTA_BUCKET)

    new_df = pd.DataFrame(rows)
    before = len(new_df)
    new_df = new_df.drop_duplicates(subset=[MERGE_KEY], keep="last")
    if len(new_df) < before:
        logger.info("Dedup no write: %d -> %d rows", before, len(new_df))
    new_table = pa.Table.from_pandas(new_df)
    uri = f"s3://{DELTA_BUCKET}/{DELTA_PATH}"

    if not _delta_table_exists(uri, opts):
        write_deltalake(uri, new_table, mode="overwrite", storage_options=opts)
        logger.info("Tabela criada com %d registros em %s", len(new_df), uri)
        return uri

    dt = DeltaTable(uri, storage_options=opts)
    existing_cols = [f.name for f in dt.schema().fields]
    if MERGE_KEY not in existing_cols:
        write_deltalake(uri, new_table, mode="overwrite", storage_options=opts)
        logger.info("Tabela recriada (schema mudou): %d registros", len(new_df))
        return uri

    merge_result = (
        dt.merge(
            source=new_table,
            predicate=f"target.{MERGE_KEY} = source.{MERGE_KEY}",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    metrics = merge_result if isinstance(merge_result, dict) else {}
    logger.info(
        "Merge em %s: updated=%s, inserted=%s, deleted=%s, source=%d",
        uri,
        metrics.get("num_target_rows_updated", "?"),
        metrics.get("num_target_rows_inserted", "?"),
        metrics.get("num_target_rows_deleted", 0),
        len(new_df),
    )
    return uri

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "datafabric",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

dag = DAG(
    "aghuse_ambulatorio_scraper",
    default_args=default_args,
    description="Scraper AGHUse: prontuario -> ambulatorio -> Delta Lake (requests only)",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["aghuse", "scraper", "ambulatorio", "delta-lake"],
    is_paused_upon_creation=False,
)

scrape_task = PythonOperator(
    task_id="scrape_consultas_ambulatoriais",
    python_callable=scrape_consultas_ambulatoriais,
    dag=dag,
)

write_task = PythonOperator(
    task_id="write_to_delta",
    python_callable=write_to_delta,
    dag=dag,
)

scrape_task >> write_task
