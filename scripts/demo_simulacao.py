"""
demo_simulacao.py -- Simulacao completa do pipeline Auto Escala via Airflow
===========================================================================
Corre a demo completa de forma automatica:

  [1] RESET        - Apaga PostgreSQL DW + Bronze Delta + Silver Delta
  [2] FULL LOAD    - Processa historico 2022-2023 via Airflow
  [3] SIMULACAO    - Simula semana a semana + mes a mes (2024)
                     via Airflow REST API, esperando que cada Run
                     termine antes de iniciar o seguinte.

Por omissao (sem argumentos) corre TUDO automaticamente.

Uso:
  # Demo completa (reset + full load + 2024 completo)
  python scripts/demo_simulacao.py

  # So a simulacao (sem reset nem full load)
  python scripts/demo_simulacao.py --skip-reset --skip-full-load

  # Um mes especifico
  python scripts/demo_simulacao.py --skip-reset --skip-full-load --desde 2024-03 --ate 2024-03

  # Full load + simulacao sem reset
  python scripts/demo_simulacao.py --skip-reset

Airflow UI: http://localhost:8080  (admin / admin)
pgAdmin  : http://localhost:5052

Projeto Auto Escala -- CDGE 2025/2026
"""

from __future__ import annotations

import argparse
import calendar
import json
import os
import shutil
import socket
import sys
import time
import urllib.error
import urllib.request
import base64
from datetime import date, datetime
from pathlib import Path

# ─── CONFIGURACAO ─────────────────────────────────────────────────────────────

BASE_DIR     = Path(__file__).resolve().parent.parent
SCRIPTS_DIR  = BASE_DIR / "scripts"
DATA_LAKE    = BASE_DIR / "data_lake"

AIRFLOW_URL  = "http://localhost:8080"
AIRFLOW_USER = "admin"
AIRFLOW_PASS = "admin"

DAG_MENSAL   = "auto_escala_pipeline"
DAG_SEMANAL  = "auto_escala_hashtags_semanal"

DESDE_PADRAO = date(2024, 1, 1)
ATE_PADRAO   = date(2024, 12, 31)

SEP = "=" * 62


# ─── UTILITARIOS ──────────────────────────────────────────────────────────────

def _cor(texto: str, codigo: str) -> str:
    """ANSI color codes para output mais legivel."""
    cores = {"verde": "32", "vermelho": "31", "amarelo": "33", "cinzento": "90", "bold": "1"}
    c = cores.get(codigo, "0")
    return f"\033[{c}m{texto}\033[0m"


def _ok(msg: str)   -> str: return _cor(f"  [OK] {msg}", "verde")
def _info(msg: str) -> str: return _cor(f"  {msg}", "cinzento")
def _err(msg: str)  -> str: return _cor(f"  [ERRO] {msg}", "vermelho")


def _auth_header() -> dict:
    token = base64.b64encode(f"{AIRFLOW_USER}:{AIRFLOW_PASS}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json", "Accept": "application/json"}


def _request(method: str, path: str, body: dict | None = None) -> dict:
    url  = f"{AIRFLOW_URL}/api/v1{path}"
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(url, data=data, headers=_auth_header(), method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Airflow API {e.code} em {path}: {e.read().decode()}")


def _verificar_airflow() -> bool:
    try:
        info = _request("GET", "/health")
        return info.get("metadatabase", {}).get("status") == "healthy"
    except Exception:
        return False


def _despausar_dag(dag_id: str):
    _request("PATCH", f"/dags/{dag_id}", {"is_paused": False})


def _trigger_dag(dag_id: str, conf: dict) -> str:
    run_id = f"demo_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{dag_id[-6:]}"
    resp   = _request("POST", f"/dags/{dag_id}/dagRuns", {"dag_run_id": run_id, "conf": conf})
    return resp.get("dag_run_id", run_id)


def _estado_run(dag_id: str, run_id: str) -> str:
    try:
        resp = _request("GET", f"/dags/{dag_id}/dagRuns/{run_id}")
        return resp.get("state", "unknown")
    except Exception:
        return "unknown"


def _aguardar_run(dag_id: str, run_id: str, timeout: int = 900, poll: int = 3) -> str:
    inicio = time.time()
    ultimo_estado = ""
    while time.time() - inicio < timeout:
        estado = _estado_run(dag_id, run_id)
        if estado != ultimo_estado:
            ultimo_estado = estado
        if estado in ("success", "failed"):
            return estado
        time.sleep(poll)
    return "timeout"


def _trigger_e_aguardar(dag_id: str, conf: dict, descricao: str) -> bool:
    """Dispara um DAG Run, aguarda conclusao e mostra resultado."""
    t0 = time.time()
    run_id = _trigger_dag(dag_id, conf)
    print(_info(f"  {descricao:<35} aguardar..."), end="", flush=True)
    estado = _aguardar_run(dag_id, run_id)
    dur    = time.time() - t0
    if estado == "success":
        print(f"\r{_ok(f'{descricao:<35} {dur:.1f}s')}")
        return True
    elif estado == "failed":
        print(f"\r{_err(f'{descricao:<35} FALHOU ({dur:.1f}s)')}")
        print(_err(f"  Ver logs em: {AIRFLOW_URL}/dags/{dag_id}/grid"))
        return False
    else:  # timeout ou skipped
        print(f"\r{_info(f'{descricao:<35} {estado} ({dur:.1f}s)')}")
        return True   # skipped nao e erro


# ─── SEMANAS ISO POR MES ──────────────────────────────────────────────────────

def semanas_do_mes(ano: int, mes: int) -> list[date]:
    """
    Devolve os domingos (fim de semana) das semanas ISO que pertencem a este mes.
    Uma semana 'pertence' ao mes se o domingo dessa semana cai no mes.
    """
    primeiro = date(ano, mes, 1)
    ultimo   = date(ano, mes, calendar.monthrange(ano, mes)[1])
    domingos = []
    # Comecar pelo primeiro domingo do mes ou anterior
    d = primeiro
    while d.weekday() != 6:  # 6 = domingo
        d = date.fromordinal(d.toordinal() + 1)
    while d <= ultimo:
        domingos.append(d)
        d = date.fromordinal(d.toordinal() + 7)
    return domingos


def ultimo_dia(ano: int, mes: int) -> date:
    return date(ano, mes, calendar.monthrange(ano, mes)[1])


# ─── ETAPAS DA DEMO ───────────────────────────────────────────────────────────

def etapa_reset():
    """
    Reset completo: PostgreSQL DW + Bronze Delta + Silver Delta.
    A Landing Zone (data/sources/) nao e apagada -- e o equivalente
    aos ficheiros que a empresa ja tinha antes de nos.
    """
    print(f"\n{SEP}")
    print(_cor("  RESET", "bold"))
    print(SEP)
    t0 = time.time()

    # 1. Recriar schema PostgreSQL
    sys.path.insert(0, str(SCRIPTS_DIR))
    try:
        from generate_dw import create_data_warehouse
        create_data_warehouse()
        print(_ok("Schema auto_escala_dw recriado"))
    except Exception as e:
        print(_err(f"Falha ao recriar schema: {e}"))
        sys.exit(1)

    # 2. Apagar Bronze Delta
    bronze_dir = DATA_LAKE / "bronze"
    if bronze_dir.exists():
        shutil.rmtree(bronze_dir)
        print(_ok(f"Bronze Delta apagado ({bronze_dir.name})"))
    bronze_dir.mkdir(parents=True, exist_ok=True)

    # 3. Apagar Silver Delta + Quarentena
    for subdir in ["silver", "quarantine"]:
        d = DATA_LAKE / subdir
        if d.exists():
            shutil.rmtree(d)
            print(_ok(f"{subdir.capitalize()} Delta apagado"))
        d.mkdir(parents=True, exist_ok=True)

    print(_info(f"Reset concluido em {time.time()-t0:.1f}s"))


def etapa_full_load():
    """Full Load via Airflow -- processa historico 2022-2023."""
    print(f"\n{SEP}")
    print(_cor("  FULL LOAD  (historico 2022-2023)", "bold"))
    print(SEP)
    print(_info(f"Ver progresso em: {AIRFLOW_URL}/dags/{DAG_MENSAL}/grid"))

    _despausar_dag(DAG_MENSAL)
    sucesso = _trigger_e_aguardar(
        DAG_MENSAL,
        {"modo": "full_load", "nlp_habilitado": False},
        "Full Load 2022-2023",
    )
    if not sucesso:
        print(_err("Full Load falhou. Verifica os logs no Airflow e tenta novamente."))
        sys.exit(1)


def etapa_simulacao(desde: date, ate: date):
    """Simula semana a semana + mes a mes de 'desde' ate 'ate'."""
    print(f"\n{SEP}")
    print(_cor(f"  SIMULACAO  {desde.strftime('%Y-%m')} -> {ate.strftime('%Y-%m')}", "bold"))
    print(SEP)
    print(_info(f"Ver progresso em: {AIRFLOW_URL}"))
    print(_info("Semanal: hashtags  |  Mensal: inventario + trends + forum"))
    print()

    _despausar_dag(DAG_MENSAL)
    _despausar_dag(DAG_SEMANAL)

    total_runs = 0
    falhas     = 0
    t_inicio   = time.time()

    ano, mes = desde.year, desde.month
    while date(ano, mes, 1) <= date(ate.year, ate.month, 1):
        print(_cor(f"  {ano}-{mes:02d}", "amarelo"))

        # Semanas do mes (hashtags)
        for domingo in semanas_do_mes(ano, mes):
            descricao = f"Hashtags semana {domingo.strftime('%Y-W%V')}"
            ok = _trigger_e_aguardar(
                DAG_SEMANAL,
                {"data_limite": domingo.isoformat()},
                descricao,
            )
            total_runs += 1
            if not ok:
                falhas += 1

        # Fim do mes (inventario + trends + forum)
        fim_mes   = ultimo_dia(ano, mes)
        descricao = f"Mensal {ano}-{mes:02d}"
        ok = _trigger_e_aguardar(
            DAG_MENSAL,
            {"data_limite": fim_mes.isoformat(), "modo": "incremental", "nlp_habilitado": False},
            descricao,
        )
        total_runs += 1
        if not ok:
            falhas += 1

        # Avancar para o mes seguinte
        if mes == 12:
            ano, mes = ano + 1, 1
        else:
            mes += 1

    duracao = time.time() - t_inicio
    print(f"\n{SEP}")
    estado_final = _cor("CONCLUIDA", "verde") if falhas == 0 else _cor(f"CONCLUIDA COM {falhas} FALHAS", "vermelho")
    print(f"  {estado_final}")
    print(_info(f"Runs totais : {total_runs}"))
    print(_info(f"Falhas      : {falhas}"))
    print(_info(f"Duracao     : {duracao:.1f}s  ({duracao/60:.1f} min)"))
    print(_info(f"Airflow UI  : {AIRFLOW_URL}"))
    print(SEP)


# ─── CLI ──────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="demo_simulacao.py",
        description=(
            "Auto Escala -- Demo completa via Airflow.\n"
            "Por omissao: reset + full load + simulacao 2024 completa."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exemplos:\n"
            "  python scripts/demo_simulacao.py\n"
            "  python scripts/demo_simulacao.py --skip-reset --skip-full-load --desde 2024-06 --ate 2024-06\n"
        ),
    )
    parser.add_argument("--skip-reset",     action="store_true", help="Nao apagar dados existentes.")
    parser.add_argument("--skip-full-load", action="store_true", help="Nao correr o Full Load.")
    parser.add_argument("--skip-simulacao", action="store_true", help="Nao correr a simulacao incremental.")
    parser.add_argument(
        "--desde", metavar="YYYY-MM",
        type=lambda s: datetime.strptime(s, "%Y-%m").date(),
        default=DESDE_PADRAO,
        help=f"Primeiro mes a simular (padrao: {DESDE_PADRAO.strftime('%Y-%m')}).",
    )
    parser.add_argument(
        "--ate", metavar="YYYY-MM",
        type=lambda s: datetime.strptime(s, "%Y-%m").date(),
        default=ATE_PADRAO,
        help=f"Ultimo mes (padrao: {ATE_PADRAO.strftime('%Y-%m')}).",
    )
    parser.add_argument(
        "--airflow-url", default=AIRFLOW_URL,
        help=f"URL base do Airflow (padrao: {AIRFLOW_URL}).",
    )
    return parser.parse_args()


def main():
    args = _parse_args()

    global AIRFLOW_URL
    AIRFLOW_URL = args.airflow_url

    print(f"\n{SEP}")
    print(_cor("  AUTO ESCALA -- DEMO SIMULACAO", "bold"))
    print(_info(f"  Airflow UI : {AIRFLOW_URL}"))
    print(_info(f"  pgAdmin    : http://localhost:5052"))
    print(SEP)

    # Verificar Airflow
    print(_info("A verificar Airflow..."), end="", flush=True)
    if not _verificar_airflow():
        print()
        print(_err("Airflow nao esta acessivel. Certifica-te que o Docker esta a correr:"))
        print(_info("  cd docker && docker compose up -d"))
        sys.exit(1)
    print(f"\r{_ok('Airflow acessivel')}")

    # Etapas
    if not args.skip_reset:
        etapa_reset()

    if not args.skip_full_load:
        etapa_full_load()

    if not args.skip_simulacao:
        desde = date(args.desde.year, args.desde.month, 1)
        ate   = date(args.ate.year,   args.ate.month,   1)
        if ate < desde:
            print(_err(f"--ate ({ate}) e anterior a --desde ({desde})."))
            sys.exit(1)
        etapa_simulacao(desde, ate)

    print()
    print(_ok("Demo concluida. Ver resultados em:"))
    print(_info(f"  Airflow : {AIRFLOW_URL}"))
    print(_info("  pgAdmin : http://localhost:5052"))
    print()


if __name__ == "__main__":
    main()
