"""
airflow_trigger_simulation.py -- Simulacao de batches via Airflow REST API
===========================================================================
Substitui o simulate_batches.py no contexto do Airflow:
chama a REST API do Airflow para disparar a DAG 'auto_escala_pipeline'
mes a mes com uma pausa configuravel entre triggers.

Na UI do Airflow (http://localhost:8080) ve-se cada DAG Run a entrar
em execucao em sequencia -- equivalente a simulate_batches --pausa N
mas com visibilidade total do progresso na interface grafica.

Uso:
  # Simular 2024 completo (12 meses), 10 segundos entre triggers
  python scripts/airflow_trigger_simulation.py

  # Com opcoes
  python scripts/airflow_trigger_simulation.py --desde 2024-01 --ate 2024-06 --pausa 5

  # Ver progresso na UI (enquanto corre):
  http://localhost:8080/dags/auto_escala_pipeline/grid

Pre-requisitos:
  - Docker Compose com Airflow a correr: cd docker && docker compose up -d
  - Airflow UI acessivel em http://localhost:8080 (admin/admin)
  - Full Load ja executado (watermarks presentes) -- ou primeiro trigger manualmente

Projeto Auto Escala -- CDGE 2025/2026
"""

from __future__ import annotations

import argparse
import calendar
import sys
import time
import json
from datetime import date, datetime

import urllib.request
import urllib.error
import base64


# ─── CONFIGURACAO ─────────────────────────────────────────────────────────────

AIRFLOW_URL  = "http://localhost:8080"
AIRFLOW_USER = "admin"
AIRFLOW_PASS = "admin"
DAG_ID       = "auto_escala_pipeline"

INCREMENTAL_INICIO     = date(2024, 1, 1)
INCREMENTAL_FIM_PADRAO = date(2026, 4, 1)


# ─── UTILITARIOS HTTP ─────────────────────────────────────────────────────────

def _auth_header() -> dict:
    """Basic Auth header para a REST API do Airflow."""
    token = base64.b64encode(f"{AIRFLOW_USER}:{AIRFLOW_PASS}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }


def _request(method: str, path: str, body: dict | None = None) -> dict:
    """Envia um pedido HTTP para a API do Airflow e devolve o JSON de resposta."""
    url  = f"{AIRFLOW_URL}/api/v1{path}"
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(url, data=data, headers=_auth_header(), method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        corpo = e.read().decode()
        raise RuntimeError(f"Airflow API erro {e.code} em {path}: {corpo}")


def _verificar_airflow() -> bool:
    """Verifica que o Airflow esta acessivel e a DAG existe."""
    try:
        info = _request("GET", "/health")
        if info.get("metadatabase", {}).get("status") != "healthy":
            return False
        _request("GET", f"/dags/{DAG_ID}")
        return True
    except Exception as e:
        print(f"  ERRO: Airflow nao acessivel -- {e}")
        print(f"  Verifica que o Docker Compose esta a correr: cd docker && docker compose up -d")
        return False


def _despausar_dag():
    """Garante que a DAG nao esta pausada antes de disparar."""
    _request("PATCH", f"/dags/{DAG_ID}", {"is_paused": False})


def _trigger_dag(data_limite: date, nlp_habilitado: bool) -> str:
    """
    Dispara um DAG Run para o mes dado.
    Devolve o dag_run_id para tracking.
    """
    run_id = f"sim_{data_limite.strftime('%Y%m%d')}_{int(time.time())}"
    resp   = _request(
        "POST",
        f"/dags/{DAG_ID}/dagRuns",
        {
            "dag_run_id": run_id,
            "conf": {
                "data_limite":    data_limite.isoformat(),
                "nlp_habilitado": nlp_habilitado,
            },
        },
    )
    return resp.get("dag_run_id", run_id)


def _estado_run(run_id: str) -> str:
    """Devolve o estado actual do DAG Run: queued, running, success, failed."""
    try:
        resp = _request("GET", f"/dags/{DAG_ID}/dagRuns/{run_id}")
        return resp.get("state", "unknown")
    except Exception:
        return "unknown"


def _aguardar_run(run_id: str, timeout_s: int = 600, poll_s: int = 5) -> str:
    """
    Aguarda que o DAG Run termine (success/failed/skipped).
    Devolve o estado final.
    """
    inicio = time.time()
    while time.time() - inicio < timeout_s:
        estado = _estado_run(run_id)
        if estado in ("success", "failed"):
            return estado
        time.sleep(poll_s)
    return "timeout"


# ─── LOGICA DE SIMULACAO ──────────────────────────────────────────────────────

def ultimo_dia_do_mes(ano: int, mes: int) -> date:
    return date(ano, mes, calendar.monthrange(ano, mes)[1])


def gerar_meses(inicio: date, fim: date) -> list[date]:
    meses = []
    ano, mes = inicio.year, inicio.month
    while date(ano, mes, 1) <= date(fim.year, fim.month, 1):
        meses.append(ultimo_dia_do_mes(ano, mes))
        if mes == 12:
            ano, mes = ano + 1, 1
        else:
            mes += 1
    return meses


def simular(
    desde: date,
    ate: date,
    pausa_entre_triggers: float = 10.0,
    nlp_habilitado: bool = False,
    aguardar_conclusao: bool = False,
):
    """
    Dispara a DAG do Airflow para cada mes no intervalo [desde, ate].

    Args:
        desde:                 Primeiro mes a simular.
        ate:                   Ultimo mes (inclusive).
        pausa_entre_triggers:  Segundos entre cada trigger (permite ver na UI).
        nlp_habilitado:        Activar NLP em todos os batches.
        aguardar_conclusao:    Se True, espera que cada Run termine antes
                               de disparar o proximo (sequencial estrito).
                               Se False, dispara todos e deixa o Airflow gerir.
    """
    sep = "=" * 60
    print(f"\n{sep}")
    print(f"  AIRFLOW TRIGGER SIMULATION")
    print(f"  DAG            : {DAG_ID}")
    print(f"  Periodo        : {desde.strftime('%Y-%m')} -> {ate.strftime('%Y-%m')}")
    print(f"  NLP            : {'activo' if nlp_habilitado else 'desabilitado'}")
    print(f"  Pausa          : {pausa_entre_triggers}s entre triggers")
    print(f"  Aguardar fim   : {'sim' if aguardar_conclusao else 'nao (paralelo na UI)'}")
    print(f"  UI             : {AIRFLOW_URL}/dags/{DAG_ID}/grid")
    print(sep)

    if not _verificar_airflow():
        sys.exit(1)

    _despausar_dag()

    meses   = gerar_meses(desde, ate)
    total   = len(meses)
    runs    = []

    for i, data_limite in enumerate(meses, start=1):
        print(f"\n  [{i:>2}/{total}]  A disparar batch  {data_limite.strftime('%Y-%m')} ...")
        run_id = _trigger_dag(data_limite, nlp_habilitado)
        runs.append((data_limite, run_id))
        print(f"         DAG Run ID : {run_id}")
        print(f"         Estado     : queued -> ver em {AIRFLOW_URL}/dags/{DAG_ID}/grid")

        if aguardar_conclusao:
            print(f"         A aguardar conclusao...")
            estado = _aguardar_run(run_id)
            icon   = "OK" if estado == "success" else "FALHOU"
            print(f"         [{icon}] {estado.upper()}")

        if i < total and pausa_entre_triggers > 0:
            print(f"         Pausa de {pausa_entre_triggers}s...")
            time.sleep(pausa_entre_triggers)

    print(f"\n{sep}")
    print(f"  {total} DAG Runs disparados.")
    print(f"  Acompanha em: {AIRFLOW_URL}/dags/{DAG_ID}/grid")
    print(sep)


# ─── CLI ──────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="airflow_trigger_simulation.py",
        description=(
            "Auto Escala -- Simulacao de batches mensais via Airflow REST API.\n"
            "Dispara a DAG 'auto_escala_pipeline' para cada mes no periodo dado."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exemplos:\n"
            "  python scripts/airflow_trigger_simulation.py\n"
            "  python scripts/airflow_trigger_simulation.py --desde 2024-01 --ate 2024-06\n"
            "  python scripts/airflow_trigger_simulation.py --pausa 30 --aguardar\n"
            "  python scripts/airflow_trigger_simulation.py --no-nlp --pausa 5\n"
        ),
    )
    parser.add_argument(
        "--desde", metavar="YYYY-MM",
        type=lambda s: datetime.strptime(s, "%Y-%m").date(),
        default=INCREMENTAL_INICIO,
        help=f"Primeiro mes (padrao: {INCREMENTAL_INICIO.strftime('%Y-%m')}).",
    )
    parser.add_argument(
        "--ate", metavar="YYYY-MM",
        type=lambda s: datetime.strptime(s, "%Y-%m").date(),
        default=INCREMENTAL_FIM_PADRAO,
        help=f"Ultimo mes (padrao: {INCREMENTAL_FIM_PADRAO.strftime('%Y-%m')}).",
    )
    parser.add_argument(
        "--pausa", metavar="SEG", type=float, default=10.0,
        help="Segundos entre triggers (padrao: 10). Use 0 para disparar tudo de imediato.",
    )
    parser.add_argument(
        "--no-nlp", action="store_true", default=False, dest="no_nlp",
        help="Desabilitar NLP em todos os batches (mais rapido).",
    )
    parser.add_argument(
        "--aguardar", action="store_true", default=False,
        help=(
            "Aguardar que cada Run termine antes de disparar o seguinte. "
            "Por omissao dispara todos e deixa o Airflow gerir a concorrencia "
            "(max_active_runs=1 garante sequencial)."
        ),
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

    desde = date(args.desde.year, args.desde.month, 1)
    ate   = date(args.ate.year,   args.ate.month,   1)

    if ate < desde:
        print(f"ERRO: --ate ({ate}) e anterior a --desde ({desde}).")
        sys.exit(1)

    simular(
        desde=desde,
        ate=ate,
        pausa_entre_triggers=args.pausa,
        nlp_habilitado=not args.no_nlp,
        aguardar_conclusao=args.aguardar,
    )


if __name__ == "__main__":
    main()
