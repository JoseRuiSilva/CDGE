"""
main.py — Orquestrador Unificado do Pipeline Auto Escala
=========================================================
Modos de execução (--mode):
  full_load:   Processa o histórico completo (2022-01 → 2023-12).
  incremental: Processa apenas ficheiros novos até --data_limite.
  simulate:    Simulação local multi-batch (histórico + incremental mês a mês).
  demo:        Simulação via Airflow REST API (dispara DAGs sequencialmente).
  reset:       Limpa o Data Lake e recria o Data Warehouse.

Uso:
  python scripts/main.py --mode full_load
  python scripts/main.py --mode incremental --data_limite 2024-03-31
  python scripts/main.py --mode simulate --desde 2024-01 --ate 2024-06
  python scripts/main.py --mode demo --airflow-url http://localhost:8080
  python scripts/main.py --mode reset

Projeto Auto Escala — CDGE 2025/2026
"""

from __future__ import annotations
import argparse
import sys
import time
import socket
import json
import shutil
import urllib.request
import urllib.error
import base64
import calendar
from datetime import date, datetime, timezone
from pathlib import Path

# ─── PATH SETUP ───────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "scripts"))

from sqlalchemy import text
from bronze_pipeline import run_bronze
from silver_pipeline import run_silver
from generate_dw import create_data_warehouse, setup_sandbox, copy_to_sandbox
from load_to_postgres import run_load_to_postgres
from data_profiling import run_profiling
from prev_tendencias import run_sarima
from prev_gain import run_xgboost

# ─── CONFIGURAÇÃO ─────────────────────────────────────────────────────────────
import os as _os
_PG_HOST = _os.environ.get("PG_HOST", "localhost")
_PG_PORT = _os.environ.get("PG_PORT", "5432")
DW_URL    = f"postgresql+psycopg2://ae_user:ae_pass_2026@{_PG_HOST}:{_PG_PORT}/auto_escala?connect_timeout=10"
DW_SCHEMA = "auto_escala_dw"

FULL_LOAD_LIMITE = date(2023, 12, 31)

STANDS_DIR     = BASE_DIR / "data/sources/stands"
TRENDS_DIR     = BASE_DIR / "data/sources/trends"
FORUM_DIR      = BASE_DIR / "data/sources/forum"
HASHTAGS_DIR   = BASE_DIR / "data/sources/hashtags"
CLIENTES_DIR   = BASE_DIR / "data/sources/clientes"
DEMOGRAFIA_DIR = BASE_DIR / "data/sources/demografia"

FONTES = ["inventario", "trends", "forum", "hashtags", "clientes", "demografia"]

# ─── LOGGING ─────────────────────────────────────────────────────────────────
def _log(msg: str, nivel: str = "INFO") -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    # Substituir setas e outros chars problemáticos para Windows CMD
    safe_msg = msg.replace("→", "->").replace("•", "-")
    print(f"  [{ts}] [{nivel}] {safe_msg}")

# ─── LIGAÇÃO POSTGRESQL ───────────────────────────────────────────────────────
def _verificar_postgres_tcp(host: str = "localhost", porta: int = 5432, timeout: float = 3.0) -> bool:
    import socket
    try:
        with socket.create_connection((host, porta), timeout=timeout):
            return True
    except (OSError, ConnectionRefusedError):
        return False

def _criar_engine():
    if not _verificar_postgres_tcp(host=_PG_HOST, porta=int(_PG_PORT)):
        _log(f"PostgreSQL nao acessivel em {_PG_HOST}:{_PG_PORT}.", "WARN")
        return None
    try:
        from sqlalchemy import create_engine
        engine = create_engine(DW_URL, echo=False, connect_args={"connect_timeout": 5})
        with engine.connect(): pass
        return engine
    except Exception as e:
        _log(f"PostgreSQL indisponivel ({e}).", "WARN")
        return None


def dw_is_ready(engine) -> bool:
    if engine is None:
        return False
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT 1 FROM information_schema.views "
                    "WHERE table_schema = :schema AND table_name = :view LIMIT 1"
                ),
                {"schema": DW_SCHEMA, "view": "vw_mart_prev_tendencias"},
            ).fetchone()
            return row is not None
    except Exception:
        return False

# ─── WATERMARK ───────────────────────────────────────────────────────────────
def ler_watermark(engine, fonte: str) -> date | None:
    if engine is None: return None
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            linha = conn.execute(
                text(f"SELECT data_fim::date FROM {DW_SCHEMA}.pipeline_control WHERE nome_pipeline = 'main' AND camada = :fonte AND estado = 'completo' ORDER BY data_fim DESC LIMIT 1"),
                {"fonte": fonte},
            ).fetchone()
        return linha[0] if linha else None
    except Exception: return None

def escrever_watermark(engine, fonte: str, data_processada: date, n_ficheiros: int, ficheiro_origem: str = None):
    if engine is None: return
    try:
        from sqlalchemy import text
        with engine.begin() as conn:
            conn.execute(
                text(f"INSERT INTO {DW_SCHEMA}.pipeline_control (nome_pipeline, camada, estado, data_inicio, data_fim, linhas_processadas, mensagem_erro) VALUES ('main', :fonte, 'completo', :agora, :data_fim, :n, NULL)"),
                {"fonte": fonte, "agora": datetime.now(timezone.utc), "data_fim": datetime.combine(data_processada, datetime.min.time()), "n": n_ficheiros},
            )
    except Exception as e: _log(f"Erro ao gravar watermark ({fonte}): {e}", "WARN")

# ─── DESCOBERTA DE FICHEIROS ──────────────────────────────────────────────────
def _data_inventario(fp: Path) -> date | None:
    try: partes = fp.stem.split("_"); return date(int(partes[0]), int(partes[1]), 1)
    except: return None
def _data_trends(fp: Path) -> date | None:
    try: stem = fp.stem; return date(int(stem[-6:-2]), int(stem[-2:]), 1)
    except: return None
def _data_forum(fp: Path) -> date | None:
    try:
        partes = fp.stem.split("_")
        if len(partes) == 2 and len(partes[1]) == 6:
            ano = int(partes[1][:4])
            mes = int(partes[1][4:])
            return date(ano, mes, 1)
        return date.fromisoformat(partes[1])
    except: return None
def _data_hashtags(fp: Path) -> date | None:
    try: stem = fp.stem; partes = stem.split("_")[1]; ano = int(partes[:4]); semana = int(partes[5:]); return date.fromisocalendar(ano, semana, 1)
    except: return None
def _data_estatica(fp: Path) -> date | None: return FULL_LOAD_LIMITE

_PARSERS = {"inventario": _data_inventario, "trends": _data_trends, "forum": _data_forum, "hashtags": _data_hashtags, "clientes": _data_estatica, "demografia": _data_estatica}

def descobrir_ficheiros(data_min: date | None, data_max: date, watermarks: dict[str, date | None] | None = None) -> dict[str, list[Path]]:
    pares = {
        "inventario": [(fp, _data_inventario(fp)) for fp in sorted(STANDS_DIR.rglob("*.csv"))],
        "trends":     [(fp, _data_trends(fp))     for fp in sorted(TRENDS_DIR.rglob("trends_*.json"))],
        "forum":      [(fp, _data_forum(fp))       for fp in sorted(FORUM_DIR.rglob("forum_*.txt"))],
        "hashtags":   [(fp, _data_hashtags(fp))    for fp in sorted(HASHTAGS_DIR.rglob("hashtags_*.xml"))],
        "clientes":   [(fp, _data_estatica(fp))    for fp in sorted(CLIENTES_DIR.rglob("*.csv"))],
        "demografia": [(fp, _data_estatica(fp))    for fp in sorted(DEMOGRAFIA_DIR.rglob("*.csv"))],
    }
    resultado = {}
    for fonte, lista in pares.items():
        wm = (watermarks or {}).get(fonte, data_min)
        resultado[fonte] = sorted([fp for fp, dt in lista if dt is not None and (wm is None or dt > wm) and dt <= data_max])
    return resultado

# ─── MODOS CORE ───────────────────────────────────────────────────────────────
def correr_full_load(engine, nlp_habilitado: bool = True, skip_models: bool = False):
    _log(f"Iniciando FULL LOAD ate {FULL_LOAD_LIMITE}...")
    if not dw_is_ready(engine):
        _log("DW não encontrado ou incompleto. A recriar o Data Warehouse...", "WARN")
        create_data_warehouse()
        setup_sandbox()

    ficheiros = descobrir_ficheiros(data_min=None, data_max=FULL_LOAD_LIMITE)
    run_bronze(**{f"ficheiros_{k}": v for k, v in ficheiros.items()})
    run_silver(**{f"ficheiros_{k}": [str(f) for f in v] for k, v in ficheiros.items()}, nlp_habilitado=nlp_habilitado)
    #run_profiling()
    run_load_to_postgres(mode="full_load", data_limite=FULL_LOAD_LIMITE)
    copy_to_sandbox(DW_URL)
    if not skip_models:
        _log("A executar modelos preditivos (SARIMA + XGBoost)...")
        run_sarima(schema=DW_SCHEMA)
        run_xgboost(schema=DW_SCHEMA)
    else:
        _log("Skip models ativado; modelos preditivos nao serao executados.")
    for fonte in FONTES:
        if ficheiros[fonte]:
            dt_max = max((_PARSERS[fonte](f) for f in ficheiros[fonte]), default=FULL_LOAD_LIMITE)
            escrever_watermark(engine, fonte, dt_max, len(ficheiros[fonte]), "Lote Histórico")
    _log("FULL LOAD concluido.")

def correr_incremental(engine, data_limite: date, nlp_habilitado: bool = True, skip_models: bool = False):
    _log(f"Iniciando INCREMENTAL ate {data_limite}...")
    watermarks = {f: ler_watermark(engine, f) for f in FONTES}
    ficheiros = descobrir_ficheiros(None, data_limite, watermarks)
    if sum(len(v) for v in ficheiros.values()) == 0:
        _log("Nenhum ficheiro novo."); return
    if not dw_is_ready(engine):
        _log("DW não encontrado ou incompleto. A recriar o Data Warehouse...", "WARN")
        create_data_warehouse()
        setup_sandbox()
    run_bronze(**{f"ficheiros_{k}": v for k, v in ficheiros.items()})
    # Silver aplica CDC-like deduplicação por id_viatura e mantém apenas o último snapshot conhecido.
    run_silver(**{f"ficheiros_{k}": [str(f) for f in v] for k, v in ficheiros.items()}, nlp_habilitado=nlp_habilitado)
    # O carregamento para Postgres é idempotente: upserts em dim_veiculo, fct_venda e fct_inventario_mensal.
    run_load_to_postgres(mode="incremental", data_limite=data_limite)
    copy_to_sandbox(DW_URL)
    if not skip_models:
        _log("A executar modelos preditivos (SARIMA + XGBoost)...")
        run_sarima(schema=DW_SCHEMA)
        run_xgboost(schema=DW_SCHEMA)
    else:
        _log("Skip models ativado; modelos preditivos nao serao executados.")
    for fonte in FONTES:
        if ficheiros[fonte]:
            dt_max = max((_PARSERS[fonte](f) for f in ficheiros[fonte]), default=data_limite)
            escrever_watermark(engine, fonte, dt_max, len(ficheiros[fonte]), ", ".join(f.name for f in ficheiros[fonte]))
    _log("INCREMENTAL concluido.")

# ─── MODO: SIMULATE (LOCAL) ──────────────────────────────────────────────────
def correr_simulacao(engine, desde: date, ate: date, nlp: bool, pausa: float):
    _log(f"Iniciando SIMULACAO LOCAL {desde} -> {ate}")
    if engine and not ler_watermark(engine, "inventario"): correr_full_load(engine, nlp)
    
    ano, mes = desde.year, desde.month
    while date(ano, mes, 1) <= date(ate.year, ate.month, 1):
        limite = date(ano, mes, calendar.monthrange(ano, mes)[1])
        _log(f"Processando batch {ano}-{mes:02d}...")
        correr_incremental(engine, limite, nlp)
        if pausa > 0: time.sleep(pausa)
        if mes == 12: ano, mes = ano + 1, 1
        else: mes += 1

# ─── MODO: DEMO (AIRFLOW) ─────────────────────────────────────────────────────
def correr_demo_airflow(url: str, user: str, psw: str, desde: date, ate: date, nlp: bool, pausa: float, aguardar: bool):
    _log(f"Iniciando DEMO AIRFLOW em {url}")
    auth = base64.b64encode(f"{user}:{psw}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}
    
    def _req(method, path, body=None):
        req = urllib.request.Request(f"{url}/api/v1{path}", data=json.dumps(body).encode() if body else None, headers=headers, method=method)
        with urllib.request.urlopen(req) as r: return json.loads(r.read())

    _req("PATCH", "/dags/auto_escala_pipeline", {"is_paused": False})
    
    ano, mes = desde.year, desde.month
    while date(ano, mes, 1) <= date(ate.year, ate.month, 1):
        limite = date(ano, mes, calendar.monthrange(ano, mes)[1])
        run_id = f"demo_{ano}{mes:02d}_{int(time.time())}"
        _log(f"Triggering Airflow: {ano}-{mes:02d} (Run ID: {run_id})")
        _req("POST", "/dags/auto_escala_pipeline/dagRuns", {"dag_run_id": run_id, "conf": {"data_limite": limite.isoformat(), "modo": "incremental", "nlp_habilitado": nlp}})
        
        if aguardar:
            while True:
                st = _req("GET", f"/dags/auto_escala_pipeline/dagRuns/{run_id}")["state"]
                if st in ("success", "failed"): break
                time.sleep(5)
        
        if pausa > 0: time.sleep(pausa)
        if mes == 12: ano, mes = ano + 1, 1
        else: mes += 1

# ─── CLI & MAIN ───────────────────────────────────────────────────────────────
# ─── GERAÇÃO AUTOMÁTICA ───────────────────────────────────────────────────────
def verificar_e_gerar_dados():
    """Verifica se as pastas de dados fonte estão vazias e gera dados se necessário."""
    _log(f"Verificando fontes em {BASE_DIR.resolve()}", "DEBUG")
    fontes_vazias = False
    for pasta in [STANDS_DIR, TRENDS_DIR, FORUM_DIR, HASHTAGS_DIR]:
        abs_pasta = pasta.resolve()
        if not abs_pasta.exists():
            _log(f"Pasta {abs_pasta} nao existe.", "DEBUG")
            fontes_vazias = True
            break
        # Contar ficheiros reais (recursivo), ignorar pastas vazias criadas pelo reset
        ficheiros = [f for f in abs_pasta.rglob("*") if f.is_file() and not f.name.startswith(".")]
        if not ficheiros:
            _log(f"Pasta {abs_pasta} sem ficheiros.", "DEBUG")
            fontes_vazias = True
            break
        else:
            _log(f"Pasta {abs_pasta} tem {len(ficheiros)} ficheiros.", "DEBUG")
    
    if fontes_vazias:
        _log("Fontes de dados vazias. A iniciar geracao automatica...", "WARN")
        try:
            from generate_inventory import generate_inventory
            from generate_trends import gerar_trends, exportar_json_por_mes
            from generate_forum import exportar_forum
            from generate_hashtags import exportar_hashtags
            from generate_clientes import generate_clientes
            from generate_demografia import generate_demografia
            
            _log("Gerando Clientes...")
            generate_clientes()
            _log("Gerando Trends...")
            exportar_json_por_mes(gerar_trends())
            _log("Gerando Forum...")
            exportar_forum()
            _log("Gerando Hashtags...")
            exportar_hashtags()
            _log("Gerando Inventario...")
            generate_inventory()
            _log("Gerando Demografia...")
            generate_demografia()
            _log("Geracao concluida com sucesso.")
        except Exception as e:
            _log(f"Erro na geracao automatica: {e}", "ERROR")

# ─── CLI & MAIN ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(prog="main.py", description="Auto Escala — Orquestrador Unificado")
    parser.add_argument("--mode", choices=["full_load", "incremental", "simulate", "demo", "reset", "sandbox"], required=True)
    parser.add_argument("--data_limite", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date())
    parser.add_argument("--desde", type=lambda s: datetime.strptime(s, "%Y-%m").date(), default=date(2024,1,1))
    parser.add_argument("--ate", type=lambda s: datetime.strptime(s, "%Y-%m").date(), default=date(2024,12,31))
    parser.add_argument("--no-nlp", action="store_true")
    parser.add_argument("--pausa", type=float, default=0)
    parser.add_argument("--airflow-url", default="http://localhost:8080")
    parser.add_argument("--aguardar", action="store_true")
    parser.add_argument(
        "--skip-models",
        action="store_true",
        help=(
            "Omite a execução dos modelos preditivos (SARIMA e XGBoost) neste ciclo. "
            "Útil quando se quer carregar dados sem aguardar o re-treino completo. "
            "Os modelos treinam sempre sobre toda a série histórica disponível "
            "(walk-forward expanding window), pelo que o tempo de execução cresce "
            "com o histórico. Por defeito, os modelos correm em ambos os modos "
            "full_load e incremental."
        ),
    )
    parser.add_argument("--reset", action="store_true", help="Faz reset antes de correr")
    args = parser.parse_args()

    # MODO RESET: Limpa TUDO (Data Lake + Landing Zone)
    if args.mode == "reset" or args.reset:
        _log("Iniciando RESET TOTAL (Data Lake + Sources)...")
        # 1. Limpar Data Lake
        for p in ["bronze", "silver", "quarantine"]:
            path = BASE_DIR / "data_lake" / p
            if path.exists(): shutil.rmtree(path); _log(f"Limpo Data Lake: {p}")
        
        # 2. Limpar Sources (Landing Zone)
        sources_path = BASE_DIR / "data" / "sources"
        if sources_path.exists():
            for d in sources_path.iterdir():
                if d.is_dir():
                    shutil.rmtree(d)
                    d.mkdir() # Recria a pasta vazia
            _log("Limpas fontes em data/sources")
            
        create_data_warehouse()
        setup_sandbox()
        copy_to_sandbox(DW_URL)
        if args.mode == "reset": return

    # Verificação de dados antes de qualquer carga
    if args.mode in ["full_load", "incremental", "simulate"]:
        verificar_e_gerar_dados()

    engine = _criar_engine()
    nlp = not args.no_nlp

    if args.mode == "full_load": correr_full_load(engine, nlp, skip_models=args.skip_models)
    elif args.mode == "incremental": correr_incremental(engine, args.data_limite, nlp, skip_models=args.skip_models)
    elif args.mode == "simulate": correr_simulacao(engine, args.desde, args.ate, nlp, args.pausa)
    elif args.mode == "demo": correr_demo_airflow(args.airflow_url, "admin", "admin", args.desde, args.ate, nlp, args.pausa, args.aguardar)
    elif args.mode == "sandbox":
        _log("Executando copia para a Sandbox...")
        copy_to_sandbox(DW_URL)

    if engine: engine.dispose()


if __name__ == "__main__":
    main()