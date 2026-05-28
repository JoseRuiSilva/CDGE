"""
auto_escala_dag.py -- DAG principal do pipeline Auto Escala
============================================================
Orquestra o pipeline de dados mensal: Bronze -> Silver -> PostgreSQL -> Prophet.

Agendamento: @monthly (1o dia de cada mes, meia-noite UTC)
Para batches historicos de demo, usar trigger manual com conf:
  {"data_limite": "2024-01-31", "nlp_habilitado": false}

Grafo de tasks:
  check_postgres -> run_pipeline -> atualizar_watermarks

Nota de design: a DAG chama directamente correr_incremental() de main.py
(que ja inclui Bronze + Silver + Load + Prophet + Watermarks internamente).
As tasks estao separadas para visibilidade na UI, mas o pipeline e atomico.

Variaveis de ambiente (injectadas pelo docker-compose.yaml):
  PG_HOST  = "postgres"  (dentro do Docker; "localhost" fora)
  PG_PORT  = "5432"
  PYTHONUTF8 = "1"

Projeto Auto Escala -- CDGE 2025/2026
"""

from __future__ import annotations

import sys
import socket
import os
from datetime import datetime, date
import calendar
from pathlib import Path

# O Airflow monta os scripts em /opt/airflow/scripts
SCRIPTS_DIR = Path("/opt/airflow/scripts")
sys.path.insert(0, str(SCRIPTS_DIR))

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.exceptions import AirflowSkipException


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _ultimo_dia_do_mes(ano: int, mes: int) -> date:
    return date(ano, mes, calendar.monthrange(ano, mes)[1])


def _parse_data_limite(conf: dict, logical_date: datetime) -> date:
    """
    Resolve a data_limite a partir do conf da DAG Run ou da logical_date.
    Permite trigger manual com {"data_limite": "2024-01-31"} ou automatico
    (usa o ultimo dia do mes da execucao agendada).
    """
    dl = (conf or {}).get("data_limite", "")
    if dl:
        return datetime.strptime(dl, "%Y-%m-%d").date()
    # Modo automatico: ultimo dia do mes da execucao
    return _ultimo_dia_do_mes(logical_date.year, logical_date.month)


# ─── DAG ──────────────────────────────────────────────────────────────────────

@dag(
    dag_id="auto_escala_pipeline",
    description="Pipeline mensal Auto Escala: Bronze -> Silver -> PostgreSQL -> Prophet",
    # Primeiro domingo do mes as 23:00 UTC
    # "1-7 * 0" = dia entre 1 e 7, domingo -- primeiro domingo do mes.
    # Logica: o mes anterior fecha, o stand esta fechado ao domingo,
    # o pipeline corre durante a noite e os resultados estao prontos
    # quando o stand abre na segunda-feira seguinte.
    schedule="0 23 1-7 * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,          # Nao re-executar meses passados automaticamente
    max_active_runs=1,      # Evitar runs concorrentes (watermarks nao sao thread-safe)
    params={
        "data_limite": Param(
            default="",
            type="string",
            description=(
                "Data limite do batch no formato YYYY-MM-DD (ex: '2024-01-31'). "
                "Se vazio, usa o ultimo dia do mes da execucao agendada."
            ),
        ),
        "nlp_habilitado": Param(
            default=False,
            type="boolean",
            description="Activar NLP de sentimento (pysentimiento/BERT). Lento e nao disponivel no Docker.",
        ),
        "modo": Param(
            default="incremental",
            enum=["incremental", "full_load"],
            description="'incremental' para batch delta; 'full_load' para reprocessar historico 2022-2023.",
        ),
    },
    tags=["cdge", "auto-escala", "medallion"],
    doc_md="""
## Auto Escala — Pipeline de Dados

Pipeline de analise de tendencias de aquisicao de veiculos usados.

### Fontes de Dados (Landing Zone em /opt/airflow/data/sources/)
| Fonte        | Formato | Cadencia |
|---|---|---|
| Inventario   | CSV     | Mensal   |
| Google Trends| JSON    | Mensal   |
| Forum        | TXT     | Mensal   |
| Hashtags     | XML     | Semanal (agrupado mensalmente) |

### Arquitectura Medallion
`Landing Zone` -> `Bronze (Delta Lake)` -> `Silver (Delta Lake)` -> `PostgreSQL Star Schema` -> `Prophet ML`

### CDC batch e regras de merge
- O pipeline incremental lê apenas ficheiros novos usando watermarks em `main.py`.
- A camada Silver de `inventario` preserva apenas o snapshot mais recente por `id_viatura`.
- O carregamento PostgreSQL aplica upserts em `dim_veiculo`, `fact_venda` e `fact_inventario_mensal`, garantindo que o mesmo veículo não duplica e que alterações de venda/inventário são reprocessáveis.

### Trigger Manual para Demo
```json
{"data_limite": "2024-01-31", "nlp_habilitado": false, "modo": "incremental"}
```

### Full Load (primeira vez)
```json
{"modo": "full_load", "nlp_habilitado": false}
```
    """,
)
def auto_escala_pipeline():

    # ── TASK 1: Verificar PostgreSQL ──────────────────────────────────────────
    @task(task_id="check_postgres", retries=2, retry_delay=5)
    def check_postgres():
        """
        Verifica que o PostgreSQL esta acessivel via TCP antes de iniciar o pipeline.
        Usa PG_HOST do ambiente (='postgres' no Docker, ='localhost' fora).
        Falha a DAG imediatamente se nao acessivel.
        """
        pg_host = os.environ.get("PG_HOST", "localhost")
        pg_port = int(os.environ.get("PG_PORT", "5432"))
        try:
            with socket.create_connection((pg_host, pg_port), timeout=5):
                print(f"  PostgreSQL acessivel em {pg_host}:{pg_port}")
        except (OSError, ConnectionRefusedError) as e:
            raise RuntimeError(
                f"PostgreSQL nao acessivel em {pg_host}:{pg_port}. "
                f"Certifica-te que o servico 'postgres' esta healthy no Docker Compose. Erro: {e}"
            )

    # ── TASK 2: Correr o Pipeline ─────────────────────────────────────────────
    @task(task_id="run_pipeline")
    def run_pipeline(**context):
        """
        Executa o pipeline completo para o batch:
          - Modo 'incremental': ler watermarks -> Bronze -> Silver -> Load -> Prophet -> Watermarks
          - Modo 'full_load':   Bronze (historico) -> Silver -> Load -> Prophet -> Watermarks iniciais

        Reutiliza correr_incremental() / correr_full_load() de main.py directamente,
        garantindo a mesma logica CDC, deduplicacao e atomicidade dos watermarks.
        """
        from main import (
            _criar_engine,
            correr_incremental,
            correr_full_load,
            descobrir_ficheiros,
            FONTES,
            ler_watermark,
        )

        params       = context["params"]
        logical_date = context["logical_date"]
        modo         = params.get("modo", "incremental")
        nlp          = params.get("nlp_habilitado", False)

        engine = _criar_engine()

        if modo == "full_load":
            print("  Modo: FULL LOAD")
            correr_full_load(engine, nlp_habilitado=nlp)
        else:
            data_limite = _parse_data_limite(params, logical_date)
            print(f"  Modo: INCREMENTAL | data_limite = {data_limite} | NLP = {nlp}")

            # Verificar antecipadamente se ha ficheiros novos
            # (evitar que a task corra e termine silenciosamente sem fazer nada)
            watermarks = {fonte: ler_watermark(engine, fonte) for fonte in FONTES}
            ficheiros  = descobrir_ficheiros(
                data_min=None,
                data_max=data_limite,
                watermarks=watermarks,
            )
            total = sum(len(v) for v in ficheiros.values())

            if total == 0:
                if engine:
                    engine.dispose()
                raise AirflowSkipException(
                    f"Nenhum ficheiro novo encontrado para o batch ate {data_limite}. "
                    "DAG Run ignorada (SKIPPED)."
                )

            print(f"  Ficheiros novos encontrados: {total}")
            for fonte, lista in ficheiros.items():
                print(f"    {fonte:<12}: {len(lista)} ficheiros")

            correr_incremental(engine, data_limite=data_limite, nlp_habilitado=nlp)

        if engine:
            engine.dispose()

    # ── GRAFO DE DEPENDENCIAS ──────────────────────────────────────────────────
    check_postgres() >> run_pipeline()


# Instanciar a DAG
auto_escala_pipeline()
