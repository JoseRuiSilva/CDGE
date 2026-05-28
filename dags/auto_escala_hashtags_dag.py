"""
auto_escala_hashtags_dag.py -- DAG semanal de hashtags Auto Escala
===================================================================
Processa apenas a fonte de hashtags semanalmente (todos os ficheiros
hashtags_YYYYWNN.xml que ainda nao foram ingeridos).

Justificacao arquitectural:
  As hashtags chegam com cadencia semanal (4 ficheiros por mes), ao
  contrario das restantes fontes que sao mensais. Correr a pipeline
  completa mensalmente implicaria atrasos de ate 3 semanas na
  actualizacao dos volumes de social listening. Esta DAG dedicada
  garante que os dados de hashtags sao processados assim que chegam
  (dentro da semana).

Agendamento: segunda-feira as 06:00 UTC (@weekly)
  Semana 1: hashtags_2024W01.xml -> Bronze -> Silver -> fact_hashtag_volume
  Semana 2: hashtags_2024W02.xml -> Bronze -> Silver -> fact_hashtag_volume
  ...

Grafo de tasks:
  check_postgres -> run_hashtags_pipeline

Relacao com a DAG mensal (auto_escala_pipeline):
  - A DAG mensal continua a existir e processa inventario + trends + forum
    no inicio de cada mes.
  - As hashtags sao processadas por ESTA DAG semanalmente.
  - Nao ha conflito: max_active_runs=1 em cada DAG e os watermarks
    sao independentes por fonte em pipeline_control.

Para trigger manual de batches historicos:
  {"data_limite": "2024-01-07"}  <- primeiro domingo de janeiro 2024

Projeto Auto Escala -- CDGE 2025/2026
"""

from __future__ import annotations

import sys
import socket
import os
import calendar
from datetime import datetime, date, timedelta
from pathlib import Path

SCRIPTS_DIR = Path("/opt/airflow/scripts")
sys.path.insert(0, str(SCRIPTS_DIR))

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.exceptions import AirflowSkipException


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _parse_data_limite(conf: dict, logical_date: datetime) -> date:
    """
    Resolve a data_limite:
    - Trigger manual: usar o valor de conf["data_limite"] (ex: "2024-01-07")
    - Agendamento automatico: usar o ultimo dia da semana ISO actual
      (domingo = fim da semana que acabou de passar)
    """
    dl = (conf or {}).get("data_limite", "")
    if dl:
        return datetime.strptime(dl, "%Y-%m-%d").date()
    # Ultimo dia da semana da execucao (domingo)
    # logical_date e segunda-feira 00:00 do agendamento @weekly
    return logical_date.date() + timedelta(days=6)


# ─── DAG ──────────────────────────────────────────────────────────────────────

@dag(
    dag_id="auto_escala_hashtags_semanal",
    description=(
        "Pipeline semanal de hashtags Auto Escala. "
        "Processa os ficheiros XML semanais de social listening: "
        "Bronze -> Silver -> fact_hashtag_volume no PostgreSQL."
    ),
    # Domingo 23:30 UTC -- stand fechado, dados da semana "fechados"
    # O pipeline corre enquanto ninguem esta a trabalhar.
    # Resultados prontos quando o stand abre na segunda-feira de manha.
    schedule="30 23 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,      # Nao re-executar semanas passadas automaticamente
    max_active_runs=1,  # Evitar runs concorrentes sobre o mesmo watermark
    params={
        "data_limite": Param(
            default="",
            type="string",
            description=(
                "Data limite no formato YYYY-MM-DD (ex: '2024-01-07'). "
                "Se vazio, usa o ultimo dia (domingo) da semana actual."
            ),
        ),
    },
    tags=["cdge", "auto-escala", "hashtags", "semanal"],
    doc_md="""
## Auto Escala — Pipeline Semanal de Hashtags

Processa os ficheiros de social listening com cadencia semanal.

### Fonte
`data/sources/hashtags/hashtags_YYYYWNN.xml` (1 ficheiro por semana ISO)

### Fluxo
```
Landing Zone (XML semanal)
       ↓
  Bronze Delta    ← append do ficheiro semanal
       ↓
  Silver Delta    ← limpeza, validacao, MERGE
       ↓
PostgreSQL        ← UPSERT em dim_hashtag + fact_hashtag_volume
       ↓
Watermark         ← pipeline_control actualizado para 'hashtags'
```

### Relacao com a DAG mensal
A `auto_escala_pipeline` (mensal) processa inventario + trends + forum.
Esta DAG processa apenas hashtags, com mais frequencia.

### Trigger manual para demo historica
```json
{"data_limite": "2024-01-07"}
```
    """,
)
def auto_escala_hashtags_semanal():

    # ── TASK 1: Verificar PostgreSQL ──────────────────────────────────────────
    @task(task_id="check_postgres", retries=2, retry_delay=5)
    def check_postgres():
        """Verifica conectividade TCP ao PostgreSQL antes de iniciar."""
        pg_host = os.environ.get("PG_HOST", "localhost")
        pg_port = int(os.environ.get("PG_PORT", "5432"))
        try:
            with socket.create_connection((pg_host, pg_port), timeout=5):
                print(f"  PostgreSQL acessivel em {pg_host}:{pg_port}")
        except (OSError, ConnectionRefusedError) as e:
            raise RuntimeError(
                f"PostgreSQL nao acessivel em {pg_host}:{pg_port}. "
                f"Certifica-te que o servico 'postgres' esta healthy. Erro: {e}"
            )

    # ── TASK 2: Pipeline de Hashtags ──────────────────────────────────────────
    @task(task_id="run_hashtags_pipeline")
    def run_hashtags_pipeline(**context):
        """
        Executa o pipeline completo APENAS para hashtags:
          1. Ler watermark de 'hashtags' em pipeline_control
          2. Descobrir ficheiros XML novos (data > watermark E <= data_limite)
          3. Bronze: append dos ficheiros semanais novos
          4. Silver: MERGE/UPSERT com os novos ficheiros
          5. Load: UPSERT em dim_hashtag + fact_hashtag_volume no PostgreSQL
          6. Actualizar watermark 'hashtags' (so apos sucesso)
        """
        from main import (
            _criar_engine,
            ler_watermark,
            escrever_watermark,
            descobrir_ficheiros,
            _data_maxima_por_fonte,
        )
        from bronze_pipeline import run_bronze
        from silver_pipeline import run_silver
        from load_to_postgres import run_load_to_postgres

        params       = context["params"]
        logical_date = context["logical_date"]
        data_limite  = _parse_data_limite(params, logical_date)

        print(f"  Batch hashtags: data_limite = {data_limite}")

        engine = _criar_engine()

        # 1. Watermark actual da fonte 'hashtags'
        wm_hashtags = ler_watermark(engine, "hashtags")
        print(f"  Watermark hashtags: {wm_hashtags if wm_hashtags else 'sem watermark'}")

        # 2. Descobrir ficheiros novos (so hashtags)
        ficheiros = descobrir_ficheiros(
            data_min=None,
            data_max=data_limite,
            watermarks={"hashtags": wm_hashtags},
        )
        novos_hashtags = ficheiros.get("hashtags", [])

        if not novos_hashtags:
            if engine:
                engine.dispose()
            raise AirflowSkipException(
                f"Nenhum ficheiro de hashtags novo ate {data_limite}. "
                "Watermark ja actualizado para esta semana."
            )

        print(f"  Ficheiros novos encontrados: {len(novos_hashtags)}")
        for fp in novos_hashtags:
            print(f"    {fp.name}")

        # 3. Bronze — apenas hashtags (listas vazias para as outras fontes)
        print("\n  A correr Bronze (hashtags)...")
        run_bronze(
            ficheiros_inventario=[],
            ficheiros_trends=[],
            ficheiros_forum=[],
            ficheiros_hashtags=novos_hashtags,
        )

        # 4. Silver — apenas hashtags
        print("\n  A correr Silver (hashtags)...")
        run_silver(
            ficheiros_inventario=[],
            ficheiros_trends=[],
            ficheiros_forum=[],
            ficheiros_hashtags=[str(fp) for fp in novos_hashtags],
            nlp_habilitado=False,  # NLP nao se aplica a hashtags
        )

        # 5. Load — corre o load completo (idempotente via UPSERT/ON CONFLICT)
        # As outras tabelas nao mudaram, por isso o UPSERT e rapido e seguro.
        print("\n  A carregar para PostgreSQL...")
        run_load_to_postgres()

        # 6. Actualizar watermark de hashtags
        datas = {
            "hashtags": _data_maxima_por_fonte({"hashtags": novos_hashtags}).get("hashtags")
        }
        dt_max = datas["hashtags"] or data_limite
        escrever_watermark(engine, "hashtags", dt_max, len(novos_hashtags))

        if engine:
            engine.dispose()

        print(f"\n  Pipeline semanal hashtags concluida: {len(novos_hashtags)} ficheiros processados.")

    # ── GRAFO ─────────────────────────────────────────────────────────────────
    check_postgres() >> run_hashtags_pipeline()


# Instanciar a DAG
auto_escala_hashtags_semanal()
