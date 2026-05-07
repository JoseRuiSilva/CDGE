"""
main.py — Orquestrador do pipeline Auto Escala
=================================================
Modos de execução:
  full_load:   Processa o histórico completo (2022-01 → 2023-12).
               Inicializa os watermarks em pipeline_control no final.
  incremental: Lê os watermarks de pipeline_control e processa apenas
               os ficheiros novos até --data_limite (inclusive).

Uso:
  python scripts/main.py --mode full_load
  python scripts/main.py --mode incremental --data_limite 2024-03-31
  python scripts/main.py --mode incremental --data_limite 2024-06-30

Fluxo por modo:
  full_load   → Bronze (histórico) → Silver (histórico) → Watermark inicial
  incremental → Ler watermark → Bronze (delta) → Silver (delta) → Actualizar watermark

Convenções CDC:
  - Watermark guardado em auto_escala_dw.pipeline_control (não em ficheiro externo).
  - last_processed_date só actualiza após todas as pipelines concluírem com sucesso.
  - Se o PostgreSQL não estiver acessível, o watermark é ignorado graciosamente
    e o controlo de duplicados passa a ser feito pelo Bronze (source_file já ingerido).

Projeto Auto Escala — CDGE 2025/2026
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path


# ─── LOGGING ─────────────────────────────────────────────────────────────────

def _log(msg: str, nivel: str = "INFO") -> None:
    """Print com timestamp ISO para rastreabilidade durante debug."""
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"  [{ts}] [{nivel}] {msg}")

# ─── PATH SETUP ───────────────────────────────────────────────────────────────
# main.py vive em scripts/ — BASE_DIR é a raiz do projecto (parent.parent)

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "scripts"))

from bronze_pipeline import run_bronze  # noqa: E402
from silver_pipeline import run_silver  # noqa: E402
from generate_dw import create_data_warehouse  # noqa: E402
from load_to_postgres import run_load_to_postgres  # noqa: E402
from prophet_model import run_prophet  # noqa: E402
from data_profiling import run_profiling  # noqa: E402


# ─── CONFIGURAÇÃO ─────────────────────────────────────────────────────────────

import os as _os

# Host do PostgreSQL: 'localhost' localmente, 'postgres' dentro do Docker Airflow.
# Substituivel via variavel de ambiente PG_HOST.
_PG_HOST = _os.environ.get("PG_HOST", "localhost")
_PG_PORT = _os.environ.get("PG_PORT", "5432")

DW_URL    = f"postgresql+psycopg://ae_user:ae_pass_2026@{_PG_HOST}:{_PG_PORT}/auto_escala?connect_timeout=10"
DW_SCHEMA = "auto_escala_dw"

# Fronteira do batch histórico — full_load processa ATÉ ESTE DIA (inclusive)
FULL_LOAD_LIMITE = date(2023, 12, 31)

# Raízes das fontes (espelham o que está no bronze_pipeline.py)
STANDS_DIR   = BASE_DIR / "data/sources/stands"
TRENDS_DIR   = BASE_DIR / "data/sources/trends"
FORUM_DIR    = BASE_DIR / "data/sources/forum"
HASHTAGS_DIR = BASE_DIR / "data/sources/hashtags"

FONTES = ["inventario", "trends", "forum", "hashtags"]


# ─── LIGAÇÃO POSTGRESQL ───────────────────────────────────────────────────────

def _verificar_postgres_tcp(host: str = "localhost", porta: int = 5432, timeout: float = 3.0) -> bool:
    """
    Testa via socket TCP se o PostgreSQL está acessível ANTES de criar o engine.
    Falha em 'timeout' segundos em vez de aguardar o timeout TCP padrão (minutos).
    """
    import socket
    try:
        with socket.create_connection((host, porta), timeout=timeout):
            return True
    except (OSError, ConnectionRefusedError):
        return False


def _criar_engine():
    """
    Cria o engine SQLAlchemy para o PostgreSQL.
    Faz primeiro um check TCP rápido (3s) antes de tentar a ligação SQLAlchemy.
    Devolve None se a ligação falhar — a pipeline continua sem watermarks.
    """
    if not _verificar_postgres_tcp():
        print("  AVISO: PostgreSQL nao acessivel na porta 5432. Certifica-te que o Docker esta a correr.")
        print("  Watermarks e quality log desativados. Pipeline corre em modo degradado.")
        return None
    try:
        from sqlalchemy import create_engine
        engine = create_engine(DW_URL, echo=False, connect_args={"connect_timeout": 5})
        with engine.connect():
            pass
        print("  PostgreSQL: ligacao estabelecida.")
        return engine
    except Exception as e:
        print(f"  AVISO: PostgreSQL indisponivel ({e}). Watermarks e quality log desativados.")
        return None


# ─── WATERMARK (CDC — Audit-Based Incremental Load) ──────────────────────────

def ler_watermark(engine, fonte: str) -> date | None:
    """
    Devolve o último data_fim registado em pipeline_control para a fonte.
    Devolve None se ainda não existir (primeira execução incremental).
    """
    if engine is None:
        return None
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            linha = conn.execute(
                text(f"""
                    SELECT data_fim::date
                    FROM   {DW_SCHEMA}.pipeline_control
                    WHERE  nome_pipeline = 'main'
                      AND  camada        = :fonte
                      AND  estado        = 'completo'
                    ORDER  BY data_fim DESC
                    LIMIT  1
                """),
                {"fonte": fonte},
            ).fetchone()
        return linha[0] if linha else None
    except Exception as e:
        print(f"  AVISO: erro ao ler watermark ({fonte}): {e}")
        return None


def escrever_watermark(engine, fonte: str, data_processada: date, n_ficheiros: int):
    """
    Insere uma linha em pipeline_control com o watermark actualizado.
    A data_fim é o último dia de dados processado — próxima execução
    incremental filtrará ficheiros com data > data_processada.
    """
    if engine is None:
        return
    try:
        from sqlalchemy import text
        with engine.begin() as conn:
            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.pipeline_control
                        (nome_pipeline, camada, estado,
                         data_inicio, data_fim,
                         linhas_processadas, mensagem_erro)
                    VALUES
                        ('main', :fonte, 'completo',
                         :agora, :data_fim,
                         :n, NULL)
                """),
                {
                    "fonte":    fonte,
                    "agora":    datetime.now(timezone.utc),
                    "data_fim": datetime.combine(data_processada, datetime.min.time()),
                    "n":        n_ficheiros,
                },
            )
        print(f"  Watermark gravado: {fonte} → {data_processada}")
    except Exception as e:
        print(f"  AVISO: erro ao gravar watermark ({fonte}): {e}")


# ─── PARSERS DE DATA POR FONTE ────────────────────────────────────────────────
# Cada parser extrai a data representativa de um ficheiro a partir do nome.
# Devolve date(YYYY, MM, 1) para fontes mensais e o primeiro dia da semana
# ISO para hashtags semanais.

def _data_inventario(fp: Path) -> date | None:
    """YYYY_MM_stand.csv → date(YYYY, MM, 1)"""
    try:
        partes = fp.stem.split("_")
        return date(int(partes[0]), int(partes[1]), 1)
    except Exception:
        return None


def _data_trends(fp: Path) -> date | None:
    """trends_YYYYMM.json → date(YYYY, MM, 1)"""
    try:
        stem = fp.stem          # "trends_202401"
        return date(int(stem[-6:-2]), int(stem[-2:]), 1)
    except Exception:
        return None


def _data_forum(fp: Path) -> date | None:
    """forum_YYYYMM.txt → date(YYYY, MM, 1)"""
    try:
        stem = fp.stem          # "forum_202401"
        return date(int(stem[-6:-2]), int(stem[-2:]), 1)
    except Exception:
        return None


def _data_hashtags(fp: Path) -> date | None:
    """hashtags_YYYYWNN.xml → primeiro dia (segunda-feira) da semana ISO"""
    try:
        stem   = fp.stem        # "hashtags_2024W03"
        partes = stem.split("_")[1]
        ano    = int(partes[:4])
        semana = int(partes[5:])
        return date.fromisocalendar(ano, semana, 1)
    except Exception:
        return None


_PARSERS: dict[str, callable] = {
    "inventario": _data_inventario,
    "trends":     _data_trends,
    "forum":      _data_forum,
    "hashtags":   _data_hashtags,
}


# ─── DESCOBERTA DE FICHEIROS ──────────────────────────────────────────────────

def _filtrar_por_intervalo(
    pares: list[tuple[Path, date | None]],
    data_min: date | None,
    data_max: date,
) -> list[Path]:
    """
    Filtra pares (filepath, data) para o intervalo ]data_min, data_max].
    data_min é exclusivo (> watermark); data_max é inclusivo (≤ limite).
    Se data_min for None, inclui desde o início.
    """
    resultado = []
    for fp, dt in pares:
        if dt is None:
            continue
        if data_min is not None and dt <= data_min:
            continue
        if dt > data_max:
            continue
        resultado.append(fp)
    return sorted(resultado)


def descobrir_ficheiros(
    data_min: date | None,
    data_max: date,
    watermarks: dict[str, date | None] | None = None,
) -> dict[str, list[Path]]:
    """
    Descobre ficheiros de cada fonte dentro do intervalo ]data_min, data_max].

    Se watermarks for fornecido (modo incremental), aplica o watermark
    individual de cada fonte em vez do data_min global.
    """
    # Pares (filepath, data) para cada fonte
    pares: dict[str, list[tuple[Path, date | None]]] = {
        "inventario": [(fp, _data_inventario(fp)) for fp in sorted(STANDS_DIR.rglob("*.csv"))],
        "trends":     [(fp, _data_trends(fp))     for fp in sorted(TRENDS_DIR.rglob("trends_*.json"))],
        "forum":      [(fp, _data_forum(fp))       for fp in sorted(FORUM_DIR.rglob("forum_*.txt"))],
        "hashtags":   [(fp, _data_hashtags(fp))    for fp in sorted(HASHTAGS_DIR.rglob("hashtags_*.xml"))],
    }

    resultado: dict[str, list[Path]] = {}
    for fonte, lista in pares.items():
        wm = (watermarks or {}).get(fonte, data_min)
        resultado[fonte] = _filtrar_por_intervalo(lista, wm, data_max)

    return resultado


def _data_maxima_por_fonte(ficheiros: dict[str, list[Path]]) -> dict[str, date | None]:
    """Devolve a data mais recente encontrada para cada fonte."""
    return {
        fonte: max(
            (d for fp in lista if (d := _PARSERS[fonte](fp)) is not None),
            default=None,
        )
        for fonte, lista in ficheiros.items()
    }


# ─── MODO: FULL LOAD ─────────────────────────────────────────────────────────

def correr_full_load(engine, nlp_habilitado: bool = True):
    """
    Batch histórico completo: início → FULL_LOAD_LIMITE (2023-12-31).

    Não verifica watermarks — processa tudo desde o princípio.
    No final, inicializa os watermarks para que a próxima execução
    incremental saiba de onde partir.
    """
    separador = "=" * 60

    print(f"\n{separador}")
    print(f"  FULL LOAD  (histórico até {FULL_LOAD_LIMITE})")
    print(separador)

    t_inicio = time.time()

    _log(f"Descoberta de ficheiros ate {FULL_LOAD_LIMITE}...")
    ficheiros = descobrir_ficheiros(data_min=None, data_max=FULL_LOAD_LIMITE)

    total_ficheiros = sum(len(v) for v in ficheiros.values())
    if total_ficheiros == 0:
        _log("Nenhum ficheiro encontrado. Verifica os caminhos em data/sources/.", "WARN")
        return

    print("\n  Ficheiros a processar:")
    for fonte, lista in ficheiros.items():
        print(f"    {fonte:<12}: {len(lista):>4} ficheiros")
    _log(f"Total: {total_ficheiros} ficheiros descobertos")

    # ── Bronze ──────────────────────────────────────────────────────────────
    _log("A iniciar Bronze...")
    t_bronze = time.time()
    run_bronze(
        ficheiros_inventario=ficheiros["inventario"],
        ficheiros_trends=ficheiros["trends"],
        ficheiros_forum=ficheiros["forum"],
        ficheiros_hashtags=ficheiros["hashtags"],
    )
    _log(f"Bronze concluido em {time.time()-t_bronze:.1f}s")

    # ── Silver ──────────────────────────────────────────────────────────────
    _log(f"A iniciar Silver (NLP={'activo' if nlp_habilitado else 'desabilitado'})...")
    t_silver = time.time()
    run_silver(
        ficheiros_inventario=[str(f) for f in ficheiros["inventario"]],
        ficheiros_trends=[str(f) for f in ficheiros["trends"]],
        ficheiros_forum=[str(f) for f in ficheiros["forum"]],
        ficheiros_hashtags=[str(f) for f in ficheiros["hashtags"]],
        nlp_habilitado=nlp_habilitado,
    )
    _log(f"Silver concluido em {time.time()-t_silver:.1f}s")

    # ── Profiling ───────────────────────────────────────────────────────────
    # Corre profiling histórico nas tabelas raw do Bronze (agora com o dataset completo)
    run_profiling()

    # ── Load to PostgreSQL ──────────────────────────────────────────────────
    run_load_to_postgres()

    # ── Prophet Forecasting ─────────────────────────────────────────────────
    run_prophet()

    # ── Watermarks iniciais ──────────────────────────────────────────────────
    # Grava FULL_LOAD_LIMITE para cada fonte mesmo que não haja ficheiros
    # (garante que incremental não re-processa o histórico).
    datas_max = _data_maxima_por_fonte(ficheiros)
    print("\n  A inicializar watermarks...")
    for fonte in FONTES:
        dt_gravada = datas_max.get(fonte) or FULL_LOAD_LIMITE
        escrever_watermark(engine, fonte, dt_gravada, len(ficheiros.get(fonte, [])))

    _log(f"Full Load total: {time.time()-t_inicio:.1f}s")
    print(f"\n{separador}")
    print("  FULL LOAD concluido.")
    print(separador)


# ─── MODO: INCREMENTAL ───────────────────────────────────────────────────────

def correr_incremental(engine, data_limite: date, nlp_habilitado: bool = True):
    """
    Batch incremental: watermark_por_fonte → data_limite.

    Fluxo CDC (Audit-Based Incremental Load):
      1. Ler watermark individual de cada fonte em pipeline_control
      2. Descobrir ficheiros com data > watermark E ≤ data_limite
      3. Bronze: append dos novos ficheiros
      4. Silver: MERGE/UPSERT com os novos ficheiros
      5. Actualizar watermark (só após sucesso — atomicidade)
    """
    separador = "=" * 60

    print(f"\n{separador}")
    print(f"  INCREMENTAL  (até {data_limite})")
    print(separador)

    # 1. Ler watermarks
    watermarks = {fonte: ler_watermark(engine, fonte) for fonte in FONTES}
    print("\n  Watermarks actuais:")
    for fonte, wm in watermarks.items():
        print(f"    {fonte:<12}: {str(wm) if wm else 'sem watermark (processa tudo)'}")

    # 2. Descobrir ficheiros novos por fonte
    ficheiros = descobrir_ficheiros(
        data_min=None,          # data_min global não é usado — watermarks individuais abaixo
        data_max=data_limite,
        watermarks=watermarks,  # watermark individual por fonte
    )

    total_ficheiros = sum(len(v) for v in ficheiros.values())
    if total_ficheiros == 0:
        _log("Nenhum ficheiro novo encontrado -- pipeline terminada sem alteracoes.")
        print("\n  Nenhum ficheiro novo encontrado -- pipeline terminada sem alteracoes.")
        return

    t_inicio = time.time()

    print("\n  Ficheiros novos a processar:")
    for fonte, lista in ficheiros.items():
        print(f"    {fonte:<12}: {len(lista):>4} ficheiros")

    # 3. Bronze
    _log("A iniciar Bronze...")
    t_bronze = time.time()
    run_bronze(
        ficheiros_inventario=ficheiros["inventario"],
        ficheiros_trends=ficheiros["trends"],
        ficheiros_forum=ficheiros["forum"],
        ficheiros_hashtags=ficheiros["hashtags"],
    )
    _log(f"Bronze concluido em {time.time()-t_bronze:.1f}s")

    # 4. Silver
    _log(f"A iniciar Silver (NLP={'activo' if nlp_habilitado else 'desabilitado'})...")
    t_silver = time.time()
    run_silver(
        ficheiros_inventario=[str(f) for f in ficheiros["inventario"]],
        ficheiros_trends=[str(f) for f in ficheiros["trends"]],
        ficheiros_forum=[str(f) for f in ficheiros["forum"]],
        ficheiros_hashtags=[str(f) for f in ficheiros["hashtags"]],
        nlp_habilitado=nlp_habilitado,
    )
    _log(f"Silver concluido em {time.time()-t_silver:.1f}s")
    
    # 5. Load para o PostgreSQL
    run_load_to_postgres()
    
    # 6. ML Forecasting
    run_prophet()

    # 7. Actualizar watermarks (atomicidade: só aqui, após tudo correr bem)
    datas_max = _data_maxima_por_fonte(ficheiros)
    print("\n  A actualizar watermarks...")
    for fonte in FONTES:
        if ficheiros[fonte]:    # só actualiza se houve ficheiros novos para esta fonte
            dt = datas_max.get(fonte) or data_limite
            escrever_watermark(engine, fonte, dt, len(ficheiros[fonte]))
        else:
            print(f"  Watermark {fonte}: sem novos ficheiros — mantido.")

    _log(f"Incremental total: {time.time()-t_inicio:.1f}s")
    print(f"\n{separador}")
    print("  INCREMENTAL concluido.")
    print(separador)


# ─── CLI ──────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description=(
            "Auto Escala — Orquestrador do pipeline de dados.\n"
            "Modos: full_load (histórico 2022-2023) | incremental (delta até --data_limite)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exemplos:\n"
            "  python scripts/main.py --mode full_load\n"
            "  python scripts/main.py --mode incremental --data_limite 2024-03-31\n"
            "  python scripts/main.py --mode incremental --data_limite 2024-06-30\n"
        ),
    )
    parser.add_argument(
        "--no-nlp",
        action="store_true",
        default=False,
        dest="no_nlp",
        help=(
            "Desabilita NLP (pysentimiento) neste run. "
            "Score sentimento fica 0.0 para ficheiros de forum. "
            "Util para debugging rapido ou quando BERT ainda nao esta instalado."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        dest="dry_run",
        help=(
            "Descobre e lista os ficheiros que seriam processados sem executar "
            "Bronze nem Silver. Util para verificar watermarks e contagens."
        ),
    )
    parser.add_argument(
        "--mode",
        choices=["full_load", "incremental"],
        required=True,
        help="full_load: batch histórico (2022-2023). incremental: batch regular.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        default=False,
        help="ATENÇÃO: Apaga as diretorias do Delta Lake (Bronze/Silver/Quarentena) e faz o drop/recreate do schema no PostgreSQL antes de iniciar.",
    )
    parser.add_argument(
        "--data_limite",
        metavar="YYYY-MM-DD",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=None,
        help="Data limite para o batch incremental (inclusive). Obrigatório no modo incremental.",
    )
    return parser.parse_args()


def _limpar_tudo():
    import shutil
    print("\n" + "=" * 60)
    print("  RESET: A limpar pipeline...")
    print("=" * 60)
    
    # 1. Limpar pastas do Delta Lake
    for pasta in ["bronze", "silver", "quarantine"]:
        p = BASE_DIR / "data_lake" / pasta
        if p.exists():
            try:
                shutil.rmtree(p)
                print(f"  Apagado: {p}")
            except Exception as e:
                print(f"  AVISO: Erro ao apagar {p} ({e})")
                
    # 2. Recriar schema no Postgres
    print("  A recriar Star Schema no PostgreSQL...")
    create_data_warehouse()
    print("=" * 60 + "\n")


def main():
    args = _parse_args()

    # Validação
    if args.mode == "incremental":
        if args.data_limite is None:
            print("ERRO: --data_limite é obrigatório no modo incremental.")
            print("  Exemplo: python scripts/main.py --mode incremental --data_limite 2024-03-31")
            sys.exit(1)
        if args.data_limite <= FULL_LOAD_LIMITE:
            print(
                f"AVISO: --data_limite {args.data_limite} é anterior ou igual ao limite "
                f"do full_load ({FULL_LOAD_LIMITE}). "
                f"Usa --mode full_load para dados históricos."
            )
            sys.exit(1)

    nlp_habilitado = not args.no_nlp

    if args.dry_run:
        print("\n[DRY RUN] Apenas descoberta de ficheiros -- nada sera processado.")
        engine = _criar_engine()
        if args.mode == "full_load":
            ficheiros = descobrir_ficheiros(data_min=None, data_max=FULL_LOAD_LIMITE)
        else:
            watermarks = {fonte: ler_watermark(engine, fonte) for fonte in FONTES}
            ficheiros = descobrir_ficheiros(data_min=None, data_max=args.data_limite, watermarks=watermarks)
            _log("Watermarks actuais:")
            for fonte, wm in watermarks.items():
                _log(f"  {fonte:<12}: {str(wm) if wm else 'sem watermark'}")
        print("\n  Ficheiros que seriam processados:")
        for fonte, lista in ficheiros.items():
            print(f"    {fonte:<12}: {len(lista):>4} ficheiros")
            for fp in lista[:5]:
                print(f"                {fp.name}")
            if len(lista) > 5:
                print(f"                ... (+{len(lista)-5} ficheiros)")
        if engine:
            engine.dispose()
        return

    if args.reset:
        _limpar_tudo()

    engine = _criar_engine()

    try:
        if args.mode == "full_load":
            correr_full_load(engine, nlp_habilitado=nlp_habilitado)
        else:
            correr_incremental(engine, args.data_limite, nlp_habilitado=nlp_habilitado)
    finally:
        if engine:
            engine.dispose()


if __name__ == "__main__":
    main()