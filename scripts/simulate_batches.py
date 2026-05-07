"""
simulate_batches.py -- Simulacao de batches mensais incrementais
=================================================================
Simula a chegada mensal de dados chamando a pipeline incremental
para cada mes entre --desde e --ate.

Cadencia:
  - Inventario, Trends, Forum : mensais  (data_limite = ultimo dia do mes)
  - Hashtags                  : semanais, agrupados no mesmo batch mensal
                                (data_limite = ultimo dia do mes captura todas
                                as semanas cujo primeiro dia <= esse limite)

Fluxo por batch:
  correr_full_load  (uma vez, se ainda nao tiver sido feito)
  correr_incremental(data_limite=2024-01-31)
  correr_incremental(data_limite=2024-02-29)
  ...

Uso:
  python scripts/simulate_batches.py
  python scripts/simulate_batches.py --desde 2024-01 --ate 2024-06
  python scripts/simulate_batches.py --desde 2024-01 --ate 2024-12 --skip-full-load
  python scripts/simulate_batches.py --no-nlp     # rapido, sem BERT

Projeto Auto Escala - CDGE 2025/2026
"""

from __future__ import annotations

import argparse
import calendar
import sys
import time
from datetime import date, datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "scripts"))

from main import (   # noqa: E402
    _log,
    _criar_engine,
    correr_full_load,
    correr_incremental,
    ler_watermark,
    FULL_LOAD_LIMITE,
    FONTES,
)


# --- CONFIGURACAO ─────────────────────────────────────────────────────────────

INCREMENTAL_INICIO    = date(2024, 1, 1)
INCREMENTAL_FIM_PADRAO = date(2026, 4, 1)


# --- UTILITARIOS ──────────────────────────────────────────────────────────────

def ultimo_dia_do_mes(ano: int, mes: int) -> date:
    """Devolve o ultimo dia calendario do mes (ex: 2024-02 -> 2024-02-29)."""
    return date(ano, mes, calendar.monthrange(ano, mes)[1])


def gerar_meses(inicio: date, fim: date) -> list[date]:
    """
    Lista de datas = ultimo dia de cada mes entre inicio e fim (inclusive).
    Ex: 2024-01 a 2024-03 -> [2024-01-31, 2024-02-29, 2024-03-31]
    """
    meses = []
    ano, mes = inicio.year, inicio.month
    while date(ano, mes, 1) <= date(fim.year, fim.month, 1):
        meses.append(ultimo_dia_do_mes(ano, mes))
        if mes == 12:
            ano, mes = ano + 1, 1
        else:
            mes += 1
    return meses


def full_load_ja_feito(engine) -> bool:
    """Verifica se o full_load ja foi executado (existe pelo menos um watermark)."""
    if engine is None:
        return False
    for fonte in FONTES:
        if ler_watermark(engine, fonte) is not None:
            return True
    return False


def _formatar_duracao(segundos: float) -> str:
    """Converte segundos para string legivel: '1m 23s' ou '45.3s'."""
    if segundos >= 60:
        return f"{int(segundos // 60)}m {int(segundos % 60):02d}s"
    return f"{segundos:.1f}s"


def _eta(tempo_medio: float, restantes: int) -> str:
    """Calcula ETA com base na media de duracoes anteriores."""
    if tempo_medio <= 0 or restantes <= 0:
        return "N/D"
    return _formatar_duracao(tempo_medio * restantes)


# --- SIMULACAO ────────────────────────────────────────────────────────────────

def simular(
    desde: date,
    ate: date,
    skip_full_load: bool = False,
    pausa_entre_batches: float = 0.0,
    nlp_habilitado: bool = True,
):
    """
    Corre o full_load (se necessario) e depois um batch incremental
    por cada mes no intervalo [desde, ate].

    Args:
        desde:               Primeiro mes incremental.
        ate:                 Ultimo mes a processar (inclusive).
        skip_full_load:      Ignora full_load mesmo sem watermarks.
        pausa_entre_batches: Pausa em segundos entre batches (demo ao vivo).
        nlp_habilitado:      False = --no-nlp em todos os batches.
    """
    separador = "=" * 60
    t_total = time.time()

    print(f"\n{separador}")
    print(f"  SIMULATE BATCHES")
    print(f"  Periodo incremental : {desde.strftime('%Y-%m')} -> {ate.strftime('%Y-%m')}")
    print(f"  NLP                 : {'activo' if nlp_habilitado else 'DESABILITADO (--no-nlp)'}")
    print(separador)

    engine = _criar_engine()

    # --- Full Load ------------------------------------------------------------
    if skip_full_load:
        _log("--skip-full-load activo: full load ignorado.")
    elif full_load_ja_feito(engine):
        _log("Full load ja executado (watermarks presentes). A saltar para incremental.")
    else:
        _log("Nenhum watermark encontrado -- a correr full_load primeiro...")
        t_fl = time.time()
        correr_full_load(engine, nlp_habilitado=nlp_habilitado)
        _log(f"Full load concluido em {_formatar_duracao(time.time()-t_fl)}")
        if pausa_entre_batches > 0:
            time.sleep(pausa_entre_batches)

    # --- Batches incrementais ------------------------------------------------
    meses = gerar_meses(desde, ate)
    total_meses = len(meses)

    _log(f"{total_meses} batches mensais a processar.")
    print(separador)

    duracoes: list[float] = []

    for i, data_limite in enumerate(meses, start=1):
        tempo_medio = sum(duracoes) / len(duracoes) if duracoes else 0.0
        restantes   = total_meses - i
        eta_str     = _eta(tempo_medio, restantes)
        pct         = int(100 * (i - 1) / total_meses)

        print(
            f"\n  BATCH {i:>3}/{total_meses}  [{pct:>3}%]  "
            f"{data_limite.strftime('%Y-%m')}  (limite: {data_limite})  "
            f"ETA restante: {eta_str}"
        )
        print(f"  {'-' * 55}")

        t_batch = time.time()
        correr_incremental(engine, data_limite, nlp_habilitado=nlp_habilitado)
        duracao = time.time() - t_batch
        duracoes.append(duracao)

        _log(
            f"Batch {i}/{total_meses} concluido em {_formatar_duracao(duracao)}  "
            f"(media ate agora: {_formatar_duracao(sum(duracoes)/len(duracoes))})"
        )

        if pausa_entre_batches > 0 and i < total_meses:
            _log(f"Pausa de {pausa_entre_batches}s...")
            time.sleep(pausa_entre_batches)

    # --- Sumario final -------------------------------------------------------
    t_total_dur = time.time() - t_total
    media_batch = sum(duracoes) / len(duracoes) if duracoes else 0.0

    print(f"\n{separador}")
    print(f"  SIMULACAO CONCLUIDA")
    print(f"  Batches processados : {total_meses}")
    print(f"  Periodo             : {desde.strftime('%Y-%m')} -> {ate.strftime('%Y-%m')}")
    print(f"  Duracao total       : {_formatar_duracao(t_total_dur)}")
    print(f"  Media por batch     : {_formatar_duracao(media_batch)}")
    if duracoes:
        print(f"  Mais rapido         : {_formatar_duracao(min(duracoes))}")
        print(f"  Mais lento          : {_formatar_duracao(max(duracoes))}")
    print(separador)

    if engine:
        engine.dispose()


# --- CLI ──────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="simulate_batches.py",
        description=(
            "Auto Escala - Simulacao de batches mensais incrementais.\n"
            "Corre full_load (se necessario) seguido de um batch incremental por mes."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exemplos:\n"
            "  python scripts/simulate_batches.py\n"
            "  python scripts/simulate_batches.py --desde 2024-01 --ate 2024-06\n"
            "  python scripts/simulate_batches.py --ate 2024-12 --skip-full-load\n"
            "  python scripts/simulate_batches.py --no-nlp   # rapido, sem BERT\n"
        ),
    )
    parser.add_argument(
        "--desde",
        metavar="YYYY-MM",
        type=lambda s: datetime.strptime(s, "%Y-%m").date(),
        default=INCREMENTAL_INICIO,
        help=f"Primeiro mes incremental (padrao: {INCREMENTAL_INICIO.strftime('%Y-%m')}).",
    )
    parser.add_argument(
        "--ate",
        metavar="YYYY-MM",
        type=lambda s: datetime.strptime(s, "%Y-%m").date(),
        default=INCREMENTAL_FIM_PADRAO,
        help=f"Ultimo mes a simular (padrao: {INCREMENTAL_FIM_PADRAO.strftime('%Y-%m')}).",
    )
    parser.add_argument(
        "--skip-full-load",
        action="store_true",
        default=False,
        help="Nao executa o full_load mesmo sem watermarks.",
    )
    parser.add_argument(
        "--pausa",
        metavar="SEGUNDOS",
        type=float,
        default=0.0,
        help="Pausa em segundos entre batches (util para demo ao vivo; padrao: 0).",
    )
    parser.add_argument(
        "--no-nlp",
        action="store_true",
        default=False,
        dest="no_nlp",
        help=(
            "Desabilita NLP (pysentimiento) em todos os batches. "
            "Score sentimento fica 0.0. "
            "Muito mais rapido -- util para testar o pipeline sem BERT."
        ),
    )
    return parser.parse_args()


def main():
    args = _parse_args()

    desde = date(args.desde.year, args.desde.month, 1)
    ate   = date(args.ate.year,   args.ate.month,   1)

    if desde <= FULL_LOAD_LIMITE:
        _log(
            f"--desde {desde.strftime('%Y-%m')} esta dentro do periodo historico "
            f"(full_load cobre ate {FULL_LOAD_LIMITE}). "
            f"Incremental comecara a partir de {INCREMENTAL_INICIO.strftime('%Y-%m')}.",
            "WARN"
        )
        desde = INCREMENTAL_INICIO

    if ate < desde:
        print(f"ERRO: --ate ({ate.strftime('%Y-%m')}) e anterior a --desde ({desde.strftime('%Y-%m')}).")
        sys.exit(1)

    simular(
        desde=desde,
        ate=ate,
        skip_full_load=args.skip_full_load,
        pausa_entre_batches=args.pausa,
        nlp_habilitado=not args.no_nlp,
    )


if __name__ == "__main__":
    main()