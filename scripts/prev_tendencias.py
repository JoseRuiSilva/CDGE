"""
prev_tendencias.py
------------------
Modelo SARIMA para previsão de métricas de tendências e indicadores sociais.

Prevê para t+1 as seguintes métricas por (marca × tipo × combustivel × localizacao):
  - valor_interesse
  - total_posts
  - forum_mencoes
  - analise_sentimento

Resultados guardados em fact_previsoes_sarima + dim_model_run.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import warnings
from datetime import date

import numpy as np
import pandas as pd
import pmdarima as pm
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
DW_URL = f"postgresql+psycopg2://ae_user:ae_pass_2026@{_PG_HOST}:{_PG_PORT}/auto_escala"
engine = create_engine(DW_URL, echo=False)

MIN_SERIES_LEN = 18
N_TEST_FOLDS = 3          # era 6; reduzido a metade para acelerar walk-forward
MIN_SERIES_LEN_SPARSE = 12  # mínimo relaxado para métricas esparsas (forum_mencoes, analise_sentimento)
MODEL_NAME = "SARIMA_Tendencias"
MODEL_VERSION = "1.0"
SERIES_KEYS = ["marca_key", "tipo_key", "combustivel_key", "localizacao_key"]


def get_sql_valor_interesse(schema):
    return f"""
        SELECT marca_key, tipo_key, combustivel_key, localizacao_key, ano, mes, valor_interesse AS valor
        FROM {schema}.vw_mart_prev_tendencias
        WHERE valor_interesse IS NOT NULL
        ORDER BY 1, 2, 3, 4, 5, 6
    """


def get_sql_total_posts(schema):
    return f"""
        SELECT marca_key, tipo_key, combustivel_key, localizacao_key, ano, mes, total_posts AS valor
        FROM {schema}.vw_mart_prev_tendencias
        WHERE total_posts IS NOT NULL
        ORDER BY 1, 2, 3, 4, 5, 6
    """


def get_sql_forum_mencoes(schema):
    return f"""
        SELECT marca_key, tipo_key, combustivel_key, localizacao_key, ano, mes, forum_mencoes AS valor
        FROM {schema}.vw_mart_prev_tendencias
        WHERE forum_mencoes IS NOT NULL
        ORDER BY 1, 2, 3, 4, 5, 6
    """


def get_sql_sentimento(schema):
    return f"""
        SELECT marca_key, tipo_key, combustivel_key, localizacao_key, ano, mes, analise_sentimento AS valor
        FROM {schema}.vw_mart_prev_tendencias
        WHERE analise_sentimento IS NOT NULL
        ORDER BY 1, 2, 3, 4, 5, 6
    """


def get_sql_tempo_key(schema):
    return f"""
        SELECT tempo_key
        FROM {schema}.dim_tempo
        WHERE ano = :ano AND mes = :mes AND dia = 1
        LIMIT 1
    """


def next_month(ano: int, mes: int) -> tuple[int, int]:
    return (ano + 1, 1) if mes == 12 else (ano, mes + 1)


def choose_arima_seasonality(series: pd.Series) -> tuple[bool, int]:
    """Use seasonal ARIMA apenas quando há dados suficientes para m=12."""
    if len(series) < 30 or series.nunique() <= 3:
        return False, 1
    return True, 12


def get_tempo_key(conn, schema: str, ano: int, mes: int) -> int | None:
    row = conn.execute(text(get_sql_tempo_key(schema)), {"ano": ano, "mes": mes}).fetchone()
    return int(row[0]) if row else None


def walk_forward_sarima(
    series: pd.Series,
    n_folds: int,
    min_train: int = MIN_SERIES_LEN,
    log_transform: bool = False,
) -> tuple:
    """
    Walk-forward SARIMA.

    min_train    : mínimo de obs de treino por fold; permite valor relaxado
                   para métricas esparsas sem alterar a constante global.
    log_transform: aplica log1p antes de ajustar e expm1 nas previsões;
                   métricas calculadas sempre na escala original.
    """
    n = len(series)
    reais, previstos = [], []

    for i in range(n_folds, 0, -1):
        idx_cut = n - i
        if idx_cut < min_train:
            continue
        treino_raw = series.iloc[:idx_cut]
        real = float(series.iloc[idx_cut])

        treino = np.log1p(treino_raw) if log_transform else treino_raw
        seasonal, m = choose_arima_seasonality(treino)

        previsto_log = None
        try:
            modelo = pm.auto_arima(
                treino,
                seasonal=seasonal,
                m=m,
                stepwise=True,
                suppress_warnings=True,
                error_action="ignore",
                information_criterion="aic",
            )
            previsto_log = float(modelo.predict(n_periods=1)[0])
        except Exception as exc:
            log.debug(f"    fold {i}: SARIMA falhou — {exc}")

        if previsto_log is None and seasonal:
            try:
                modelo = pm.auto_arima(
                    treino,
                    seasonal=False,
                    stepwise=True,
                    suppress_warnings=True,
                    error_action="ignore",
                    information_criterion="aic",
                )
                previsto_log = float(modelo.predict(n_periods=1)[0])
                log.debug(f"    fold {i}: fallback ARIMA não-sazonal usado (n={len(treino)})")
            except Exception as exc:
                log.debug(f"    fold {i}: fallback ARIMA não-sazonal falhou — {exc}")

        if previsto_log is None:
            log.debug(f"    fold {i}: fallback naive usado (n={len(treino)})")
            previsto_log = float(treino.iloc[-1])

        # Back-transform para escala original antes de guardar métricas
        previsto = float(np.expm1(previsto_log)) if log_transform else previsto_log

        reais.append(real)
        previstos.append(previsto)

    if len(reais) < 2:
        return None, None, None

    r = np.array(reais)
    p = np.array(previstos)
    mae = float(mean_absolute_error(r, p))
    rmse = float(np.sqrt(mean_squared_error(r, p)))

    # MAPE é enganoso quando a série oscila próximo de zero (ex: analise_sentimento):
    # denominadores minúsculos inflacionam o erro para milhares de %.
    # Suprimimos MAPE sempre que a média absoluta da série < 0.1.
    mean_abs = float(np.abs(r).mean())
    if mean_abs < 0.1:
        mape = None
    else:
        mask = r != 0
        mape = float(np.mean(np.abs((r[mask] - p[mask]) / r[mask])) * 100) if mask.any() else None
    return mae, rmse, mape


def fit_and_forecast(series: pd.Series, log_transform: bool = False) -> tuple:
    """
    Ajusta SARIMA/ARIMA e devolve (previsao, yhat_lower, yhat_upper) na escala original.
    Se log_transform=True, ajusta em log1p e converte output com expm1.
    """
    s = np.log1p(series) if log_transform else series

    def _back(v):
        return float(np.expm1(v)) if log_transform else float(v)

    if s.nunique() <= 1:
        last = _back(s.iloc[-1])
        return last, last, last

    if len(s) >= 24:
        try:
            modelo = pm.auto_arima(
                s,
                seasonal=True,
                m=12,
                stepwise=True,
                suppress_warnings=True,
                error_action="ignore",
                information_criterion="aic",
            )
            fc, ci = modelo.predict(n_periods=1, return_conf_int=True, alpha=0.05)
            return _back(fc[0]), _back(ci[0][0]), _back(ci[0][1])
        except Exception as exc:
            log.debug(f"    fit_and_forecast SARIMA falhou — {exc}")

    try:
        modelo = pm.auto_arima(
            s,
            seasonal=False,
            stepwise=True,
            suppress_warnings=True,
            error_action="ignore",
            information_criterion="aic",
        )
        fc, ci = modelo.predict(n_periods=1, return_conf_int=True, alpha=0.05)
        log.debug(f"    fit_and_forecast fallback ARIMA não-sazonal usado (n={len(s)})")
        return _back(fc[0]), _back(ci[0][0]), _back(ci[0][1])
    except Exception as exc:
        log.debug(f"    fit_and_forecast ARIMA não-sazonal falhou — {exc}")

    last_log = float(s.iloc[-1])
    std_log = float(s.std()) if s.std() > 0 else 0.0
    log.debug(f"    fit_and_forecast fallback naive usado (n={len(s)}, std={std_log:.4f})")
    return _back(last_log), _back(last_log - 1.96 * std_log), _back(last_log + 1.96 * std_log)


def save_model_run(conn, schema: str, mae_global: float | None, train_cutoff: date) -> int:
    features_info = {
        "metricas": ["valor_interesse", "total_posts", "forum_mencoes", "analise_sentimento"],
        "min_series_len": MIN_SERIES_LEN,
        "n_test_folds": N_TEST_FOLDS,
        "seasonal_m": 12,
    }
    fhash = hashlib.md5(json.dumps(features_info, sort_keys=True).encode()).hexdigest()

    row = conn.execute(
        text(f"""
            INSERT INTO {schema}.dim_model_run
                (model_name, model_version, features_hash, train_cutoff, mae, notas)
            VALUES (:name, :ver, :fh, :tc, :mae, :notas)
            RETURNING model_run_id
        """),
        {
            "name": MODEL_NAME,
            "ver": MODEL_VERSION,
            "fh": fhash,
            "tc": train_cutoff,
            "mae": round(mae_global, 4) if mae_global else None,
            "notas": (
                f"SARIMA seasonal m=12 | walk-forward {N_TEST_FOLDS} folds | "
                f"granularidade: marca×tipo×combustivel×localizacao | "
                f"fonte: vw_mart_prev_tendencias"
            ),
        },
    ).fetchone()
    return int(row[0])


def save_predictions(conn, schema: str, predictions: list[dict], model_run_id: int) -> int:
    inserted = 0
    for p in predictions:
        tempo_ref_key = get_tempo_key(conn, schema, p["ano_ref"], p["mes_ref"])
        tempo_alvo_key = get_tempo_key(conn, schema, p["ano_alvo"], p["mes_alvo"])
        if tempo_ref_key is None or tempo_alvo_key is None:
            log.warning(
                f"  tempo_key não encontrado para ref={p['ano_ref']}-{p['mes_ref']} "
                f"ou alvo={p['ano_alvo']}-{p['mes_alvo']}. A saltar."
            )
            continue

        conn.execute(
            text(f"""
                INSERT INTO {schema}.fact_previsoes_sarima
                    (model_run_id, tempo_alvo_key, tempo_ref_key,
                     marca_key, tipo_key, combustivel_key, localizacao_key,
                     metrica, valor_previsto, yhat_lower, yhat_upper)
                VALUES
                    (:run_id, :alvo, :ref, :marca, :tipo, :comb, :loc,
                     :metrica, :val, :lower, :upper)
                ON CONFLICT (model_run_id, tempo_alvo_key, marca_key, tipo_key, combustivel_key, localizacao_key, metrica)
                DO UPDATE SET
                    valor_previsto = EXCLUDED.valor_previsto,
                    yhat_lower     = EXCLUDED.yhat_lower,
                    yhat_upper     = EXCLUDED.yhat_upper
            """),
            {
                "run_id": model_run_id,
                "alvo": tempo_alvo_key,
                "ref": tempo_ref_key,
                "marca": p["marca_key"],
                "tipo": p["tipo_key"],
                "comb": p["combustivel_key"],
                "loc": p["localizacao_key"],
                "metrica": p["metrica"],
                "val": p["valor_previsto"],
                "lower": p["yhat_lower"],
                "upper": p["yhat_upper"],
            },
        )
        inserted += 1
    return inserted


def run_sarima(schema: str = "auto_escala_dw") -> None:
    log.info("=" * 60)
    log.info(f"AUTO ESCALA — SARIMA Tendencias (SCHEMA={schema})")
    log.info("=" * 60)

    METRICAS = {
        "valor_interesse": get_sql_valor_interesse(schema),
        "total_posts": get_sql_total_posts(schema),
        "forum_mencoes": get_sql_forum_mencoes(schema),
        "analise_sentimento": get_sql_sentimento(schema),
    }

    all_predictions: list[dict] = []
    global_maes: list[float] = []

    with engine.connect() as conn:
        for metrica, sql in METRICAS.items():
            log.info(f"\n{'─'*50}")
            log.info(f"  Métrica: {metrica}")
            log.info(f"{'─'*50}")

            df = pd.read_sql(text(sql), conn)
            if df.empty:
                log.warning(f"  Sem dados para '{metrica}'. A saltar.")
                continue

            grupos = df.groupby(SERIES_KEYS)
            log.info(f"  {len(grupos)} séries encontradas.")

            for (mk, tk, ck, loc), grupo in grupos:
                grupo = grupo.sort_values(["ano", "mes"]).reset_index(drop=True)
                n_obs = len(grupo)
                label = f"(marca={mk}, tipo={tk}, comb={ck}, loc={loc})"

                # Métricas esparsas toleram séries mais curtas
                min_len = (
                    MIN_SERIES_LEN_SPARSE
                    if metrica in ("forum_mencoes", "analise_sentimento")
                    else MIN_SERIES_LEN
                )
                if n_obs < min_len:
                    log.debug(f"  {label} — série curta ({n_obs} obs, mínimo={min_len}). A saltar.")
                    continue

                serie = grupo["valor"].ffill().bfill().astype(float)
                if serie.isna().all():
                    log.debug(f"  {label} — série com todos os valores NA após imputação. A saltar.")
                    continue

                # total_posts: série de contagens com tendência crescente → log1p estabiliza variância
                # analise_sentimento / forum_mencoes: esparsas, min_train relaxado
                use_log = (metrica == "total_posts")

                mae, rmse, mape = walk_forward_sarima(
                    serie, N_TEST_FOLDS, min_train=min_len, log_transform=use_log
                )
                serie_mean = float(serie.mean())
                serie_std  = float(serie.std()) if serie.std() > 0 else 0.0
                if mae is not None:
                    log.info(
                        f"  {label} | n={n_obs} | mean={serie_mean:.2f} std={serie_std:.2f} | "
                        f"MAE={mae:.3f}  RMSE={rmse:.3f}  MAPE={f'{mape:.1f}%' if mape else 'N/A'}"
                    )
                    global_maes.append(mae)
                else:
                    log.warning(f"  {label} — walk-forward sem resultados.")

                val_prev, yhat_lower, yhat_upper = fit_and_forecast(serie, log_transform=use_log)
                if val_prev is None:
                    continue

                ano_ref = int(grupo["ano"].iloc[-1])
                mes_ref = int(grupo["mes"].iloc[-1])
                ano_alvo, mes_alvo = next_month(ano_ref, mes_ref)

                all_predictions.append({
                    "marca_key": int(mk),
                    "tipo_key": int(tk),
                    "combustivel_key": int(ck),
                    "localizacao_key": int(loc),
                    "metrica": metrica,
                    "ano_ref": ano_ref,
                    "mes_ref": mes_ref,
                    "ano_alvo": ano_alvo,
                    "mes_alvo": mes_alvo,
                    "valor_previsto": round(val_prev, 4),
                    "yhat_lower": round(yhat_lower, 4) if yhat_lower is not None else None,
                    "yhat_upper": round(yhat_upper, 4) if yhat_upper is not None else None,
                })

    if not all_predictions:
        log.error("Nenhuma previsão gerada. A terminar.")
        return

    max_ano = max(p["ano_ref"] for p in all_predictions)
    max_mes = max(p["mes_ref"] for p in all_predictions if p["ano_ref"] == max_ano)
    train_cutoff = date(max_ano, max_mes, 1)
    mae_global = float(np.mean(global_maes)) if global_maes else None

    log.info(f"\n{'─'*50}")
    log.info(f"  Total de previsões a guardar: {len(all_predictions)}")
    log.info(f"  MAE médio global: {mae_global:.4f}" if mae_global else "  MAE: N/A")
    log.info(f"  Train cutoff: {train_cutoff}")

    with engine.begin() as conn:
        model_run_id = save_model_run(conn, schema, mae_global, train_cutoff)
        n_inserted = save_predictions(conn, schema, all_predictions, model_run_id)

    log.info(f"\n  ✓ model_run_id = {model_run_id}")
    log.info(f"  ✓ {n_inserted} previsões inseridas em fact_previsoes_sarima")
    log.info("SARIMA concluído.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Modelo SARIMA")
    parser.add_argument("--schema", default="auto_escala_dw", help="Esquema da Base de Dados")
    args = parser.parse_args()

    run_sarima(schema=args.schema)