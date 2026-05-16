"""
prev_gain.py
------------
Modelo XGBoost para previsão de expected_gain.

Target: expected_gain(t+1) = p_venda(t+1) × mean_margem
Granularidade: marca × tipo × combustivel × localizacao × mês

Resultados guardados em fact_previsoes_xgboost + dim_model_run.
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
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sqlalchemy import create_engine, text
from xgboost import XGBRegressor

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

N_TEST_FOLDS = 6
MODEL_NAME = "XGBoost_Gain"
MODEL_VERSION = "1.0"
SERIES_KEYS = ["marca_key", "tipo_key", "combustivel_key", "localizacao_key"]


def get_sql_features_base(schema):
    return f"""
WITH stock_base AS (
    -- Âncora: todos os combos marca×tipo×combustivel×localizacao×mês com inventário.
    -- Inclui meses sem vendas → modelo aprende expected_gain=0 (dados de treino críticos).
    -- localizacao_key vem de dim_stand via stand_key (fct_inventario_mensal → dim_stand).
    SELECT
        dv.marca_key,
        dv.tipo_key,
        dv.combustivel_key,
        ds.localizacao_key,
        dtp.ano,
        dtp.mes,
        dtp.tempo_key,
        COUNT(fim.inventario_key)  AS n_stock,
        AVG(fim.valor_em_stock)    AS mean_valor_stock
    FROM {schema}.fct_inventario_mensal fim
    JOIN {schema}.dim_veiculo dv  ON fim.veiculo_key = dv.veiculo_key
    JOIN {schema}.dim_stand   ds  ON fim.stand_key   = ds.stand_key
    JOIN {schema}.dim_tempo   dtp ON fim.tempo_key   = dtp.tempo_key
    WHERE dv.veiculo_key <> -1
      AND dv.marca_key   <> -1
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),
trends_marca AS (
    -- Google Trends: granularidade marca apenas (tipo=-1, combustivel=-1 sempre).
    -- Join feito só por marca+ano+mes — sem tipo/combustivel porque Trends não os distingue.
    SELECT
        ft.marca_key,
        dtp.ano,
        dtp.mes,
        AVG(ft.valor_interesse) AS valor_interesse
    FROM {schema}.fact_trends ft
    JOIN {schema}.dim_tempo dtp ON ft.tempo_key = dtp.tempo_key
    WHERE ft.marca_key <> -1
    GROUP BY 1, 2, 3
),
hashtags_mensal AS (
    SELECT
        fh.marca_key, fh.tipo_key, fh.combustivel_key,
        EXTRACT(YEAR FROM (dtp.data + INTERVAL '3 days'))::int AS ano,
        EXTRACT(MONTH FROM (dtp.data + INTERVAL '3 days'))::int AS mes,
        SUM(fh.volume)            AS volume_hashtag,
        AVG(fh.variacao_semanal)  AS variacao_semanal
    FROM {schema}.fct_hashtag_volume fh
    JOIN {schema}.dim_tempo dtp ON fh.tempo_key = dtp.tempo_key
    WHERE fh.marca_key <> -1
    GROUP BY 1, 2, 3, 4, 5
),
forum_mensal AS (
    SELECT
        ffs.marca_key, ffs.tipo_key, ffs.combustivel_key,
        dtp.ano, dtp.mes,
        AVG(ffs.score_sentimento) AS sentimento_medio,
        SUM(ffs.n_mencoes)        AS n_mencoes
    FROM {schema}.fact_forum_sentiment ffs
    JOIN {schema}.dim_tempo dtp ON ffs.tempo_key = dtp.tempo_key
    WHERE ffs.marca_key <> -1
    GROUP BY 1, 2, 3, 4, 5
),
compradores AS (
    SELECT
        dv.marca_key, dv.tipo_key, dv.combustivel_key,
        dtp.ano, dtp.mes,
        COUNT(fv.venda_key)                                      AS n_vendas_t,
        AVG(dc.idade)                                            AS mean_age_buyers,
        AVG(CASE WHEN dc.genero = 'M' THEN 1.0 ELSE 0.0 END)    AS pct_masculino_compradores,
        AVG(fv.margem / NULLIF(fv.preco_venda, 0))               AS margem_pct_media
    FROM {schema}.fct_venda fv
    JOIN {schema}.dim_veiculo dv  ON fv.veiculo_key    = dv.veiculo_key
    JOIN {schema}.dim_tempo   dtp ON fv.tempo_venda_key = dtp.tempo_key
    JOIN {schema}.dim_cliente dc  ON fv.cliente_key    = dc.cliente_key
    WHERE dv.veiculo_key <> -1 AND fv.tempo_venda_key <> -1
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    sb.marca_key,
    sb.tipo_key,
    sb.combustivel_key,
    sb.localizacao_key,
    sb.ano,
    sb.mes,
    sb.tempo_key,
    sb.n_stock,
    sb.mean_valor_stock,
    -- valor_interesse pode ser NULL para marcas sem dados de Trends (tratado em build_feature_matrix)
    tm.valor_interesse,
    COALESCE(ht.volume_hashtag,    0) AS volume_hashtag,
    COALESCE(ht.variacao_semanal,  0) AS variacao_semanal_hashtag,
    fm.sentimento_medio,
    COALESCE(fm.n_mencoes,         0) AS n_mencoes,
    COALESCE(cp.n_vendas_t,        0) AS n_vendas_t,
    cp.mean_age_buyers,
    COALESCE(cp.pct_masculino_compradores, 0) AS pct_masculino_compradores,
    COALESCE(cp.margem_pct_media,  0) AS margem_pct_media
FROM stock_base sb
-- Trends: join só por marca+ano+mes (sem tipo/combustivel — Trends não os distingue)
LEFT JOIN trends_marca tm
       ON tm.marca_key = sb.marca_key
      AND tm.ano = sb.ano AND tm.mes = sb.mes
LEFT JOIN hashtags_mensal ht
       ON ht.marca_key = sb.marca_key AND ht.tipo_key = sb.tipo_key
      AND ht.combustivel_key = sb.combustivel_key
      AND ht.ano = sb.ano AND ht.mes = sb.mes
LEFT JOIN forum_mensal fm
       ON fm.marca_key = sb.marca_key AND fm.tipo_key = sb.tipo_key
      AND fm.combustivel_key = sb.combustivel_key
      AND fm.ano = sb.ano AND fm.mes = sb.mes
LEFT JOIN compradores cp
       ON cp.marca_key = sb.marca_key AND cp.tipo_key = sb.tipo_key
      AND cp.combustivel_key = sb.combustivel_key
      AND cp.ano = sb.ano AND cp.mes = sb.mes
ORDER BY sb.marca_key, sb.tipo_key, sb.combustivel_key, sb.localizacao_key, sb.ano, sb.mes
"""


def get_sql_target(schema):
    return f"""
WITH stock AS (
    SELECT
        dv.marca_key, dv.tipo_key, dv.combustivel_key,
        dtp.ano, dtp.mes,
        COUNT(*)                  AS n_stock,
        AVG(fim.valor_em_stock)   AS mean_valor_stock
    FROM {schema}.fct_inventario_mensal fim
    JOIN {schema}.dim_veiculo dv  ON fim.veiculo_key = dv.veiculo_key
    JOIN {schema}.dim_tempo dtp ON fim.tempo_key   = dtp.tempo_key
    WHERE dv.veiculo_key <> -1
      AND dv.marca_key   <> -1
    GROUP BY 1, 2, 3, 4, 5
),
vendas AS (
    SELECT
        dv.marca_key, dv.tipo_key, dv.combustivel_key,
        dtp.ano, dtp.mes,
        COUNT(*)        AS n_vendas,
        AVG(fv.margem)  AS mean_margem
    FROM {schema}.fct_venda fv
    JOIN {schema}.dim_veiculo dv  ON fv.veiculo_key    = dv.veiculo_key
    JOIN {schema}.dim_tempo dtp ON fv.tempo_venda_key = dtp.tempo_key
    WHERE dv.veiculo_key <> -1
      AND fv.tempo_venda_key <> -1
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    s.marca_key, s.tipo_key, s.combustivel_key,
    s.ano, s.mes,
    s.n_stock,
    COALESCE(v.n_vendas, 0)     AS n_vendas,
    COALESCE(v.mean_margem, 0)  AS mean_margem,
    CAST(COALESCE(v.n_vendas, 0) AS FLOAT)
        / NULLIF(s.n_stock, 0)  AS p_venda,
    CAST(COALESCE(v.n_vendas, 0) AS FLOAT)
        / NULLIF(s.n_stock, 0)
        * COALESCE(v.mean_margem, 0) AS expected_gain
FROM stock s
LEFT JOIN vendas v USING (marca_key, tipo_key, combustivel_key, ano, mes)
"""


def get_sql_demographics(schema):
    return f"""
SELECT 
    localizacao_key, 
    ano_referencia AS ano,
    mean_age AS mean_age_regiao,
    pct_masculino
FROM {schema}.dim_demografia_regional
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


def get_tempo_key(conn, schema: str, ano: int, mes: int) -> int | None:
    row = conn.execute(text(get_sql_tempo_key(schema)), {"ano": ano, "mes": mes}).fetchone()
    return int(row[0]) if row else None


def load_data(conn, schema: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    log.info("  A carregar features (vw_mart_prev_gain expandida)...")
    df_feat = pd.read_sql(text(get_sql_features_base(schema)), conn)
    log.info(f"    {len(df_feat)} linhas de features")

    log.info("  A carregar target (expected_gain)...")
    df_tgt = pd.read_sql(text(get_sql_target(schema)), conn)
    log.info(f"    {len(df_tgt)} linhas de target")

    log.info("  A carregar demographics...")
    df_dem = pd.read_sql(text(get_sql_demographics(schema)), conn)

    return df_feat, df_tgt, df_dem


def build_feature_matrix(
    df_feat: pd.DataFrame,
    df_tgt: pd.DataFrame,
    df_dem: pd.DataFrame,
) -> pd.DataFrame:
    df = df_feat.merge(
        df_tgt[["marca_key", "tipo_key", "combustivel_key",
                "ano", "mes", "expected_gain"]],   # n_stock removido: já vem de stock_base em df_feat
        on=["marca_key", "tipo_key", "combustivel_key", "ano", "mes"],
        how="left",
    )

    df = df.merge(
        df_dem[["localizacao_key", "ano", "mean_age_regiao", "pct_masculino"]],
        on=["localizacao_key", "ano"],
        how="left",
    )

    df["forum_sem_dados"] = df["sentimento_medio"].isna().astype(int)
    df["sentimento_medio"] = df["sentimento_medio"].fillna(0.5)

    # valor_interesse: NULL para marcas sem dados de Google Trends → imputar com 0
    df["valor_interesse"] = df["valor_interesse"].fillna(0)

    # pct_masculino: proporção regional (de df_dem); pct_masculino_compradores: proporção de compradores (de compradores CTE)
    df["pct_masculino"] = df["pct_masculino"].fillna(df["pct_masculino"].median())
    df["pct_masculino_compradores"] = df["pct_masculino_compradores"].fillna(df["pct_masculino_compradores"].median())

    df["age_match"] = 1.0 / (1.0 + (df["mean_age_buyers"] - df["mean_age_regiao"]).abs())
    df["age_match"] = df["age_match"].fillna(0.5)

    df["mes_sin"] = np.sin(2 * np.pi * df["mes"] / 12)
    df["mes_cos"] = np.cos(2 * np.pi * df["mes"] / 12)

    df = df.sort_values(SERIES_KEYS + ["ano", "mes"]).reset_index(drop=True)

    def add_lags(group: pd.DataFrame) -> pd.DataFrame:
        group["interesse_lag1"] = group["valor_interesse"].shift(1)
        group["interesse_lag2"] = group["valor_interesse"].shift(2)
        group["interesse_lag3"] = group["valor_interesse"].shift(3)
        group["interesse_rolling3"] = group["valor_interesse"].shift(1).rolling(3).mean()

        group["hashtag_lag1"] = group["volume_hashtag"].shift(1)

        group["sentimento_lag1"] = group["sentimento_medio"].shift(1)
        group["n_mencoes_lag1"] = group["n_mencoes"].shift(1)

        group["target_lag1"] = group["expected_gain"].shift(1)
        group["target_lag2"] = group["expected_gain"].shift(2)

        group["n_stock_lag1"] = group["n_stock"].shift(1)
        return group

    df = df.groupby(SERIES_KEYS, group_keys=False).apply(add_lags).reset_index(drop=True)

    def add_target_next(group: pd.DataFrame) -> pd.DataFrame:
        group["target_next"] = group["expected_gain"].shift(-1)
        return group

    df = df.groupby(SERIES_KEYS, group_keys=False).apply(add_target_next).reset_index(drop=True)

    df = df.dropna(subset=["target_next", "interesse_lag3", "target_lag2"]).reset_index(drop=True)

    log.info(f"    Dataset final: {len(df)} linhas")
    return df


def get_feature_cols(df: pd.DataFrame) -> list[str]:
    exclude = set(SERIES_KEYS + [
        "ano", "mes", "tempo_key",
        "expected_gain", "target_next",
        "mean_age_buyers", "mean_age_regiao",
        "variacao_semanal_hashtag",
        "n_vendas_t",
    ])
    return [c for c in df.columns if c not in exclude]


def walk_forward_xgboost(df: pd.DataFrame, feature_cols: list[str]) -> tuple[float, float, pd.DataFrame]:
    meses_unicos = sorted(df[["ano", "mes"]].drop_duplicates().apply(tuple, axis=1).tolist())
    if len(meses_unicos) <= N_TEST_FOLDS:
        log.warning(
            f"  Dataset com apenas {len(meses_unicos)} meses. "
            f"Reduzindo folds para {max(1, len(meses_unicos)-1)}."
        )

    n_test = min(N_TEST_FOLDS, len(meses_unicos) - 1)
    resultados = []

    for i in range(n_test, 0, -1):
        cutoff_tuple = meses_unicos[-(i + 1)]
        test_tuple = meses_unicos[-i]

        treino = df[
            (df["ano"] < cutoff_tuple[0]) |
            ((df["ano"] == cutoff_tuple[0]) & (df["mes"] <= cutoff_tuple[1]))
        ]
        teste = df[(df["ano"] == test_tuple[0]) & (df["mes"] == test_tuple[1])]

        if treino.empty or teste.empty:
            continue

        modelo = XGBRegressor(
            n_estimators=300,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            verbosity=0,
        )
        modelo.fit(treino[feature_cols], treino["target_next"], eval_set=[(teste[feature_cols], teste["target_next"])], verbose=False)
        previsoes = modelo.predict(teste[feature_cols])

        fold_mae = mean_absolute_error(teste["target_next"], previsoes)
        fold_rmse = np.sqrt(mean_squared_error(teste["target_next"], previsoes))

        resultados.append({
            "mes_teste": f"{test_tuple[0]}-{test_tuple[1]:02d}",
            "n_teste": len(teste),
            "mae": round(fold_mae, 4),
            "rmse": round(fold_rmse, 4),
        })
        log.info(
            f"  Fold {test_tuple[0]}-{test_tuple[1]:02d} | "
            f"n={len(teste):3d} | MAE={fold_mae:.4f} | RMSE={fold_rmse:.4f}"
        )

    df_res = pd.DataFrame(resultados)
    if df_res.empty:
        return None, None, df_res

    mae_global = float(df_res["mae"].mean())
    rmse_global = float(df_res["rmse"].mean())
    log.info(f"\n  Walk-forward: MAE médio={mae_global:.4f}  RMSE médio={rmse_global:.4f}")
    return mae_global, rmse_global, df_res


def train_final_model(df: pd.DataFrame, feature_cols: list[str]) -> XGBRegressor:
    modelo = XGBRegressor(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbosity=0,
    )
    modelo.fit(df[feature_cols], df["target_next"])

    importances = pd.Series(modelo.feature_importances_, index=feature_cols).sort_values(ascending=False)
    log.info("\n  Top 15 features por importância:")
    for feat, imp in importances.head(15).items():
        log.info(f"    {feat:<35} {imp:.4f}")

    return modelo


def build_prediction_rows(df: pd.DataFrame) -> pd.DataFrame:
    # Chave escalar ano*100+mes; transform("max") é vectorizado e não entrega
    # Series multi-coluna ao lambda — evita o KeyError anterior.
    ym = df["ano"] * 100 + df["mes"]
    ym_max = df.groupby(SERIES_KEYS, group_keys=False).apply(
        lambda g: pd.Series((g["ano"] * 100 + g["mes"]).max(), index=g.index)
    )
    return df[ym == ym_max].copy()


def save_model_run(
    conn,
    schema: str,
    mae: float | None,
    rmse: float | None,
    train_cutoff: date,
    feature_cols: list[str],
) -> int:
    features_info = {
        "features": feature_cols,
        "n_test_folds": N_TEST_FOLDS,
        "xgb_params": {"n_estimators": 300, "max_depth": 5, "lr": 0.05},
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
            "mae": round(mae, 4) if mae else None,
            "notas": (
                f"XGBoost | walk-forward {N_TEST_FOLDS} folds | "
                f"target: expected_gain(t+1) | RMSE={round(rmse,4) if rmse else 'N/A'} | "
                f"{len(feature_cols)} features"
            ),
        },
    ).fetchone()
    return int(row[0])


def save_predictions(
    conn,
    schema: str,
    df_pred: pd.DataFrame,
    modelo: XGBRegressor,
    feature_cols: list[str],
    model_run_id: int,
) -> int:
    previsoes = modelo.predict(df_pred[feature_cols])
    inserted = 0

    for idx, (_, row) in enumerate(df_pred.iterrows()):
        ano_ref = int(row["ano"])
        mes_ref = int(row["mes"])
        ano_alvo, mes_alvo = next_month(ano_ref, mes_ref)

        tempo_ref_key = get_tempo_key(conn, schema, ano_ref, mes_ref)
        tempo_alvo_key = get_tempo_key(conn, schema, ano_alvo, mes_alvo)
        if tempo_ref_key is None or tempo_alvo_key is None:
            log.warning(f"  tempo_key não encontrado para {ano_alvo}-{mes_alvo}. A saltar.")
            continue

        conn.execute(
            text(f"""
                INSERT INTO {schema}.fact_previsoes_xgboost
                    (model_run_id, tempo_alvo_key, tempo_ref_key,
                     marca_key, tipo_key, combustivel_key, localizacao_key,
                     expected_gain_previsto)
                VALUES
                    (:run_id, :alvo, :ref,
                     :marca, :tipo, :comb, :loc,
                     :eg)
                ON CONFLICT
                    (model_run_id, tempo_alvo_key, marca_key, tipo_key, combustivel_key, localizacao_key)
                DO UPDATE SET
                    expected_gain_previsto = EXCLUDED.expected_gain_previsto
            """),
            {
                "run_id": model_run_id,
                "alvo": tempo_alvo_key,
                "ref": tempo_ref_key,
                "marca": int(row["marca_key"]),
                "tipo": int(row["tipo_key"]),
                "comb": int(row["combustivel_key"]),
                "loc": int(row["localizacao_key"]),
                "eg": round(float(previsoes[idx]), 4),
            },
        )
        inserted += 1
    return inserted


def run_xgboost(schema: str = "auto_escala_dw") -> None:
    log.info("=" * 60)
    log.info(f"AUTO ESCALA — XGBoost Expected Gain (SCHEMA={schema})")
    log.info("=" * 60)

    with engine.connect() as conn:
        df_feat, df_tgt, df_dem = load_data(conn, schema)

    if df_feat.empty or df_tgt.empty:
        log.error("Dados insuficientes para treinar XGBoost. A terminar.")
        return

    log.info("\nA construir feature matrix...")
    df = build_feature_matrix(df_feat, df_tgt, df_dem)

    if len(df) < N_TEST_FOLDS * 5:
        log.error(f"Dataset com apenas {len(df)} linhas após preparação. A terminar.")
        return

    feature_cols = get_feature_cols(df)
    log.info(f"  Features finais ({len(feature_cols)}): {feature_cols}")

    log.info("\nWalk-forward validation:")
    mae_global, rmse_global, df_wf = walk_forward_xgboost(df, feature_cols)

    log.info("\nTreino final (todos os dados):")
    modelo = train_final_model(df, feature_cols)

    df_pred = build_prediction_rows(df)
    log.info(f"\n  {len(df_pred)} séries para prever (t+1)")

    max_ano = int(df_pred["ano"].max())
    max_mes = int(df_pred.loc[df_pred["ano"] == max_ano, "mes"].max())
    train_cutoff = date(max_ano, max_mes, 1)

    log.info(f"  Train cutoff: {train_cutoff}")
    log.info(f"  MAE global walk-forward: {mae_global:.4f}" if mae_global else "  MAE: N/A")

    with engine.begin() as conn:
        model_run_id = save_model_run(conn, schema, mae_global, rmse_global, train_cutoff, feature_cols)
        n_inserted = save_predictions(conn, schema, df_pred, modelo, feature_cols, model_run_id)

    log.info(f"\n  ✓ model_run_id = {model_run_id}")
    log.info(f"  ✓ {n_inserted} previsões inseridas em fact_previsoes_xgboost")
    log.info("XGBoost concluído.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Modelo XGBoost")
    parser.add_argument("--schema", default="auto_escala_dw", help="Esquema da Base de Dados")
    args = parser.parse_args()

    run_xgboost(schema=args.schema)