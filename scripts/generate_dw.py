"""
generate_dw.py
---------------------------------------------------------------
Cria o schema do Data Warehouse (Star Schema com dimensões conformadas)
na base de dados PostgreSQL auto_escala.
Inclui infraestrutura completa: dicionário, auditoria, qualidade e demografia.
---------------------------------------------------------------
"""

import os
import sys
from sqlalchemy import create_engine, text
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR / "scripts"))

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
DW_URL   = f"postgresql+psycopg2://ae_user:ae_pass_2026@{_PG_HOST}:{_PG_PORT}/auto_escala"
dw_engine = create_engine(DW_URL, echo=False)

CREATE_DW_SQL = """
DROP SCHEMA IF EXISTS auto_escala_dw CASCADE;
CREATE SCHEMA auto_escala_dw;
SET search_path TO auto_escala_dw;

-- =============================================================================
-- DIMENSÕES
-- =============================================================================

CREATE TABLE dim_localizacao (
    localizacao_key SERIAL PRIMARY KEY,
    distrito        VARCHAR(100) NOT NULL,
    pais            VARCHAR(100) DEFAULT 'Portugal',
    UNIQUE (distrito)
);

CREATE TABLE dim_stand (
    stand_key       SERIAL PRIMARY KEY,
    nome_stand      VARCHAR(100) NOT NULL UNIQUE,
    localizacao_key INTEGER REFERENCES dim_localizacao(localizacao_key)
);

CREATE TABLE dim_tempo (
    tempo_key   SERIAL PRIMARY KEY,
    data        DATE    NOT NULL UNIQUE,
    ano         INTEGER NOT NULL,
    mes         INTEGER NOT NULL,
    dia         INTEGER NOT NULL,
    trimestre   INTEGER NOT NULL,
    nome_mes    VARCHAR(20),
    semana_ano  INTEGER
);

CREATE TABLE dim_fonte (
    fonte_key   SERIAL PRIMARY KEY,
    nome_fonte  VARCHAR(100) NOT NULL UNIQUE,
    tipo_fonte  VARCHAR(50)  NOT NULL,
    descricao   TEXT
);

CREATE TABLE dim_marca (
    marca_key   SERIAL PRIMARY KEY,
    marca       VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_tipo (
    tipo_key        SERIAL PRIMARY KEY,
    tipo_automovel  VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_combustivel (
    combustivel_key SERIAL PRIMARY KEY,
    combustivel     VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_modelo (
    modelo_key     SERIAL PRIMARY KEY,
    modelo         VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_veiculo (
    veiculo_key    SERIAL PRIMARY KEY,
    id_viatura     VARCHAR(50) UNIQUE,
    matricula      VARCHAR(20),
    marca_key      INTEGER NOT NULL REFERENCES dim_marca(marca_key),
    modelo_key     INTEGER NOT NULL REFERENCES dim_modelo(modelo_key),
    tipo_key       INTEGER NOT NULL REFERENCES dim_tipo(tipo_key),
    combustivel_key INTEGER NOT NULL REFERENCES dim_combustivel(combustivel_key),
    num_lugares    INTEGER,
    ano_viatura    INTEGER
);

CREATE TABLE dim_cliente (
    cliente_key     SERIAL PRIMARY KEY,
    nif             VARCHAR(20),
    nome            VARCHAR(100),
    idade           INTEGER,
    faixa_etaria    VARCHAR(20),
    genero          VARCHAR(20),
    localizacao_key INTEGER REFERENCES dim_localizacao(localizacao_key),
    is_ativo        BOOLEAN DEFAULT TRUE,
    data_inicio     DATE NOT NULL,
    data_fim        DATE DEFAULT '9999-12-31'
);

CREATE TABLE dim_demografia_regional (
    demografia_key  SERIAL PRIMARY KEY,
    localizacao_key INTEGER NOT NULL REFERENCES dim_localizacao(localizacao_key),
    ano_referencia  INTEGER NOT NULL,
    populacao_total INTEGER,
    mean_age        NUMERIC(5,2),   -- Substitui as 5 colunas de faixas etárias
    pct_masculino   NUMERIC(5,2),
    pct_feminino    NUMERIC(5,2),
    UNIQUE (localizacao_key, ano_referencia)
);

CREATE TABLE dim_model_run (
    model_run_id    SERIAL PRIMARY KEY,
    run_timestamp   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    model_name      VARCHAR(100) NOT NULL,
    model_version   VARCHAR(50),
    features_hash   VARCHAR(64),
    train_cutoff    DATE NOT NULL,
    mae             NUMERIC(10,4),
    mape            NUMERIC(10,4),
    notas           TEXT
);

-- =============================================================================
-- DICIONÁRIO DE NORMALIZAÇÃO
-- =============================================================================
CREATE TABLE dim_dicionario_veiculo (
    dicionario_key    SERIAL PRIMARY KEY,
    campo             VARCHAR(50)  NOT NULL,
    valor_original    VARCHAR(255) NOT NULL,
    valor_normalizado VARCHAR(255) NOT NULL,
    ativo             BOOLEAN      DEFAULT TRUE,
    data_criacao      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (campo, valor_original)
);

-- =============================================================================
-- FACTOS
-- =============================================================================

CREATE TABLE fact_venda (
    venda_key         SERIAL PRIMARY KEY,
    veiculo_key       INTEGER NOT NULL REFERENCES dim_veiculo(veiculo_key),
    stand_key         INTEGER NOT NULL REFERENCES dim_stand(stand_key),
    tempo_entrada_key INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    tempo_venda_key   INTEGER REFERENCES dim_tempo(tempo_key),
    cliente_key       INTEGER REFERENCES dim_cliente(cliente_key),
    quilometragem     INTEGER,
    preco_aquisicao   NUMERIC(12,2),
    preco_venda       NUMERIC(12,2),
    margem            NUMERIC(12,2),
    dias_em_stock     INTEGER,
    UNIQUE (veiculo_key, stand_key, tempo_entrada_key)
);

CREATE TABLE fact_inventario_mensal (
    inventario_key  SERIAL PRIMARY KEY,
    tempo_key       INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    stand_key       INTEGER NOT NULL REFERENCES dim_stand(stand_key),
    veiculo_key     INTEGER NOT NULL REFERENCES dim_veiculo(veiculo_key),
    valor_em_stock  NUMERIC(12,2),
    dias_em_parque  INTEGER,
    UNIQUE (tempo_key, stand_key, veiculo_key)
);

CREATE TABLE fact_trends (
    tendencia_key       SERIAL PRIMARY KEY,
    tempo_key           INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    marca_key           INTEGER NOT NULL REFERENCES dim_marca(marca_key),
    modelo_key          INTEGER NOT NULL REFERENCES dim_modelo(modelo_key),
    tipo_key            INTEGER NOT NULL REFERENCES dim_tipo(tipo_key),
    combustivel_key     INTEGER NOT NULL REFERENCES dim_combustivel(combustivel_key),
    localizacao_key     INTEGER NOT NULL REFERENCES dim_localizacao(localizacao_key),
    valor_interesse     INTEGER,
    crescimento_mom_pct NUMERIC(10,4),
    trending_flag       BOOLEAN DEFAULT FALSE,
    UNIQUE (tempo_key, marca_key, modelo_key, tipo_key, combustivel_key, localizacao_key)
);

CREATE TABLE fact_forum_sentiment (
    sentimento_key      SERIAL PRIMARY KEY,
    tempo_key           INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    marca_key           INTEGER NOT NULL REFERENCES dim_marca(marca_key),
    modelo_key          INTEGER NOT NULL REFERENCES dim_modelo(modelo_key),
    tipo_key            INTEGER NOT NULL REFERENCES dim_tipo(tipo_key),
    combustivel_key     INTEGER NOT NULL REFERENCES dim_combustivel(combustivel_key),
    n_mencoes           INTEGER,
    score_sentimento    NUMERIC(5,4),
    delta_sentimento    NUMERIC(5,4),
    UNIQUE (tempo_key, marca_key, modelo_key, tipo_key, combustivel_key)
);

CREATE TABLE fact_hashtag_volume (
    hashtag_volume_key SERIAL PRIMARY KEY,
    tempo_key          INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    fonte_key          INTEGER NOT NULL REFERENCES dim_fonte(fonte_key),
    marca_key          INTEGER NOT NULL REFERENCES dim_marca(marca_key),
    modelo_key         INTEGER NOT NULL REFERENCES dim_modelo(modelo_key),
    tipo_key           INTEGER NOT NULL REFERENCES dim_tipo(tipo_key),
    combustivel_key    INTEGER NOT NULL REFERENCES dim_combustivel(combustivel_key),
    volume             INTEGER NOT NULL DEFAULT 0,
    posts_instagram    INTEGER DEFAULT 0,
    posts_twitter      INTEGER DEFAULT 0,
    posts_youtube      INTEGER DEFAULT 0,
    variacao_semanal   NUMERIC(10,4),
    UNIQUE (tempo_key, fonte_key, marca_key, modelo_key, tipo_key, combustivel_key)
);

CREATE TABLE fact_previsoes_sarima (
    previsao_sarima_key SERIAL PRIMARY KEY,
    model_run_id        INTEGER NOT NULL REFERENCES dim_model_run(model_run_id),
    tempo_alvo_key      INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    tempo_ref_key       INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    marca_key           INTEGER NOT NULL REFERENCES dim_marca(marca_key),
    tipo_key            INTEGER NOT NULL REFERENCES dim_tipo(tipo_key),
    combustivel_key     INTEGER NOT NULL REFERENCES dim_combustivel(combustivel_key),
    localizacao_key     INTEGER NOT NULL REFERENCES dim_localizacao(localizacao_key),
    metrica             VARCHAR(50) NOT NULL,
    valor_previsto      NUMERIC(12,4),
    yhat_lower          NUMERIC(12,4),
    yhat_upper          NUMERIC(12,4),
    UNIQUE (model_run_id, tempo_alvo_key, marca_key, tipo_key, combustivel_key, localizacao_key, metrica)
);

CREATE TABLE fact_previsoes_xgboost (
    previsao_xgb_key       SERIAL PRIMARY KEY,
    model_run_id           INTEGER NOT NULL REFERENCES dim_model_run(model_run_id),
    tempo_alvo_key         INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    tempo_ref_key          INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    marca_key              INTEGER NOT NULL REFERENCES dim_marca(marca_key),
    tipo_key               INTEGER NOT NULL REFERENCES dim_tipo(tipo_key),
    combustivel_key        INTEGER NOT NULL REFERENCES dim_combustivel(combustivel_key),
    localizacao_key        INTEGER NOT NULL REFERENCES dim_localizacao(localizacao_key),
    expected_gain_previsto NUMERIC(12,4),
    UNIQUE (model_run_id, tempo_alvo_key, marca_key, tipo_key, combustivel_key, localizacao_key)
);

-- =============================================================================
-- ADMINISTRAÇÃO E AUDITORIA
-- =============================================================================

CREATE TABLE data_quality_log (
    log_id              SERIAL PRIMARY KEY,
    fonte               VARCHAR(50),
    data_run            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_registos      INTEGER,
    registos_ok         INTEGER,
    registos_quarentena INTEGER,
    taxa_quarentena_pct NUMERIC(6,2),
    n_linhas_duplicadas INTEGER,
    n_valores_ausentes  INTEGER,
    campo_mais_nulo     VARCHAR(100),
    notas               TEXT
);

CREATE TABLE pipeline_control (
    pipeline_id        SERIAL PRIMARY KEY,
    nome_pipeline      VARCHAR(100) NOT NULL,
    camada             VARCHAR(50)  NOT NULL,
    estado             VARCHAR(50)  NOT NULL,
    data_inicio        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_fim           TIMESTAMP,
    linhas_lidas       INTEGER DEFAULT 0,
    linhas_processadas INTEGER DEFAULT 0,
    linhas_rejeitadas  INTEGER DEFAULT 0,
    mensagem_erro      TEXT
);

CREATE TABLE IF NOT EXISTS auto_escala_dw.audit_log_dimensions (
    log_id          SERIAL PRIMARY KEY,
    tabela_afetada  VARCHAR(50)  NOT NULL,
    operacao        VARCHAR(10)  NOT NULL,
    registo_antigo  JSONB,
    registo_novo    JSONB,
    data_alteracao  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION auto_escala_dw.log_dimension_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO auto_escala_dw.audit_log_dimensions (tabela_afetada, operacao, registo_novo)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' AND OLD IS DISTINCT FROM NEW THEN
        INSERT INTO auto_escala_dw.audit_log_dimensions (tabela_afetada, operacao, registo_antigo, registo_novo)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD)::jsonb, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO auto_escala_dw.audit_log_dimensions (tabela_afetada, operacao, registo_antigo)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD)::jsonb);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_dim_localizacao AFTER INSERT OR UPDATE OR DELETE ON auto_escala_dw.dim_localizacao FOR EACH ROW EXECUTE FUNCTION auto_escala_dw.log_dimension_changes();
CREATE TRIGGER trg_audit_dim_stand AFTER INSERT OR UPDATE OR DELETE ON auto_escala_dw.dim_stand FOR EACH ROW EXECUTE FUNCTION auto_escala_dw.log_dimension_changes();
CREATE TRIGGER trg_audit_dim_marca AFTER INSERT OR UPDATE OR DELETE ON auto_escala_dw.dim_marca FOR EACH ROW EXECUTE FUNCTION auto_escala_dw.log_dimension_changes();
CREATE TRIGGER trg_audit_dim_modelo AFTER INSERT OR UPDATE OR DELETE ON auto_escala_dw.dim_modelo FOR EACH ROW EXECUTE FUNCTION auto_escala_dw.log_dimension_changes();
CREATE TRIGGER trg_audit_dim_tipo AFTER INSERT OR UPDATE OR DELETE ON auto_escala_dw.dim_tipo FOR EACH ROW EXECUTE FUNCTION auto_escala_dw.log_dimension_changes();
CREATE TRIGGER trg_audit_dim_combustivel AFTER INSERT OR UPDATE OR DELETE ON auto_escala_dw.dim_combustivel FOR EACH ROW EXECUTE FUNCTION auto_escala_dw.log_dimension_changes();
CREATE TRIGGER trg_audit_dim_veiculo AFTER INSERT OR UPDATE OR DELETE ON auto_escala_dw.dim_veiculo FOR EACH ROW EXECUTE FUNCTION auto_escala_dw.log_dimension_changes();

-- =============================================================================
-- VIEWS DE NEGÓCIO
-- =============================================================================

CREATE OR REPLACE VIEW vw_mart_compras AS
WITH 
forum_mensal AS (
    SELECT 
        marca_key, modelo_key, tipo_key, combustivel_key, 
        dtp.ano, dtp.mes,
        AVG(score_sentimento) AS score_sentimento_modelo,
        SUM(n_mencoes) AS mencoes_modelo
    FROM fact_forum_sentiment ffs
    JOIN dim_tempo dtp ON ffs.tempo_key = dtp.tempo_key
    GROUP BY 1, 2, 3, 4, 5, 6
),
hashtags_mensal AS (
    SELECT 
        marca_key, modelo_key, tipo_key, combustivel_key, 
        dtp.ano, dtp.mes,
        SUM(volume) AS volume_hashtag_modelo,
        AVG(variacao_semanal) AS variacao_semanal_hashtag
    FROM fact_hashtag_volume fh
    JOIN dim_tempo dtp ON fh.tempo_key = dtp.tempo_key
    GROUP BY 1, 2, 3, 4, 5, 6
),
sarima_hashtags AS (
    SELECT
        fh.marca_key,
        fh.tipo_key,
        fh.combustivel_key,
        0 AS localizacao_key,
        dtp.ano,
        dtp.mes,
        SUM(fh.volume) AS ml_total_posts
    FROM fact_hashtag_volume fh
    JOIN dim_tempo dtp ON fh.tempo_key = dtp.tempo_key
    WHERE NOT (fh.marca_key = -1 AND fh.tipo_key = -1 AND fh.combustivel_key = -1)
    GROUP BY 1, 2, 3, 4, 5, 6
),
sarima_forum AS (
    SELECT
        ffs.marca_key,
        ffs.tipo_key,
        ffs.combustivel_key,
        0 AS localizacao_key,
        dtp.ano,
        dtp.mes,
        SUM(ffs.n_mencoes) AS ml_forum_mencoes,
        AVG(ffs.score_sentimento) AS ml_analise_sentimento
    FROM fact_forum_sentiment ffs
    JOIN dim_tempo dtp ON ffs.tempo_key = dtp.tempo_key
    WHERE score_sentimento IS NOT NULL
      AND NOT (ffs.marca_key = -1 AND ffs.tipo_key = -1 AND ffs.combustivel_key = -1)
    GROUP BY 1, 2, 3, 4, 5, 6
),
xgb_hashtags AS (
    SELECT
        fh.marca_key,
        fh.tipo_key,
        fh.combustivel_key,
        0 AS localizacao_key,
        dtp.ano,
        dtp.mes,
        SUM(fh.volume) AS ml_volume_hashtag,
        AVG(fh.variacao_semanal) AS ml_variacao_semanal_media
    FROM fact_hashtag_volume fh
    JOIN dim_tempo dtp ON fh.tempo_key = dtp.tempo_key
    WHERE NOT (fh.marca_key = -1 AND fh.tipo_key = -1 AND fh.combustivel_key = -1)
    GROUP BY 1, 2, 3, 4, 5, 6
),
xgb_vendas AS (
    SELECT
        dv.marca_key,
        dv.tipo_key,
        dv.combustivel_key,
        ds.localizacao_key,
        dtp.ano,
        dtp.mes,
        AVG(fv.margem / NULLIF(fv.preco_venda, 0)) AS ml_margem_pct_media
    FROM fact_venda fv
    JOIN dim_veiculo dv ON fv.veiculo_key = dv.veiculo_key
    JOIN dim_stand ds ON fv.stand_key = ds.stand_key
    JOIN dim_tempo dtp ON fv.tempo_venda_key = dtp.tempo_key
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
    COALESCE(
        NULLIF(dm.marca, 'Unknown'),
        (
            SELECT dm2.marca 
            FROM dim_veiculo dv 
            JOIN dim_marca dm2 ON dv.marca_key = dm2.marca_key 
            WHERE dv.modelo_key = ft.modelo_key 
              AND dv.modelo_key != -1 
            LIMIT 1
        ),
        'Unknown'
    )                                               AS marca,
    dmo.modelo,
    dt.tipo_automovel,
    dc.combustivel,
    dl.distrito,
    dtp.ano,
    dtp.mes,

    -- Trends
    ft.valor_interesse,
    ft.crescimento_mom_pct                          AS tendencia_crescimento,
    ft.trending_flag,

    -- Social e Sentimento (Agora agregado ao Mês)
    COALESCE(fm.score_sentimento_modelo, 0)         AS score_sentimento_modelo,
    COALESCE(fm.mencoes_modelo, 0)                  AS mencoes_modelo,
    COALESCE(hm.volume_hashtag_modelo, 0)           AS volume_hashtag_modelo,
    COALESCE(hm.variacao_semanal_hashtag, 0)        AS variacao_semanal_hashtag,

    -- Previsão M1
    COALESCE(pm1.valor_previsto, 0)                 AS previsao_interesse_prox_mes,
    pm1.yhat_lower,
    pm1.yhat_upper,
    COALESCE(m1h.ml_total_posts, 0)                 AS ml_total_posts,
    COALESCE(m1f.ml_forum_mencoes, 0)               AS ml_forum_mencoes,
    COALESCE(m1f.ml_analise_sentimento, 0)          AS ml_analise_sentimento,
    COALESCE(m3h.ml_volume_hashtag, 0)              AS ml_volume_hashtag,
    COALESCE(m3h.ml_variacao_semanal_media, 0)       AS ml_variacao_semanal_media,
    COALESCE(m3v.ml_margem_pct_media, 0)            AS ml_margem_pct_media,

    -- Stock e Histórico
    (
        SELECT AVG(fv.dias_em_stock)
        FROM fact_venda fv
        JOIN dim_veiculo dv ON fv.veiculo_key = dv.veiculo_key
        WHERE (dv.marca_key = ft.marca_key OR ft.marca_key = -1)
          AND (dv.modelo_key = ft.modelo_key OR ft.modelo_key = -1)
          AND (dv.tipo_key = ft.tipo_key OR ft.tipo_key = -1)
          AND (dv.combustivel_key = ft.combustivel_key OR ft.combustivel_key = -1)
    )                                               AS media_dias_stock_historico,

    (
        SELECT COUNT(*)
        FROM fact_inventario_mensal fim
        JOIN dim_veiculo dv ON fim.veiculo_key = dv.veiculo_key
        JOIN dim_tempo   dtp2 ON fim.tempo_key = dtp2.tempo_key
        WHERE (dv.marca_key = ft.marca_key OR ft.marca_key = -1)
          AND (dv.modelo_key = ft.modelo_key OR ft.modelo_key = -1)
          AND (dv.tipo_key = ft.tipo_key OR ft.tipo_key = -1)
          AND (dv.combustivel_key = ft.combustivel_key OR ft.combustivel_key = -1)
          AND (dtp2.ano, dtp2.mes) = (
              SELECT dtp3.ano, dtp3.mes
              FROM fact_inventario_mensal fim3
              JOIN dim_tempo dtp3 ON fim3.tempo_key = dtp3.tempo_key
              ORDER BY dtp3.ano DESC, dtp3.mes DESC LIMIT 1
          )
    )                                               AS unidades_em_stock_atual

FROM fact_trends ft
JOIN dim_marca       dm  ON ft.marca_key       = dm.marca_key
JOIN dim_modelo      dmo ON ft.modelo_key      = dmo.modelo_key
JOIN dim_tipo        dt  ON ft.tipo_key        = dt.tipo_key
JOIN dim_combustivel dc  ON ft.combustivel_key = dc.combustivel_key
JOIN dim_localizacao dl  ON ft.localizacao_key = dl.localizacao_key
JOIN dim_tempo       dtp ON ft.tempo_key       = dtp.tempo_key

-- Junção pelas subqueries mensais
LEFT JOIN forum_mensal fm
       ON fm.marca_key       = ft.marca_key
      AND fm.modelo_key      = ft.modelo_key
      AND fm.tipo_key        = ft.tipo_key
      AND fm.combustivel_key = ft.combustivel_key
      AND fm.ano             = dtp.ano
      AND fm.mes             = dtp.mes

LEFT JOIN hashtags_mensal hm
       ON hm.marca_key       = ft.marca_key
      AND hm.modelo_key      = ft.modelo_key
      AND hm.tipo_key        = ft.tipo_key
      AND hm.combustivel_key = ft.combustivel_key
      AND hm.ano             = dtp.ano
      AND hm.mes             = dtp.mes

LEFT JOIN sarima_hashtags m1h
       ON m1h.marca_key       = ft.marca_key
      AND m1h.tipo_key        = ft.tipo_key
      AND m1h.combustivel_key = ft.combustivel_key
      AND m1h.ano             = dtp.ano
      AND m1h.mes             = dtp.mes

LEFT JOIN sarima_forum m1f
       ON m1f.marca_key       = ft.marca_key
      AND m1f.tipo_key        = ft.tipo_key
      AND m1f.combustivel_key = ft.combustivel_key
      AND m1f.ano             = dtp.ano
      AND m1f.mes             = dtp.mes

LEFT JOIN xgb_hashtags m3h
       ON m3h.marca_key       = ft.marca_key
      AND m3h.tipo_key        = ft.tipo_key
      AND m3h.combustivel_key = ft.combustivel_key
      AND m3h.ano             = dtp.ano
      AND m3h.mes             = dtp.mes

LEFT JOIN xgb_vendas m3v
       ON m3v.marca_key       = ft.marca_key
      AND m3v.tipo_key        = ft.tipo_key
      AND m3v.combustivel_key = ft.combustivel_key
      AND m3v.localizacao_key = ft.localizacao_key
      AND m3v.ano             = dtp.ano
      AND m3v.mes             = dtp.mes

LEFT JOIN fact_previsoes_sarima pm1
       ON pm1.marca_key       = ft.marca_key
      AND pm1.tipo_key        = ft.tipo_key
      AND pm1.combustivel_key = ft.combustivel_key
      AND pm1.tempo_ref_key   = ft.tempo_key
      AND pm1.metrica         = 'valor_interesse';

CREATE OR REPLACE VIEW vw_mart_stock AS
SELECT
    ds.nome_stand,
    dm.marca,
    dmo.modelo,
    dti.tipo_automovel,
    dc.combustivel,
    dl.distrito,
    dv.matricula,
    dv.ano_viatura,
    fim.dias_em_parque,
    fim.valor_em_stock,
    CASE 
        WHEN fim.dias_em_parque > 90 THEN 'Crítico (>90d)'
        WHEN fim.dias_em_parque > 60 THEN 'Alerta (>60d)'
        ELSE 'Normal'
    END AS status_envelhecimento,
    dt.data AS data_referencia
FROM fact_inventario_mensal fim
JOIN dim_veiculo dv ON fim.veiculo_key = dv.veiculo_key
JOIN dim_marca dm ON dv.marca_key = dm.marca_key
JOIN dim_modelo dmo ON dv.modelo_key = dmo.modelo_key
JOIN dim_tipo dti ON dv.tipo_key = dti.tipo_key
JOIN dim_combustivel dc ON dv.combustivel_key = dc.combustivel_key
JOIN dim_stand ds ON fim.stand_key = ds.stand_key
JOIN dim_localizacao dl ON ds.localizacao_key = dl.localizacao_key
JOIN dim_tempo dt ON fim.tempo_key = dt.tempo_key;

CREATE OR REPLACE VIEW vw_mart_direcao AS
WITH forum_mensal AS (
    -- 1. Agrega o sentimento por Marca, Ano e Mês (ignora o dia)
    SELECT 
        ffs.marca_key, 
        dtp.ano, 
        dtp.mes, 
        AVG(ffs.score_sentimento) AS sentimento_medio,
        SUM(ffs.n_mencoes) AS total_mencoes
    FROM fact_forum_sentiment ffs
    JOIN dim_tempo dtp ON ffs.tempo_key = dtp.tempo_key
    GROUP BY 1, 2, 3
),
vendas_mensais AS (
    SELECT 
        dv.marca_key,
        dt.ano,
        dt.mes,
        COUNT(fv.venda_key) AS total_vendas,
        SUM(fv.preco_venda) AS faturacao,
        SUM(fv.margem) AS lucro_total,
        AVG(fv.margem / NULLIF(fv.preco_venda, 0)) * 100 AS margem_media_pct
    FROM fact_venda fv
    JOIN dim_veiculo dv ON fv.veiculo_key = dv.veiculo_key
    JOIN dim_tempo dt ON fv.tempo_venda_key = dt.tempo_key
    GROUP BY 1, 2, 3
)
SELECT
    dm.marca,
    dt.ano,
    dt.mes,
    COALESCE(vm.total_vendas, 0) AS total_vendas,
    COALESCE(vm.faturacao, 0) AS faturacao,
    COALESCE(vm.lucro_total, 0) AS lucro_total,
    COALESCE(vm.margem_media_pct, 0) AS margem_media_pct,
    COALESCE(fm.sentimento_medio, 0) AS sentimento_forum,
    COALESCE(fm.total_mencoes, 0) AS mencoes_forum,
    COALESCE(fm.sentimento_medio, 0) AS sentimento_forum_medio
FROM fact_venda fv
JOIN dim_veiculo dv ON fv.veiculo_key = dv.veiculo_key
JOIN dim_marca dm ON dv.marca_key = dm.marca_key
JOIN dim_tempo dt ON fv.tempo_venda_key = dt.tempo_key
-- 2. Faz o JOIN usando o Ano e o Mês em vez da tempo_key (dia)
LEFT JOIN vendas_mensais vm
       ON vm.marca_key = dm.marca_key
      AND vm.ano = dt.ano
      AND vm.mes = dt.mes
LEFT JOIN forum_mensal fm 
       ON fm.marca_key = dm.marca_key 
      AND fm.ano = dt.ano 
      AND fm.mes = dt.mes
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;

CREATE OR REPLACE VIEW vw_mart_prev_tendencias AS
WITH
-- Apenas combinações (marca, tipo, combustivel) com valores reais (sem Unknown/-1)
trends_mensal AS (
    SELECT
        ft.marca_key, ft.tipo_key, ft.combustivel_key, ft.localizacao_key,
        dtp.ano, dtp.mes,
        AVG(ft.valor_interesse) AS valor_interesse
    FROM auto_escala_dw.fact_trends ft
    JOIN auto_escala_dw.dim_tempo dtp ON ft.tempo_key = dtp.tempo_key
    WHERE NOT (ft.marca_key = -1 AND ft.tipo_key = -1 AND ft.combustivel_key = -1)
    GROUP BY 1, 2, 3, 4, 5, 6
),
social_mensal AS (
    SELECT
        fh.marca_key,
        fh.tipo_key,
        fh.combustivel_key,
        dtp.ano,
        dtp.mes,
        SUM(fh.volume) AS total_posts
    FROM auto_escala_dw.fact_hashtag_volume fh
    JOIN auto_escala_dw.dim_tempo dtp ON fh.tempo_key = dtp.tempo_key
    WHERE NOT (fh.marca_key = -1 AND fh.tipo_key = -1 AND fh.combustivel_key = -1)
    GROUP BY 1, 2, 3, 4, 5
),
forum_mensal AS (
    SELECT
        ffs.marca_key,
        ffs.tipo_key,
        ffs.combustivel_key,
        dtp.ano,
        dtp.mes,
        SUM(ffs.n_mencoes) AS forum_mencoes,
        AVG(ffs.score_sentimento) AS analise_sentimento
    FROM auto_escala_dw.fact_forum_sentiment ffs
    JOIN auto_escala_dw.dim_tempo dtp ON ffs.tempo_key = dtp.tempo_key
    WHERE ffs.score_sentimento IS NOT NULL
      AND NOT (ffs.marca_key = -1 AND ffs.tipo_key = -1 AND ffs.combustivel_key = -1)
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    tm.marca_key,
    tm.tipo_key,
    tm.combustivel_key,
    tm.localizacao_key,
    tm.ano,
    tm.mes,
    tm.valor_interesse,
    NULL::numeric AS total_posts,
    NULL::numeric AS forum_mencoes,
    NULL::numeric AS analise_sentimento
FROM trends_mensal tm
UNION ALL
SELECT
    sm.marca_key,
    sm.tipo_key,
    sm.combustivel_key,
    0 AS localizacao_key,
    sm.ano,
    sm.mes,
    NULL::numeric AS valor_interesse,
    COALESCE(sm.total_posts, 0) AS total_posts,
    NULL::numeric AS forum_mencoes,
    NULL::numeric AS analise_sentimento
FROM social_mensal sm
UNION ALL
SELECT
    fm.marca_key,
    fm.tipo_key,
    fm.combustivel_key,
    0 AS localizacao_key,
    fm.ano,
    fm.mes,
    NULL::numeric AS valor_interesse,
    NULL::numeric AS total_posts,
    COALESCE(fm.forum_mencoes, 0) AS forum_mencoes,
    COALESCE(fm.analise_sentimento, 0) AS analise_sentimento
FROM forum_mensal fm;


CREATE OR REPLACE VIEW vw_mart_prev_gain AS
WITH
social_mensal AS (
    SELECT
        marca_key, tipo_key, combustivel_key, dtp.ano, dtp.mes,
        SUM(volume) AS volume_hashtag,
        AVG(variacao_semanal) AS variacao_semanal_media
    FROM auto_escala_dw.fact_hashtag_volume fh
    JOIN auto_escala_dw.dim_tempo dtp ON fh.tempo_key = dtp.tempo_key
    WHERE NOT (fh.marca_key = -1 AND fh.tipo_key = -1 AND fh.combustivel_key = -1)
    GROUP BY 1, 2, 3, 4, 5
),
forum_por_marca AS (
    SELECT
        marca_key, tipo_key, combustivel_key, dtp.ano, dtp.mes,
        AVG(score_sentimento) AS sentimento_medio,
        SUM(n_mencoes) AS total_mencoes_forum
    FROM auto_escala_dw.fact_forum_sentiment ffs
    JOIN auto_escala_dw.dim_tempo dtp ON ffs.tempo_key = dtp.tempo_key
    WHERE NOT (ffs.marca_key = -1 AND ffs.tipo_key = -1 AND ffs.combustivel_key = -1)
    GROUP BY 1, 2, 3, 4, 5
),
base_combinacoes AS (
    SELECT
        marca_key, tipo_key, combustivel_key, localizacao_key, dtp.ano, dtp.mes, dtp.tempo_key,
        AVG(valor_interesse) AS valor_interesse_medio
    FROM auto_escala_dw.fact_trends ft
    JOIN auto_escala_dw.dim_tempo dtp ON ft.tempo_key = dtp.tempo_key
    -- Filtrar apenas linhas completamente sem informação
    WHERE NOT (ft.marca_key = -1 AND ft.tipo_key = -1 AND ft.combustivel_key = -1)
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),
trends_com_lags AS (
    SELECT bc.*,
        LAG(valor_interesse_medio, 1) OVER w AS interesse_lag_1m,
        LAG(valor_interesse_medio, 2) OVER w AS interesse_lag_2m,
        LAG(valor_interesse_medio, 3) OVER w AS interesse_lag_3m
    FROM base_combinacoes bc
    WINDOW w AS (PARTITION BY marca_key, tipo_key, combustivel_key, localizacao_key ORDER BY ano, mes)
)
SELECT
    dm.marca, dti.tipo_automovel, dcomb.combustivel, dl.distrito, tl.ano, tl.mes,
    COALESCE(tl.valor_interesse_medio, 0) AS valor_interesse,
    COALESCE(tl.interesse_lag_1m, 0)      AS interesse_lag_1m,
    COALESCE(tl.interesse_lag_2m, 0)      AS interesse_lag_2m,
    COALESCE(tl.interesse_lag_3m, 0)      AS interesse_lag_3m,
    COALESCE(sm.volume_hashtag, 0)        AS volume_hashtag,
    COALESCE(fm.sentimento_medio, 0)      AS sentimento_medio,
    COALESCE(cb.n_vendas, 0)              AS n_vendas_historico,
    cb.mean_age_buyers,
    cb.pct_masculino,
    COALESCE(cb.margem_pct_media, 0)      AS margem_pct_media
FROM trends_com_lags tl
JOIN dim_marca       dm    ON tl.marca_key       = dm.marca_key
JOIN dim_tipo        dti   ON tl.tipo_key        = dti.tipo_key
JOIN dim_combustivel dcomb ON tl.combustivel_key = dcomb.combustivel_key
JOIN dim_localizacao dl    ON tl.localizacao_key = dl.localizacao_key
LEFT JOIN social_mensal sm
       ON sm.marca_key = tl.marca_key AND sm.tipo_key = tl.tipo_key
      AND sm.combustivel_key = tl.combustivel_key AND sm.ano = tl.ano AND sm.mes = tl.mes
LEFT JOIN forum_por_marca fm
       ON fm.marca_key = tl.marca_key AND fm.tipo_key = tl.tipo_key
      AND fm.combustivel_key = tl.combustivel_key AND fm.ano = tl.ano AND fm.mes = tl.mes

-- JOIN LATERAL para vendas: usa OR por coluna para lidar com chaves parciais.
-- Se a chave do trends tiver valor (ex: só marca preenchida), filtra apenas por essa.
-- dim_veiculo tem sempre as 4 colunas preenchidas, por isso fazemos OR-match.
LEFT JOIN LATERAL (
    SELECT
        COUNT(fv.venda_key)                                          AS n_vendas,
        AVG(dc_cli.idade)                                            AS mean_age_buyers,
        AVG(CASE WHEN dc_cli.genero = 'M' THEN 1.0 ELSE 0.0 END)   AS pct_masculino,
        AVG(fv.margem / NULLIF(fv.preco_venda, 0))                  AS margem_pct_media
    FROM fact_venda fv
    JOIN dim_veiculo dv   ON fv.veiculo_key    = dv.veiculo_key
    JOIN dim_tempo   dtp  ON fv.tempo_venda_key = dtp.tempo_key
    -- stand do mesmo distrito que a localização do trends
    JOIN dim_stand   ds   ON fv.stand_key       = ds.stand_key
    LEFT JOIN dim_cliente dc_cli ON fv.cliente_key = dc_cli.cliente_key
    WHERE
        -- Filtrar por distrito do stand (não da morada do cliente)
        ds.localizacao_key = tl.localizacao_key
        AND dtp.ano = tl.ano
        AND dtp.mes = tl.mes
        -- Match parcial: se a chave trends não for -1, filtra; se for -1, aceita tudo
        AND (tl.marca_key       = -1 OR dv.marca_key       = tl.marca_key)
        AND (tl.tipo_key        = -1 OR dv.tipo_key        = tl.tipo_key)
        AND (tl.combustivel_key = -1 OR dv.combustivel_key = tl.combustivel_key)
) cb ON true;

-- =============================================================================
-- ÍNDICES
-- =============================================================================
CREATE INDEX idx_dim_cliente_nif ON dim_cliente(nif);
CREATE INDEX idx_fact_venda_veiculo ON auto_escala_dw.fact_venda(veiculo_key);
CREATE INDEX idx_fact_venda_tempo_venda ON auto_escala_dw.fact_venda(tempo_venda_key);
CREATE INDEX idx_fact_trends_tempo ON auto_escala_dw.fact_trends(tempo_key);
CREATE INDEX idx_fact_forum_tempo ON auto_escala_dw.fact_forum_sentiment(tempo_key);

CREATE INDEX idx_sarima_lookup ON auto_escala_dw.fact_previsoes_sarima(marca_key, tipo_key, combustivel_key, tempo_ref_key, metrica);
CREATE INDEX idx_xgboost_lookup ON auto_escala_dw.fact_previsoes_xgboost(marca_key, tipo_key, combustivel_key, localizacao_key, tempo_ref_key);

-- =============================================================================
-- REGISTOS UNKNOWN (-1)
-- =============================================================================
INSERT INTO dim_marca (marca_key, marca) VALUES (-1, 'Unknown');
INSERT INTO dim_tipo (tipo_key, tipo_automovel) VALUES (-1, 'Unknown');
INSERT INTO dim_combustivel (combustivel_key, combustivel) VALUES (-1, 'Unknown');
INSERT INTO dim_modelo (modelo_key, modelo) VALUES (-1, 'Unknown');
INSERT INTO dim_localizacao (localizacao_key, distrito, pais) VALUES (-1, 'Unknown', 'Unknown');
INSERT INTO dim_tempo (tempo_key, data, ano, mes, dia, trimestre, nome_mes, semana_ano) VALUES (-1, '1900-01-01', 1900, 1, 1, 1, 'Unknown', 1);

-- =============================================================================
-- SEEDS
-- =============================================================================

INSERT INTO dim_localizacao (localizacao_key, distrito, pais)
VALUES (0, 'Portugal - Nacional', 'Portugal');

INSERT INTO dim_fonte (nome_fonte, tipo_fonte, descricao) VALUES
    ('Inventário Stands',  'Interna',  'Ficheiros CSV mensais dos stands'),
    ('Google Trends',      'Externa',  'Score de interesse de pesquisa no Google em Portugal'),
    ('Fórum motorguia.net','Externa',  'Posts scrapeados; análise de sentimento NLP'),
    ('Hashtags Sociais',   'Externa',  'Feed XML semanal Talkwalker/Mention');

INSERT INTO dim_dicionario_veiculo (campo, valor_original, valor_normalizado) VALUES
    ('marca', 'VW',             'Volkswagen'),
    ('marca', 'volkswagen',     'Volkswagen'),
    ('marca', 'Volksvagen',     'Volkswagen'),
    ('marca', 'V.W.',           'Volkswagen'),
    ('marca', 'mercedes benz',  'Mercedes'),
    ('marca', 'Mercedes-Benz',  'Mercedes'),
    ('marca', 'Merc',           'Mercedes'),
    ('marca', 'mercedes',       'Mercedes'),
    ('marca', 'bmw',            'BMW'),
    ('marca', 'B.M.W',          'BMW'),
    ('marca', 'Bmw',            'BMW'),
    ('marca', 'Citroen',        'Citroën'),
    ('marca', 'CITROEN',        'Citroën'),
    ('marca', 'citroen',        'Citroën'),
    ('marca', 'citroën',        'Citroën'),
    ('marca', 'Citroën',        'Citroën'),
    ('marca', 'hyundai',        'Hyundai'),
    ('marca', 'Hundai',         'Hyundai'),
    ('marca', 'Hyunday',        'Hyundai'),
    ('marca', 'peugeot',        'Peugeot'),
    ('marca', 'Peguot',         'Peugeot'),
    ('marca', 'PEUGEOT',        'Peugeot'),
    ('marca', 'renault',        'Renault'),
    ('marca', 'Renaul',         'Renault'),
    ('marca', 'RENAULT',        'Renault'),
    ('marca', 'kia',            'Kia'),
    ('marca', 'KIA',            'Kia'),
    ('marca', 'opel',           'Opel'),
    ('marca', 'OPEL',           'Opel'),
    ('marca', 'fiat',           'Fiat'),
    ('marca', 'FIAT',           'Fiat'),
    ('marca', 'toyota',         'Toyota'),
    ('marca', 'TOYOTA',         'Toyota'),
    ('marca', 'Toyotta',        'Toyota'),
    ('marca', 'tesla',          'Tesla'),
    ('marca', 'TESLA',          'Tesla'),
    ('marca', 'audi',           'Audi'),
    ('marca', 'AUDI',           'Audi'),
    ('marca', 'nissan',         'Nissan'),
    ('marca', 'NISSAN',         'Nissan'),
    ('marca', 'seat',           'Seat'),
    ('marca', 'SEAT',           'Seat'),
    ('modelo', 'gla',           'GLA'),
    ('modelo', 'x1',            'X1'),
    ('modelo', 'série 1',       'Série 1'),
    ('modelo', 'serie 1',       'Série 1'),
    ('modelo', 'SERIE 1',       'Série 1'),
    ('modelo', 'golf',          'Golf'),
    ('modelo', 'GOLF',          'Golf'),
    ('modelo', '3008',          '3008'),
    ('modelo', 'qashqai',       'Qashqai'),
    ('modelo', 'ibiza',         'Ibiza'),
    ('modelo', 'clio',          'Clio'),
    ('modelo', 'model 3',       'Model 3'),
    ('modelo', 'model3',        'Model 3'),
    ('modelo', 'zoe',           'Zoe'),
    ('modelo', 'ZOE',           'Zoe'),
    ('modelo', 'leaf',          'Leaf'),
    ('modelo', 'kona',          'Kona'),
    ('modelo', 'niro',          'Niro'),
    ('modelo', 'tucson',        'Tucson'),
    ('modelo', 'sportage',      'Sportage'),
    ('modelo', 'tiguan',        'Tiguan'),
    ('modelo', 'id4',           'ID.4'),
    ('modelo', 'id.4',          'ID.4'),
    ('modelo', 'a3',            'A3'),
    ('modelo', 'astra',         'Astra'),
    ('modelo', 'corsa',         'Corsa'),
    ('modelo', 'yaris',         'Yaris'),
    ('modelo', '208',           '208'),
    ('modelo', '500',           '500'),
    ('modelo', 'classe a',      'Classe A'),
    ('marca_modelo', 'vw golf',                  'Volkswagen|Golf'),
    ('marca_modelo', 'volkswagen golf',           'Volkswagen|Golf'),
    ('marca_modelo', 'VW Golf',                  'Volkswagen|Golf'),
    ('marca_modelo', 'vw tiguan',                'Volkswagen|Tiguan'),
    ('marca_modelo', 'volkswagen tiguan',         'Volkswagen|Tiguan'),
    ('marca_modelo', 'vw id4',                   'Volkswagen|ID.4'),
    ('marca_modelo', 'volkswagen id.4',           'Volkswagen|ID.4'),
    ('marca_modelo', 'mercedes gla',             'Mercedes|GLA'),
    ('marca_modelo', 'mercedes-benz gla',        'Mercedes|GLA'),
    ('marca_modelo', 'mercedes classe a',        'Mercedes|Classe A'),
    ('marca_modelo', 'mercedes-benz classe a',   'Mercedes|Classe A'),
    ('marca_modelo', 'bmw x1',                   'BMW|X1'),
    ('marca_modelo', 'bmw serie 1',              'BMW|Série 1'),
    ('marca_modelo', 'bmw série 1',              'BMW|Série 1'),
    ('marca_modelo', 'peugeot 208',              'Peugeot|208'),
    ('marca_modelo', 'peugeot 3008',             'Peugeot|3008'),
    ('marca_modelo', 'renault clio',             'Renault|Clio'),
    ('marca_modelo', 'renault zoe',              'Renault|Zoe'),
    ('marca_modelo', 'renault zoé',              'Renault|Zoe'),
    ('marca_modelo', 'nissan qashqai',           'Nissan|Qashqai'),
    ('marca_modelo', 'nissan leaf',              'Nissan|Leaf'),
    ('marca_modelo', 'seat ibiza',               'Seat|Ibiza'),
    ('marca_modelo', 'seat arona',               'Seat|Arona'),
    ('marca_modelo', 'citroen c3',               'Citroën|C3'),
    ('marca_modelo', 'citroën c3',               'Citroën|C3'),
    ('marca_modelo', 'fiat 500',                 'Fiat|500'),
    ('marca_modelo', 'tesla model 3',            'Tesla|Model 3'),
    ('marca_modelo', 'tesla model3',             'Tesla|Model 3'),
    ('marca_modelo', 'hyundai kona',             'Hyundai|Kona'),
    ('marca_modelo', 'hyundai tucson',           'Hyundai|Tucson'),
    ('marca_modelo', 'kia niro',                 'Kia|Niro'),
    ('marca_modelo', 'kia sportage',             'Kia|Sportage'),
    ('marca_modelo', 'audi a3',                  'Audi|A3'),
    ('marca_modelo', 'toyota yaris',             'Toyota|Yaris'),
    ('marca_modelo', 'opel astra',               'Opel|Astra'),
    ('marca_modelo', 'opel corsa',               'Opel|Corsa'),
    -- x AUSENTES (quarentena): combinações não listadas continuam sem match

    -- -------------------------------------------------------------------------
    -- COMBUSTÍVEIS  (campo = 'combustivel')
    -- coberto
    -- -------------------------------------------------------------------------
    ('combustivel', '100% Eletrico',         'Elétrico'),
    ('combustivel', '100% Elétrico',         'Elétrico'),
    ('combustivel', 'Eletrico',              'Elétrico'),
    ('combustivel', 'eletrico',              'Elétrico'),
    ('combustivel', 'Electrico',             'Elétrico'),
    ('combustivel', '100%Elétrico',          'Elétrico'),
    ('combustivel', '100%Eletrico',          'Elétrico'),
    ('combustivel', 'elétrico',              'Elétrico'),
    ('combustivel', 'elétricos',             'Elétrico'),
    ('combustivel', 'eletricos',             'Elétrico'),
    ('combustivel', 'elétricas',             'Elétrico'),
    ('combustivel', 'eletricas',             'Elétrico'),
    ('combustivel', 'gasolina',              'Gasolina'),
    ('combustivel', 'GASOLINA',              'Gasolina'),
    ('combustivel', 'Gasoline',              'Gasolina'),
    ('combustivel', 'gasoleo',               'Gasóleo'),
    ('combustivel', 'Gasoleo',               'Gasóleo'),
    ('combustivel', 'GASOLEO',               'Gasóleo'),
    ('combustivel', 'gasóleo',               'Gasóleo'),
    ('combustivel', 'Gasóleo',               'Gasóleo'),
    ('combustivel', 'GASÓLEO',               'Gasóleo'),
    ('combustivel', 'Diesel',                'Gasóleo'),
    ('combustivel', 'diesel',                'Gasóleo'),
    ('combustivel', 'hibrido a gasolina',    'Híbrido'),
    ('combustivel', 'híbrido a gasolina',    'Híbrido'),
    ('combustivel', 'Híbrido a Gasolina',    'Híbrido'),
    ('combustivel', 'Hibrido Gasolina',      'Híbrido'),
    ('combustivel', 'Híbrido Gasolina',      'Híbrido'),
    ('combustivel', 'hybrid gasolina',       'Híbrido'),
    ('combustivel', 'hibrido a gasoleo',      'Híbrido'),
    ('combustivel', 'híbrido a gasóleo',      'Híbrido'),
    ('combustivel', 'Híbrido a Gasóleo',      'Híbrido'),
    ('combustivel', 'híbrido',               'Híbrido'),
    ('combustivel', 'hibrido',               'Híbrido'),
    ('combustivel', 'híbridos',              'Híbrido'),
    ('combustivel', 'hibridos',              'Híbrido'),
    ('combustivel', 'gpl',                   'GPL'),
    ('combustivel', 'G.P.L.',                'GPL'),
    -- x AUSENTES (quarentena): H2, Biogás, Solar, Ar

    -- -------------------------------------------------------------------------
    -- TIPOS DE AUTOMÓVEL  (campo = 'tipo_automovel')
    -- coberto
    -- -------------------------------------------------------------------------
    ('tipo_automovel', 'suv',               'SUV'),
    ('tipo_automovel', 'S.U.V',             'SUV'),
    ('tipo_automovel', 'Suv',               'SUV'),
    ('tipo_automovel', '4x4',               'SUV'),
    ('tipo_automovel', 'Todo-o-Terreno',    'SUV'),
    ('tipo_automovel', 'suvs',              'SUV'),
    ('tipo_automovel', 'hatchback',         'Hatchback'),
    ('tipo_automovel', 'HATCHBACK',         'Hatchback'),
    ('tipo_automovel', 'Hatch',             'Hatchback'),
    ('tipo_automovel', 'hatchbacks',        'Hatchback'),
    ('tipo_automovel', 'citadino',          'Citadino'),
    ('tipo_automovel', 'CITADINO',          'Citadino'),
    ('tipo_automovel', 'City',              'Citadino'),
    ('tipo_automovel', 'city car',          'Citadino'),
    ('tipo_automovel', 'citadinos',         'Citadino'),
    ('tipo_automovel', 'sedan',             'Sedan'),
    ('tipo_automovel', 'sedans',            'Sedan'),
    ('tipo_automovel', 'elétrico',          'Elétrico'),
    ('tipo_automovel', 'elétricos',         'Elétrico'),
    ('tipo_automovel', 'eletrico',          'Elétrico'),
    ('tipo_automovel', 'eletricos',         'Elétrico'),

    -- -------------------------------------------------------------------------
    -- NOVOS MODELOS DO CATÁLOGO EXPANDIDO (1)
    -- -------------------------------------------------------------------------
    ('modelo', 'polo',          'Polo'),
    ('modelo', 't-cross',       'T-Cross'),
    ('modelo', 't-roc',         'T-Roc'),
    ('modelo', 'id.3',          'ID.3'),
    ('modelo', 'id3',           'ID.3'),
    ('modelo', 'corolla',       'Corolla'),
    ('modelo', 'c-hr',          'C-HR'),
    ('modelo', 'chr',           'C-HR'),
    ('modelo', 'rav4',          'RAV4'),
    ('modelo', '2008',          '2008'),
    ('modelo', '5008',          '5008'),
    ('modelo', 'captur',        'Captur'),
    ('modelo', 'mégane',        'Mégane'),
    ('modelo', 'megane',        'Mégane'),
    ('modelo', 'arkana',        'Arkana'),
    ('modelo', 'série 3',       'Série 3'),
    ('modelo', 'serie 3',       'Série 3'),
    ('modelo', 'série 5',       'Série 5'),
    ('modelo', 'x3',            'X3'),
    ('modelo', 'i3',            'i3'),
    ('modelo', 'classe c',      'Classe C'),
    ('modelo', 'cla',           'CLA'),
    ('modelo', 'glc',           'GLC'),
    ('modelo', 'eqa',           'EQA'),
    ('modelo', 'a1',            'A1'),
    ('modelo', 'a4',            'A4'),
    ('modelo', 'q2',            'Q2'),
    ('modelo', 'q3',            'Q3'),
    ('modelo', 'q5',            'Q5'),
    ('modelo', 'model y',       'Model Y'),
    ('modelo', 'modely',        'Model Y'),
    ('modelo', 'model s',       'Model S'),
    ('modelo', 'i20',           'i20'),
    ('modelo', 'i30',           'i30'),
    ('modelo', 'ioniq 5',       'Ioniq 5'),
    ('modelo', 'rio',           'Rio'),
    ('modelo', 'ceed',          'Ceed'),
    ('modelo', 'stonic',        'Stonic'),
    ('modelo', 'ev6',           'EV6'),
    ('modelo', 'micra',         'Micra'),
    ('modelo', 'juke',          'Juke'),
    ('modelo', 'ariya',         'Ariya'),
    ('modelo', 'mokka',         'Mokka'),
    ('modelo', 'grandland',     'Grandland'),
    ('modelo', 'c4',            'C4'),
    ('modelo', 'c3 aircross',   'C3 Aircross'),
    ('modelo', 'c5 aircross',   'C5 Aircross'),
    ('modelo', 'panda',         'Panda'),
    ('modelo', 'tipo',          'Tipo'),
    ('modelo', '500x',          '500X'),
    ('modelo', 'leon',          'Leon'),
    ('modelo', 'ateca',         'Ateca'),
    ('modelo', '308',            '308'),
    ('modelo', 'c3',             'C3'),
    ('modelo', 'arona',          'Arona'),
    ('modelo', 'serie 5',        'Série 5'),

    -- -------------------------------------------------------------------------
    -- NOVAS COMBINAÇÕES MARCA_MODELO PARA O FÓRUM (Exemplos críticos)
    -- -------------------------------------------------------------------------
    ('marca_modelo', 'vw polo',                 'Volkswagen|Polo'),
    ('marca_modelo', 'vw t-roc',                'Volkswagen|T-Roc'),
    ('marca_modelo', 'volkswagen t-roc',        'Volkswagen|T-Roc'),
    ('marca_modelo', 'vw id3',                  'Volkswagen|ID.3'),
    ('marca_modelo', 'toyota corolla',          'Toyota|Corolla'),
    ('marca_modelo', 'toyota c-hr',             'Toyota|C-HR'),
    ('marca_modelo', 'peugeot 2008',            'Peugeot|2008'),
    ('marca_modelo', 'renault megane',          'Renault|Mégane'),
    ('marca_modelo', 'renault captur',          'Renault|Captur'),
    ('marca_modelo', 'bmw serie 3',             'BMW|Série 3'),
    ('marca_modelo', 'bmw i3',                  'BMW|i3'),
    ('marca_modelo', 'mercedes classe c',       'Mercedes|Classe C'),
    ('marca_modelo', 'audi a4',                 'Audi|A4'),
    ('marca_modelo', 'audi q3',                 'Audi|Q3'),
    ('marca_modelo', 'tesla model y',           'Tesla|Model Y'),
    ('marca_modelo', 'hyundai tucson',          'Hyundai|Tucson'),
    ('marca_modelo', 'hyundai ioniq 5',         'Hyundai|Ioniq 5'),
    ('marca_modelo', 'kia ev6',                 'Kia|EV6'),
    ('marca_modelo', 'nissan juke',             'Nissan|Juke'),
    ('marca_modelo', 'opel mokka',              'Opel|Mokka'),
    ('marca_modelo', 'citroen c4',              'Citroën|C4'),
    ('marca_modelo', 'citroën c4',              'Citroën|C4'),
    ('marca_modelo', 'seat leon',               'Seat|Leon'),
    ('marca_modelo', 'peugeot 308',             'Peugeot|308'),
    ('marca_modelo', 'bmw serie 5',             'BMW|Série 5'),
    ('marca_modelo', 'bmw série 5',             'BMW|Série 5'),
    ('marca_modelo', 'volkswagen t-cross',      'Volkswagen|T-Cross'),
    ('marca_modelo', 'vw t-cross',              'Volkswagen|T-Cross'),
    ('marca_modelo', 'toyota rav4',             'Toyota|RAV4'),
    ('marca_modelo', 'peugeot 5008',            'Peugeot|5008'),
    ('marca_modelo', 'renault arkana',          'Renault|Arkana'),
    ('marca_modelo', 'bmw x3',                  'BMW|X3'),
    ('marca_modelo', 'mercedes cla',            'Mercedes|CLA'),
    ('marca_modelo', 'mercedes glc',            'Mercedes|GLC'),
    ('marca_modelo', 'mercedes eqa',            'Mercedes|EQA'),
    ('marca_modelo', 'audi a1',                 'Audi|A1'),
    ('marca_modelo', 'audi q2',                 'Audi|Q2'),
    ('marca_modelo', 'audi q5',                 'Audi|Q5'),
    ('marca_modelo', 'tesla model s',           'Tesla|Model S'),
    ('marca_modelo', 'hyundai i20',             'Hyundai|i20'),
    ('marca_modelo', 'hyundai i30',             'Hyundai|i30'),
    ('marca_modelo', 'kia rio',                 'Kia|Rio'),
    ('marca_modelo', 'kia ceed',                'Kia|Ceed'),
    ('marca_modelo', 'kia stonic',              'Kia|Stonic'),
    ('marca_modelo', 'nissan micra',            'Nissan|Micra'),
    ('marca_modelo', 'nissan ariya',            'Nissan|Ariya'),
    ('marca_modelo', 'opel grandland',          'Opel|Grandland'),
    ('marca_modelo', 'citroen c3 aircross',     'Citroën|C3 Aircross'),
    ('marca_modelo', 'citroën c3 aircross',     'Citroën|C3 Aircross'),
    ('marca_modelo', 'citroen c5 aircross',     'Citroën|C5 Aircross'),
    ('marca_modelo', 'citroën c5 aircross',     'Citroën|C5 Aircross'),
    ('marca_modelo', 'fiat panda',              'Fiat|Panda'),
    ('marca_modelo', 'fiat tipo',               'Fiat|Tipo'),
    ('marca_modelo', 'fiat 500x',               'Fiat|500X'),
    ('marca_modelo', 'seat ateca',              'Seat|Ateca')
ON CONFLICT (campo, valor_original) DO NOTHING;

-- =============================================================================
-- AUDITORIA (LOG DE ALTERAÇÕES)
-- =============================================================================

CREATE TABLE IF NOT EXISTS auto_escala_dw.audit_log_dimensions (
    log_id          SERIAL PRIMARY KEY,
    tabela_afetada  VARCHAR(100),
    operacao        VARCHAR(10),
    registo_antigo  JSONB,
    registo_novo    JSONB,
    data_alteracao  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION auto_escala_dw.audit_dim_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'UPDATE') THEN
        INSERT INTO auto_escala_dw.audit_log_dimensions (tabela_afetada, operacao, registo_antigo, registo_novo)
        VALUES (TG_TABLE_NAME, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW));
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO auto_escala_dw.audit_log_dimensions (tabela_afetada, operacao, registo_novo)
        VALUES (TG_TABLE_NAME, 'INSERT', to_jsonb(NEW));
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_audit_cliente ON auto_escala_dw.dim_cliente;
CREATE TRIGGER trg_audit_cliente
AFTER INSERT OR UPDATE ON auto_escala_dw.dim_cliente
FOR EACH ROW EXECUTE FUNCTION auto_escala_dw.audit_dim_changes();

DROP TRIGGER IF EXISTS trg_audit_veiculo ON auto_escala_dw.dim_veiculo;
CREATE TRIGGER trg_audit_veiculo
AFTER INSERT OR UPDATE ON auto_escala_dw.dim_veiculo
FOR EACH ROW EXECUTE FUNCTION auto_escala_dw.audit_dim_changes();
"""

def create_data_warehouse():
    try:
        with dw_engine.begin() as conn:
            # Use SQLAlchemy text() to avoid DBAPI percent-sign interpolation in literal strings
            conn.execute(text(CREATE_DW_SQL))
            
            # --- VERIFICAÇÃO RÁPIDA DAS VIEWS CRIADAS ---
            view_check = conn.execute(text(
                "SELECT table_name FROM information_schema.views WHERE table_schema = 'auto_escala_dw' ORDER BY table_name"
            )).fetchall()
            print("  Views criadas no DW:", [row[0] for row in view_check])
            
            # --- POPULAR DIM_TEMPO (2020-2030) ---
            print("  A popular dim_tempo (2020-2030)...")
            import pandas as pd
            datas = pd.date_range(start='2020-01-01', end='2030-12-31', freq='D')
            df_tempo = pd.DataFrame({'data': datas.date})
            df_tempo['ano'] = datas.year
            df_tempo['mes'] = datas.month
            df_tempo['dia'] = datas.day
            df_tempo['trimestre'] = datas.quarter
            df_tempo['nome_mes'] = datas.month_name()
            df_tempo['semana_ano'] = datas.isocalendar().week.values.astype(int)
            
            query_tempo = text(f"""
                INSERT INTO auto_escala_dw.dim_tempo
                    (data, ano, mes, dia, trimestre, nome_mes, semana_ano)
                VALUES (:data, :ano, :mes, :dia, :trimestre, :nome_mes, :semana_ano)
                ON CONFLICT (data) DO NOTHING
            """)
            conn.execute(query_tempo, df_tempo.to_dict(orient="records"))

            # --- POPULAR SEEDS ADICIONAIS (Unknowns) ---
            conn.execute(text("INSERT INTO auto_escala_dw.dim_marca (marca_key, marca) VALUES (-1, 'Unknown') ON CONFLICT DO NOTHING"))
            conn.execute(text("INSERT INTO auto_escala_dw.dim_modelo (modelo_key, modelo) VALUES (-1, 'Unknown') ON CONFLICT DO NOTHING"))
            conn.execute(text("INSERT INTO auto_escala_dw.dim_tipo (tipo_key, tipo_automovel) VALUES (-1, 'Unknown') ON CONFLICT DO NOTHING"))
            conn.execute(text("INSERT INTO auto_escala_dw.dim_combustivel (combustivel_key, combustivel) VALUES (-1, 'Unknown') ON CONFLICT DO NOTHING"))
            conn.execute(text("INSERT INTO auto_escala_dw.dim_localizacao (localizacao_key, distrito, pais) VALUES (-1, 'Unknown', 'N/A') ON CONFLICT DO NOTHING"))
            
        print("Data Warehouse completo criado com sucesso!")
    except Exception as e:
        print("Erro ao criar DW:")
        print(repr(e))

def setup_sandbox():
    print("A configurar a Analytical Sandbox...")
    
    # IMPORTANTE: execution_options="AUTOCOMMIT" é obrigatório para criar Roles no Postgres via Python
    engine_admin = create_engine(DW_URL, execution_options={"isolation_level": "AUTOCOMMIT"})
    
    comandos_sql = [
        "CREATE SCHEMA IF NOT EXISTS auto_escala_sandbox;",
        
        # O bloco DO apanha o erro caso a Role/User já exista, para poderes correr o script várias vezes
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'data_scientist_role') THEN
                CREATE ROLE data_scientist_role;
            END IF;
            IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'ae_analista') THEN
                CREATE USER ae_analista WITH PASSWORD 'senha_super_segura';
            END IF;
        END
        $$;
        """,
        
        "GRANT data_scientist_role TO ae_analista;",
        
        # Permissões de Leitura no DW (Ouro)
        "GRANT USAGE ON SCHEMA auto_escala_dw TO data_scientist_role;",
        "GRANT SELECT ON ALL TABLES IN SCHEMA auto_escala_dw TO data_scientist_role;",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA auto_escala_dw GRANT SELECT ON TABLES TO data_scientist_role;",
        
        # Permissões Totais na Sandbox
        "GRANT ALL ON SCHEMA auto_escala_sandbox TO data_scientist_role;",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA auto_escala_sandbox GRANT ALL ON TABLES TO data_scientist_role;"
    ]
    
    try:
        with engine_admin.connect() as conn:
            for cmd in comandos_sql:
                conn.execute(text(cmd))
        print("Sandbox configurada com sucesso!")
    except Exception as e:
        print(f"Erro ao criar Sandbox: {e}")

def copy_to_sandbox(dw_url):
    print("\nA iniciar a clonagem do DW para a Sandbox...")
    engine = create_engine(dw_url)
    
    try:
        with engine.begin() as conn:
            query_tabelas = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'auto_escala_dw' AND table_type = 'BASE TABLE';
            """)
            
            tabelas = conn.execute(query_tabelas).fetchall()
            
            if not tabelas:
                print("Nenhuma tabela encontrada no schema auto_escala_dw.")
                return

            for (nome_tabela,) in tabelas:
                print(f"  -> A copiar tabela: {nome_tabela}...")
                
                # Opcional: Apagar a tabela na sandbox se já existir
                conn.execute(text(f"DROP TABLE IF EXISTS auto_escala_sandbox.{nome_tabela} CASCADE;"))
                
                # Criar a tabela na sandbox como uma cópia exata (estrutura + dados)
                conn.execute(text(f"""
                    CREATE TABLE auto_escala_sandbox.{nome_tabela} AS 
                    SELECT * FROM auto_escala_dw.{nome_tabela};
                """))

            query_views = text("""
                SELECT table_name, view_definition
                FROM information_schema.views
                WHERE table_schema = 'auto_escala_dw';
            """)
            views = conn.execute(query_views).fetchall()

            if views:
                for nome_view, definicao in views:
                    print(f"  -> A copiar view: {nome_view}...")
                    conn.execute(text(f"DROP VIEW IF EXISTS auto_escala_sandbox.{nome_view} CASCADE;"))
                    conn.execute(text(f"CREATE OR REPLACE VIEW auto_escala_sandbox.{nome_view} AS {definicao}"))

            print("\nCópia para a Sandbox concluída com sucesso!")
            
    except Exception as e:
        print(f"Erro ao copiar para a sandbox: {e}")

if __name__ == "__main__":
    create_data_warehouse()
    setup_sandbox()
    copy_to_sandbox(DW_URL)