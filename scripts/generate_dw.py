"""
generate_dw.py
---------------------------------------------------------------
Cria o schema do Data Warehouse (Star Schema + Snowflake parcial)
na base de dados PostgreSQL auto_escala.

Inclui a dim_dicionario_veiculo com três níveis de normalização:
  ① Lookup resolve  — variações semânticas (marca, modelo, combustível, tipo)
  ② Silver resolve  — trim / lower (NÃO estão aqui; o pipeline trata)
  ③ Quarentena      — erros propositadamente ausentes (marca_gibberish,
                      preco_negativo, ano_impossivel, etc.)

Formato especial marca|modelo
  Quando o campo é "marca_modelo", o valor_normalizado usa o separador "|"
  (ex: "Volkswagen|Golf") para que o Silver possa fazer split e popular
  separadamente dim_modelo.marca e dim_modelo.modelo.

Projeto Auto Escala — CDGE 2025/2026
---------------------------------------------------------------
"""

import os
from sqlalchemy import create_engine, text

# ──────────────────────────────────────────────────────────────────────────────
# 1. Configuração da ligação
# ──────────────────────────────────────────────────────────────────────────────

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
DW_URL   = f"postgresql+psycopg2://ae_user:ae_pass_2026@{_PG_HOST}:{_PG_PORT}/auto_escala"
dw_engine = create_engine(DW_URL, echo=False)

# ──────────────────────────────────────────────────────────────────────────────
# 2. DDL do Star Schema
# ──────────────────────────────────────────────────────────────────────────────

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

-- SNOWFLAKE: modelo abstrato (cruza com tendências e hashtags)
CREATE TABLE dim_modelo (
    modelo_key     SERIAL PRIMARY KEY,
    marca          VARCHAR(100) NOT NULL,
    modelo         VARCHAR(100) NOT NULL,
    tipo_automovel VARCHAR(100),
    combustivel    VARCHAR(100),
    UNIQUE (marca, modelo, tipo_automovel, combustivel)
);

-- Veículo físico (liga ao modelo)
CREATE TABLE dim_veiculo (
    veiculo_key    SERIAL PRIMARY KEY,
    id_viatura     VARCHAR(50) UNIQUE,
    matricula      VARCHAR(20),
    modelo_key     INTEGER NOT NULL REFERENCES dim_modelo(modelo_key),
    num_lugares    INTEGER,
    ano_viatura    INTEGER
);

-- =============================================================================
-- DICIONÁRIO DE NORMALIZAÇÃO
-- =============================================================================
-- Convenções:
--   campo = 'marca'        → valor_normalizado = 'Mercedes'
--   campo = 'modelo'       → valor_normalizado = 'Golf'
--   campo = 'marca_modelo' → valor_normalizado = 'Volkswagen|Golf'
--   campo = 'combustivel'  → valor_normalizado = 'Gasóleo'
--   campo = 'tipo_automovel' → valor_normalizado = 'SUV'
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

CREATE TABLE dim_cliente (
    cliente_key     SERIAL PRIMARY KEY,
    nif             VARCHAR(20) UNIQUE,
    nome            VARCHAR(100),
    idade           INTEGER,
    faixa_etaria    VARCHAR(20),
    genero          VARCHAR(20),
    localizacao_key INTEGER REFERENCES dim_localizacao(localizacao_key),
    data_criacao    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_demografia_regional (
    demografia_key  SERIAL PRIMARY KEY,
    localizacao_key INTEGER NOT NULL REFERENCES dim_localizacao(localizacao_key),
    ano_referencia  INTEGER NOT NULL,
    populacao_total INTEGER,
    pct_18_24       NUMERIC(5,2),
    pct_25_34       NUMERIC(5,2),
    pct_35_49       NUMERIC(5,2),
    pct_50_64       NUMERIC(5,2),
    pct_65_mais     NUMERIC(5,2),
    pct_masculino   NUMERIC(5,2),
    pct_feminino    NUMERIC(5,2),
    UNIQUE (localizacao_key, ano_referencia)
);

-- =============================================================================
-- FACTOS
-- =============================================================================

CREATE TABLE fct_venda (
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

CREATE TABLE fct_inventario_mensal (
    inventario_key  SERIAL PRIMARY KEY,
    tempo_key       INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    stand_key       INTEGER NOT NULL REFERENCES dim_stand(stand_key),
    veiculo_key     INTEGER NOT NULL REFERENCES dim_veiculo(veiculo_key),
    valor_em_stock  NUMERIC(12,2),
    dias_em_parque  INTEGER,
    UNIQUE (tempo_key, stand_key, veiculo_key)
);

-- Substitui o fct_trends original
CREATE TABLE fact_trends (
    tendencia_key       SERIAL PRIMARY KEY,
    tempo_key           INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    modelo_key          INTEGER NOT NULL REFERENCES dim_modelo(modelo_key),
    localizacao_key     INTEGER NOT NULL REFERENCES dim_localizacao(localizacao_key),
    valor_interesse     INTEGER,                       -- 0-100, escala Google Trends
    crescimento_mom_pct NUMERIC(10,4),                 -- variação % face ao mês anterior
    trending_flag       BOOLEAN DEFAULT FALSE,         -- Tier 1: crescimento_mom_pct >= 30%
    UNIQUE (tempo_key, modelo_key, localizacao_key)
);

CREATE TABLE fact_forum_sentiment (
    sentimento_key      SERIAL PRIMARY KEY,
    tempo_key           INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    modelo_key          INTEGER NOT NULL REFERENCES dim_modelo(modelo_key),
    n_mencoes           INTEGER,
    score_sentimento    NUMERIC(5,4),
    delta_sentimento    NUMERIC(5,4),
    UNIQUE (tempo_key, modelo_key)
);

CREATE TABLE fct_hashtag_volume (
    hashtag_volume_key SERIAL PRIMARY KEY,
    tempo_key          INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    fonte_key          INTEGER NOT NULL REFERENCES dim_fonte(fonte_key),
    modelo_key         INTEGER REFERENCES dim_modelo(modelo_key),
    volume             INTEGER NOT NULL DEFAULT 0,
    posts_instagram    INTEGER DEFAULT 0,
    posts_twitter      INTEGER DEFAULT 0,
    posts_youtube      INTEGER DEFAULT 0,
    variacao_semanal   NUMERIC(10,4),
    UNIQUE (tempo_key, fonte_key, modelo_key)
);

CREATE TABLE fact_previsao (
    previsao_key     SERIAL PRIMARY KEY,
    modelo_key       INTEGER NOT NULL REFERENCES dim_modelo(modelo_key),
    tempo_ref_key    INTEGER NOT NULL REFERENCES dim_tempo(tempo_key), -- mês em que foi gerada
    tempo_alvo_key   INTEGER NOT NULL REFERENCES dim_tempo(tempo_key), -- mês previsto (t+1)
    valor_previsto   NUMERIC(10,4),
    yhat_lower       NUMERIC(10,4), -- intervalo de confiança inferior
    yhat_upper       NUMERIC(10,4), -- intervalo de confiança superior
    mae              NUMERIC(10,4), -- métricas de avaliação
    mape             NUMERIC(10,4),
    UNIQUE (modelo_key, tempo_ref_key, tempo_alvo_key)
);

-- =============================================================================
-- QUALIDADE DE DADOS & LOGS
-- =============================================================================

CREATE TABLE data_quality_log (
    id                  SERIAL PRIMARY KEY,
    fonte               VARCHAR(50)  NOT NULL,
    data_run            TIMESTAMPTZ  DEFAULT CURRENT_TIMESTAMP,
    total_registos      INTEGER,
    registos_ok         INTEGER,
    registos_quarentena INTEGER,
    taxa_quarentena_pct NUMERIC(6,2),
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

CREATE TABLE audit_log_dimensions (
    log_id          SERIAL PRIMARY KEY,
    tabela_afetada  VARCHAR(50)  NOT NULL,
    operacao        VARCHAR(10)  NOT NULL,
    registo_antigo  JSONB,
    registo_novo    JSONB,
    data_alteracao  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Função genérica de trigger CDC (SCD Tipo 1)
CREATE OR REPLACE FUNCTION log_dimension_changes()
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

CREATE TRIGGER trg_audit_dim_localizacao
    AFTER INSERT OR UPDATE OR DELETE ON dim_localizacao
    FOR EACH ROW EXECUTE FUNCTION log_dimension_changes();

CREATE TRIGGER trg_audit_dim_stand
    AFTER INSERT OR UPDATE OR DELETE ON dim_stand
    FOR EACH ROW EXECUTE FUNCTION log_dimension_changes();

CREATE TRIGGER trg_audit_dim_modelo
    AFTER INSERT OR UPDATE OR DELETE ON dim_modelo
    FOR EACH ROW EXECUTE FUNCTION log_dimension_changes();

CREATE TRIGGER trg_audit_dim_veiculo
    AFTER INSERT OR UPDATE OR DELETE ON dim_veiculo
    FOR EACH ROW EXECUTE FUNCTION log_dimension_changes();

CREATE TRIGGER trg_audit_dim_cliente
    AFTER INSERT OR UPDATE OR DELETE ON dim_cliente
    FOR EACH ROW EXECUTE FUNCTION log_dimension_changes();

-- =============================================================================
-- VIEWS
-- =============================================================================

CREATE VIEW vw_recomendacao_alocacao_stock AS
SELECT
    ds.nome_stand,
    dl.distrito,
    dm.marca,
    dm.modelo,
    ft.crescimento_mom_pct,
    -- Pegamos na última previsão disponível para este modelo
    (SELECT valor_previsto FROM auto_escala_dw.fact_previsao fp 
     WHERE fp.modelo_key = dm.modelo_key 
     ORDER BY tempo_ref_key DESC LIMIT 1) AS previsao_prox_mes,
    dr.pct_25_34    AS proporcao_jovens_stand,
    dr.pct_feminino AS proporcao_feminina_stand
FROM auto_escala_dw.fact_trends ft
JOIN auto_escala_dw.dim_modelo             dm  ON ft.modelo_key      = dm.modelo_key
JOIN auto_escala_dw.dim_stand              ds  ON 1=1
JOIN auto_escala_dw.dim_localizacao        dl  ON ds.localizacao_key = dl.localizacao_key
JOIN auto_escala_dw.dim_demografia_regional dr ON dl.localizacao_key = dr.localizacao_key
WHERE ft.trending_flag = TRUE;

-- =============================================================================
-- ÍNDICES
-- =============================================================================

CREATE INDEX idx_fct_venda_veiculo        ON fct_venda(veiculo_key);
CREATE INDEX idx_fct_venda_stand          ON fct_venda(stand_key);
CREATE INDEX idx_fct_venda_tempo_entrada  ON fct_venda(tempo_entrada_key);
CREATE INDEX idx_fct_venda_tempo_venda    ON fct_venda(tempo_venda_key);

CREATE INDEX idx_fct_inventario_tempo     ON fct_inventario_mensal(tempo_key);
CREATE INDEX idx_fct_inventario_stand     ON fct_inventario_mensal(stand_key);
CREATE INDEX idx_fct_inventario_veiculo   ON fct_inventario_mensal(veiculo_key);

CREATE INDEX idx_fact_trends_tempo      ON fact_trends(tempo_key);
CREATE INDEX idx_fact_trends_modelo     ON fact_trends(modelo_key);
CREATE INDEX idx_fact_forum_tempo       ON fact_forum_sentiment(tempo_key);
CREATE INDEX idx_fact_forum_modelo      ON fact_forum_sentiment(modelo_key);
CREATE INDEX idx_fact_previsao_alvo     ON fact_previsao(tempo_alvo_key);

CREATE INDEX idx_fct_hashtag_tempo        ON fct_hashtag_volume(tempo_key);

-- =============================================================================
-- SEED: dim_fonte
-- =============================================================================

INSERT INTO dim_fonte (nome_fonte, tipo_fonte, descricao) VALUES
    ('Inventário Stands',  'Interna',  'Ficheiros CSV mensais dos stands'),
    ('Google Trends',      'Externa',  'Score de interesse de pesquisa no Google em Portugal'),
    ('Fórum motorguia.net','Externa',  'Posts scrapeados; análise de sentimento NLP'),
    ('Hashtags Sociais',   'Externa',  'Feed XML semanal Talkwalker/Mention')
ON CONFLICT (nome_fonte) DO NOTHING;

-- =============================================================================
-- SEED: dim_dicionario_veiculo
--
-- Níveis de cobertura intencional:
--   ✔ COBERTO — lookup table resolve
--   ✘ AUSENTE  — vai para quarentena (marcas gibberish, combustíveis
--                impossíveis, anos futuros, etc. — gerados em generate_inventory)
-- =============================================================================

INSERT INTO dim_dicionario_veiculo (campo, valor_original, valor_normalizado) VALUES

    -- -------------------------------------------------------------------------
    -- MARCAS  (campo = 'marca')
    -- ✔ coberto
    -- -------------------------------------------------------------------------
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

    -- -------------------------------------------------------------------------
    -- MODELOS  (campo = 'modelo')
    -- ✔ coberto
    -- -------------------------------------------------------------------------
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
    ('modelo', 'TIGUAN',        'Tiguan'),
    ('modelo', 'id4',           'ID.4'),
    ('modelo', 'id.4',          'ID.4'),
    ('modelo', 'a3',            'A3'),
    ('modelo', 'astra',         'Astra'),
    ('modelo', 'corsa',         'Corsa'),
    ('modelo', 'yaris',         'Yaris'),
    ('modelo', '208',           '208'),
    ('modelo', '500',           '500'),
    ('modelo', 'classe a',      'Classe A'),

    -- -------------------------------------------------------------------------
    -- MARCA + MODELO  (campo = 'marca_modelo')
    -- Valor normalizado usa '|' como separador → Silver faz split('|')
    --   e obtém (marca, modelo) para dim_modelo.
    -- ✔ coberto — variantes comuns encontradas nos CSVs e no fórum
    -- -------------------------------------------------------------------------
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
    -- ✘ AUSENTES (quarentena): combinações não listadas continuam sem match

    -- -------------------------------------------------------------------------
    -- COMBUSTÍVEIS  (campo = 'combustivel')
    -- ✔ coberto
    -- -------------------------------------------------------------------------
    ('combustivel', '100% Eletrico',         'Elétrico'),
    ('combustivel', '100% Elétrico',         'Elétrico'),
    ('combustivel', 'Eletrico',              'Elétrico'),
    ('combustivel', 'eletrico',              'Elétrico'),
    ('combustivel', 'Electrico',             'Elétrico'),
    ('combustivel', '100%Elétrico',          'Elétrico'),
    ('combustivel', '100%Eletrico',          'Elétrico'),
    ('combustivel', 'elétrico',              'Elétrico'),
    ('combustivel', 'gasolina',              'Gasolina'),
    ('combustivel', 'GASOLINA',              'Gasolina'),
    ('combustivel', 'Gasoline',              'Gasolina'),
    ('combustivel', 'gasoleo',               'Gasóleo'),
    ('combustivel', 'Gasoleo',               'Gasóleo'),
    ('combustivel', 'GASOLEO',               'Gasóleo'),
    ('combustivel', 'Diesel',                'Gasóleo'),
    ('combustivel', 'diesel',                'Gasóleo'),
    ('combustivel', 'hibrido a gasolina',    'Híbrido'),
    ('combustivel', 'Hibrido Gasolina',      'Híbrido'),
    ('combustivel', 'Híbrido Gasolina',      'Híbrido'),
    ('combustivel', 'hybrid gasolina',       'Híbrido'),
    ('combustivel', 'híbrido',               'Híbrido'),
    ('combustivel', 'hibrido',               'Híbrido'),
    ('combustivel', 'gpl',                   'GPL'),
    ('combustivel', 'G.P.L.',                'GPL'),
    -- ✘ AUSENTES (quarentena): H2, Biogás, Solar, Ar

    -- -------------------------------------------------------------------------
    -- TIPOS DE AUTOMÓVEL  (campo = 'tipo_automovel')
    -- ✔ coberto
    -- -------------------------------------------------------------------------
    ('tipo_automovel', 'suv',               'SUV'),
    ('tipo_automovel', 'S.U.V',             'SUV'),
    ('tipo_automovel', 'Suv',               'SUV'),
    ('tipo_automovel', '4x4',               'SUV'),
    ('tipo_automovel', 'Todo-o-Terreno',    'SUV'),
    ('tipo_automovel', 'hatchback',         'Hatchback'),
    ('tipo_automovel', 'HATCHBACK',         'Hatchback'),
    ('tipo_automovel', 'Hatch',             'Hatchback'),
    ('tipo_automovel', 'citadino',          'Citadino'),
    ('tipo_automovel', 'CITADINO',          'Citadino'),
    ('tipo_automovel', 'City',              'Citadino'),
    ('tipo_automovel', 'city car',          'Citadino'),
    ('tipo_automovel', 'elétrico',          'N/A'),
    ('tipo_automovel', 'elétricos',         'N/A'),
    ('tipo_automovel', 'Eletrico',          'N/A'),
    ('tipo_automovel', 'Electrico',         'N/A')

ON CONFLICT (campo, valor_original) DO NOTHING;
"""

# ──────────────────────────────────────────────────────────────────────────────
# 3. Criação do Data Warehouse
# ──────────────────────────────────────────────────────────────────────────────

def create_data_warehouse():
    try:
        with dw_engine.begin() as conn:
            conn.execute(text(CREATE_DW_SQL))
        print("Data Warehouse criado com sucesso!")
    except Exception as e:
        print("Ocorreu um erro ao criar o Data Warehouse.")
        print(e)


if __name__ == "__main__":
    create_data_warehouse()