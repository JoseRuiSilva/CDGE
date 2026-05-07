import os
from sqlalchemy import create_engine, text

# ==========================================
# 1. Configuracao da ligacao ao PostgreSQL
# ==========================================

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
DW_URL     = f"postgresql+psycopg://ae_user:ae_pass_2026@{_PG_HOST}:{_PG_PORT}/auto_escala"
dw_engine  = create_engine(DW_URL, echo=False)

# ==========================================
# 2. Script SQL para criação do Star Schema
# ==========================================

CREATE_DW_SQL = """
DROP SCHEMA IF EXISTS auto_escala_dw CASCADE;
CREATE SCHEMA auto_escala_dw;
SET search_path TO auto_escala_dw;

-- =========================
-- DIMENSÕES
-- =========================

CREATE TABLE dim_stand (
    stand_key SERIAL PRIMARY KEY,
    nome_stand VARCHAR(100) NOT NULL UNIQUE,
    cidade VARCHAR(100),
    distrito VARCHAR(100),
    pais VARCHAR(100) DEFAULT 'Portugal'
);

CREATE TABLE dim_tempo (
    tempo_key SERIAL PRIMARY KEY,
    data DATE NOT NULL UNIQUE,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    trimestre INTEGER NOT NULL,
    nome_mes VARCHAR(20),
    semana_ano INTEGER
);

CREATE TABLE dim_fonte (
    fonte_key SERIAL PRIMARY KEY,
    nome_fonte VARCHAR(100) NOT NULL UNIQUE,
    tipo_fonte VARCHAR(50) NOT NULL,
    descricao TEXT
);

CREATE TABLE dim_hashtag (
    hashtag_key SERIAL PRIMARY KEY,
    hashtag VARCHAR(100) NOT NULL UNIQUE,
    categoria VARCHAR(100)
);

-- SNOWFLAKE: Dimensão abstrata do modelo do carro (para cruzar com tendências)
CREATE TABLE dim_modelo (
    modelo_key SERIAL PRIMARY KEY,
    marca VARCHAR(100) NOT NULL,
    modelo VARCHAR(100) NOT NULL,
    tipo_automovel VARCHAR(100),
    combustivel VARCHAR(100),
    UNIQUE (marca, modelo)
);

-- Dimensão específica do veículo físico (liga-se ao modelo)
CREATE TABLE dim_veiculo (
    veiculo_key SERIAL PRIMARY KEY,
    id_viatura VARCHAR(50) UNIQUE,
    matricula VARCHAR(20),
    modelo_key INTEGER NOT NULL REFERENCES dim_modelo(modelo_key),
    num_lugares INTEGER,
    ano_viatura INTEGER
);

-- Dicionário de normalização — Silver lê daqui para resolver variantes gráficas.
CREATE TABLE dim_dicionario_veiculo (
    dicionario_key SERIAL PRIMARY KEY,
    campo VARCHAR(50) NOT NULL,
    valor_original VARCHAR(255) NOT NULL,
    valor_normalizado VARCHAR(255) NOT NULL,
    ativo BOOLEAN DEFAULT TRUE,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (campo, valor_original)
);

-- =========================
-- FACTOS
-- =========================

CREATE TABLE fct_venda (
    venda_key SERIAL PRIMARY KEY,
    veiculo_key INTEGER NOT NULL REFERENCES dim_veiculo(veiculo_key),
    stand_key INTEGER NOT NULL REFERENCES dim_stand(stand_key),
    tempo_entrada_key INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    tempo_venda_key INTEGER REFERENCES dim_tempo(tempo_key),
    quilometragem INTEGER,
    preco_aquisicao NUMERIC(12,2),
    preco_venda NUMERIC(12,2),
    margem NUMERIC(12,2),
    dias_em_stock INTEGER,
    vendido BOOLEAN DEFAULT FALSE,
    UNIQUE (veiculo_key, stand_key, tempo_entrada_key)
);

CREATE TABLE fct_tendencia (
    tendencia_key SERIAL PRIMARY KEY,
    tempo_key INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    fonte_key INTEGER NOT NULL REFERENCES dim_fonte(fonte_key),
    modelo_key INTEGER REFERENCES dim_modelo(modelo_key),
    valor_interesse INTEGER,
    score_sentimento NUMERIC(5,4),
    crescimento_mom_pct NUMERIC(10,4),
    delta_sentimento NUMERIC(5,4),
    previsao_prox_mes NUMERIC(10,4),
    UNIQUE (tempo_key, fonte_key, modelo_key)
);

CREATE TABLE fct_hashtag_volume (
    hashtag_volume_key SERIAL PRIMARY KEY,
    tempo_key INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    fonte_key INTEGER NOT NULL REFERENCES dim_fonte(fonte_key),
    hashtag_key INTEGER NOT NULL REFERENCES dim_hashtag(hashtag_key),
    volume INTEGER NOT NULL DEFAULT 0,
    posts_instagram INTEGER DEFAULT 0,
    posts_twitter INTEGER DEFAULT 0,
    posts_youtube INTEGER DEFAULT 0,
    variacao_semanal NUMERIC(10,4),
    UNIQUE (tempo_key, fonte_key, hashtag_key)
);

-- =========================
-- QUALIDADE DE DADOS & LOGS (TRIGGER-BASED CDC)
-- =========================

CREATE TABLE data_quality_log (
    id                   SERIAL PRIMARY KEY,
    fonte                VARCHAR(50)  NOT NULL,
    data_run             TIMESTAMPTZ  DEFAULT CURRENT_TIMESTAMP,
    total_registos       INTEGER,
    registos_ok          INTEGER,
    registos_quarentena  INTEGER,
    taxa_quarentena_pct  NUMERIC(6,2),
    campo_mais_nulo      VARCHAR(100),
    notas                TEXT
);

CREATE TABLE pipeline_control (
    pipeline_id SERIAL PRIMARY KEY,
    nome_pipeline VARCHAR(100) NOT NULL,
    camada VARCHAR(50) NOT NULL,
    estado VARCHAR(50) NOT NULL,
    data_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_fim TIMESTAMP,
    ficheiro_origem TEXT,
    linhas_lidas INTEGER DEFAULT 0,
    linhas_processadas INTEGER DEFAULT 0,
    linhas_rejeitadas INTEGER DEFAULT 0,
    mensagem_erro TEXT
);

-- Tabela de Audit Log para capturar alterações SCD Tipo 1 (Trigger-Based CDC)
CREATE TABLE audit_log_dimensions (
    log_id SERIAL PRIMARY KEY,
    tabela_afetada VARCHAR(50) NOT NULL,
    operacao VARCHAR(10) NOT NULL,
    registo_antigo JSONB,
    registo_novo JSONB,
    data_alteracao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Função genérica de trigger para gravar logs em JSON
CREATE OR REPLACE FUNCTION log_dimension_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD IS DISTINCT FROM NEW THEN
        INSERT INTO audit_log_dimensions (tabela_afetada, operacao, registo_antigo, registo_novo)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD)::jsonb, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit_log_dimensions (tabela_afetada, operacao, registo_antigo)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD)::jsonb);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Acoplar o trigger às dimensões que queremos auditar
CREATE TRIGGER trg_audit_dim_stand
    AFTER UPDATE OR DELETE ON dim_stand
    FOR EACH ROW EXECUTE FUNCTION log_dimension_changes();

CREATE TRIGGER trg_audit_dim_modelo
    AFTER UPDATE OR DELETE ON dim_modelo
    FOR EACH ROW EXECUTE FUNCTION log_dimension_changes();

CREATE TRIGGER trg_audit_dim_veiculo
    AFTER UPDATE OR DELETE ON dim_veiculo
    FOR EACH ROW EXECUTE FUNCTION log_dimension_changes();

-- =========================
-- ÍNDICES
-- =========================

CREATE INDEX idx_fct_venda_veiculo ON fct_venda(veiculo_key);
CREATE INDEX idx_fct_venda_stand ON fct_venda(stand_key);
CREATE INDEX idx_fct_venda_tempo_entrada ON fct_venda(tempo_entrada_key);
CREATE INDEX idx_fct_venda_tempo_venda ON fct_venda(tempo_venda_key);

CREATE INDEX idx_fct_tendencia_tempo ON fct_tendencia(tempo_key);
CREATE INDEX idx_fct_tendencia_fonte ON fct_tendencia(fonte_key);
CREATE INDEX idx_fct_tendencia_modelo ON fct_tendencia(modelo_key);

CREATE INDEX idx_fct_hashtag_volume_tempo ON fct_hashtag_volume(tempo_key);
CREATE INDEX idx_fct_hashtag_volume_hashtag ON fct_hashtag_volume(hashtag_key);

-- =========================
-- SEED: dicionário de normalização e Tipos Genéricos
-- =========================

INSERT INTO dim_modelo (marca, modelo, tipo_automovel, combustivel) VALUES
    ('N/A', 'N/A', 'SUV', 'N/A'),
    ('N/A', 'N/A', 'Elétrico', 'N/A'),
    ('N/A', 'N/A', 'Híbrido', 'N/A')
ON CONFLICT (marca, modelo) DO NOTHING;

INSERT INTO dim_fonte (nome_fonte, tipo_fonte, descricao) VALUES
    ('Inventário Stands', 'Interna', 'Ficheiros CSV diários/mensais dos stands'),
    ('Google Trends', 'Externa', 'Score de interesse de pesquisa no Google em Portugal'),
    ('Fórum motorguia.net', 'Externa', 'Posts não estruturados submetidos a análise de sentimento NLP'),
    ('Hashtags Sociais', 'Externa', 'Feed XML de parceiro (Talkwalker/Mention)')
ON CONFLICT (nome_fonte) DO NOTHING;

INSERT INTO dim_dicionario_veiculo (campo, valor_original, valor_normalizado) VALUES
    -- Marcas
    ('marca', 'volkswagen',    'Volkswagen'),
    ('marca', 'vw',            'Volkswagen'),
    ('marca', 'mercedes',      'Mercedes'),
    ('marca', 'bmw',           'BMW'),
    ('marca', 'peugeot',       'Peugeot'),
    ('marca', 'nissan',        'Nissan'),
    ('marca', 'seat',          'Seat'),
    ('marca', 'renault',       'Renault'),
    ('marca', 'citroën',       'Citroën'),
    ('marca', 'fiat',          'Fiat'),
    ('marca', 'audi',          'Audi'),
    ('marca', 'tesla',         'Tesla'),
    ('marca', 'hyundai',       'Hyundai'),
    ('marca', 'kia',           'Kia'),
    -- Modelos
    ('modelo', 'gla',          'GLA'),
    ('modelo', 'x1',           'X1'),
    ('modelo', 'tiguan',       'Tiguan'),
    ('modelo', '3008',         '3008'),
    ('modelo', 'qashqai',      'Qashqai'),
    ('modelo', 'arona',        'Arona'),
    ('modelo', 'golf',         'Golf'),
    ('modelo', 'golf mk7',     'Golf'),
    ('modelo', 'clio',         'Clio'),
    ('modelo', 'ibiza',        'Ibiza'),
    ('modelo', 'c3',           'C3'),
    ('modelo', '500',          '500'),
    ('modelo', '208',          '208'),
    ('modelo', 'série 1',      'Série 1'),
    ('modelo', 'serie 1',      'Série 1'),
    ('modelo', 'classe a',     'Classe A'),
    ('modelo', 'a3',           'A3'),
    ('modelo', 'model 3',      'Model 3'),
    ('modelo', 'zoe',          'Zoe'),
    ('modelo', 'leaf',         'Leaf'),
    ('modelo', 'kona',         'Kona'),
    ('modelo', 'niro',         'Niro'),
    ('modelo', 'yaris',        'Yaris'),
    ('modelo', 'toyota yaris', 'Yaris'),
    ('modelo', 'série 3',      'Série 3'),
    ('modelo', 'serie 3',      'Série 3'),
    ('modelo', 'bmw série 3',  'Série 3'),
    ('modelo', 'bmw serie 3',  'Série 3'),
    ('modelo', '320d',         'Série 3'),
    ('modelo', 'classe c',     'Classe C'),
    ('modelo', 'ateca',        'Ateca'),
    ('modelo', 'seat ateca',   'Ateca'),
    ('modelo', 'karoq',        'Karoq'),
    ('modelo', 'skoda karoq',  'Karoq'),
    -- Hashtags
    ('hashtag', 'volkswagengolf', 'Golf'),
    ('hashtag', 'toyotayaris',    'Yaris'),
    ('hashtag', 'bmw',            'BMW'),
    ('hashtag', 'mercedes',       'Mercedes'),
    ('hashtag', 'suv',            'SUV'),
    ('hashtag', 'carroeletrico',  'Elétrico'),
    ('hashtag', 'hibrido',        'Híbrido')
ON CONFLICT (campo, valor_original) DO NOTHING;
"""

# ==========================================
# 3. Função de criação do Data Warehouse
# ==========================================

def create_data_warehouse():
    try:
        with dw_engine.begin() as conn:
            conn.execute(text(CREATE_DW_SQL))
        print("Data Warehouse criado com sucesso!")

    except Exception as e:
        print("Ocorreu um erro ao criar o Data Warehouse.")
        print(e)

# ==========================================
# 4. Ponto de entrada
# ==========================================

if __name__ == "__main__":
    create_data_warehouse()