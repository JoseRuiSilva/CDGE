from sqlalchemy import create_engine, text

# ==========================================
# 1. Configuração da ligação ao PostgreSQL
# ==========================================

DW_URL = "postgresql+psycopg://ae_user:ae_pass_2026@localhost:5432/auto_escala"
dw_engine=create_engine(DW_URL,echo=False)
#para usar têm de colocar no terminal python criar_auto_escala_dw.py

# ==========================================
# 2. Script SQL para criação do Star Schema
# ==========================================

CREATE_DW_SQL="""
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

CREATE TABLE dim_veiculo (
    veiculo_key SERIAL PRIMARY KEY,
    id_viatura VARCHAR(50) UNIQUE,
    matricula VARCHAR(20),
    marca VARCHAR(100),
    modelo VARCHAR(100),
    tipo_automovel VARCHAR(100),
    num_lugares INTEGER,
    ano_viatura INTEGER,
    combustivel VARCHAR(100),
    quilometragem INTEGER
);

CREATE TABLE dim_dicionario_veiculo (
    dicionario_key SERIAL PRIMARY KEY,
    campo VARCHAR(100) NOT NULL,
    valor_original VARCHAR(255) NOT NULL,
    valor_normalizado VARCHAR(255) NOT NULL,
    ativo BOOLEAN DEFAULT TRUE,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (campo, valor_original)
);

-- =========================
-- FACTOS
-- =========================

CREATE TABLE fact_vendas (
    venda_key SERIAL PRIMARY KEY,
    veiculo_key INTEGER NOT NULL REFERENCES dim_veiculo(veiculo_key),
    stand_key INTEGER NOT NULL REFERENCES dim_stand(stand_key),
    tempo_entrada_key INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    tempo_venda_key INTEGER REFERENCES dim_tempo(tempo_key),
    preco_aquisicao NUMERIC(12,2),
    preco_venda NUMERIC(12,2),
    margem NUMERIC(12,2),
    dias_em_stock INTEGER,
    vendido BOOLEAN DEFAULT FALSE
);

CREATE TABLE fact_tendencias (
    tendencia_key SERIAL PRIMARY KEY,
    tempo_key INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    fonte_key INTEGER NOT NULL REFERENCES dim_fonte(fonte_key),
    marca VARCHAR(100),
    modelo VARCHAR(100),
    tipo_automovel VARCHAR(100),
    combustivel VARCHAR(100),
    visualizacoes INTEGER DEFAULT 0,
    pesquisas INTEGER DEFAULT 0,
    contactos INTEGER DEFAULT 0,
    indice_tendencia NUMERIC(10,4)
);

CREATE TABLE fact_hashtag_volume (
    hashtag_volume_key SERIAL PRIMARY KEY,
    tempo_key INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    fonte_key INTEGER NOT NULL REFERENCES dim_fonte(fonte_key),
    hashtag_key INTEGER NOT NULL REFERENCES dim_hashtag(hashtag_key),
    volume INTEGER NOT NULL DEFAULT 0
);

-- =========================
-- CONTROLO DO PIPELINE
-- =========================

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

-- =========================
-- ÍNDICES
-- =========================

CREATE INDEX idx_fact_vendas_veiculo ON fact_vendas(veiculo_key);
CREATE INDEX idx_fact_vendas_stand ON fact_vendas(stand_key);
CREATE INDEX idx_fact_vendas_tempo_entrada ON fact_vendas(tempo_entrada_key);
CREATE INDEX idx_fact_vendas_tempo_venda ON fact_vendas(tempo_venda_key);

CREATE INDEX idx_fact_tendencias_tempo ON fact_tendencias(tempo_key);
CREATE INDEX idx_fact_tendencias_fonte ON fact_tendencias(fonte_key);
CREATE INDEX idx_fact_tendencias_marca_modelo ON fact_tendencias(marca, modelo);

CREATE INDEX idx_fact_hashtag_volume_tempo ON fact_hashtag_volume(tempo_key);
CREATE INDEX idx_fact_hashtag_volume_hashtag ON fact_hashtag_volume(hashtag_key);
"""

# ==========================================
# 3. Função de criação do Data Warehouse
# ==========================================

def create_data_warehouse():
    """
    Cria o schema auto_escala_dw no PostgreSQL e todas as tabelas
    do modelo dimensional em estrela.
    """
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