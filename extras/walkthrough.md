# Resumo de Desenvolvimento — Auto Escala CDGE

> **Baseline:** commit `8c84749 Correção e melhoria do bronze`  
> **Estado atual:** Finalização do Data Warehouse e Reporting (V1.0)

---

## 1. Novos Ficheiros Criados (A — Added)

### DAGs Airflow

| Ficheiro                           | Descrição                                                                                                                                                          |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `dags/auto_escala_dag.py`          | DAG mensal — processa Inventário + Trends + Fórum no 1º domingo de cada mês às 23:00 UTC. Orquestra Bronze → Silver → PostgreSQL → Prophet em tarefas sequenciais. |
| `dags/auto_escala_hashtags_dag.py` | DAG semanal — processa Hashtags todos os domingos às 23:30 UTC. Separada da mensal para respeitar a frequência diferente dos dados XML.                            |

### Scripts de Geração de Dados

| Ficheiro                       | Descrição                                                                                         |
| ------------------------------ | ------------------------------------------------------------------------------------------------- |
| `scripts/generate_trends.py`   | Gera JSONs de tendências Google Trends (por modelo/mês) com sazonalidade e crescimento simulados. |
| `scripts/generate_forum.py`    | Gera posts TXT do fórum motorguia.net com menções de marcas/modelos e sentimento variável.        |
| `scripts/generate_hashtags.py` | Gera XMLs de hashtags sociais no formato Atom Feed (Talkwalker/Mention) — semana a semana.        |
| `scripts/generate_clientes.py` | Gera dados sintéticos de clientes (NIF, Nome, Idade, Género, Localização) para análise demográfica. |
| `scripts/generate_demografia.py` | Gera estatísticas populacionais por distrito (faixas etárias, género) baseadas em dados do INE. |

### Scripts ETL e DW

| Ficheiro                      | Descrição                                                                                                                                                                                                                                |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scripts/generate_dw.py`      | DDL completo do Star Schema PostgreSQL. Inclui dimensões de clientes, demografia, factos, triggers SCD Tipo 1 e **Data Marts (Views)**. |
| `scripts/load_to_postgres.py` | Loader Silver → PostgreSQL (722 linhas). Faz UPSERT de dimensões (`dim_cliente`, `dim_demografia_regional`, etc.) e factos. Implementa lógica de Snapshot Mensal de Inventário. |
| `scripts/forecast_simple.py`  | Forecasting Heurístico Composto: integra Trends, Sentimento, Hashtags e Vendas para prever a procura do próximo mês (substituiu o Prophet). |
| `scripts/main.py`             | Orquestrador manual (604 linhas). Suporta `--mode full_load` e `--mode incremental`. Permite reset do schema, flag `--no-nlp`, e controlo de watermarks.                                                                                 |
| `scripts/simulate_batches.py` | Variante simplificada do orquestrador para simular batches mensais sem precisar do Airflow.                                                                                                                                              |

### Scripts de Suporte

| Ficheiro                                | Descrição                                                                                                                                                                              |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scripts/airflow_trigger_simulation.py` | Utilitário para disparar DAG Runs individuais via Airflow REST API (usado durante testes).                                                                                             |
| `scripts/data_profiling.py`             | Gera relatórios HTML de qualidade de dados (ydata-profiling) sobre a camada Bronze.                                                                                                    |
| `scripts/demo_simulacao.py`             | Script de demo completa (382 linhas): Reset → Full Load (2022–2023) → Simulação mês a mês do ano 2024 via Airflow API. Suporta `--skip-reset`, `--skip-full-load`, `--desde`, `--ate`. |

---

## 2. Ficheiros Modificados (M — Modified)

### `scripts/silver_pipeline.py`

O maior conjunto de alterações de toda a sessão:

- **Fix crítico de tipo (`TypeError`):** `valor_interesse` era convertido diretamente para `Int64` sem passar por `float` primeiro, causando crash na Silver. Corrigido com `.astype(float).round()` antes do cast.
- **Verbosidade configurável:** `AE_VERBOSE` env var (discreto / informativo) e flag `nlp_habilitado` para desativar o NLP (RoBERTa) em runs rápidos com `--no-nlp`.
- **NLP lazy-loading:** O modelo RoBERTa só é carregado quando necessário (não no import).
- **Forum dual-format:** O limpador de texto do fórum agora suporta o formato de bloco contínuo do `generate_forum.py` (era só multi-linha), evitando que documentos inteiros fossem descartados.
- **Análise de sentimento em janelas:** Textos longos são agora divididos em janelas de 80 palavras com overlap de 20, cobrindo o documento inteiro em vez dos primeiros 500 chars.
- **Timings e logs:** Cada fase (inventário, trends, fórum, hashtags) agora imprime o tempo de execução.
- **Pre-check TCP:** Antes de tentar ligar ao PostgreSQL, faz um socket connect rápido (timeout 3s) para não ficar pendurado se o Docker não estiver ativo.

### `scripts/generate_inventory.py`

- **Lógica de Inventário Ativo (hoje):** Substituída a geração por mês independente por um sistema de `active_inventory` por stand. Carros não vendidos transitam de mês em mês. No CSV, um carro não vendido aparece com `data_venda=""`. Só no mês em que é vendido aparece com `data_venda` preenchida e sai da lista ativa.

### `scripts/bronze_pipeline.py`

- Conversão explícita para PyArrow com `preserve_index=False` para evitar coluna extra `__index_level_0__`.
- `schema_mode="merge"` nos writes para suportar ficheiros com colunas diferentes (ex: hashtags com plataformas variáveis).

### `scripts/generate_dw.py` (hoje)

- Adicionada a tabela `fct_inventario_mensal` (Periodic Snapshot Fact Table):
  ```sql
  CREATE TABLE fct_inventario_mensal (
      inventario_key SERIAL PRIMARY KEY,
      tempo_key INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
      stand_key INTEGER NOT NULL REFERENCES dim_stand(stand_key),
      veiculo_key INTEGER NOT NULL REFERENCES dim_veiculo(veiculo_key),
      valor_em_stock NUMERIC(12,2),
      dias_em_parque INTEGER,
      UNIQUE (tempo_key, stand_key, veiculo_key)
  );
  ```
- Adicionados 3 índices: `idx_fct_inventario_tempo/stand/veiculo`.

### `scripts/load_to_postgres.py`

- **Suporte a Clientes e Demografia:** Adicionado carregamento das novas dimensões `dim_cliente` e `dim_demografia_regional`.
- **Enriquecimento de Factos:** A `fct_venda` agora inclui a `cliente_key`, permitindo cruzar vendas com perfis demográficos.
- **Datas de fim de mês:** `MonthEnd` inseridas automaticamente na `dim_tempo` para suportar snapshots.
- **Bloco ELT de Snapshot Mensal:** Query SQL otimizada que cruza `fct_venda` com datas de fim de mês para popular `fct_inventario_mensal` sem mover dados para Python.

### `docker/docker-compose.yaml`

- Adicionados serviços do **Apache Airflow 2.9**: `airflow-init`, `airflow-scheduler`, `airflow-webserver`.
- Todos os serviços na mesma rede Docker (`ae-network`).
- Volume de DAGs partilhado entre o host e os contentores Airflow.

### `README.md`

- Reescrita completa com arquitetura atualizada, tabela de serviços e credenciais, secção de Airflow, instruções de demo.
- Hoje: `fct_inventario_mensal` adicionada ao diagrama de arquitetura.

### `requirements.txt`

- Adicionados: `prophet`, `apache-airflow`, `pysentimiento`, `ydata-profiling`, `delta-rs`, `psycopg` (driver psycopg3).

---

## 3. Ficheiros Eliminados (D — Deleted)

| Ficheiro                          | Motivo                                                                                |
| --------------------------------- | ------------------------------------------------------------------------------------- |
| `scripts/criar_auto_escala_dw.py` | Substituído pelo `generate_dw.py` com schema completo, triggers, índices e seed data. |

---

## 4. Novas Tabelas no Star Schema (desde o commit silver)

| Tabela                     | Tipo               | O que contém                                                            |
| -------------------------- | ------------------ | ----------------------------------------------------------------------- |
| `dim_cliente`              | Dimensão           | Perfil de clientes (NIF, idade, género, localização).                   |
| `dim_demografia_regional`  | Dimensão           | Estatísticas do INE por distrito (população, faixas etárias, género).   |
| `dim_dicionario_veiculo`   | Dimensão auxiliar  | Lookup de normalização marca/modelo (Silver lê daqui).                   |
| `fact_trends`              | Transactional Fact | Score de interesse Google Trends (normalizado por Localização).          |
| `fact_forum_sentiment`     | Transactional Fact | Score de sentimento e menções do Fórum por modelo/mês.                   |
| `fact_previsao`            | Facto de Previsão  | Resultados da heurística de previsão (momento t+1).                     |
| `fct_inventario_mensal`    | Periodic Snapshot  | Estado do inventário por veiculo/stand no último dia de cada mês.        |
| `pipeline_control`         | Controlo           | Watermarks CDC por pipeline e camada.                                   |
| `audit_log_dimensions`     | Auditoria          | Trigger-based: regista todas as alterações SCD Tipo 1 em JSONB.         |

---

## 5. Sessão de Refatoração Final (hoje)

- **Divisão de Factos:** `fct_tendencia` foi dividida em `fact_trends` e `fact_forum_sentiment` para eliminar a esparsidade dos dados (modelos com interesse mas sem menções e vice-versa).
- **Normalização Geográfica:** `fact_trends` agora liga-se diretamente à `dim_localizacao` via `localizacao_key`.
- **Limpeza do Fórum:** Implementado filtro para ignorar blocos de texto com 0 menções reais ao modelo, reduzindo ruído no sentimento.
- **Dicionário Robusto:** Adicionados mapeamentos para "100% Elétrico" e variantes, e supressão de combustíveis que apareciam erroneamente como tipos de automóvel.
- **Consolidação:** Orquestração unificada no `main.py`, eliminando scripts duplicados de simulação e trigger de DAGs.
- **Aviso Prophet:** O script `prophet_model.py` foi descontinuado em favor do `forecast_simple.py` para garantir estabilidade e integração de métricas heterogéneas.

---

## 6. Consolidação do DW e Reporting (Fase Final)

Nesta fase de fecho, o sistema foi otimizado para consumo imediato por ferramentas de BI (Power BI):

- **Data Marts (Views de Negócio):** Criadas vistas estratégicas que simplificam o consumo de dados:
    - `vw_mart_compras`: Integra tendências, previsões e rotação histórica para apoio à decisão de stock.
    - `vw_mart_stock`: Monitoriza o envelhecimento do inventário (alertas >60d e >90d).
    - `vw_mart_direcao`: Performance global, margens de lucro e sentimento de mercado consolidado.
- **Modelo de Previsão Multicritério:** Implementada uma heurística no `forecast_simple.py` (substituindo o Prophet por questões de estabilidade) que correlaciona Trends, Sentimento e Hashtags.
- **Refatoração Profunda:**
    - **Descoberta Dinâmica de Modelos:** O loader agora associa automaticamente marcas a modelos através de uma análise multi-fonte, eliminando a necessidade de mapeamentos manuais exaustivos.
    - **Normalização Robusta:** Expansão do `dim_dicionario_veiculo` para cobrir variantes complexas de combustíveis (ex: "100% Elétrico") e tipos de veículo.
    - **Orquestração Unificada:** O `main.py` agora gere todo o ciclo de vida, desde a geração de dados até ao carregamento final, com suporte a modos `full_load` e `incremental`.
