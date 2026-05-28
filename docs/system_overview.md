# Auto Escala — System Overview

## 1. Contexto e Problema de Negócio
A **Auto Escala** é uma rede nacional de stands de automóveis usados (Lisboa, Porto, Braga). O principal desafio do negócio é a **antecipação da procura**: identificar o que os clientes vão querer comprar a curto/médio prazo para otimizar o stock e evitar capital parado.

O sistema responde à pergunta: *"O que devo comprar hoje para maximizar o lucro amanhã?"*, cruzando dados internos de vendas com sinais externos de interesse (Google Trends, Fóruns, Redes Sociais).

## 2. Arquitetura do Sistema (Medallion)
O projeto utiliza uma arquitetura **Medallion** implementada sobre **Delta Lake** (sem Spark, via `delta-rs`) e um **Data Warehouse** final em **PostgreSQL**.

- **Bronze (Ingestão):** Armazena os dados brutos exatamente como chegam, adicionando metadados de rastreabilidade (`ingestion_timestamp`, `source_file`).
- **Silver (Limpeza e Qualidade):** Aplica tipagem, normalização de marcas/modelos via dicionário, limpeza de texto e **análise de sentimento (NLP RoBERTa)**. Registos inválidos são movidos para a **Quarentena**.
- **PostgreSQL (Star Schema):** Camada final otimizada para Business Intelligence (Power BI), com dimensões (`dim_veiculo`, `dim_cliente`, `dim_demografia`) e factos (`fact_venda`, `fact_trends`, `fact_forum_sentiment`).

## 3. Fontes de Dados
O sistema ingere os três tipos de dados exigidos pelo enunciado:
1.  **Estruturados (CSV):** Inventário e vendas dos stands, dados de clientes e estatísticas demográficas do INE.
2.  **Semi-estruturados (JSON/XML):** Tendências de pesquisa do Google Trends e volume de hashtags sociais (Atom Feed).
3.  **Não estruturados (Texto):** Publicações de fóruns (motorguia.net) sujeitas a análise de sentimento para medir a perceção pública de cada marca/modelo.

## 4. Modelos de Previsão (ML)
O sistema utiliza dois modelos principais para apoio à decisão:
- **M1 — Tendências de Mercado (SARIMA):** Analisa séries temporais de interesse (Trends) e sentimento para prever o volume de procura no mês seguinte.
- **M3 — Expected Gain (XGBoost):** Modelo de regressão que utiliza *lag features*, dados de inventário e demografia para prever o lucro esperado por modelo/região, permitindo priorizar aquisições.

## 5. Orquestração e Simulação
- **Apache Airflow:** Gere o pipeline com dois DAGs:
    - `auto_escala_pipeline` (Mensal): Processa o ciclo completo de inventário e treino de modelos.
    - `auto_escala_hashtags_semanal` (Semanal): Monitoriza o volume social.
- **Modo Demo (`main.py`):** Permite simular o progresso do projeto (2022 a 2024) em poucos minutos, disparando os DAGs via API para demonstrar a evolução dos dashboards.

## 6. Justificação Big Data
- **Volume:** Escalável para centenas de stands e milhares de transações mensais.
- **Variedade:** Integração de CSV, JSON, XML e Texto Livre.
- **Velocidade:** Pipeline incremental com **Change Data Capture (CDC)** baseado em watermarks no PostgreSQL.
- **Veracidade:** Camada Silver com validação rigorosa e sistema de auditoria/quarentena.

---
*Documento gerado como parte da documentação técnica do projeto CDGE — Auto Escala.*
