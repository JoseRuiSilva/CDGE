# Auto Escala — Pipeline de Análise de Tendências

> Trabalho Prático — Ciências de Dados em Grande Escala (CDGE)  
> Licenciatura em Ciência de Dados, 3º Ano | Universidade do Minho | 2025/2026

Sistema de análise de tendências de aquisição de veículos usados para uma rede fictícia de stands em Portugal (Lisboa, Porto, Braga). O pipeline ingere dados de inventário, tendências de pesquisa, publicações em fóruns e hashtags sociais, transforma-os em camadas Bronze → Silver, expõe um star schema em PostgreSQL enriquecido por um modelo preditivo (Prophet), e é **orquestrado automaticamente pelo Apache Airflow**.

---

## Arquitectura

```
Landing Zone (data/sources/)
       │
       ▼
  Bronze Delta Lake   ← append + deduplicação por source_file
       │
       ▼
  Silver Delta Lake   ← limpeza, tipagem, NLP, quarentena
       │
       ▼
PostgreSQL Star Schema (auto_escala_dw)
  ├── dim_stand / dim_modelo / dim_veiculo / dim_tempo / dim_fonte / dim_hashtag
  ├── fct_venda / fct_tendencia / fct_hashtag_volume
  ├── data_quality_log / pipeline_control (watermarks CDC)
  └── audit_log_dimensions (trigger-based SCD Tipo 1)
       │
       ▼
  Facebook Prophet    ← previsão de interesse por modelo/marca
```

### Agendamento Airflow

| DAG | Schedule | Fontes |
|---|---|---|
| `auto_escala_pipeline` | 1º domingo do mês, 23:00 UTC | Inventário + Trends + Fórum |
| `auto_escala_hashtags_semanal` | Todos os domingos, 23:30 UTC | Hashtags (XML semanal) |

> **Justificação:** os stands estão fechados ao domingo. O pipeline corre durante a noite, os resultados estão prontos quando o stand abre na segunda-feira.

---

## Estrutura do Projecto

```
projeto_auto_escala/
├── dags/
│   ├── auto_escala_dag.py            # DAG mensal (inventário + trends + fórum)
│   └── auto_escala_hashtags_dag.py   # DAG semanal (hashtags)
├── data/
│   ├── sources/                      # Landing Zone (gerada pelos scripts generate_*)
│   └── profiling_reports/            # Relatórios HTML ydata-profiling
├── data_lake/
│   ├── bronze/                       # Ingestão raw em Delta Lake
│   ├── silver/                       # Dados limpos e validados em Delta Lake
│   └── quarantine/                   # Registos rejeitados na validação Silver
├── docker/
│   └── docker-compose.yaml           # PostgreSQL 16 + pgAdmin 4 + Apache Airflow 2.9
├── scripts/
│   ├── generate_inventory.py         # Gerador de CSVs de inventário (stands)
│   ├── generate_trends.py            # Gerador de JSON de tendências Google
│   ├── generate_forum.py             # Gerador de posts TXT do fórum
│   ├── generate_hashtags.py          # Gerador de métricas XML de hashtags
│   ├── generate_dw.py                # DDL do Star Schema + lookup table de normalização
│   ├── bronze_pipeline.py            # Landing Zone → Bronze (Delta append)
│   ├── silver_pipeline.py            # Bronze → Silver (limpeza, NLP, quarentena)
│   ├── load_to_postgres.py           # Silver → PostgreSQL (UPSERT / SCD Tipo 1)
│   ├── prophet_model.py              # Forecasting com Facebook Prophet
│   ├── data_profiling.py             # Relatórios de qualidade de dados (Bronze)
│   ├── main.py                       # Orquestrador (Full Load / Incremental)
│   ├── demo_simulacao.py             # Demo completa via Airflow REST API  ← USAR PARA DEMO
│   └── airflow_trigger_simulation.py # Utilitário de trigger individual por DAG
├── extras/
│   └── lembretes.txt                 # Notas internas da equipa
├── requirements.txt
└── README.md
```

---

## Pré-requisitos

- Python 3.11+
- Docker Desktop (com pelo menos 4 GB de RAM alocados)

---

## Início Rápido — Demo Completa

### 1 — Clonar e instalar dependências

```bash
git clone https://github.com/JoseRuiSilva/projeto_auto_escala.git
cd projeto_auto_escala
pip install -r requirements.txt
```

### 2 — Gerar os dados históricos (Landing Zone)

```bash
python scripts/generate_inventory.py
python scripts/generate_trends.py
python scripts/generate_forum.py
python scripts/generate_hashtags.py
```

### 3 — Arrancar todos os serviços (Docker)

```bash
cd docker
docker compose up -d
cd ..
```

Serviços disponíveis após arranque (~2 min):

| Serviço | URL | Credenciais |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| pgAdmin | http://localhost:5052 | admin@autoescala.pt / admin2026 |
| PostgreSQL | localhost:5432 | ae_user / ae_pass_2026 |

### 4 — Correr a demo completa

```bash
python scripts/demo_simulacao.py
```

O script faz automaticamente, por esta ordem:

1. **Reset** — recria o schema PostgreSQL e limpa o Delta Lake
2. **Full Load** — processa o histórico 2022–2023 via Airflow
3. **Simulação 2024** — simula semana a semana (hashtags) e mês a mês (restantes fontes), esperando que cada DAG Run conclua antes de iniciar o seguinte

Podes acompanhar o progresso em tempo real na **Airflow UI** (http://localhost:8080).

**Opções do script:**

```bash
# Apenas a simulação (sem reset nem full load)
python scripts/demo_simulacao.py --skip-reset --skip-full-load

# Um mês específico
python scripts/demo_simulacao.py --skip-reset --skip-full-load --desde 2024-06 --ate 2024-06

# Full load sem simulação
python scripts/demo_simulacao.py --skip-simulacao
```

---

## Execução Manual (alternativa ao Airflow)

Para executar o pipeline directamente sem Airflow:

```bash
# Carga histórica completa (2022-2023)
python scripts/main.py --mode full_load

# Carga incremental (novos ficheiros desde o último watermark)
python scripts/main.py --mode incremental --data_limite 2024-03-31 --no-nlp
```

---

## Stack Tecnológico

| Componente | Tecnologia |
|---|---|
| Orquestração | Apache Airflow 2.9 (Docker, LocalExecutor) |
| Pipeline ETL | Python 3.11 + pandas + ydata-profiling |
| Armazenamento local | Delta Lake (delta-rs, sem Spark) |
| Base de Dados | PostgreSQL 16 (Star Schema, triggers CDC, SCD Tipo 1) |
| Administração BD | pgAdmin 4 |
| Análise de Sentimento | pysentimiento (RoBERTa em Português) |
| Contentorização | Docker Compose |
| Análise Preditiva | Facebook Prophet |

---

## Equipa

Trabalho desenvolvido no âmbito da UC de CDGE.  
Professor: Orlando Belo — obelo@di.uminho.pt