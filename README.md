# Auto Escala — Pipeline de Análise de Tendências

> Trabalho Prático — Ciências de Dados em Grande Escala (CDGE)  
> Licenciatura em Ciência de Dados, 3º Ano | Universidade do Minho | 2025/2026

**Auto Escala** é um sistema de análise de tendências de aquisição de veículos usados para uma rede de stands em Portugal. O pipeline ingere dados de inventário, tendências de pesquisa, fóruns e hashtags sociais, transformando-os numa arquitetura Medallion (Bronze/Silver/Gold) para alimentar modelos preditivos e dashboards de Business Intelligence.

---

## 🏗️ Arquitetura

O sistema segue o padrão **Medallion Architecture** orquestrado pelo **Apache Airflow**:

```text
Landing Zone (data/sources/)
   │
   ▼
Bronze Delta Lake   ← Ingestão bruta (Append)
   │
   ▼
Silver Delta Lake   ← Limpeza, NLP Sentiment, Normalização
   │
   ▼
PostgreSQL DW       ← Star Schema (Dimensões e Factos)
   │
   ▼
ML Models (SARIMA/XGBoost) → Previsão de Ganho e Procura
```

---

## 🚀 Estado do Projeto

- ✅ **Data Warehouse:** Schema relacional completo com Views de Negócio (`vw_mart_*`).
- ✅ **ML Pipeline:** Modelos M1 (SARIMA) e M3 (XGBoost) integrados e validados.
- ✅ **Orquestração:** DAGs Airflow configuradas para execução Mensal e Semanal.
- ✅ **Demo Automática:** Script `main.py --mode demo` para simulação completa do histórico.

---

## 📖 Documentação Técnica

Toda a documentação detalhada do sistema encontra-se na pasta [`docs/`](docs/):

- 📄 [**System Overview**](docs/system_overview.md) — Visão geral da arquitetura, fontes e modelos.
- 📊 [**Modelo Dimensional**](docs/mod-dim.mmd) — Diagrama Entidade-Relacionamento do Star Schema.
- 🔄 [**Pipeline BPMN**](docs/bpmn.mmd) — Fluxo de dados detalhado da Medallion.
- 🕐 [**Orquestração Airflow**](docs/airflow.mmd) — Estrutura dos DAGs e cronogramas.

---

## 🛠️ Instalação e Execução

### 1. Requisitos

- Python 3.10+
- Docker & Docker Compose

### 2. Setup (Uma linha de cada vez)

```bash
git clone https://github.com/JoseRuiSilva/projeto_auto_escala.git
cd projeto_auto_escala
pip install -r requirements.txt

# Iniciar infraestrutura (DB + Airflow)
cd docker
docker compose up -d
cd ..
```

### 3. Execução Demo

Para popular o sistema com dados históricos (2022-2024) e simular a operação:

```bash
python scripts/main.py --mode demo --aguardar
```

O comando faz automaticamente:

1. **Full Load (2022–2023)** — Processa o histórico via Airflow.
2. **Simulação 2024** — Dispara batches mensais sequencialmente, aguardando que cada um termine antes de iniciar o seguinte.

Podes acompanhar o progresso em tempo real na **Airflow UI** (http://localhost:8080).

**Outros Modos de Simulação:**

```bash
# Simulação local (sem precisar do Airflow - útil para debug rápido)
python scripts/main.py --mode simulate --desde 2024-01 --ate 2024-06

# Reset completo do ambiente (Data Lake + Data Warehouse)
python scripts/main.py --mode reset

# Simulação sem NLP (processamento ultra-rápido)
python scripts/main.py --mode demo --no-nlp
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

| Componente            | Tecnologia                                            |
| --------------------- | ----------------------------------------------------- |
| Orquestração          | Apache Airflow 2.9 (Docker, LocalExecutor)            |
| Pipeline ETL          | Python 3.11 + pandas + ydata-profiling                |
| Armazenamento local   | Delta Lake (delta-rs, sem Spark)                      |
| Base de Dados         | PostgreSQL 16 (Star Schema, triggers CDC, SCD Tipo 1) |
| Administração BD      | pgAdmin 4                                             |
| Análise de Sentimento | pysentimiento (RoBERTa em Português)                  |
| Contentorização       | Docker Compose                                        |
| Análise Preditiva     | Facebook Prophet                                      |

---

## Estrutura do Repositório

- `scripts/`: Pipelines ETL, modelos ML e geradores de dados.
- `docs/`: Documentação técnica e diagramas Mermaid.
- `dags/`: Definições de workflows para o Apache Airflow.
- `docker/`: Configurações de infraestrutura (PostgreSQL, pgAdmin, Airflow).

## Equipa

Trabalho desenvolvido para a UC de CDGE.  
**Docente:** Orlando Belo
