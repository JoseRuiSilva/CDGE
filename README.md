# Auto Escala — Pipeline de Análise de Tendências

> Trabalho Prático — Ciências de Dados em Grande Escala (CDGE)  
> Licenciatura em Ciência de Dados, 3º Ano | Universidade do Minho | 2025/2026

Sistema de análise de tendências de aquisição de veículos usados para uma rede fictícia de stands em Portugal (Lisboa, Porto, Braga). O pipeline ingere dados de inventário, tendências de pesquisa, publicações em fóruns e hashtags em redes sociais, transforma-os em camadas Bronze → Silver e expõe um star schema em PostgreSQL enriquecido por um modelo preditivo (Prophet).

---

## Estrutura do Projeto

```
projeto_auto_escala/
├── data/
│   ├── sources/               # Dados brutos gerados
│   └── profiling_reports/     # Relatórios de qualidade ydata-profiling
├── data_lake/
│   ├── bronze/                # Ingestão raw em Delta Lake
│   ├── silver/                # Dados limpos e validados em Delta Lake
│   └── quarantine/            # Registos rejeitados na limpeza
├── scripts/
│   ├── generate_inventory.py  # Gerador de CSVs de inventário
│   ├── generate_trends.py     # Gerador de tendências Google (JSON)
│   ├── generate_forum.py      # Gerador de posts no fórum (TXT)
│   ├── generate_hashtags.py   # Gerador de métricas sociais (XML)
│   ├── generate_dw.py         # DDL do Star Schema no PostgreSQL com Auditoria CDC
│   ├── bronze_pipeline.py     # Ingestão → Bronze (Delta append)
│   ├── silver_pipeline.py     # Bronze → Silver (limpeza, tipagem, NLP)
│   ├── load_to_postgres.py    # Silver → PostgreSQL (UPSERT / SCD Tipo 1)
│   ├── prophet_model.py       # Modelo Preditivo (Forecasting com Facebook Prophet)
│   ├── data_profiling.py      # Relatórios HTML da qualidade de dados (Bronze)
│   ├── simulate_batches.py    # Utilitário para simulação cronológica
│   └── main.py                # Orquestrador da pipeline (Full Load / Incremental)
├── docker/
│   └── docker-compose.yaml    # PostgreSQL + pgAdmin
├── requirements.txt
└── README.md
```

---

## Pré-requisitos

- Python 3.11+
- Docker Desktop
- Power BI Desktop (para os dashboards)

---

## Início Rápido

### 1 — Clonar e configurar o ambiente

```bash
git clone https://github.com/JoseRuiSilva/projeto_auto_escala.git
cd projeto_auto_escala

pip install -r requirements.txt
```

### 2 — Arrancar a base de dados

```bash
cd docker
docker compose up -d
# pgAdmin disponível em http://localhost:5052
cd ..
```

### 3 — Gerar dados base (Histórico)

```bash
python scripts/generate_inventory.py
python scripts/generate_trends.py
python scripts/generate_forum.py
python scripts/generate_hashtags.py
```

### 4 — Iniciar a Base de Dados (Star Schema)

```bash
python scripts/generate_dw.py
```

### 5 — Correr o pipeline

Existem duas formas de executar a pipeline e carregar os dados. Recomendamos a Opção A para ver o processo CDC em ação automaticamente.

#### Opção A: Automática (Simulação Mês a Mês) [RECOMENDADO]

O script `simulate_batches.py` verifica automaticamente se o histórico (2022-2023) já foi carregado e, se não, corre o `full_load`. De seguida, processa incrementalmente os dados de 2024 em diante, mês a mês.

```bash
python scripts/simulate_batches.py
```

**Opções úteis do simulador:**
- `--pausa 2.0` : Adiciona uma pausa (em segundos) entre batches para acompanhares melhor o processo no terminal.
- `--no-nlp` : Corre a pipeline sem o modelo BERT de Sentimentos (muito mais rápido, ideal para testes rápidos).
- `--skip-full-load` : Ignora o carregamento histórico caso queiras apenas testar a lógica incremental.
- `--desde YYYY-MM --ate YYYY-MM` : Controla os meses exatos a simular na fase incremental.

#### Opção B: Manual (Passo a passo)

Se preferires ter controlo total sobre os períodos processados, podes usar o orquestrador principal `main.py`:

**1. Carga Histórica (até dezembro de 2023):**
```bash
python scripts/main.py --mode full_load --reset
```
> **Nota**: A flag `--reset` é útil para forçar uma limpeza do Data Lake e da BD antes de correr o pipeline e começar do zero.

**2. Carga Incremental Lote a Lote:**
```bash
python scripts/main.py --mode incremental --data_limite 2024-03-31
```
*(Lê os watermarks na BD e processa apenas ficheiros novos inseridos desde o último batch até à data limite).*

---

## Stack Tecnológico

| Componente | Tecnologia |
|---|---|
| Pipeline ETL | Python + pandas + ydata-profiling |
| Armazenamento local | Delta Lake (delta-rs, sem Spark) |
| Base de Dados / MDM / CDC | PostgreSQL 16 (Auditoria via Triggers + SCD 1) |
| Administração BD | pgAdmin 4 |
| Análise de Sentimento (NLP) | pysentimiento (RoBERTa em Português) |
| Contentorização | Docker Compose |
| Análise Preditiva (ML) | Facebook Prophet |
| Visualização | Power BI Desktop |

---

## Equipa

Trabalho desenvolvido no âmbito da UC de CDGE.  
Professor: Orlando Belo — obelo@di.uminho.pt