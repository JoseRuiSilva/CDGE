# Auto Escala — Pipeline de Análise de Tendências

> Trabalho Prático — Ciências de Dados em Grande Escala (CDGE)  
> Licenciatura em Ciência de Dados, 3º Ano | Universidade do Minho | 2025/2026

Sistema de análise de tendências de aquisição de veículos usados para uma rede fictícia de stands em Portugal (Lisboa, Porto, Braga). O pipeline ingere dados de inventário, tendências de pesquisa e reviews, transforma-os em camadas Bronze → Silver → Gold e expõe um star schema em PostgreSQL para visualização em Power BI.

---

## Estrutura do Projeto

```
projeto_auto_escala/
├── data/
│   └── sources/               # Dados brutos gerados (ignorado pelo git)
│       ├── stands/            # CSVs mensais por stand
│       ├── trends/            # google_trends.json
│       └── reviews/           # reviews.csv
├── data_lake/
│   ├── bronze/                # Ingestão raw em Delta Lake
│   ├── silver/                # Dados limpos e validados
│   └── gold/                  # Agregados prontos para consumo
├── scripts/
│   ├── generate_inventory.py  # Gerador de CSVs de inventário
│   ├── generate_trends.py     # Gerador de tendências Google
│   ├── generate_reviews.py    # Gerador de reviews
│   ├── bronze_pipeline.py     # Ingestão → Bronze (Delta append)
│   ├── silver_pipeline.py     # Bronze → Silver (limpeza, validação)
│   ├── gold_pipeline.py       # Silver → Gold (agregações)
│   ├── cdc_pipeline.py        # Controlo de watermark incremental
│   ├── load_to_postgres.py    # UPSERT no star schema PostgreSQL
│   └── main.py                # Orquestrador (--mode, --data_limite)
├── docker/
│   ├── docker-compose.yaml
│   └── .env.example
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
git clone https://github.com/JoseRuiSilva/CDGE.git
cd projeto_auto_escala

pip install -r requirements.txt
```

### 2 — Arrancar a base de dados

```bash
cd docker
docker compose up -d
# pgAdmin disponível em http://localhost:5052
```

### 3 — Gerar dados de exemplo

```bash
python scripts/generate_inventory.py
python scripts/generate_trends.py
#python scripts/generate_reviews.py
```

### 4 — Correr o pipeline completo (a partir daqui AINDA NÃO)

```bash
python scripts/main.py --mode full
```

### Simulação mês a mês (demo)

```bash
python scripts/main.py --mode batch --data_limite 2024-12-31
```

---

## Stack Tecnológico

| Componente | Tecnologia |
|---|---|
| Pipeline ETL | Python + pandas |
| Armazenamento local | Delta Lake (delta-rs, sem Spark) |
| Base de dados | PostgreSQL 16 |
| Administração BD | pgAdmin 4 |
| Contentorização | Docker Compose |
| Análise preditiva | Prophet |
| Visualização | Power BI Desktop |

---

## Equipa

Trabalho desenvolvido por 4 elementos no âmbito da UC de CDGE.  
Professor: Orlando Belo — obelo@di.uminho.pt