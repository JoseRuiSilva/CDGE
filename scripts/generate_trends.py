from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


# ============================================================
# Configuração geral do gerador
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_BASE = BASE_DIR / "data" / "sources" / "trends"

DATA_INICIO = datetime(2022, 1, 1)
DATA_FIM = datetime(2026, 5, 1)

SEED = 42

REGIOES = ["Lisboa", "Porto", "Braga"]


# ============================================================
# Catálogo central de veículos
# ============================================================
# Este catálogo deve ser mantido igual nos restantes scripts:
#   - generate_inventory.py
#   - generate_trends.py
#   - generate_forum.py
#   - generate_hashtags.py
#
# A vantagem é garantir que todas as fontes sintéticas falam das
# mesmas marcas/modelos, facilitando a integração e melhorando os
# dados disponíveis para previsão.

@dataclass(frozen=True)
class VehicleModel:
    marca: str
    modelo: str
    tipo: str
    num_lugares: int
    combustiveis: tuple[str, ...]
    base_price: int
    age_target: int
    km_range: tuple[int, int]


VEHICLES: tuple[VehicleModel, ...] = (
    # Volkswagen
    VehicleModel("Volkswagen", "Golf", "Hatchback", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 18500, 5, (30000, 140000)),
    VehicleModel("Volkswagen", "Polo", "Citadino", 5, ("Gasolina", "Gasóleo"), 14200, 5, (25000, 135000)),
    VehicleModel("Volkswagen", "T-Cross", "SUV", 5, ("Gasolina", "Gasóleo"), 19700, 4, (18000, 105000)),
    VehicleModel("Volkswagen", "T-Roc", "SUV", 5, ("Gasolina", "Gasóleo"), 22600, 4, (20000, 115000)),
    VehicleModel("Volkswagen", "Tiguan", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 24800, 5, (35000, 125000)),
    VehicleModel("Volkswagen", "ID.3", "Elétrico", 5, ("100% Elétrico",), 26800, 3, (12000, 85000)),
    VehicleModel("Volkswagen", "ID.4", "Elétrico", 5, ("100% Elétrico",), 33200, 3, (14000, 90000)),

    # Toyota
    VehicleModel("Toyota", "Yaris", "Citadino", 5, ("Gasolina", "Híbrido a Gasolina"), 15400, 4, (18000, 110000)),
    VehicleModel("Toyota", "Corolla", "Hatchback", 5, ("Gasolina", "Híbrido a Gasolina"), 21800, 4, (20000, 120000)),
    VehicleModel("Toyota", "C-HR", "SUV", 5, ("Híbrido a Gasolina",), 23600, 4, (18000, 105000)),
    VehicleModel("Toyota", "RAV4", "SUV", 5, ("Gasolina", "Híbrido a Gasolina"), 31500, 5, (30000, 135000)),

    # Peugeot
    VehicleModel("Peugeot", "208", "Citadino", 5, ("Gasolina", "Gasóleo", "100% Elétrico"), 14800, 3, (14000, 95000)),
    VehicleModel("Peugeot", "308", "Hatchback", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 20500, 4, (22000, 120000)),
    VehicleModel("Peugeot", "2008", "SUV", 5, ("Gasolina", "Gasóleo", "100% Elétrico"), 21300, 4, (18000, 105000)),
    VehicleModel("Peugeot", "3008", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 22100, 4, (26000, 118000)),
    VehicleModel("Peugeot", "5008", "SUV", 7, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 28400, 5, (30000, 135000)),

    # Renault
    VehicleModel("Renault", "Clio", "Citadino", 5, ("Gasolina", "Gasóleo", "GPL"), 12400, 4, (25000, 135000)),
    VehicleModel("Renault", "Captur", "SUV", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 17900, 4, (22000, 115000)),
    VehicleModel("Renault", "Mégane", "Hatchback", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 18400, 5, (30000, 140000)),
    VehicleModel("Renault", "Arkana", "SUV", 5, ("Gasolina", "Híbrido a Gasolina"), 24700, 3, (15000, 85000)),
    VehicleModel("Renault", "Zoe", "Elétrico", 5, ("100% Elétrico",), 16900, 3, (10000, 70000)),

    # BMW
    VehicleModel("BMW", "Série 1", "Hatchback", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 20900, 4, (18000, 105000)),
    VehicleModel("BMW", "Série 3", "Sedan", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 29400, 5, (30000, 145000)),
    VehicleModel("BMW", "Série 5", "Sedan", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 38500, 5, (35000, 150000)),
    VehicleModel("BMW", "X1", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 27600, 4, (22000, 110000)),
    VehicleModel("BMW", "X3", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 39200, 5, (32000, 145000)),
    VehicleModel("BMW", "i3", "Elétrico", 4, ("100% Elétrico",), 21400, 4, (12000, 80000)),

    # Mercedes
    VehicleModel("Mercedes", "Classe A", "Hatchback", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 21800, 4, (20000, 108000)),
    VehicleModel("Mercedes", "Classe C", "Sedan", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 31600, 5, (30000, 145000)),
    VehicleModel("Mercedes", "CLA", "Sedan", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 28600, 4, (22000, 115000)),
    VehicleModel("Mercedes", "GLA", "SUV", 5, ("Gasolina", "Híbrido a Gasolina", "Gasóleo"), 28500, 3, (18000, 95000)),
    VehicleModel("Mercedes", "GLC", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 42500, 5, (35000, 150000)),
    VehicleModel("Mercedes", "EQA", "Elétrico", 5, ("100% Elétrico",), 36500, 3, (12000, 85000)),

    # Audi
    VehicleModel("Audi", "A1", "Citadino", 5, ("Gasolina", "Gasóleo"), 16800, 4, (20000, 105000)),
    VehicleModel("Audi", "A3", "Sedan", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 22500, 4, (22000, 112000)),
    VehicleModel("Audi", "A4", "Sedan", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 29200, 5, (32000, 145000)),
    VehicleModel("Audi", "Q2", "SUV", 5, ("Gasolina", "Gasóleo"), 23400, 4, (20000, 105000)),
    VehicleModel("Audi", "Q3", "SUV", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 31800, 4, (24000, 120000)),
    VehicleModel("Audi", "Q5", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 41500, 5, (35000, 150000)),

    # Tesla
    VehicleModel("Tesla", "Model 3", "Elétrico", 5, ("100% Elétrico",), 31800, 2, (12000, 85000)),
    VehicleModel("Tesla", "Model Y", "Elétrico", 5, ("100% Elétrico",), 38200, 2, (10000, 80000)),
    VehicleModel("Tesla", "Model S", "Elétrico", 5, ("100% Elétrico",), 54800, 4, (20000, 110000)),

    # HyundaiS
    VehicleModel("Hyundai", "i20", "Citadino", 5, ("Gasolina",), 12600, 4, (18000, 95000)),
    VehicleModel("Hyundai", "i30", "Hatchback", 5, ("Gasolina", "Gasóleo"), 16800, 5, (25000, 125000)),
    VehicleModel("Hyundai", "Kona", "SUV", 5, ("100% Elétrico", "Gasolina", "Híbrido a Gasolina"), 22900, 3, (14000, 98000)),
    VehicleModel("Hyundai", "Tucson", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 27200, 4, (24000, 120000)),
    VehicleModel("Hyundai", "Ioniq 5", "Elétrico", 5, ("100% Elétrico",), 38400, 2, (10000, 70000)),

    # Kia
    VehicleModel("Kia", "Rio", "Citadino", 5, ("Gasolina",), 11800, 4, (18000, 95000)),
    VehicleModel("Kia", "Ceed", "Hatchback", 5, ("Gasolina", "Gasóleo"), 16400, 5, (26000, 125000)),
    VehicleModel("Kia", "Stonic", "SUV", 5, ("Gasolina",), 16700, 4, (18000, 95000)),
    VehicleModel("Kia", "Sportage", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 26900, 4, (24000, 125000)),
    VehicleModel("Kia", "Niro", "SUV", 5, ("100% Elétrico", "Híbrido a Gasolina"), 23300, 3, (16000, 96000)),
    VehicleModel("Kia", "EV6", "Elétrico", 5, ("100% Elétrico",), 41200, 2, (10000, 75000)),

    # Nissan
    VehicleModel("Nissan", "Micra", "Citadino", 5, ("Gasolina",), 11200, 5, (25000, 125000)),
    VehicleModel("Nissan", "Juke", "SUV", 5, ("Gasolina", "Híbrido a Gasolina"), 17800, 4, (20000, 105000)),
    VehicleModel("Nissan", "Qashqai", "SUV", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 21500, 5, (30000, 130000)),
    VehicleModel("Nissan", "Leaf", "Elétrico", 5, ("100% Elétrico",), 17400, 4, (18000, 90000)),
    VehicleModel("Nissan", "Ariya", "Elétrico", 5, ("100% Elétrico",), 39200, 2, (10000, 75000)),

    # Opel
    VehicleModel("Opel", "Corsa", "Citadino", 5, ("Gasolina", "Gasóleo", "100% Elétrico"), 13500, 4, (20000, 105000)),
    VehicleModel("Opel", "Astra", "Hatchback", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 17600, 5, (25000, 125000)),
    VehicleModel("Opel", "Mokka", "SUV", 5, ("Gasolina", "Gasóleo", "100% Elétrico"), 20400, 3, (16000, 95000)),
    VehicleModel("Opel", "Grandland", "SUV", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 24800, 4, (24000, 120000)),

    # Citroën
    VehicleModel("Citroën", "C3", "Citadino", 5, ("Gasolina", "Gasóleo"), 11200, 5, (30000, 145000)),
    VehicleModel("Citroën", "C4", "Hatchback", 5, ("Gasolina", "Gasóleo", "100% Elétrico"), 17600, 4, (22000, 115000)),
    VehicleModel("Citroën", "C3 Aircross", "SUV", 5, ("Gasolina", "Gasóleo"), 17200, 4, (22000, 115000)),
    VehicleModel("Citroën", "C5 Aircross", "SUV", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 23800, 4, (26000, 125000)),

    # Fiat
    VehicleModel("Fiat", "500", "Citadino", 4, ("Gasolina", "Híbrido a Gasolina", "100% Elétrico"), 13200, 3, (15000, 90000)),
    VehicleModel("Fiat", "Panda", "Citadino", 5, ("Gasolina", "GPL"), 10400, 5, (25000, 125000)),
    VehicleModel("Fiat", "Tipo", "Hatchback", 5, ("Gasolina", "Gasóleo"), 14200, 5, (30000, 140000)),
    VehicleModel("Fiat", "500X", "SUV", 5, ("Gasolina", "Gasóleo"), 17400, 4, (22000, 115000)),

    # Seat
    VehicleModel("Seat", "Ibiza", "Citadino", 5, ("Gasolina", "Gasóleo", "GPL"), 11800, 5, (32000, 150000)),
    VehicleModel("Seat", "Leon", "Hatchback", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 18100, 5, (30000, 140000)),
    VehicleModel("Seat", "Arona", "SUV", 5, ("Gasolina",), 16800, 3, (12000, 82000)),
    VehicleModel("Seat", "Ateca", "SUV", 5, ("Gasolina", "Gasóleo"), 22600, 4, (22000, 115000)),
)


# ============================================================
# Termos genéricos de pesquisa
# ============================================================
# Além de termos específicos por modelo, são gerados também termos
# por segmento/necessidade. Isto simula pesquisas reais, onde muitos
# utilizadores procuram "SUV usado" ou "carro elétrico usado", sem
# especificar logo uma marca/modelo.

TERMOS_EXTRA = [
    {"termo": "SUV usado", "tipo": "SUV"},
    {"termo": "citadino usado", "tipo": "Citadino"},
    {"termo": "hatchback usado", "tipo": "Hatchback"},
    {"termo": "sedan usado", "tipo": "Sedan"},
    {"termo": "carros elétricos usados", "tipo": "Elétrico"},
    {"termo": "carros híbridos usados", "tipo": "Híbrido"},
    {"termo": "carros a gasóleo usados", "tipo": "Gasóleo"},
    {"termo": "carros gasolina usados", "tipo": "Gasolina"},
    {"termo": "carros usados baratos", "tipo": "Geral"},
    {"termo": "carros usados com garantia", "tipo": "Geral"},
]


# ============================================================
# Tendência base por marca e por tipo
# ============================================================
# Estes valores não são dados reais. São parâmetros sintéticos para
# criar séries temporais com comportamentos diferentes por marca/tipo.

TENDENCIA_MARCA: dict[str, int] = {
    "Tesla": 26,
    "Hyundai": 18,
    "Kia": 17,
    "Toyota": 15,
    "Volkswagen": 12,
    "Peugeot": 10,
    "BMW": 9,
    "Mercedes": 8,
    "Audi": 8,
    "Renault": 7,
    "Nissan": 6,
    "Opel": 5,
    "Seat": 5,
    "Citroën": 4,
    "Fiat": 3,
}

TENDENCIA_TIPO: dict[str, int] = {
    "Elétrico": 24,
    "SUV": 15,
    "Híbrido": 14,
    "Citadino": 7,
    "Hatchback": 6,
    "Sedan": 4,
    "Gasóleo": -3,
    "Gasolina": 2,
    "Geral": 5,
}


# ============================================================
# Funções auxiliares
# ============================================================

def gerar_lista_meses() -> list[datetime]:
    """
    Gera a lista de meses entre DATA_INICIO e DATA_FIM.
    Cada mês é representado pelo primeiro dia desse mês.
    """
    meses: list[datetime] = []
    data = DATA_INICIO

    while data <= DATA_FIM:
        meses.append(data)

        if data.month == 12:
            data = datetime(data.year + 1, 1, 1)
        else:
            data = datetime(data.year, data.month + 1, 1)

    return meses


def regiao_factor(regiao: str, tipo: str, marca: str) -> float:
    """
    Introduz diferenças regionais ligeiras.

    Lisboa:
        maior interesse por elétricos, citadinos e Tesla.

    Porto:
        comportamento equilibrado, com algum peso em SUV e premium.

    Braga:
        maior interesse relativo por SUV e gasóleo.
    """
    if regiao == "Lisboa":
        if tipo == "Elétrico":
            return 1.18
        if tipo == "Citadino":
            return 1.10
        if marca == "Tesla":
            return 1.15

    elif regiao == "Porto":
        if tipo in {"SUV", "Hatchback"}:
            return 1.08
        if marca in {"BMW", "Mercedes", "Audi"}:
            return 1.08

    elif regiao == "Braga":
        if tipo == "SUV":
            return 1.16
        if "Gasóleo" == tipo:
            return 1.12
        if marca in {"Volkswagen", "Peugeot", "Renault", "Seat"}:
            return 1.06

    return 1.0


def seasonal_factor(month: int, tipo: str, termo: str) -> float:
    """
    Fator sazonal de procura.

    - SUVs ganham força no inverno.
    - Elétricos ganham força no verão.
    - Citadinos ganham algum peso no regresso às rotinas.
    """
    termo_lower = termo.lower()

    if month in {11, 12, 1, 2}:
        if tipo == "SUV":
            return 1.25
        if "gasóleo" in termo_lower or "gasoleo" in termo_lower:
            return 1.10
        if tipo == "Elétrico":
            return 0.92

    elif month in {6, 7, 8}:
        if tipo == "Elétrico":
            return 1.28
        if "elétrico" in termo_lower or "eletrico" in termo_lower:
            return 1.30
        if tipo == "Citadino":
            return 1.10

    elif month in {9, 10}:
        if tipo == "Citadino":
            return 1.15
        if "baratos" in termo_lower or "garantia" in termo_lower:
            return 1.08

    return 1.0


def calcular_tendencia_modelo(vehicle: VehicleModel) -> float:
    """
    Combina a tendência da marca com a tendência do tipo de automóvel.

    Isto permite que, por exemplo, um elétrico de uma marca em crescimento
    tenha uma evolução mais forte do que um sedan tradicional.
    """
    tendencia_marca = TENDENCIA_MARCA.get(vehicle.marca, 5)
    tendencia_tipo = TENDENCIA_TIPO.get(vehicle.tipo, 5)

    return tendencia_marca * 0.65 + tendencia_tipo * 0.35


def gerar_valor_interesse(
    rng: random.Random,
    base: float,
    mes_idx: int,
    total_meses: int,
    tendencia: float,
    tipo: str,
    termo: str,
    regiao: str,
    marca: str,
    month: int,
) -> float:
    """
    Gera um valor sintético de interesse entre 0 e 100.

    Componentes:
      - base: popularidade inicial;
      - tendência: crescimento/queda ao longo do tempo;
      - sazonalidade anual;
      - fator regional;
      - ruído aleatório.
    """
    progress = mes_idx / max(1, total_meses - 1)

    delta = tendencia * progress

    sazonalidade_seno = 6 * math.sin(2 * math.pi * (month - 1) / 12)
    sazonalidade_tipo = seasonal_factor(month, tipo, termo)
    fator_regiao = regiao_factor(regiao, tipo, marca)

    ruido = rng.gauss(0, 4.5)

    valor = (base + delta + sazonalidade_seno + ruido)
    valor *= sazonalidade_tipo
    valor *= fator_regiao

    return max(0.0, min(100.0, valor))


# ============================================================
# Geração dos dados de Trends
# ============================================================

def gerar_trends() -> list[dict]:
    """
    Gera os registos sintéticos de interesse de pesquisa.

    Para cada modelo do catálogo:
      - cria o termo "<marca> <modelo> usado";
      - gera uma série mensal por região;
      - mantém consistência com o catálogo usado no inventário.

    Além dos modelos, gera também termos genéricos por segmento.
    """
    rng = random.Random(SEED)

    resultado: list[dict] = []

    meses = gerar_lista_meses()
    total_meses = len(meses)

    # --------------------------------------------------------
    # Termos específicos por marca/modelo
    # --------------------------------------------------------
    for vehicle in VEHICLES:
        termo = f"{vehicle.marca} {vehicle.modelo} usado"
        tendencia = calcular_tendencia_modelo(vehicle)

        # A base tem em conta preço e tipo.
        # Modelos premium tendem a ter procura alta, mas não necessariamente
        # tão massificada como modelos citadinos ou SUV generalistas.
        if vehicle.tipo == "Citadino":
            base_min, base_max = 35, 68
        elif vehicle.tipo == "SUV":
            base_min, base_max = 42, 78
        elif vehicle.tipo == "Elétrico":
            base_min, base_max = 30, 72
        elif vehicle.tipo == "Sedan":
            base_min, base_max = 28, 62
        else:
            base_min, base_max = 32, 68

        base = rng.randint(base_min, base_max)

        # Pequena variação por modelo para evitar séries demasiado parecidas.
        tendencia += rng.choice([-5, -3, 0, 2, 4, 6])

        for regiao in REGIOES:
            for i, mes in enumerate(meses):
                ym = f"{mes.year}-{mes.month:02d}"

                valor = gerar_valor_interesse(
                    rng=rng,
                    base=base,
                    mes_idx=i,
                    total_meses=total_meses,
                    tendencia=tendencia,
                    tipo=vehicle.tipo,
                    termo=termo,
                    regiao=regiao,
                    marca=vehicle.marca,
                    month=mes.month,
                )

                resultado.append(
                    {
                        "termo": termo,
                        "marca": vehicle.marca,
                        "modelo": vehicle.modelo,
                        "tipo_automovel": vehicle.tipo,
                        "regiao": regiao,
                        "mes": ym,
                        "valor_interesse": round(valor, 1),
                    }
                )

    # --------------------------------------------------------
    # Termos genéricos por segmento / intenção de pesquisa
    # --------------------------------------------------------
    for item in TERMOS_EXTRA:
        termo = item["termo"]
        tipo = item["tipo"]

        tendencia = TENDENCIA_TIPO.get(tipo, 5)
        base = rng.randint(38, 76)

        for regiao in REGIOES:
            for i, mes in enumerate(meses):
                ym = f"{mes.year}-{mes.month:02d}"

                valor = gerar_valor_interesse(
                    rng=rng,
                    base=base,
                    mes_idx=i,
                    total_meses=total_meses,
                    tendencia=tendencia,
                    tipo=tipo,
                    termo=termo,
                    regiao=regiao,
                    marca="",
                    month=mes.month,
                )

                resultado.append(
                    {
                        "termo": termo,
                        "marca": "",
                        "modelo": "",
                        "tipo_automovel": tipo,
                        "regiao": regiao,
                        "mes": ym,
                        "valor_interesse": round(valor, 1),
                    }
                )

    return resultado


# ============================================================
# Exportação dos ficheiros JSON
# ============================================================

def exportar_json_por_mes(trends: list[dict]) -> None:
    """
    Exporta os dados para ficheiros JSON mensais.

    Estrutura gerada:
        data/sources/trends/2022/01/trends_202201.json
        data/sources/trends/2022/02/trends_202202.json
        ...
    """
    por_mes: dict[tuple[str, str], list[dict]] = {}

    for registo in trends:
        ano, mes = registo["mes"].split("-")
        por_mes.setdefault((ano, mes), []).append(registo)

    for (ano, mes), lista in sorted(por_mes.items()):
        out_dir = OUT_BASE / ano / mes
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / f"trends_{ano}{mes}.json"

        with out_file.open("w", encoding="utf-8") as f:
            json.dump(lista, f, ensure_ascii=False, indent=2)

        print(f"{out_file}  -> {len(lista)} registos gerados.")


# ============================================================
# Execução direta
# ============================================================

if __name__ == "__main__":
    print("AUTO ESCALA — GERAÇÃO DE GOOGLE TRENDS SINTÉTICO")

    dados = gerar_trends()
    exportar_json_por_mes(dados)

    print("Ficheiros mensais de tendências gerados com sucesso.")