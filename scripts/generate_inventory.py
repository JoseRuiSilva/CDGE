from __future__ import annotations

import csv
import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable


# ============================================================
# Configuração geral do gerador
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_ROOT = BASE_DIR / "data" / "sources" / "stands"
CLIENTES_CSV = BASE_DIR / "data" / "sources" / "clientes" / "clientes_ativos.csv"

START_DATE = date(2022, 1, 1)
END_DATE = date(2024, 12, 31)

SEED = 42

# Probabilidade de introduzir pequenos problemas de qualidade nos dados.
# Estes erros servem para testar a limpeza na camada Silver.
ERROR_PROBABILITY = 0.01

COLUMNS = [
    "id_viatura",
    "matricula",
    "marca",
    "modelo",
    "tipo_automovel",
    "num_lugares",
    "ano_viatura",
    "combustivel",
    "quilometragem",
    "preco_aquisicao",
    "data_entrada_stock",
    "preco_venda",
    "data_venda",
    "nif_cliente",
    "stand",
]

STANDS = ["lisboa", "porto", "braga"]

MONTHLY_VOLUME = {
    "lisboa": 14,
    "porto": 12,
    "braga": 9,
}

# De quantos em quantos veículos o gerador força maior diversidade.
# Isto evita que apenas os modelos mais fortes sazonalmente apareçam muitas vezes.
DIVERSITY_INTERVAL = 4


# ============================================================
# Catálogo central de veículos
# ============================================================
# Este catálogo deve ser mantido igual nos restantes scripts de geração:
#   - generate_trends.py
#   - generate_forum.py
#   - generate_hashtags.py
#
# Assim, todos os ficheiros sintéticos falam das mesmas marcas e modelos,
# o que facilita a integração e melhora a utilidade dos dados para previsões.

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
    # ────────────────────────────────────────────────────────
    # Volkswagen
    # ────────────────────────────────────────────────────────
    VehicleModel("Volkswagen", "Golf", "Hatchback", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 18500, 5, (30000, 140000)),
    VehicleModel("Volkswagen", "Polo", "Citadino", 5, ("Gasolina", "Gasóleo"), 14200, 5, (25000, 135000)),
    VehicleModel("Volkswagen", "T-Cross", "SUV", 5, ("Gasolina", "Gasóleo"), 19700, 4, (18000, 105000)),
    VehicleModel("Volkswagen", "T-Roc", "SUV", 5, ("Gasolina", "Gasóleo"), 22600, 4, (20000, 115000)),
    VehicleModel("Volkswagen", "Tiguan", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 24800, 5, (35000, 125000)),
    VehicleModel("Volkswagen", "ID.3", "Elétrico", 5, ("100% Elétrico",), 26800, 3, (12000, 85000)),
    VehicleModel("Volkswagen", "ID.4", "Elétrico", 5, ("100% Elétrico",), 33200, 3, (14000, 90000)),

    # ────────────────────────────────────────────────────────
    # Toyota
    # ────────────────────────────────────────────────────────
    VehicleModel("Toyota", "Yaris", "Citadino", 5, ("Gasolina", "Híbrido a Gasolina"), 15400, 4, (18000, 110000)),
    VehicleModel("Toyota", "Corolla", "Hatchback", 5, ("Gasolina", "Híbrido a Gasolina"), 21800, 4, (20000, 120000)),
    VehicleModel("Toyota", "C-HR", "SUV", 5, ("Híbrido a Gasolina",), 23600, 4, (18000, 105000)),
    VehicleModel("Toyota", "RAV4", "SUV", 5, ("Gasolina", "Híbrido a Gasolina"), 31500, 5, (30000, 135000)),

    # ────────────────────────────────────────────────────────
    # Peugeot
    # ────────────────────────────────────────────────────────
    VehicleModel("Peugeot", "208", "Citadino", 5, ("Gasolina", "Gasóleo", "100% Elétrico"), 14800, 3, (14000, 95000)),
    VehicleModel("Peugeot", "308", "Hatchback", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 20500, 4, (22000, 120000)),
    VehicleModel("Peugeot", "2008", "SUV", 5, ("Gasolina", "Gasóleo", "100% Elétrico"), 21300, 4, (18000, 105000)),
    VehicleModel("Peugeot", "3008", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 22100, 4, (26000, 118000)),
    VehicleModel("Peugeot", "5008", "SUV", 7, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 28400, 5, (30000, 135000)),

    # ────────────────────────────────────────────────────────
    # Renault
    # ────────────────────────────────────────────────────────
    VehicleModel("Renault", "Clio", "Citadino", 5, ("Gasolina", "Gasóleo", "GPL"), 12400, 4, (25000, 135000)),
    VehicleModel("Renault", "Captur", "SUV", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 17900, 4, (22000, 115000)),
    VehicleModel("Renault", "Mégane", "Hatchback", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 18400, 5, (30000, 140000)),
    VehicleModel("Renault", "Arkana", "SUV", 5, ("Gasolina", "Híbrido a Gasolina"), 24700, 3, (15000, 85000)),
    VehicleModel("Renault", "Zoe", "Elétrico", 5, ("100% Elétrico",), 16900, 3, (10000, 70000)),

    # ────────────────────────────────────────────────────────
    # BMW
    # ────────────────────────────────────────────────────────
    VehicleModel("BMW", "Série 1", "Hatchback", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 20900, 4, (18000, 105000)),
    VehicleModel("BMW", "Série 3", "Sedan", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 29400, 5, (30000, 145000)),
    VehicleModel("BMW", "Série 5", "Sedan", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 38500, 5, (35000, 150000)),
    VehicleModel("BMW", "X1", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 27600, 4, (22000, 110000)),
    VehicleModel("BMW", "X3", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 39200, 5, (32000, 145000)),
    VehicleModel("BMW", "i3", "Elétrico", 4, ("100% Elétrico",), 21400, 4, (12000, 80000)),

    # ────────────────────────────────────────────────────────
    # Mercedes
    # ────────────────────────────────────────────────────────
    VehicleModel("Mercedes", "Classe A", "Hatchback", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 21800, 4, (20000, 108000)),
    VehicleModel("Mercedes", "Classe C", "Sedan", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 31600, 5, (30000, 145000)),
    VehicleModel("Mercedes", "CLA", "Sedan", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 28600, 4, (22000, 115000)),
    VehicleModel("Mercedes", "GLA", "SUV", 5, ("Gasolina", "Híbrido a Gasolina", "Gasóleo"), 28500, 3, (18000, 95000)),
    VehicleModel("Mercedes", "GLC", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 42500, 5, (35000, 150000)),
    VehicleModel("Mercedes", "EQA", "Elétrico", 5, ("100% Elétrico",), 36500, 3, (12000, 85000)),

    # ────────────────────────────────────────────────────────
    # Audi
    # ────────────────────────────────────────────────────────
    VehicleModel("Audi", "A1", "Citadino", 5, ("Gasolina", "Gasóleo"), 16800, 4, (20000, 105000)),
    VehicleModel("Audi", "A3", "Sedan", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 22500, 4, (22000, 112000)),
    VehicleModel("Audi", "A4", "Sedan", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 29200, 5, (32000, 145000)),
    VehicleModel("Audi", "Q2", "SUV", 5, ("Gasolina", "Gasóleo"), 23400, 4, (20000, 105000)),
    VehicleModel("Audi", "Q3", "SUV", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 31800, 4, (24000, 120000)),
    VehicleModel("Audi", "Q5", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 41500, 5, (35000, 150000)),

    # ────────────────────────────────────────────────────────
    # Tesla
    # ────────────────────────────────────────────────────────
    VehicleModel("Tesla", "Model 3", "Elétrico", 5, ("100% Elétrico",), 31800, 2, (12000, 85000)),
    VehicleModel("Tesla", "Model Y", "Elétrico", 5, ("100% Elétrico",), 38200, 2, (10000, 80000)),
    VehicleModel("Tesla", "Model S", "Elétrico", 5, ("100% Elétrico",), 54800, 4, (20000, 110000)),

    # ────────────────────────────────────────────────────────
    # Hyundai
    # ────────────────────────────────────────────────────────
    VehicleModel("Hyundai", "i20", "Citadino", 5, ("Gasolina",), 12600, 4, (18000, 95000)),
    VehicleModel("Hyundai", "i30", "Hatchback", 5, ("Gasolina", "Gasóleo"), 16800, 5, (25000, 125000)),
    VehicleModel("Hyundai", "Kona", "SUV", 5, ("100% Elétrico", "Gasolina", "Híbrido a Gasolina"), 22900, 3, (14000, 98000)),
    VehicleModel("Hyundai", "Tucson", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 27200, 4, (24000, 120000)),
    VehicleModel("Hyundai", "Ioniq 5", "Elétrico", 5, ("100% Elétrico",), 38400, 2, (10000, 70000)),

    # ────────────────────────────────────────────────────────
    # Kia
    # ────────────────────────────────────────────────────────
    VehicleModel("Kia", "Rio", "Citadino", 5, ("Gasolina",), 11800, 4, (18000, 95000)),
    VehicleModel("Kia", "Ceed", "Hatchback", 5, ("Gasolina", "Gasóleo"), 16400, 5, (26000, 125000)),
    VehicleModel("Kia", "Stonic", "SUV", 5, ("Gasolina",), 16700, 4, (18000, 95000)),
    VehicleModel("Kia", "Sportage", "SUV", 5, ("Gasóleo", "Gasolina", "Híbrido a Gasolina"), 26900, 4, (24000, 125000)),
    VehicleModel("Kia", "Niro", "SUV", 5, ("100% Elétrico", "Híbrido a Gasolina"), 23300, 3, (16000, 96000)),
    VehicleModel("Kia", "EV6", "Elétrico", 5, ("100% Elétrico",), 41200, 2, (10000, 75000)),

    # ────────────────────────────────────────────────────────
    # Nissan
    # ────────────────────────────────────────────────────────
    VehicleModel("Nissan", "Micra", "Citadino", 5, ("Gasolina",), 11200, 5, (25000, 125000)),
    VehicleModel("Nissan", "Juke", "SUV", 5, ("Gasolina", "Híbrido a Gasolina"), 17800, 4, (20000, 105000)),
    VehicleModel("Nissan", "Qashqai", "SUV", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 21500, 5, (30000, 130000)),
    VehicleModel("Nissan", "Leaf", "Elétrico", 5, ("100% Elétrico",), 17400, 4, (18000, 90000)),
    VehicleModel("Nissan", "Ariya", "Elétrico", 5, ("100% Elétrico",), 39200, 2, (10000, 75000)),

    # ────────────────────────────────────────────────────────
    # Opel
    # ────────────────────────────────────────────────────────
    VehicleModel("Opel", "Corsa", "Citadino", 5, ("Gasolina", "Gasóleo", "100% Elétrico"), 13500, 4, (20000, 105000)),
    VehicleModel("Opel", "Astra", "Hatchback", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 17600, 5, (25000, 125000)),
    VehicleModel("Opel", "Mokka", "SUV", 5, ("Gasolina", "Gasóleo", "100% Elétrico"), 20400, 3, (16000, 95000)),
    VehicleModel("Opel", "Grandland", "SUV", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 24800, 4, (24000, 120000)),

    # ────────────────────────────────────────────────────────
    # Citroën
    # ────────────────────────────────────────────────────────
    VehicleModel("Citroën", "C3", "Citadino", 5, ("Gasolina", "Gasóleo"), 11200, 5, (30000, 145000)),
    VehicleModel("Citroën", "C4", "Hatchback", 5, ("Gasolina", "Gasóleo", "100% Elétrico"), 17600, 4, (22000, 115000)),
    VehicleModel("Citroën", "C3 Aircross", "SUV", 5, ("Gasolina", "Gasóleo"), 17200, 4, (22000, 115000)),
    VehicleModel("Citroën", "C5 Aircross", "SUV", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 23800, 4, (26000, 125000)),

    # ────────────────────────────────────────────────────────
    # Fiat
    # ────────────────────────────────────────────────────────
    VehicleModel("Fiat", "500", "Citadino", 4, ("Gasolina", "Híbrido a Gasolina", "100% Elétrico"), 13200, 3, (15000, 90000)),
    VehicleModel("Fiat", "Panda", "Citadino", 5, ("Gasolina", "GPL"), 10400, 5, (25000, 125000)),
    VehicleModel("Fiat", "Tipo", "Hatchback", 5, ("Gasolina", "Gasóleo"), 14200, 5, (30000, 140000)),
    VehicleModel("Fiat", "500X", "SUV", 5, ("Gasolina", "Gasóleo"), 17400, 4, (22000, 115000)),

    # ────────────────────────────────────────────────────────
    # Seat
    # ────────────────────────────────────────────────────────
    VehicleModel("Seat", "Ibiza", "Citadino", 5, ("Gasolina", "Gasóleo", "GPL"), 11800, 5, (32000, 150000)),
    VehicleModel("Seat", "Leon", "Hatchback", 5, ("Gasolina", "Gasóleo", "Híbrido a Gasolina"), 18100, 5, (30000, 140000)),
    VehicleModel("Seat", "Arona", "SUV", 5, ("Gasolina",), 16800, 3, (12000, 82000)),
    VehicleModel("Seat", "Ateca", "SUV", 5, ("Gasolina", "Gasóleo"), 22600, 4, (22000, 115000)),
)


# ============================================================
# Fatores de preço por combustível
# ============================================================

PRICE_FACTOR = {
    "Gasolina": 1.00,
    "Gasóleo": 1.03,
    "Híbrido a Gasolina": 1.08,
    "Híbrido a Gasóleo": 1.10,
    "100% Elétrico": 1.18,
    "GPL": 0.95,
}


# ============================================================
# Funções auxiliares de datas
# ============================================================

def daterange_months(start: date, end: date) -> Iterable[date]:
    current = date(start.year, start.month, 1)

    while current <= end:
        yield current

        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)


# ============================================================
# Sazonalidade e diversidade de modelos
# ============================================================

def seasonal_weights(month: int) -> dict[str, float]:
    weights = {
        "SUV": 1.0,
        "Citadino": 1.0,
        "Hatchback": 1.0,
        "Sedan": 0.9,
        "Elétrico": 0.8,
    }

    # Meses frios: maior interesse em SUV.
    if month in {11, 12, 1, 2}:
        weights["SUV"] = 1.8
        weights["Elétrico"] = 0.7

    # Verão: maior interesse em elétricos e citadinos.
    elif month in {6, 7, 8}:
        weights["Elétrico"] = 1.8
        weights["SUV"] = 0.95
        weights["Citadino"] = 1.1

    # Primavera e outono: procura equilibrada.
    elif month in {3, 4, 5, 9, 10}:
        weights["SUV"] = 1.1
        weights["Elétrico"] = 1.0

    return weights


def vehicle_key(vehicle: VehicleModel) -> tuple[str, str]:
    return vehicle.marca, vehicle.modelo


def diversity_factor(
    vehicle: VehicleModel,
    model_counts: dict[tuple[str, str], int],
    brand_counts: dict[str, int],
) -> float:
    """
    Penaliza suavemente modelos e marcas que já apareceram muitas vezes.

    Isto aumenta a diversidade dos ficheiros sintéticos sem eliminar
    totalmente a aleatoriedade nem os padrões sazonais.
    """
    key = vehicle_key(vehicle)

    model_penalty = 1 / (1 + model_counts[key] * 0.35)
    brand_penalty = 1 / (1 + brand_counts[vehicle.marca] * 0.07)

    return model_penalty * brand_penalty


def least_represented_vehicles(
    model_counts: dict[tuple[str, str], int],
) -> list[VehicleModel]:
    """
    Devolve os modelos menos representados até ao momento.

    Esta função é usada de forma periódica para garantir que quase todos
    os modelos do catálogo aparecem nos dados gerados.
    """
    min_count = min(model_counts[vehicle_key(vehicle)] for vehicle in VEHICLES)

    return [
        vehicle
        for vehicle in VEHICLES
        if model_counts[vehicle_key(vehicle)] == min_count
    ]


def stand_profile_factor(stand: str, vehicle: VehicleModel) -> float:
    """
    Pequena diferenciação por stand.

    Lisboa:
        ligeiro reforço em citadinos e elétricos.

    Porto:
        perfil equilibrado, com ligeiro reforço em hatchbacks e SUV.

    Braga:
        ligeiro reforço em SUV e gasóleo.
    """
    combustiveis = set(vehicle.combustiveis)

    if stand == "lisboa":
        if vehicle.tipo in {"Citadino", "Elétrico"}:
            return 1.15
        if "100% Elétrico" in combustiveis:
            return 1.10

    elif stand == "porto":
        if vehicle.tipo in {"Hatchback", "SUV"}:
            return 1.08

    elif stand == "braga":
        if vehicle.tipo == "SUV":
            return 1.15
        if "Gasóleo" in combustiveis:
            return 1.10

    return 1.0


def weighted_vehicle_choice(
    rng: random.Random,
    month: int,
    stand: str,
    model_counts: dict[tuple[str, str], int],
    brand_counts: dict[str, int],
    force_diversity: bool = False,
) -> VehicleModel:
    """
    Escolhe uma viatura tendo em conta:
      - sazonalidade;
      - perfil do stand;
      - diversidade de marcas/modelos;
      - reforço periódico dos modelos menos representados.
    """
    weights_by_type = seasonal_weights(month)

    if force_diversity:
        candidates = least_represented_vehicles(model_counts)
    else:
        candidates = list(VEHICLES)

    weights = []

    for vehicle in candidates:
        seasonal = weights_by_type.get(vehicle.tipo, 1.0)
        regional = stand_profile_factor(stand, vehicle)
        diversity = diversity_factor(vehicle, model_counts, brand_counts)

        final_weight = seasonal * regional * diversity

        # Peso mínimo para nenhum modelo desaparecer totalmente.
        weights.append(max(final_weight, 0.05))

    return rng.choices(candidates, weights=weights, k=1)[0]


def choose_fuel(rng: random.Random, vehicle: VehicleModel, month: int) -> str:
    options = list(vehicle.combustiveis)

    if len(options) == 1:
        return options[0]

    weights = []

    for fuel in options:
        weight = 1.0

        if fuel == "100% Elétrico" and month in {6, 7, 8}:
            weight = 2.1
        elif fuel == "100% Elétrico":
            weight = 0.8
        elif fuel == "Gasóleo" and month in {11, 12, 1, 2}:
            weight = 1.2
        elif fuel.startswith("Híbrido"):
            weight = 1.15

        weights.append(weight)

    return rng.choices(options, weights=weights, k=1)[0]


# ============================================================
# Geração das características da viatura
# ============================================================

def random_entry_date(rng: random.Random, month_start: date) -> date:
    if month_start.month == 12:
        next_month = date(month_start.year + 1, 1, 1)
    else:
        next_month = date(month_start.year, month_start.month + 1, 1)

    last_day = (next_month - timedelta(days=1)).day

    return date(
        month_start.year,
        month_start.month,
        rng.randint(1, last_day),
    )


def vehicle_year(
    rng: random.Random,
    entry_date: date,
    target_age: int,
) -> int:
    max_year = entry_date.year - 1
    min_year = max(2013, entry_date.year - target_age - 4)

    if min_year > max_year:
        min_year = max_year

    likely_year = max(
        min_year,
        min(
            max_year,
            entry_date.year - max(
                1,
                target_age + rng.choice([-2, -1, 0, 1, 2]),
            ),
        ),
    )

    if rng.random() < 0.25:
        year = rng.randint(min_year, likely_year)
    else:
        year = likely_year

    return min(year, max_year)


def estimate_km(
    rng: random.Random,
    model: VehicleModel,
    year: int,
    entry_date: date,
) -> int:
    age = max(1, entry_date.year - year)

    base_min, base_max = model.km_range
    expected = age * rng.randint(11000, 19000)

    km = int((base_min + base_max) / 2 * 0.25 + expected * 0.75)
    km += rng.randint(-12000, 12000)

    km = max(base_min, min(base_max, km))

    return int(round(km / 1000.0) * 1000)


def acquisition_price(
    rng: random.Random,
    model: VehicleModel,
    fuel: str,
    year: int,
    entry_date: date,
    km: int,
) -> int:
    age = max(1, entry_date.year - year)

    price = model.base_price * PRICE_FACTOR[fuel]
    price *= 0.92 ** age
    price *= max(0.72, 1 - (km / 280000))
    price *= rng.uniform(0.94, 1.06)

    return int(round(price / 100) * 100)


def sale_info(
    rng: random.Random,
    entry_date: date,
    purchase_price: int,
) -> tuple[str, str]:
    sold_probability = 0.82

    # Nos meses finais do histórico, é normal haver mais viaturas ainda em stock.
    if entry_date > date(2024, 9, 1):
        sold_probability = 0.60
    elif entry_date > date(2024, 6, 1):
        sold_probability = 0.72

    if rng.random() > sold_probability:
        return "", ""

    days_to_sell = rng.randint(12, 140)
    sale_date = entry_date + timedelta(days=days_to_sell)

    if sale_date > END_DATE:
        return "", ""

    margin = rng.uniform(1.06, 1.18)
    sale_price = int(round((purchase_price * margin) / 100) * 100)

    return str(sale_price), sale_date.isoformat()


def generate_plate(rng: random.Random, used: set[str]) -> str:
    while True:
        plate = (
            f"{rng.randint(10, 99)}-"
            f"{chr(rng.randint(65, 90))}{chr(rng.randint(65, 90))}-"
            f"{rng.randint(10, 99)}"
        )

        if plate not in used:
            used.add(plate)
            return plate


# ============================================================
# Introdução de problemas de qualidade
# ============================================================

def inject_quality_issue(
    rng: random.Random,
    row: dict[str, object],
) -> dict[str, object]:
    """
    Introduz pequenos problemas de qualidade nos dados.

    Estes problemas são propositados e servem para testar:
      - normalização de texto;
      - limpeza de espaços;
      - resolução de variantes;
      - conversão de quilometragem;
      - validação de campos obrigatórios.
    """
    if rng.random() >= ERROR_PROBABILITY:
        return row

    broken = dict(row)

    error_type = rng.choice(
        [
            "marca_variation",
            "combustivel_case_space",
            "stand_case_space",
            "km_with_unit",
            "missing_num_lugares",
            "tipo_case_space",
        ]
    )

    if error_type == "marca_variation":
        replacements = {
            "Volkswagen": "vw",
            "Mercedes": "mercedes-benz",
            "BMW": "bmw",
            "Citroën": "citroen",
            "Peugeot": "peugeot ",
            "Renault": "renault",
            "Toyota": "toyota",
            "Nissan": "nissan",
            "Hyundai": "hyundai",
        }
        broken["marca"] = replacements.get(
            str(broken["marca"]),
            str(broken["marca"]).lower(),
        )

    elif error_type == "combustivel_case_space":
        broken["combustivel"] = f"{str(broken['combustivel']).lower()} "

    elif error_type == "stand_case_space":
        broken["stand"] = f"{str(broken['stand']).upper()} "

    elif error_type == "km_with_unit":
        broken["quilometragem"] = f"{broken['quilometragem']} km"

    elif error_type == "missing_num_lugares":
        broken["num_lugares"] = ""

    elif error_type == "tipo_case_space":
        broken["tipo_automovel"] = f"{str(broken['tipo_automovel']).lower()}"

    return broken


# ============================================================
# Gerador principal
# ============================================================

def load_nifs() -> list[str]:
    """Lê os NIFs dos ficheiros sintéticos de clientes."""
    clientes_dir = BASE_DIR / "data" / "sources" / "clientes"
    if not clientes_dir.exists():
        return []
    
    nifs = set()
    for csv_file in clientes_dir.glob("*.csv"):
        with csv_file.open("r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if "nif" in row:
                    nifs.add(row["nif"])
        # Apenas precisamos ler um ficheiro para ter a lista de todos os NIFs (ou a grande maioria)
        if len(nifs) > 0:
            break
            
    return list(nifs)

def generate_inventory() -> dict[str, int]:
    rng = random.Random(SEED)

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    nifs = load_nifs()
    if not nifs:
        print("AVISO: clientes_ativos.csv não encontrado — NIFs ficarão vazios.")

    used_plates: set[str] = set()
    vehicle_counter = 1
    counts: dict[str, int] = {}

    # Contadores usados para controlar a diversidade.
    model_counts: dict[tuple[str, str], int] = defaultdict(int)
    brand_counts: dict[str, int] = defaultdict(int)

    for stand in STANDS:
        stand_dir = OUTPUT_ROOT / stand
        stand_dir.mkdir(parents=True, exist_ok=True)

        row_count = 0
        pending_sales = defaultdict(list)

        for month_start in daterange_months(START_DATE, END_DATE):
            file_name = f"{month_start.year}_{month_start.month:02d}_{stand}.csv"
            output_file = stand_dir / file_name

            with output_file.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS)
                writer.writeheader()

                current_month_str = f"{month_start.year}-{month_start.month:02d}"
                for sold_row in pending_sales[current_month_str]:
                    writer.writerow(sold_row)
                    row_count += 1

                monthly_target = MONTHLY_VOLUME[stand] + rng.randint(-2, 3)
                monthly_target = max(6, monthly_target)

                for local_idx in range(monthly_target):
                    force_diversity = (
                        vehicle_counter % DIVERSITY_INTERVAL == 0
                        or local_idx == 0
                    )

                    vehicle = weighted_vehicle_choice(
                        rng=rng,
                        month=month_start.month,
                        stand=stand,
                        model_counts=model_counts,
                        brand_counts=brand_counts,
                        force_diversity=force_diversity,
                    )

                    fuel = choose_fuel(rng, vehicle, month_start.month)
                    entry_date = random_entry_date(rng, month_start)
                    year = vehicle_year(rng, entry_date, vehicle.age_target)
                    km = estimate_km(rng, vehicle, year, entry_date)
                    purchase = acquisition_price(
                        rng,
                        vehicle,
                        fuel,
                        year,
                        entry_date,
                        km,
                    )
                    sale_price, sale_date = sale_info(
                        rng,
                        entry_date,
                        purchase,
                    )

                    nif_atribuido = rng.choice(nifs) if sale_date and nifs else ""

                    row_entry = {
                        "id_viatura": f"V{vehicle_counter:06d}",
                        "matricula": generate_plate(rng, used_plates),
                        "marca": vehicle.marca,
                        "modelo": vehicle.modelo,
                        "tipo_automovel": vehicle.tipo,
                        "num_lugares": vehicle.num_lugares,
                        "ano_viatura": year,
                        "combustivel": fuel,
                        "quilometragem": km,
                        "preco_aquisicao": purchase,
                        "data_entrada_stock": entry_date.isoformat(),
                        "preco_venda": "",
                        "data_venda": "",
                        "nif_cliente": "",
                        "stand": stand.capitalize(),
                    }

                    row_entry = inject_quality_issue(rng, row_entry)
                    writer.writerow(row_entry)
                    row_count += 1

                    if sale_date:
                        sold_date_obj = date.fromisoformat(sale_date[:10])
                        sold_month_str = f"{sold_date_obj.year}-{sold_date_obj.month:02d}"
                        
                        row_sold = dict(row_entry)
                        row_sold["preco_venda"] = sale_price
                        row_sold["data_venda"] = sale_date
                        row_sold["nif_cliente"] = nif_atribuido
                        
                        pending_sales[sold_month_str].append(row_sold)

                    model_counts[vehicle_key(vehicle)] += 1
                    brand_counts[vehicle.marca] += 1

                    vehicle_counter += 1

        counts[stand] = row_count

    return counts


if __name__ == "__main__":
    summary = generate_inventory()

    print("Ficheiros mensais gerados com sucesso:")

    for stand, total in summary.items():
        print(f"- {stand.capitalize()}: {total} registos")