"""
generate_inventory.py
---------------------------------------------------------------
Gerador de ficheiros CSV mensais por stand para o projeto
Auto Escala — CDGE 2025/2026.

Erros intencionais injectados (~1 % dos registos):
  • Lookup table resolve  → variações semânticas de marca/combustível/tipo
  • Silver resolve sozinho → trim / lower / split (NÃO estão na lookup table)
  • Quarentena (ninguém resolve) → valores impossíveis / campos em branco
---------------------------------------------------------------
"""

from __future__ import annotations

import csv
import random
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR / "scripts"))

from vehicles import VEHICLES, VehicleModel  # noqa: E402

OUTPUT_ROOT = BASE_DIR / "data" / "sources" / "stands"
CLIENTES_CSV = BASE_DIR / "data" / "sources" / "clientes" / "clientes_ativos.csv"

START_DATE = date(2022, 1, 1)
END_DATE   = date(2024, 12, 31)
SEED       = 42

# Probabilidade total de injectar algum erro num registo
ERROR_PROBABILITY = 0.035

COLUMNS = [
    "id_viatura", "matricula", "marca", "modelo", "tipo_automovel",
    "num_lugares", "ano_viatura", "combustivel", "quilometragem",
    "preco_aquisicao", "data_entrada_stock", "preco_venda", "data_venda",
    "nif_cliente", "stand",
]

STANDS = ["lisboa", "porto", "braga"]

MONTHLY_VOLUME = {"lisboa": 14, "porto": 12, "braga": 9}


# ──────────────────────────────────────────────────────────────────────────────
# Erros que a LOOKUP TABLE consegue resolver (semântica)
# ──────────────────────────────────────────────────────────────────────────────
_MARCA_VARIANTS: dict[str, list[str]] = {
    "Volkswagen": ["VW", "volkswagen", "Volksvagen", "V.W."],
    "Mercedes":   ["mercedes benz", "Mercedes-Benz", "Merc", "mercedes"],
    "BMW":        ["bmw", "B.M.W", "Bmw"],
    "Citroën":    ["Citroen", "CITROEN", "citroen"],
    "Hyundai":    ["hyundai", "Hundai", "Hyunday"],
    "Peugeot":    ["peugeot", "Peguot", "PEUGEOT"],
    "Renault":    ["renault", "Renaul", "RENAULT"],
    "Kia":        ["kia", "KIA"],
    "Opel":       ["opel", "OPEL"],
    "Fiat":       ["fiat", "FIAT"],
    "Toyota":     ["toyota", "TOYOTA", "Toyotta"],
    "Tesla":      ["tesla", "TESLA"],
    "Audi":       ["audi", "AUDI"],
    "Nissan":     ["nissan", "NISSAN"],
    "Seat":       ["seat", "SEAT"],
}

_COMBUSTIVEL_VARIANTS: dict[str, list[str]] = {
    "Gasolina":            ["gasolina", "GASOLINA", "Gasoline"],
    "Gasóleo":             ["gasoleo", "Gasoleo", "GASOLEO", "Diesel", "diesel"],
    "Híbrido a Gasolina":  ["hibrido a gasolina", "Hibrido Gasolina", "Híbrido Gasolina", "hybrid gasolina"],
    "100% Elétrico":       ["100% Eletrico", "Eletrico", "eletrico", "Electrico", "100%Elétrico"],
    "GPL":                 ["gpl", "GPL", "G.P.L."],
}

_TIPO_VARIANTS: dict[str, list[str]] = {
    "SUV":       ["suv", "S.U.V", "Suv", "4x4", "Todo-o-Terreno"],
    "Hatchback": ["hatchback", "HATCHBACK", "Hatch"],
    "Citadino":  ["citadino", "CITADINO", "City", "city car"],
    "Elétrico":  ["eletrico", "Eletrico", "electrico"],
}

# ──────────────────────────────────────────────────────────────────────────────
# Erros que o Silver resolve (trim / lower) — NÃO entram na lookup table
# ──────────────────────────────────────────────────────────────────────────────
def _silver_resolvable_error(rng: random.Random, row: dict) -> dict:
    """Introduz erros que um simples strip() / lower() / split() resolve."""
    error = rng.choice([
        "extra_whitespace_marca",
        "extra_whitespace_combustivel",
        "leading_space_stand",
        "km_with_unit",          # "45000 km" → split + cast
        "lowercase_stand",
        "trailing_newline_modelo",
    ])
    r = dict(row)
    if error == "extra_whitespace_marca":
        r["marca"] = "  " + r["marca"] + "  "
    elif error == "extra_whitespace_combustivel":
        r["combustivel"] = r["combustivel"] + "  "
    elif error == "leading_space_stand":
        r["stand"] = " " + r["stand"]
    elif error == "km_with_unit":
        r["quilometragem"] = f"{r['quilometragem']} km"
    elif error == "lowercase_stand":
        r["stand"] = r["stand"].lower()
    elif error == "trailing_newline_modelo":
        r["modelo"] = r["modelo"] + "\n"
    return r


# ──────────────────────────────────────────────────────────────────────────────
# Erros que VÃO para quarentena (ninguém apanha automaticamente)
# ──────────────────────────────────────────────────────────────────────────────
def _quarantine_error(rng: random.Random, row: dict) -> dict:
    """Introduz corrupção que deve acabar na tabela de quarentena do Silver."""
    error = rng.choice([
        "marca_gibberish",        # marca completamente irreconhecível
        "preco_negativo",         # preço de aquisição negativo
        "ano_impossivel",         # ano do veículo no futuro
        "km_zero_for_old_car",    # carro de 2014 com 0 km
        "nif_formato_errado",     # NIF com letras
        "combustivel_desconhecido",
    ])
    r = dict(row)
    if error == "marca_gibberish":
        r["marca"] = rng.choice(["N/D", "???", "OUTRA", "Desconhecido", "XX"])
    elif error == "preco_negativo":
        r["preco_aquisicao"] = -abs(int(r["preco_aquisicao"]))
    elif error == "ano_impossivel":
        r["ano_viatura"] = rng.randint(2030, 2040)
    elif error == "km_zero_for_old_car":
        r["quilometragem"] = 0
    elif error == "nif_formato_errado":
        r["nif_cliente"] = "NIF" + str(rng.randint(100000, 999999))
    elif error == "combustivel_desconhecido":
        r["combustivel"] = rng.choice(["H2", "Biogás", "Solar", "Ar"])
    return r


def _inject_error(rng: random.Random, row: dict) -> dict:
    """Distribui os erros pelos três níveis."""
    roll = rng.random()
    if roll < ERROR_PROBABILITY * 0.40:
        # lookup table resolve
        r = dict(row)
        if rng.random() < 0.5 and r["marca"] in _MARCA_VARIANTS:
            r["marca"] = rng.choice(_MARCA_VARIANTS[r["marca"]])
        elif r["combustivel"] in _COMBUSTIVEL_VARIANTS:
            r["combustivel"] = rng.choice(_COMBUSTIVEL_VARIANTS[r["combustivel"]])
        elif r["tipo_automovel"] in _TIPO_VARIANTS:
            r["tipo_automovel"] = rng.choice(_TIPO_VARIANTS[r["tipo_automovel"]])
        return r
    elif roll < ERROR_PROBABILITY * 0.75:
        # silver resolve
        return _silver_resolvable_error(rng, row)
    elif roll < ERROR_PROBABILITY:
        # quarentena
        return _quarantine_error(rng, row)
    return row


# ──────────────────────────────────────────────────────────────────────────────
# Helpers de geração
# ──────────────────────────────────────────────────────────────────────────────

def daterange_months(start: date, end: date) -> Iterable[date]:
    current = date(start.year, start.month, 1)
    while current <= end:
        yield current
        current = (
            date(current.year + 1, 1, 1)
            if current.month == 12
            else date(current.year, current.month + 1, 1)
        )


def seasonal_weights(month: int) -> dict[str, float]:
    w = {"SUV": 1.0, "Citadino": 1.0, "Hatchback": 1.0, "Elétrico": 0.8}
    if month in {11, 12, 1, 2}:
        w["SUV"] = 1.8; w["Elétrico"] = 0.65
    elif month in {6, 7, 8}:
        w["Elétrico"] = 1.8; w["SUV"] = 0.95; w["Citadino"] = 1.15
    elif month in {3, 4, 5, 9, 10}:
        w["SUV"] = 1.1; w["Elétrico"] = 1.05
    return w


def weighted_vehicle_choice(rng: random.Random, month: int) -> VehicleModel:
    wt = seasonal_weights(month)
    candidates = list(VEHICLES)
    weights = [wt.get(v.tipo, 1.0) for v in candidates]
    return rng.choices(candidates, weights=weights, k=1)[0]


def choose_fuel(rng: random.Random, vehicle: VehicleModel, month: int) -> str:
    options = list(vehicle.combustiveis)
    if len(options) == 1:
        return options[0]
    weights = []
    for fuel in options:
        w = 1.0
        if fuel == "100% Elétrico":
            w = 2.0 if month in {6, 7, 8} else 0.75
        elif fuel == "Gasóleo" and month in {11, 12, 1, 2}:
            w = 1.2
        elif fuel.startswith("Híbrido"):
            w = 1.15
        weights.append(w)
    return rng.choices(options, weights=weights, k=1)[0]


def random_entry_date(rng: random.Random, month_start: date) -> date:
    next_m = (
        date(month_start.year + 1, 1, 1)
        if month_start.month == 12
        else date(month_start.year, month_start.month + 1, 1)
    )
    last_day = (next_m - timedelta(days=1)).day
    return date(month_start.year, month_start.month, rng.randint(1, last_day))


def vehicle_year(rng: random.Random, entry_date: date, target_age: int) -> int:
    max_year = entry_date.year - 1
    min_year = max(2013, entry_date.year - target_age - 4)
    if min_year > max_year:
        min_year = max_year
    likely = max(min_year, min(max_year, entry_date.year - max(1, target_age + rng.choice([-2, -1, 0, 1, 2]))))
    return rng.randint(min_year, likely) if rng.random() < 0.25 else likely


def estimate_km(rng: random.Random, model: VehicleModel, year: int, entry_date: date) -> int:
    age = max(1, entry_date.year - year)
    bmin, bmax = model.km_range
    expected = age * rng.randint(11_000, 19_000)
    km = int((bmin + bmax) / 2 * 0.25 + expected * 0.75) + rng.randint(-12_000, 12_000)
    return int(round(max(bmin, min(bmax, km)) / 1000) * 1000)


PRICE_FACTOR = {
    "Gasolina": 1.00, "Gasóleo": 1.03, "Híbrido a Gasolina": 1.08,
    "Híbrido a Gasóleo": 1.10, "100% Elétrico": 1.18, "GPL": 0.95,
}


def acquisition_price(rng: random.Random, model: VehicleModel, fuel: str,
                      year: int, entry_date: date, km: int) -> int:
    age = max(1, entry_date.year - year)
    price = model.base_price * PRICE_FACTOR.get(fuel, 1.0)
    price *= 0.92 ** age
    price *= max(0.72, 1 - km / 280_000)
    price *= rng.uniform(0.94, 1.06)
    return int(round(price / 100) * 100)


def sale_info(rng: random.Random, entry_date: date, purchase_price: int) -> tuple[str, str]:
    prob = 0.82
    if entry_date > date(2024, 9, 1):
        prob = 0.60
    elif entry_date > date(2024, 6, 1):
        prob = 0.72
    if rng.random() > prob:
        return "", ""
    sale_date = entry_date + timedelta(days=rng.randint(12, 140))
    if sale_date > END_DATE:
        return "", ""
    sale_price = int(round(purchase_price * rng.uniform(1.06, 1.18) / 100) * 100)
    return str(sale_price), sale_date.isoformat()


def generate_plate(rng: random.Random, used: set[str]) -> str:
    while True:
        plate = f"{rng.randint(10,99)}-{chr(rng.randint(65,90))}{chr(rng.randint(65,90))}-{rng.randint(10,99)}"
        if plate not in used:
            used.add(plate)
            return plate


def load_nifs() -> list[str]:
    if not CLIENTES_CSV.exists():
        return []
    with CLIENTES_CSV.open("r", encoding="utf-8") as f:
        return [row["nif"] for row in csv.DictReader(f)]


# ──────────────────────────────────────────────────────────────────────────────
# Gerador principal
# ──────────────────────────────────────────────────────────────────────────────

def generate_inventory() -> dict[str, int]:
    rng = random.Random(SEED)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    used_plates: set[str] = set()
    vehicle_counter = 1
    counts: dict[str, int] = {}

    nifs = load_nifs()
    if not nifs:
        print("AVISO: clientes_ativos.csv não encontrado — NIFs ficarão vazios.")

    for stand in STANDS:
        stand_dir = OUTPUT_ROOT / stand
        stand_dir.mkdir(parents=True, exist_ok=True)
        row_count = 0
        active_inventory: list[dict] = []

        for month_start in daterange_months(START_DATE, END_DATE):
            file_name = f"{month_start.year}_{month_start.month:02d}_{stand}.csv"
            output_file = stand_dir / file_name

            next_m = (
                date(month_start.year + 1, 1, 1)
                if month_start.month == 12
                else date(month_start.year, month_start.month + 1, 1)
            )
            month_end = next_m - timedelta(days=1)

            monthly_target = max(6, MONTHLY_VOLUME[stand] + rng.randint(-2, 3))

            for _ in range(monthly_target):
                vehicle = weighted_vehicle_choice(rng, month_start.month)
                fuel = choose_fuel(rng, vehicle, month_start.month)
                entry_date = random_entry_date(rng, month_start)
                year = vehicle_year(rng, entry_date, vehicle.age_target)
                km = estimate_km(rng, vehicle, year, entry_date)
                purchase = acquisition_price(rng, vehicle, fuel, year, entry_date, km)
                sale_price, sale_date = sale_info(rng, entry_date, purchase)
                sale_date_obj = date.fromisoformat(sale_date) if sale_date else None

                active_inventory.append({
                    "id_viatura":        f"V{vehicle_counter:06d}",
                    "matricula":         generate_plate(rng, used_plates),
                    "marca":             vehicle.marca,
                    "modelo":            vehicle.modelo,
                    "tipo_automovel":    vehicle.tipo,
                    "num_lugares":       vehicle.num_lugares,
                    "ano_viatura":       year,
                    "combustivel":       fuel,
                    "quilometragem":     km,
                    "preco_aquisicao":   purchase,
                    "data_entrada_stock": entry_date.isoformat(),
                    "preco_venda_target": sale_price,
                    "data_venda_target":  sale_date_obj,
                    "stand":             stand.capitalize(),
                })
                vehicle_counter += 1

            with output_file.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS)
                writer.writeheader()

                still_active: list[dict] = []
                for car in active_inventory:
                    entry_d = date.fromisoformat(car["data_entrada_stock"])
                    if entry_d > month_end:
                        still_active.append(car)
                        continue

                    sold = car["data_venda_target"] and car["data_venda_target"] <= month_end
                    row = {
                        "id_viatura":        car["id_viatura"],
                        "matricula":         car["matricula"],
                        "marca":             car["marca"],
                        "modelo":            car["modelo"],
                        "tipo_automovel":    car["tipo_automovel"],
                        "num_lugares":       car["num_lugares"],
                        "ano_viatura":       car["ano_viatura"],
                        "combustivel":       car["combustivel"],
                        "quilometragem":     car["quilometragem"],
                        "preco_aquisicao":   car["preco_aquisicao"],
                        "data_entrada_stock": car["data_entrada_stock"],
                        "preco_venda":       car["preco_venda_target"] if sold else "",
                        "data_venda":        car["data_venda_target"].isoformat() if sold else "",
                        "nif_cliente":       rng.choice(nifs) if sold and nifs else "",
                        "stand":             car["stand"],
                    }
                    row = _inject_error(rng, row)
                    writer.writerow(row)
                    row_count += 1

                    if not sold:
                        still_active.append(car)
                active_inventory = still_active

        counts[stand] = row_count
    return counts


if __name__ == "__main__":
    summary = generate_inventory()
    print("Ficheiros mensais gerados com sucesso:")
    for stand, total in summary.items():
        print(f"  {stand.capitalize()}: {total} registos")