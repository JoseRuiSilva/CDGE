"""
vehicles.py
---------------------------------------------------------------
Catálogo central de modelos de veículos usado por todos os
geradores de dados do projeto Auto Escala.

Importar assim:
    from vehicles import VEHICLES, MARCAS_MODELOS

Projeto Auto Escala — CDGE 2025/2026
---------------------------------------------------------------
"""

from __future__ import annotations
from dataclasses import dataclass


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
    # ── SUV ──────────────────────────────────────────────────────────────────
    VehicleModel("Mercedes",   "GLA",      "SUV",       5, ("Gasolina", "Híbrido a Gasolina", "Gasóleo"),          28500, 3, (18_000,  95_000)),
    VehicleModel("BMW",        "X1",       "SUV",       5, ("Gasóleo",  "Gasolina", "Híbrido a Gasolina"),          27_600, 4, (22_000, 110_000)),
    VehicleModel("Volkswagen", "Tiguan",   "SUV",       5, ("Gasóleo",  "Gasolina", "Híbrido a Gasolina"),          24_800, 5, (35_000, 125_000)),
    VehicleModel("Peugeot",    "3008",     "SUV",       5, ("Gasóleo",  "Gasolina", "Híbrido a Gasolina"),          22_100, 4, (26_000, 118_000)),
    VehicleModel("Nissan",     "Qashqai",  "SUV",       5, ("Gasolina", "Gasóleo",  "Híbrido a Gasolina"),          21_500, 5, (30_000, 130_000)),
    VehicleModel("Seat",       "Arona",    "SUV",       5, ("Gasolina",),                                           16_800, 3, (12_000,  82_000)),
    VehicleModel("Hyundai",    "Tucson",   "SUV",       5, ("Gasolina", "Híbrido a Gasolina", "100% Elétrico"),     26_200, 3, (15_000, 100_000)),
    VehicleModel("Hyundai",    "Kona",     "SUV",       5, ("100% Elétrico", "Gasolina", "Híbrido a Gasolina"),    22_900, 3, (14_000,  98_000)),
    VehicleModel("Kia",        "Niro",     "SUV",       5, ("100% Elétrico", "Híbrido a Gasolina"),                23_300, 3, (16_000,  96_000)),
    VehicleModel("Kia",        "Sportage", "SUV",       5, ("Gasóleo",  "Gasolina", "Híbrido a Gasolina"),          23_800, 4, (20_000, 108_000)),
    # ── Hatchback ────────────────────────────────────────────────────────────
    VehicleModel("Volkswagen", "Golf",     "Hatchback", 5, ("Gasolina", "Gasóleo",  "Híbrido a Gasolina"),          18_500, 5, (30_000, 140_000)),
    VehicleModel("BMW",        "Série 1",  "Hatchback", 5, ("Gasóleo",  "Gasolina", "Híbrido a Gasolina"),          20_900, 4, (18_000, 105_000)),
    VehicleModel("Mercedes",   "Classe A", "Hatchback", 5, ("Gasóleo",  "Gasolina", "Híbrido a Gasolina"),          21_800, 4, (20_000, 108_000)),
    VehicleModel("Audi",       "A3",       "Hatchback", 5, ("Gasóleo",  "Gasolina", "Híbrido a Gasolina"),          22_500, 4, (22_000, 112_000)),
    VehicleModel("Opel",       "Astra",    "Hatchback", 5, ("Gasolina", "Gasóleo"),                                 16_200, 5, (30_000, 145_000)),
    # ── Citadino ─────────────────────────────────────────────────────────────
    VehicleModel("Renault",    "Clio",     "Citadino",  5, ("Gasolina", "Gasóleo", "GPL"),                          12_400, 4, (25_000, 135_000)),
    VehicleModel("Seat",       "Ibiza",    "Citadino",  5, ("Gasolina", "Gasóleo", "GPL"),                          11_800, 5, (32_000, 150_000)),
    VehicleModel("Citroën",    "C3",       "Citadino",  5, ("Gasolina", "Gasóleo"),                                 11_200, 5, (30_000, 145_000)),
    VehicleModel("Fiat",       "500",      "Citadino",  4, ("Gasolina", "Híbrido a Gasolina"),                      13_200, 3, (15_000,  90_000)),
    VehicleModel("Peugeot",    "208",      "Citadino",  5, ("Gasolina", "Gasóleo", "100% Elétrico"),                14_800, 3, (14_000,  95_000)),
    VehicleModel("Toyota",     "Yaris",    "Citadino",  5, ("Gasolina", "Híbrido a Gasolina"),                      15_100, 3, (12_000,  88_000)),
    VehicleModel("Opel",       "Corsa",    "Citadino",  5, ("Gasolina", "Gasóleo", "100% Elétrico"),                13_800, 4, (18_000, 100_000)),
    # ── Elétrico ─────────────────────────────────────────────────────────────
    VehicleModel("Tesla",      "Model 3",  "Elétrico",  5, ("100% Elétrico",),                                     31_800, 2, (12_000,  85_000)),
    VehicleModel("Renault",    "Zoe",      "Elétrico",  5, ("100% Elétrico",),                                     16_900, 3, (10_000,  70_000)),
    VehicleModel("Nissan",     "Leaf",     "Elétrico",  5, ("100% Elétrico",),                                     17_400, 4, (18_000,  90_000)),
    VehicleModel("Volkswagen", "ID.4",     "SUV",       5, ("100% Elétrico",),                                     33_500, 2, ( 8_000,  75_000)),
)

# Dicionário marca → lista de modelos (para generate_trends e generate_forum)
MARCAS_MODELOS: dict[str, list[str]] = {}
for _v in VEHICLES:
    MARCAS_MODELOS.setdefault(_v.marca, []).append(_v.modelo)