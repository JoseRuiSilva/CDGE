from __future__ import annotations

import math
import random
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path


# ============================================================
# Configuração geral do gerador
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_BASE = BASE_DIR / "data" / "sources" / "hashtags"

START_DATE = date(2022, 1, 3)   # segunda-feira ISO W01 2022
END_DATE = date(2026, 5, 31)

SEED = 42

ATOM_NS = "http://www.w3.org/2005/Atom"
SL_NS = "http://autoescala.pt/social-listening"

ET.register_namespace("", ATOM_NS)
ET.register_namespace("sl", SL_NS)


# ============================================================
# Catálogo central de veículos
# ============================================================
# Este catálogo deve ser mantido igual nos restantes scripts:
#   - generate_inventory.py
#   - generate_trends.py
#   - generate_forum.py
#   - generate_hashtags.py
#
# Assim, todas as fontes sintéticas usam as mesmas marcas/modelos,
# facilitando a integração e melhorando a qualidade dos dados para
# análise preditiva.

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

    # Hyundai
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
# Hashtags genéricas
# ============================================================
# Estas hashtags representam pesquisas sociais por segmento,
# combustível ou intenção de compra.

GENERIC_HASHTAGS: dict[str, dict] = {
    "#SUV": {
        "base": 180,
        "trend": 18,
        "category": "SUV",
        "tipo": "SUV",
        "marca": "",
        "modelo": "",
        "season": "winter",
    },
    "#citadino": {
        "base": 75,
        "trend": 8,
        "category": "Citadino",
        "tipo": "Citadino",
        "marca": "",
        "modelo": "",
        "season": "city",
    },
    "#hatchback": {
        "base": 70,
        "trend": 7,
        "category": "Hatchback",
        "tipo": "Hatchback",
        "marca": "",
        "modelo": "",
        "season": "all",
    },
    "#sedan": {
        "base": 55,
        "trend": 4,
        "category": "Sedan",
        "tipo": "Sedan",
        "marca": "",
        "modelo": "",
        "season": "all",
    },
    "#carroeletrico": {
        "base": 120,
        "trend": 35,
        "category": "Elétrico",
        "tipo": "Elétrico",
        "marca": "",
        "modelo": "",
        "season": "summer",
    },
    "#hibrido": {
        "base": 95,
        "trend": 22,
        "category": "Híbrido",
        "tipo": "Híbrido",
        "marca": "",
        "modelo": "",
        "season": "all",
    },
    "#gasoleo": {
        "base": 80,
        "trend": -4,
        "category": "Gasóleo",
        "tipo": "Gasóleo",
        "marca": "",
        "modelo": "",
        "season": "winter",
    },
    "#gasolina": {
        "base": 85,
        "trend": 3,
        "category": "Gasolina",
        "tipo": "Gasolina",
        "marca": "",
        "modelo": "",
        "season": "all",
    },
    "#carrosusados": {
        "base": 220,
        "trend": 14,
        "category": "Geral",
        "tipo": "Geral",
        "marca": "",
        "modelo": "",
        "season": "all",
    },
    "#seminovos": {
        "base": 130,
        "trend": 12,
        "category": "Geral",
        "tipo": "Geral",
        "marca": "",
        "modelo": "",
        "season": "all",
    },
    "#testdrive": {
        "base": 90,
        "trend": 9,
        "category": "Intenção de compra",
        "tipo": "Geral",
        "marca": "",
        "modelo": "",
        "season": "spring",
    },
}


# ============================================================
# Parâmetros de tendência
# ============================================================

BRAND_TREND: dict[str, int] = {
    "Tesla": 30,
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

TYPE_TREND: dict[str, int] = {
    "Elétrico": 26,
    "SUV": 16,
    "Híbrido": 15,
    "Citadino": 8,
    "Hatchback": 6,
    "Sedan": 4,
}


# ============================================================
# Funções auxiliares para hashtags
# ============================================================

def slugify_hashtag(text: str) -> str:
    """
    Converte marca/modelo numa hashtag simples.

    Exemplos:
        "Volkswagen Golf" -> "#volkswagengolf"
        "BMW Série 1"     -> "#bmwserie1"
        "Citroën C3"      -> "#citroenc3"
    """
    replacements = {
        "ã": "a", "á": "a", "à": "a", "â": "a",
        "é": "e", "ê": "e",
        "í": "i",
        "ó": "o", "ô": "o",
        "ú": "u",
        "ç": "c",
        "ï": "i",
    }

    value = text.lower()

    for old, new in replacements.items():
        value = value.replace(old, new)

    value = re.sub(r"[^a-z0-9]+", "", value)

    return f"#{value}"


def build_vehicle_hashtags() -> dict[str, dict]:
    """
    Cria automaticamente uma hashtag para cada marca/modelo do catálogo.

    Isto garante que todos os modelos usados no inventário também existem
    na fonte de social listening.
    """
    hashtags: dict[str, dict] = {}

    for vehicle in VEHICLES:
        hashtag = slugify_hashtag(f"{vehicle.marca} {vehicle.modelo}")

        brand = BRAND_TREND.get(vehicle.marca, 5)
        tipo = TYPE_TREND.get(vehicle.tipo, 5)

        trend = int(round(brand * 0.60 + tipo * 0.40))

        if vehicle.tipo == "Elétrico":
            season = "summer"
            base_min, base_max = 45, 105
        elif vehicle.tipo == "SUV":
            season = "winter"
            base_min, base_max = 55, 125
        elif vehicle.tipo == "Citadino":
            season = "city"
            base_min, base_max = 40, 95
        elif vehicle.tipo == "Sedan":
            season = "all"
            base_min, base_max = 35, 80
        else:
            season = "all"
            base_min, base_max = 40, 90

        # A base fica determinística por modelo, para resultados reproduzíveis.
        stable = sum(ord(ch) for ch in hashtag)
        base = base_min + stable % (base_max - base_min + 1)

        hashtags[hashtag] = {
            "base": base,
            "trend": trend,
            "category": f"{vehicle.marca} {vehicle.modelo}",
            "tipo": vehicle.tipo,
            "marca": vehicle.marca,
            "modelo": vehicle.modelo,
            "season": season,
        }

    return hashtags


def build_all_hashtags() -> dict[str, dict]:
    """
    Junta hashtags genéricas e hashtags de todos os modelos.
    """
    all_tags = dict(GENERIC_HASHTAGS)
    all_tags.update(build_vehicle_hashtags())

    return all_tags


HASHTAGS = build_all_hashtags()


# ============================================================
# Funções auxiliares de datas
# ============================================================

def daterange_weeks(start: date, end: date):
    current = start

    while current <= end:
        yield current
        current += timedelta(days=7)


def week_dates(week_start: date) -> list[date]:
    return [week_start + timedelta(days=i) for i in range(7)]


# ============================================================
# Fatores de sazonalidade e padrão semanal
# ============================================================

def seasonal_factor(day: date, season: str) -> float:
    month = day.month

    if season == "winter" and month in {11, 12, 1, 2}:
        return 1.35

    if season == "summer" and month in {6, 7, 8}:
        return 1.55

    if season == "spring" and month in {3, 4, 5}:
        return 1.30

    if season == "city" and month in {9, 10, 11}:
        return 1.20

    return 1.0


def weekly_pattern(day: date) -> float:
    weekday = day.weekday()

    if weekday in {5, 6}:
        return 1.25

    if weekday == 0:
        return 1.10

    return 1.0


# ============================================================
# Geração de volumes sociais
# ============================================================

def generate_total_posts(
    rng: random.Random,
    hashtag: str,
    day: date,
    day_index: int,
    total_days: int,
) -> int:
    """
    Gera o volume diário de menções para uma hashtag.

    Componentes:
      - base própria da hashtag;
      - tendência ao longo do tempo;
      - sazonalidade anual;
      - época específica da categoria;
      - padrão semanal;
      - ruído.
    """
    cfg = HASHTAGS[hashtag]

    base = cfg["base"]
    trend = cfg["trend"] * (day_index / max(1, total_days - 1))

    annual = 1 + 0.12 * math.sin(
        2 * math.pi * day.timetuple().tm_yday / 365
    )

    spec = seasonal_factor(day, cfg["season"])
    wday = weekly_pattern(day)

    noise = rng.gauss(0, max(5, base * 0.10))

    total = (base + trend + noise) * annual * spec * wday

    return max(0, int(round(total)))


def platform_breakdown(
    rng: random.Random,
    total_posts: int,
    hashtag: str,
) -> dict[str, int]:
    """
    Distribui o total de posts por plataforma.

    A distribuição varia consoante a categoria:
      - modelos premium e elétricos tendem a ter mais Instagram/YouTube;
      - hashtags gerais têm mais equilíbrio;
      - conteúdos de test drive têm mais peso no YouTube.
    """
    cfg = HASHTAGS[hashtag]
    tipo = cfg["tipo"]
    category = cfg["category"].lower()

    if "testdrive" in hashtag:
        ig_share = rng.uniform(0.45, 0.58)
        tw_share = rng.uniform(0.12, 0.22)

    elif tipo == "Elétrico":
        ig_share = rng.uniform(0.52, 0.66)
        tw_share = rng.uniform(0.18, 0.30)

    elif cfg["marca"] in {"BMW", "Mercedes", "Audi", "Tesla"}:
        ig_share = rng.uniform(0.60, 0.75)
        tw_share = rng.uniform(0.13, 0.25)

    elif category in {"geral", "gasóleo", "gasolina"}:
        ig_share = rng.uniform(0.48, 0.62)
        tw_share = rng.uniform(0.22, 0.34)

    else:
        ig_share = rng.uniform(0.55, 0.72)
        tw_share = rng.uniform(0.18, 0.32)

    instagram = int(round(total_posts * ig_share))
    twitter = int(round(total_posts * tw_share))
    youtube = max(0, total_posts - instagram - twitter)

    return {
        "instagram": instagram,
        "twitter": twitter,
        "youtube": youtube,
    }


# ============================================================
# Criação do XML Atom
# ============================================================

def create_entry(
    feed: ET.Element,
    hashtag: str,
    day: date,
    total_posts: int,
    breakdown: dict[str, int],
) -> None:
    """
    Cria uma entrada XML para uma hashtag num determinado dia.
    """
    cfg = HASHTAGS[hashtag]

    entry = ET.SubElement(feed, f"{{{ATOM_NS}}}entry")

    title = ET.SubElement(entry, f"{{{ATOM_NS}}}title")
    title.text = f"Métricas sociais para {hashtag} em Portugal"

    eid = ET.SubElement(entry, f"{{{ATOM_NS}}}id")
    eid.text = f"urn:autoescala:hashtags:{hashtag.replace('#', '')}:{day.isoformat()}"

    updated = ET.SubElement(entry, f"{{{ATOM_NS}}}updated")
    updated.text = f"{day.isoformat()}T23:59:59Z"

    fields = [
        (f"{{{SL_NS}}}hashtag", hashtag),
        (f"{{{SL_NS}}}date", day.isoformat()),
        (f"{{{SL_NS}}}country", "PT"),
        (f"{{{SL_NS}}}category", cfg["category"]),
        (f"{{{SL_NS}}}total_posts", str(total_posts)),
    ]

    for tag, text in fields:
        el = ET.SubElement(entry, tag)
        el.text = text

    breakdown_el = ET.SubElement(entry, f"{{{SL_NS}}}breakdown")

    for platform, value in breakdown.items():
        pl = ET.SubElement(
            breakdown_el,
            f"{{{SL_NS}}}platform",
            {"name": platform},
        )
        pl.text = str(value)


def generate_week_feed(
    rng: random.Random,
    week_start: date,
    day_offset: int,
    total_days: int,
) -> ET.ElementTree:
    """
    Gera o feed XML de uma semana.
    """
    feed = ET.Element(f"{{{ATOM_NS}}}feed")

    iso_year, iso_week, _ = week_start.isocalendar()

    feed_fields = [
        (f"{{{ATOM_NS}}}title", "Auto Escala Social Listening Feed"),
        (f"{{{ATOM_NS}}}id", f"urn:autoescala:hashtags:{iso_year}:W{iso_week:02d}"),
        (f"{{{ATOM_NS}}}updated", f"{week_start.isoformat()}T00:00:00Z"),
        (f"{{{SL_NS}}}source", "Synthetic Talkwalker/Mention Feed"),
        (f"{{{SL_NS}}}auth_type", "Bearer Token"),
        (f"{{{SL_NS}}}country", "PT"),
    ]

    for tag, text in feed_fields:
        el = ET.SubElement(feed, tag)
        el.text = text

    for i, day in enumerate(week_dates(week_start)):
        if day > END_DATE:
            continue

        for hashtag in HASHTAGS:
            total = generate_total_posts(
                rng=rng,
                hashtag=hashtag,
                day=day,
                day_index=day_offset + i,
                total_days=total_days,
            )

            breakdown = platform_breakdown(
                rng=rng,
                total_posts=total,
                hashtag=hashtag,
            )

            create_entry(
                feed=feed,
                hashtag=hashtag,
                day=day,
                total_posts=total,
                breakdown=breakdown,
            )

    return ET.ElementTree(feed)


def indent_xml(tree: ET.ElementTree) -> None:
    """
    Formata o XML para ficar legível.
    """
    try:
        ET.indent(tree, space="  ", level=0)
    except AttributeError:
        pass


# ============================================================
# Exportação dos ficheiros XML
# ============================================================

def exportar_hashtags() -> dict[str, int]:
    """
    Exporta os feeds XML semanais.

    Estrutura gerada:
        data/sources/hashtags/2022/W01/hashtags_2022W01.xml
        data/sources/hashtags/2022/W02/hashtags_2022W02.xml
        ...
    """
    rng = random.Random(SEED)

    OUT_BASE.mkdir(parents=True, exist_ok=True)

    total_days = (END_DATE - START_DATE).days + 1
    summary: dict[str, int] = {}

    for week_start in daterange_weeks(START_DATE, END_DATE):
        iso_year, iso_week, _ = week_start.isocalendar()

        out_dir = OUT_BASE / str(iso_year) / f"W{iso_week:02d}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / f"hashtags_{iso_year}W{iso_week:02d}.xml"

        tree = generate_week_feed(
            rng=rng,
            week_start=week_start,
            day_offset=(week_start - START_DATE).days,
            total_days=total_days,
        )

        indent_xml(tree)

        tree.write(
            out_file,
            encoding="utf-8",
            xml_declaration=True,
            short_empty_elements=False,
        )

        # Conta apenas os dias realmente gerados. Isto evita erro na última semana,
        # caso a semana ultrapasse a END_DATE.
        valid_days = [
            d
            for d in week_dates(week_start)
            if d <= END_DATE
        ]

        entries_count = len(valid_days) * len(HASHTAGS)
        summary[f"{iso_year}-W{iso_week:02d}"] = entries_count

        print(f"{out_file}  -> {entries_count} entries geradas.")

    return summary


# ============================================================
# Execução direta
# ============================================================

if __name__ == "__main__":
    print("AUTO ESCALA — GERAÇÃO DE HASHTAGS / SOCIAL LISTENING")

    resumo = exportar_hashtags()

    print(f"{len(resumo)} ficheiros semanais gerados com sucesso.")
    print(f"{len(HASHTAGS)} hashtags monitorizadas.")