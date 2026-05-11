"""
generate_hashtags.py
---------------------------------------------------------------
Gerador de feeds XML Atom sintéticos com volume de menções de
hashtags em redes sociais.

Simula uma fonte externa do tipo Talkwalker/Mention, com métricas
diárias por hashtag e plataforma.

Hashtags alinhadas com o catálogo de veículos em vehicles.py.

Projeto Auto Escala — CDGE 2025/2026
---------------------------------------------------------------
"""

from __future__ import annotations

import math
import random
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_BASE = BASE_DIR / "data" / "sources" / "hashtags"

START_DATE = date(2022, 1, 3)   # segunda-feira ISO W01 2022
END_DATE   = date(2026, 5, 31)
SEED = 42

ATOM_NS = "http://www.w3.org/2005/Atom"
SL_NS   = "http://autoescala.pt/social-listening"

ET.register_namespace("",    ATOM_NS)
ET.register_namespace("sl",  SL_NS)


# Hashtags coerentes com o catálogo de veículos
HASHTAGS: dict[str, dict] = {
    # Segmentos
    "#SUV":               {"base": 180, "trend": 18, "category": "SUV",         "season": "winter"},
    "#carroeletrico":     {"base": 110, "trend": 35, "category": "Elétrico",     "season": "summer"},
    "#hibrido":           {"base":  95, "trend": 22, "category": "Híbrido",      "season": "all"},
    "#citadino":          {"base":  60, "trend":  8, "category": "Citadino",     "season": "city"},
    # Marcas / modelos do catálogo
    "#volkswagengolf":    {"base":  85, "trend":  8, "category": "Volkswagen Golf",   "season": "all"},
    "#toyotayaris":       {"base":  75, "trend": 10, "category": "Toyota Yaris",      "season": "city"},
    "#bmw":               {"base": 150, "trend":  6, "category": "BMW",               "season": "all"},
    "#mercedes":          {"base": 145, "trend":  5, "category": "Mercedes",          "season": "all"},
    "#teslamodel3":       {"base":  90, "trend": 28, "category": "Tesla Model 3",     "season": "summer"},
    "#kianiro":           {"base":  55, "trend": 16, "category": "Kia Niro",          "season": "summer"},
    "#hyundaikona":       {"base":  60, "trend": 14, "category": "Hyundai Kona",      "season": "summer"},
    "#peugeot208":        {"base":  65, "trend":  9, "category": "Peugeot 208",       "season": "city"},
    "#renaultclio":       {"base":  70, "trend":  5, "category": "Renault Clio",      "season": "all"},
    "#volkswagenid4":     {"base":  40, "trend": 30, "category": "Volkswagen ID.4",   "season": "summer"},
}


def daterange_weeks(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=7)


def week_dates(week_start: date) -> list[date]:
    return [week_start + timedelta(days=i) for i in range(7)]


def seasonal_factor(day: date, season: str) -> float:
    m = day.month
    if season == "winter" and m in {11, 12, 1, 2}:  return 1.35
    if season == "summer" and m in {6, 7, 8}:        return 1.55
    if season == "spring" and m in {3, 4, 5}:        return 1.30
    if season == "city"   and m in {9, 10, 11}:      return 1.20
    return 1.0


def weekly_pattern(day: date) -> float:
    wd = day.weekday()
    if wd in {5, 6}: return 1.25   # fim-de-semana
    if wd == 0:      return 1.10   # segunda
    return 1.0


def generate_total_posts(
    rng: random.Random,
    hashtag: str,
    day: date,
    day_index: int,
    total_days: int,
) -> int:
    cfg = HASHTAGS[hashtag]
    base   = cfg["base"]
    trend  = cfg["trend"] * (day_index / total_days)
    annual = 1 + 0.12 * math.sin(2 * math.pi * day.timetuple().tm_yday / 365)
    spec   = seasonal_factor(day, cfg["season"])
    wday   = weekly_pattern(day)
    noise  = rng.gauss(0, base * 0.10)

    return max(0, int(round((base + trend + noise) * annual * spec * wday)))


def platform_breakdown(rng: random.Random, total_posts: int) -> dict[str, int]:
    ig_share = rng.uniform(0.55, 0.72)
    tw_share = rng.uniform(0.18, 0.32)
    instagram = int(round(total_posts * ig_share))
    twitter   = int(round(total_posts * tw_share))
    youtube   = max(0, total_posts - instagram - twitter)
    return {"instagram": instagram, "twitter": twitter, "youtube": youtube}


def create_entry(
    feed: ET.Element,
    hashtag: str,
    day: date,
    total_posts: int,
    breakdown: dict[str, int],
) -> None:
    entry = ET.SubElement(feed, f"{{{ATOM_NS}}}entry")

    title = ET.SubElement(entry, f"{{{ATOM_NS}}}title")
    title.text = f"Métricas sociais para {hashtag} em Portugal"

    eid = ET.SubElement(entry, f"{{{ATOM_NS}}}id")
    eid.text = f"urn:autoescala:hashtags:{hashtag.replace('#', '')}:{day.isoformat()}"

    updated = ET.SubElement(entry, f"{{{ATOM_NS}}}updated")
    updated.text = f"{day.isoformat()}T23:59:59Z"

    for tag, text in [
        (f"{{{SL_NS}}}hashtag",    hashtag),
        (f"{{{SL_NS}}}date",       day.isoformat()),
        (f"{{{SL_NS}}}country",    "PT"),
        (f"{{{SL_NS}}}category",   HASHTAGS[hashtag]["category"]),
        (f"{{{SL_NS}}}total_posts", str(total_posts)),
    ]:
        el = ET.SubElement(entry, tag)
        el.text = text

    breakdown_el = ET.SubElement(entry, f"{{{SL_NS}}}breakdown")
    for platform, value in breakdown.items():
        pl = ET.SubElement(breakdown_el, f"{{{SL_NS}}}platform", {"name": platform})
        pl.text = str(value)


def generate_week_feed(
    rng: random.Random,
    week_start: date,
    day_offset: int,
    total_days: int,
) -> ET.ElementTree:
    feed = ET.Element(f"{{{ATOM_NS}}}feed")

    iso_year, iso_week, _ = week_start.isocalendar()

    for tag, text in [
        (f"{{{ATOM_NS}}}title",   "Auto Escala Social Listening Feed"),
        (f"{{{ATOM_NS}}}id",      f"urn:autoescala:hashtags:{iso_year}:W{iso_week:02d}"),
        (f"{{{ATOM_NS}}}updated", f"{week_start.isoformat()}T00:00:00Z"),
        (f"{{{SL_NS}}}source",    "Synthetic Talkwalker/Mention Feed"),
        (f"{{{SL_NS}}}auth_type", "Bearer Token"),
        (f"{{{SL_NS}}}country",   "PT"),
    ]:
        el = ET.SubElement(feed, tag)
        el.text = text

    for i, day in enumerate(week_dates(week_start)):
        if day > END_DATE:
            continue
        for hashtag in HASHTAGS:
            total = generate_total_posts(
                rng, hashtag, day, day_offset + i, total_days
            )
            create_entry(feed, hashtag, day, total, platform_breakdown(rng, total))

    return ET.ElementTree(feed)


def indent_xml(tree: ET.ElementTree) -> None:
    try:
        ET.indent(tree, space="  ", level=0)
    except AttributeError:
        pass


def exportar_hashtags() -> dict[str, int]:
    rng = random.Random(SEED)
    OUT_BASE.mkdir(parents=True, exist_ok=True)
    total_days = (END_DATE - START_DATE).days + 1
    summary: dict[str, int] = {}

    for week_start in daterange_weeks(START_DATE, END_DATE):
        iso_year, iso_week, _ = week_start.isocalendar()
        out_dir  = OUT_BASE / str(iso_year) / f"W{iso_week:02d}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"hashtags_{iso_year}W{iso_week:02d}.xml"

        tree = generate_week_feed(
            rng, week_start,
            day_offset=(week_start - START_DATE).days,
            total_days=total_days,
        )
        indent_xml(tree)
        tree.write(out_file, encoding="utf-8", xml_declaration=True, short_empty_elements=False)

        entries_count = len(week_dates(week_start)) * len(HASHTAGS)
        summary[f"{iso_year}-W{iso_week:02d}"] = entries_count
        print(f"{out_file}  → {entries_count} entries geradas.")

    return summary


if __name__ == "__main__":
    print("AUTO ESCALA — GERAÇÃO DE HASHTAGS / SOCIAL LISTENING")
    resumo = exportar_hashtags()
    print(f"{len(resumo)} ficheiros semanais gerados com sucesso.")