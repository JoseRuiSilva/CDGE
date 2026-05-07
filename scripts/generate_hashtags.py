"""
generate_hashtags.py
---------------------------------------------------------------
Gerador de feeds XML Atom sintéticos com volume de menções de
hashtags em redes sociais.

Simula uma fonte externa do tipo Talkwalker/Mention, com métricas
diárias por hashtag e plataforma.

Projeto Auto Escala — CDGE 2025/2026
---------------------------------------------------------------
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import random
import math
import xml.etree.ElementTree as ET


BASE_DIR = Path(__file__).resolve().parent.parent
OUT_BASE = BASE_DIR / "data" / "sources" / "hashtags"

START_DATE = date(2022, 1, 3)   # segunda-feira da semana ISO 1 de 2022
END_DATE = date(2026, 5, 31)
SEED = 42

ATOM_NS = "http://www.w3.org/2005/Atom"
SL_NS = "http://autoescala.pt/social-listening"

ET.register_namespace("", ATOM_NS)
ET.register_namespace("sl", SL_NS)


HASHTAGS = {
    "#SUV": {
        "base": 180,
        "trend": 18,
        "category": "SUV",
        "season": "winter",
    },
    "#carrosusados": {
        "base": 240,
        "trend": 12,
        "category": "geral",
        "season": "all",
    },
    "#carroeletrico": {
        "base": 110,
        "trend": 35,
        "category": "Elétrico",
        "season": "summer",
    },
    "#hibrido": {
        "base": 95,
        "trend": 22,
        "category": "Híbrido",
        "season": "all",
    },
    "#volkswagengolf": {
        "base": 85,
        "trend": 8,
        "category": "Volkswagen Golf",
        "season": "all",
    },
    "#toyotayaris": {
        "base": 75,
        "trend": 10,
        "category": "Toyota Yaris",
        "season": "city",
    },
    "#bmw": {
        "base": 150,
        "trend": 6,
        "category": "BMW",
        "season": "all",
    },
    "#mercedes": {
        "base": 145,
        "trend": 5,
        "category": "Mercedes",
        "season": "all",
    },
    "#testdrive": {
        "base": 100,
        "trend": 14,
        "category": "intenção de compra",
        "season": "spring",
    },
    "#carroNovo": {
        "base": 130,
        "trend": 7,
        "category": "geral",
        "season": "all",
    },
}


def daterange_weeks(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=7)


def week_dates(week_start: date) -> list[date]:
    return [week_start + timedelta(days=i) for i in range(7)]


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
    # Mais atividade ao fim de semana e à segunda-feira.
    weekday = day.weekday()

    if weekday in {5, 6}:
        return 1.25

    if weekday == 0:
        return 1.10

    return 1.0


def generate_total_posts(
    rng: random.Random,
    hashtag: str,
    day: date,
    day_index: int,
    total_days: int,
) -> int:
    cfg = HASHTAGS[hashtag]

    base = cfg["base"]
    trend_component = cfg["trend"] * (day_index / total_days)

    annual_seasonality = 1 + 0.12 * math.sin(2 * math.pi * day.timetuple().tm_yday / 365)
    specific_seasonality = seasonal_factor(day, cfg["season"])
    weekday_factor = weekly_pattern(day)

    noise = rng.gauss(0, base * 0.10)

    value = (
        base
        + trend_component
        + noise
    ) * annual_seasonality * specific_seasonality * weekday_factor

    return max(0, int(round(value)))


def platform_breakdown(rng: random.Random, total_posts: int) -> dict[str, int]:
    instagram_share = rng.uniform(0.55, 0.72)
    twitter_share = rng.uniform(0.18, 0.32)

    instagram = int(round(total_posts * instagram_share))
    twitter = int(round(total_posts * twitter_share))
    youtube = max(0, total_posts - instagram - twitter)

    return {
        "instagram": instagram,
        "twitter": twitter,
        "youtube": youtube,
    }


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

    entry_id = ET.SubElement(entry, f"{{{ATOM_NS}}}id")
    entry_id.text = f"urn:autoescala:hashtags:{hashtag.replace('#', '')}:{day.isoformat()}"

    updated = ET.SubElement(entry, f"{{{ATOM_NS}}}updated")
    updated.text = f"{day.isoformat()}T23:59:59Z"

    sl_hashtag = ET.SubElement(entry, f"{{{SL_NS}}}hashtag")
    sl_hashtag.text = hashtag

    sl_date = ET.SubElement(entry, f"{{{SL_NS}}}date")
    sl_date.text = day.isoformat()

    sl_country = ET.SubElement(entry, f"{{{SL_NS}}}country")
    sl_country.text = "PT"

    sl_category = ET.SubElement(entry, f"{{{SL_NS}}}category")
    sl_category.text = HASHTAGS[hashtag]["category"]

    sl_total = ET.SubElement(entry, f"{{{SL_NS}}}total_posts")
    sl_total.text = str(total_posts)

    sl_breakdown = ET.SubElement(entry, f"{{{SL_NS}}}breakdown")

    for platform, value in breakdown.items():
        sl_platform = ET.SubElement(
            sl_breakdown,
            f"{{{SL_NS}}}platform",
            {"name": platform}
        )
        sl_platform.text = str(value)


def generate_week_feed(
    rng: random.Random,
    week_start: date,
    day_offset: int,
    total_days: int,
) -> ET.ElementTree:
    feed = ET.Element(f"{{{ATOM_NS}}}feed")

    title = ET.SubElement(feed, f"{{{ATOM_NS}}}title")
    title.text = "Auto Escala Social Listening Feed"

    feed_id = ET.SubElement(feed, f"{{{ATOM_NS}}}id")
    iso_year, iso_week, _ = week_start.isocalendar()
    feed_id.text = f"urn:autoescala:hashtags:{iso_year}:W{iso_week:02d}"

    updated = ET.SubElement(feed, f"{{{ATOM_NS}}}updated")
    updated.text = f"{week_start.isoformat()}T00:00:00Z"

    source = ET.SubElement(feed, f"{{{SL_NS}}}source")
    source.text = "Synthetic Talkwalker/Mention Feed"

    auth = ET.SubElement(feed, f"{{{SL_NS}}}auth_type")
    auth.text = "Bearer Token"

    country = ET.SubElement(feed, f"{{{SL_NS}}}country")
    country.text = "PT"

    for i, day in enumerate(week_dates(week_start)):
        if day > END_DATE:
            continue

        current_day_index = day_offset + i

        for hashtag in HASHTAGS:
            total_posts = generate_total_posts(
                rng=rng,
                hashtag=hashtag,
                day=day,
                day_index=current_day_index,
                total_days=total_days,
            )

            breakdown = platform_breakdown(rng, total_posts)

            create_entry(
                feed=feed,
                hashtag=hashtag,
                day=day,
                total_posts=total_posts,
                breakdown=breakdown,
            )

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

        out_dir = OUT_BASE / str(iso_year) / f"W{iso_week:02d}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / f"hashtags_{iso_year}W{iso_week:02d}.xml"

        day_offset = (week_start - START_DATE).days

        tree = generate_week_feed(
            rng=rng,
            week_start=week_start,
            day_offset=day_offset,
            total_days=total_days,
        )

        indent_xml(tree)

        tree.write(
            out_file,
            encoding="utf-8",
            xml_declaration=True,
            short_empty_elements=False,
        )

        entries_count = len(week_dates(week_start)) * len(HASHTAGS)
        summary[f"{iso_year}-W{iso_week:02d}"] = entries_count

        print(f"{out_file}  → {entries_count} entries geradas.")

    return summary


if __name__ == "__main__":
    print("AUTO ESCALA — GERAÇÃO DE HASHTAGS / SOCIAL LISTENING")
    resumo = exportar_hashtags()
    print(f"{len(resumo)} ficheiros semanais gerados com sucesso.")