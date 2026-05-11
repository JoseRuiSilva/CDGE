"""
generate_trends.py
---------------------------------------------------------------
Gerador de dados sintéticos que simulam o Google Trends
para o projeto Auto Escala — CDGE 2025/2026.

Os modelos provêm do catálogo central vehicles.py.
---------------------------------------------------------------
"""

from __future__ import annotations

import json
import math
import random
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR / "scripts"))

from vehicles import MARCAS_MODELOS  # noqa: E402

OUT_BASE = BASE_DIR / "data" / "sources" / "trends"

DATA_INICIO = datetime(2022, 1, 1)
DATA_FIM    = datetime(2026, 5, 1)

REGIOES = ["Lisboa", "Porto", "Braga"]

# Tendências base por marca (refletem posicionamento de mercado)
_TENDENCIA_MARCA: dict[str, int] = {
    "Tesla":      18,
    "Hyundai":    14,
    "Kia":        12,
    "Toyota":     10,
    "Volkswagen":  8,
    "BMW":         5,
    "Mercedes":    4,
    "Peugeot":     6,
    "Renault":     4,
    "Seat":        3,
    "Nissan":      2,
    "Citroën":     3,
    "Fiat":        1,
    "Audi":        5,
    "Opel":        2,
}

# Termos genéricos de pesquisa — coerentes com segmentos do inventário
TERMOS_EXTRA = [
    "SUV usado",
    "carros elétricos usados",
    "carros híbridos usados",
    "carros a gasóleo usados"
]


def gerar_valor_interesse(base: float, mes_idx: int, total_meses: int, tendencia: float) -> float:
    delta   = tendencia * (mes_idx / total_meses)
    sazonal = 8 * math.sin(2 * math.pi * (mes_idx % 12) / 12)
    ruido   = random.gauss(0, 5)
    return max(0.0, min(100.0, base + delta + sazonal + ruido))


def gerar_lista_meses() -> list[datetime]:
    meses: list[datetime] = []
    data = DATA_INICIO
    while data <= DATA_FIM:
        meses.append(data)
        data = (
            datetime(data.year + 1, 1, 1)
            if data.month == 12
            else datetime(data.year, data.month + 1, 1)
        )
    return meses


def gerar_trends() -> list[dict]:
    resultado: list[dict] = []
    meses = gerar_lista_meses()
    total_meses = len(meses)

    for marca, modelos in MARCAS_MODELOS.items():
        tendencia_base = _TENDENCIA_MARCA.get(marca, 5)

        for modelo in modelos:
            # Variação individual por modelo
            tendencia = tendencia_base + random.choice([-6, -3, 0, 3, 6])
            base = random.randint(25, 80)

            for regiao in REGIOES:
                for i, mes in enumerate(meses):
                    ym    = f"{mes.year}-{mes.month:02d}"
                    valor = round(gerar_valor_interesse(base, i, total_meses, tendencia), 1)
                    resultado.append({
                        "termo":           f"{marca} {modelo} usado",
                        "regiao":          regiao,
                        "mes":             ym,
                        "valor_interesse": valor
                    })

    for termo_gen in TERMOS_EXTRA:
        tendencia = random.choice([-5, 0, 5, 10])
        base = random.randint(35, 70)

        for regiao in REGIOES:
            for i, mes in enumerate(meses):
                ym    = f"{mes.year}-{mes.month:02d}"
                valor = round(gerar_valor_interesse(base, i, total_meses, tendencia), 1)
                resultado.append({
                    "termo":           termo_gen,
                    "regiao":          regiao,
                    "mes":             ym,
                    "valor_interesse": valor
                })

    return resultado


def exportar_json_por_mes(trends: list[dict]) -> None:
    por_mes: dict[tuple[str, str], list[dict]] = {}
    for reg in trends:
        ano, mes = reg["mes"].split("-")
        por_mes.setdefault((ano, mes), []).append(reg)

    for (ano, mes), lista in sorted(por_mes.items()):
        out_dir = OUT_BASE / ano / mes
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"trends_{ano}{mes}.json"
        with out_file.open("w", encoding="utf-8") as f:
            json.dump(lista, f, ensure_ascii=False, indent=2)
        print(f"{out_file}  → {len(lista)} registos gerados.")


if __name__ == "__main__":
    print("AUTO ESCALA — GERAÇÃO DE GOOGLE TRENDS")
    random.seed(42)
    dados = gerar_trends()
    exportar_json_por_mes(dados)
    print("Ficheiros mensais gerados com sucesso.")