"""
generate_trends.py
---------------------------------------------------------------
Gerador de dados sintéticos que simulam o Google Trends
para o projeto Auto Escala — CDGE 2025/2026.
---------------------------------------------------------------
"""

from datetime import datetime
from pathlib import Path
import random
import math
import json

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_BASE = BASE_DIR / "data" / "sources" / "trends"

DATA_INICIO = datetime(2022, 1, 1)
DATA_FIM = datetime(2026, 5, 1)
REGIOES = ["Lisboa", "Porto", "Braga"]

try:
    import sys
    sys.path.append(str(BASE_DIR / "scripts"))
    from generate_inventory import VEHICLES

    MARCAS_MODELOS = {}
    for v in VEHICLES:
        MARCAS_MODELOS.setdefault(v.marca, []).append(v.modelo)
except Exception:
    MARCAS_MODELOS = {
        "Volkswagen": ["Golf", "T-Roc", "Tiguan"],
        "Peugeot": ["208", "3008"],
        "Renault": ["Clio", "Zoe"],
        "BMW": ["Série 1", "X1", "Série 3"],
        "Mercedes": ["Classe A", "GLA"],
        "Hyundai": ["Kona"],
        "Nissan": ["Qashqai", "Leaf"],
        "Tesla": ["Model 3"],
        "Seat": ["Ibiza", "Arona"],
        "Citroën": ["C3"],
        "Fiat": ["500"],
        "Kia": ["Niro"],
        "Audi": ["A3"]
    }

TERMOS_EXTRA = [
    "SUV usado",
    "carros elétricos usados",
    "carros híbridos usados",
    "citadino económico",
    "carros familiares",
    "carros seminovos",
    "carros a gasóleo usados",
]


def gerar_valor_interesse(base: float, mes_idx: int, total_meses: int, tendencia: float) -> float:
    delta = tendencia * (mes_idx / total_meses)
    sazonal = 8 * math.sin(2 * math.pi * (mes_idx % 12) / 12)
    ruido = random.gauss(0, 5)
    return max(0, min(100, base + delta + sazonal + ruido))


def gerar_lista_meses() -> list[datetime]:
    meses = []
    data = DATA_INICIO
    while data <= DATA_FIM:
        meses.append(data)
        if data.month == 12:
            data = datetime(data.year + 1, 1, 1)
        else:
            data = datetime(data.year, data.month + 1, 1)
    return meses


def gerar_trends() -> list[dict]:
    resultado = []
    meses = gerar_lista_meses()
    total_meses = len(meses)

    for marca, modelos in MARCAS_MODELOS.items():
        for modelo in modelos:
            tendencia = random.choice([-10, -5, 0, 8, 12, 18])
            base = random.randint(25, 80)

            for regiao in REGIOES:
                for i, mes in enumerate(meses):
                    ym = f"{mes.year}-{mes.month:02d}"
                    valor = round(gerar_valor_interesse(base, i, total_meses, tendencia), 1)
                    termo = f"{marca} {modelo} usado"
                    resultado.append({
                        "termo": termo,
                        "marca": marca,
                        "modelo": modelo,
                        "regiao": regiao,
                        "mes": ym,
                        "valor_interesse": valor
                    })

    for termo_gen in TERMOS_EXTRA:
        tendencia = random.choice([-5, 5, 10])
        base = random.randint(35, 70)

        for regiao in REGIOES:
            for i, mes in enumerate(meses):
                ym = f"{mes.year}-{mes.month:02d}"
                valor = round(gerar_valor_interesse(base, i, total_meses, tendencia), 1)
                resultado.append({
                    "termo": termo_gen,
                    "marca": None,
                    "modelo": None,
                    "regiao": regiao,
                    "mes": ym,
                    "valor_interesse": valor
                })

    return resultado


def exportar_json_por_mes(trends: list[dict]) -> None:
    por_mes = {}
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
    dados = gerar_trends()
    exportar_json_por_mes(dados)
    print("Ficheiros mensais gerados com sucesso.")
