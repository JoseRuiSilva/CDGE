"""
generate_demografia.py
---------------------------------------------------------------
Gera estimativas demográficas regionais para o projeto Auto Escala.

Produz um CSV por ano (demografia_YYYY.csv), cobrindo 2022, 2023 e 2024.
Cada ficheiro contém os 18 distritos de Portugal continental.

Grain:   distrito + ano_referencia  (BK no Silver e no DW)
Bronze:  ingere ficheiros novos incrementalmente — adicionar 2025
         basta criar demografia_2025.csv e reingerir.

Projeto Auto Escala — CDGE 2025/2026
---------------------------------------------------------------
"""

import csv
from pathlib import Path

BASE_DIR    = Path(__file__).resolve().parent.parent
OUTPUT_ROOT = BASE_DIR / "data" / "sources" / "demografia"

ANOS = [2022, 2023, 2024]

DISTRITOS = [
    "Aveiro", "Beja", "Braga", "Bragança", "Castelo Branco", "Coimbra",
    "Évora", "Faro", "Guarda", "Leiria", "Lisboa", "Portalegre", "Porto",
    "Santarém", "Setúbal", "Viana do Castelo", "Vila Real", "Viseu",
]

POPULACAO_BASE = {
    "Aveiro":           700_000,
    "Beja":             144_000,
    "Braga":            846_000,
    "Bragança":         122_000,
    "Castelo Branco":   177_000,
    "Coimbra":          408_000,
    "Évora":            152_000,
    "Faro":             467_000,
    "Guarda":           142_000,
    "Leiria":           458_000,
    "Lisboa":         2_871_000,
    "Portalegre":       104_000,
    "Porto":          1_785_000,
    "Santarém":         425_000,
    "Setúbal":          877_000,
    "Viana do Castelo": 231_000,
    "Vila Real":        185_000,
    "Viseu":            351_000,
}

FIELDS = [
    "distrito", "ano_referencia", "populacao_total",
    "pct_18_24", "pct_25_34", "pct_35_49", "pct_50_64", "pct_65_mais",
    "pct_masculino", "pct_feminino",
]


def _perfil_etario(distrito: str) -> tuple:
    """Devolve (p18_24, p25_34, p35_49, p50_64, p65m) por tipo de distrito."""
    if distrito in {"Lisboa", "Porto", "Braga"}:
        return 10.5, 14.5, 24.0, 26.0, 25.0
    if distrito in {"Faro", "Aveiro", "Leiria", "Setúbal"}:
        return  9.0, 12.0, 22.0, 27.0, 30.0
    return 7.5, 10.0, 19.0, 25.5, 38.0   # interior mais envelhecido


def _variacao_populacional(distrito: str, ano: int) -> float:
    """Fator multiplicativo face à população base (ano 2022 = 1.0)."""
    delta = ano - 2022
    if distrito in {"Bragança", "Guarda", "Portalegre"}:
        return 1 - delta * 0.008   # interior perde população
    return 1 + delta * 0.005       # restantes crescem ligeiramente


def generate_demografia() -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    for ano in ANOS:
        out_file = OUTPUT_ROOT / f"demografia_{ano}.csv"
        registos = []

        for distrito in DISTRITOS:
            p18_24, p25_34, p35_49, p50_64, p65m = _perfil_etario(distrito)
            fator   = _variacao_populacional(distrito, ano)
            pop_ano = int(POPULACAO_BASE[distrito] * fator)

            registos.append({
                "distrito":        distrito,
                "ano_referencia":  ano,
                "populacao_total": pop_ano,
                "pct_18_24":       p18_24,
                "pct_25_34":       p25_34,
                "pct_35_49":       p35_49,
                "pct_50_64":       p50_64,
                "pct_65_mais":     p65m,
                "pct_masculino":   47.5,
                "pct_feminino":    52.5,
            })

        with out_file.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(registos)

        print(f"  {out_file.name}  → {len(registos)} distritos")

    print(f"\nGerados {len(ANOS)} ficheiros anuais em {OUTPUT_ROOT}")


if __name__ == "__main__":
    generate_demografia()