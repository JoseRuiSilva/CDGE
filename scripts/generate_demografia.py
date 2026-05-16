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
import random
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

# Colunas simplificadas: percentagens substituídas por mean_age
FIELDS = [
    "distrito", "ano_referencia", "populacao_total",
    "mean_age", "pct_masculino", "pct_feminino",
]


def _idade_media_distrito(distrito: str) -> float:
    """Devolve a idade média estimada por tipo de distrito."""
    # Litoral/Grandes Centros (Mais jovens)
    if distrito in {"Lisboa", "Porto", "Braga"}:
        return 41.5
    # Litoral Sul/Centro
    if distrito in {"Faro", "Aveiro", "Leiria", "Setúbal"}:
        return 43.2
    # Interior (Mais envelhecido)
    return 47.8


def _variacao_populacional(distrito: str, ano: int) -> float:
    """Fator multiplicativo face à população base (ano 2022 = 1.0)."""
    delta = ano - 2022
    if distrito in {"Bragança", "Guarda", "Portalegre"}:
        return 1 - delta * 0.008   # interior perde população
    return 1 + delta * 0.005       # restantes crescem ligeiramente


def generate_demografia() -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    random.seed(42) # Garante que a aleatoriedade é reproduzível

    for ano in ANOS:
        out_file = OUTPUT_ROOT / f"demografia_{ano}.csv"
        registos = []

        for distrito in DISTRITOS:
            mean_age = _idade_media_distrito(distrito)
            # Adiciona um ligeiro ruído de até meio ano para a média não ser estática
            mean_age += random.uniform(-0.5, 0.5)
            
            fator   = _variacao_populacional(distrito, ano)
            pop_ano = int(POPULACAO_BASE[distrito] * fator)

            registos.append({
                "distrito":        distrito,
                "ano_referencia":  ano,
                "populacao_total": pop_ano,
                "mean_age":        round(mean_age, 2),
                "pct_masculino":   47.5,
                "pct_feminino":    52.5,
            })

        with out_file.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(registos)

        print(f"  {out_file.name}  -> {len(registos)} distritos")

    print(f"\nGerados {len(ANOS)} ficheiros anuais em {OUTPUT_ROOT}")


if __name__ == "__main__":
    generate_demografia()