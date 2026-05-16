"""
generate_clientes.py
---------------------------------------------------------------
Gera snapshots mensais de clientes para o projeto Auto Escala.

Em vez de um único ficheiro estático, produz um CSV por mês
(clientes_YYYYMM.csv) cobrindo 2022-01 a 2024-12.

Dinâmica de localização:
  - Cada cliente começa com um distrito inicial (ponderado pela
    população real, tal como antes).
  - A cada mês, cada cliente tem uma probabilidade de ~0.42% de
    mudar de distrito (≈ 5% ao ano), migrando preferencialmente
    para distritos populosos (Lisboa, Porto, Braga, Setúbal).
  - A idade avança um ano a cada 12 meses (snapshot de Janeiro).

Grain do ficheiro: um registo por cliente por mês.
BK no Silver: nif + ano_mes

Projeto Auto Escala — CDGE 2025/2026
---------------------------------------------------------------
"""

from __future__ import annotations

import csv
import random
from datetime import date
from pathlib import Path

BASE_DIR    = Path(__file__).resolve().parent.parent
OUTPUT_ROOT = BASE_DIR / "data" / "sources" / "clientes"

SEED         = 42
NUM_CLIENTES = 5000

START_DATE = date(2022, 1, 1)
END_DATE   = date(2024, 12, 1)

# Probabilidade mensal de mudança de distrito (~5% / 12 meses)
PROB_MUDANCA_MENSAL = 0.05 / 12

DISTRITOS = [
    "Aveiro", "Beja", "Braga", "Bragança", "Castelo Branco", "Coimbra",
    "Évora", "Faro", "Guarda", "Leiria", "Lisboa", "Portalegre", "Porto",
    "Santarém", "Setúbal", "Viana do Castelo", "Vila Real", "Viseu",
]

# Pesos para distribuição inicial — aproximados à população real
_PESOS_INICIAL = [5, 2, 10, 2, 2, 6, 2, 5, 2, 6, 25, 1, 18, 4, 8, 3, 3, 4]

# Pesos para destino de migração — migrantes tendem para grandes centros
_PESOS_MIGRACAO = [6, 1, 12, 1, 1, 5, 1, 6, 1, 6, 30, 1, 20, 3, 10, 2, 2, 3]

NOMES_MASC = [
    "João", "Tiago", "Rui", "José", "António", "Manuel", "Carlos",
    "Pedro", "Luís", "Miguel", "Nuno", "Ricardo", "Hugo", "Bruno", "Diogo",
]
NOMES_FEM = [
    "Maria", "Ana", "Margarida", "Sofia", "Catarina", "Inês", "Joana",
    "Marta", "Diana", "Sara", "Beatriz", "Teresa", "Patrícia", "Cláudia", "Rita",
]
APELIDOS = [
    "Silva", "Santos", "Ferreira", "Pereira", "Oliveira", "Costa",
    "Rodrigues", "Martins", "Jesus", "Sousa", "Fernandes", "Gomes",
    "Marques", "Almeida", "Ribeiro",
]


def _generate_nif(rng: random.Random) -> str:
    first = rng.choice([1, 2, 3])
    rest  = [rng.randint(0, 9) for _ in range(7)]
    base  = [first] + rest
    check_sum  = sum(d * (9 - i) for i, d in enumerate(base))
    remainder  = check_sum % 11
    check_digit = 0 if remainder in [0, 1] else 11 - remainder
    return "".join(map(str, base)) + str(check_digit)


def _daterange_months(start: date, end: date):
    current = date(start.year, start.month, 1)
    while current <= end:
        yield current
        current = (
            date(current.year + 1, 1, 1)
            if current.month == 12
            else date(current.year, current.month + 1, 1)
        )


def generate_clientes() -> None:
    rng = random.Random(SEED)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    # ── 1. Gerar perfis base de todos os clientes ──────────────────────────────
    used_nifs: set[str] = set()
    clientes_base: list[dict] = []

    for _ in range(NUM_CLIENTES):
        # NIF único
        while True:
            nif = _generate_nif(rng)
            if nif not in used_nifs:
                used_nifs.add(nif)
                break

        genero = rng.choice(["M", "F"])
        nome   = (
            f"{rng.choice(NOMES_MASC)} {rng.choice(APELIDOS)} {rng.choice(APELIDOS)}"
            if genero == "M"
            else f"{rng.choice(NOMES_FEM)} {rng.choice(APELIDOS)} {rng.choice(APELIDOS)}"
        )

        # Idade no mês inicial (Jan 2022)
        idade_inicial = max(18, min(85, int(rng.gauss(42, 12))))

        # Distrito inicial
        distrito_inicial = rng.choices(DISTRITOS, weights=_PESOS_INICIAL, k=1)[0]

        clientes_base.append({
            "nif":             nif,
            "nome":            nome,
            "genero":          genero,
            "idade_jan2022":   idade_inicial,
            "distrito_atual":  distrito_inicial,
        })

    # ── 2. Iterar meses e gerar snapshot por mês ───────────────────────────────
    meses = list(_daterange_months(START_DATE, END_DATE))
    total_ficheiros = 0

    for idx_mes, mes in enumerate(meses):
        ano_mes  = f"{mes.year}{mes.month:02d}"
        out_file = OUTPUT_ROOT / f"clientes_{ano_mes}.csv"

        rows: list[dict] = []
        anos_passados = (mes.year - START_DATE.year) + (mes.month - START_DATE.month) / 12

        for cliente in clientes_base:
            # Idade actualizada: +1 em cada aniversário (simplificado: +1 por ano completo)
            idade = cliente["idade_jan2022"] + int(anos_passados)
            idade = min(idade, 85)

            # Deriva de localização
            if rng.random() < PROB_MUDANCA_MENSAL:
                novo_distrito = rng.choices(DISTRITOS, weights=_PESOS_MIGRACAO, k=1)[0]
                # Só muda se for diferente (evita "mudança" para o mesmo sítio)
                if novo_distrito != cliente["distrito_atual"]:
                    cliente["distrito_atual"] = novo_distrito

            rows.append({
                "nif":      cliente["nif"],
                "nome":     cliente["nome"],
                "idade":    idade,
                "genero":   cliente["genero"],
                "distrito": cliente["distrito_atual"],
                "ano_mes":  f"{mes.year}-{mes.month:02d}",
            })

        with out_file.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["nif", "nome", "idade", "genero", "distrito", "ano_mes"],
            )
            writer.writeheader()
            writer.writerows(rows)

        total_ficheiros += 1
        print(f"  {out_file.name}  -> {len(rows)} registos")

    total_registos = NUM_CLIENTES * len(meses)
    print(f"\nGerados {total_ficheiros} ficheiros mensais ({total_registos} registos) em {OUTPUT_ROOT}")


if __name__ == "__main__":
    generate_clientes()