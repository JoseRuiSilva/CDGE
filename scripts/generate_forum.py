"""
generate_forum.py
---------------------------------------------------------------
Gerador de dumps TXT sintéticos que simulam o output direto de
BeautifulSoup .get_text() sobre páginas do fórum motorguia.net.

Os modelos referenciados provêm do catálogo central vehicles.py,
garantindo coerência com o inventário e as tendências.

Projeto Auto Escala — CDGE 2025/2026
---------------------------------------------------------------
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterable
import random
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR / "scripts"))

from vehicles import VEHICLES  # noqa: E402

OUT_BASE = BASE_DIR / "data" / "sources" / "forum"

START_DATE = date(2022, 1, 1)
END_DATE   = date(2026, 5, 1)
SEED = 42

MIN_TOPICS_PER_MONTH =  8
MAX_TOPICS_PER_MONTH = 16
MIN_POSTS_PER_TOPIC  =  2
MAX_POSTS_PER_TOPIC  =  5

# ── Catálogo alinhado com vehicles.py ────────────────────────────────────────
MODELOS = [(v.marca, v.modelo, v.tipo, v.combustiveis) for v in VEHICLES]

# ── Utilizadores fictícios do fórum ──────────────────────────────────────────
USERNAMES = [
    "nmyguel", "LinoMarques", "AutoFan92", "MiguelCarros", "PedroGarage",
    "RuiMotor", "BragaDriver", "PortoAuto", "LisboaCar", "DieselPower",
    "EVUser", "MecanicoDoNorte", "CarSpotter", "JotaAuto", "TugaDriver",
    "OpelFan", "ToyotaNation", "KiaNiro_PT", "HyundaiPT", "AudiTuga",
]

# ── Ruído de UI do fórum ─────────────────────────────────────────────────────
FORUM_NOISE_HEADER = [
    "motorguia.net Forum Automovel Portugues Registo Login Pesquisar",
    "Bem-vindo convidado Entrar Registar Topicos Recentes Atividade",
    "Novos Posts Ajuda Calendario Comunidade Forum Regras Utilizadores",
]

FORUM_NOISE_FOOTER = [
    "Topicos Recentes 1 2 3 ... 24 Proxima Pagina Anterior Ir para o topo",
    "Contactos Arquivo Politica de Privacidade Termos de Utilizacao",
    "motorguia.net 2005-2026 Todos os direitos reservados",
]

TOPIC_PREFIXES = [
    "GERAL", "Auto Ajuda", "Compra de Usado", "Manutencao",
    "Eletricos e Hibridos", "Problemas Mecanicos", "Mercado Automovel",
]

TOPIC_TITLES = [
    "{marca} {modelo} vale o preco?",
    "Opinioes sobre {marca} {modelo} usado",
    "Consumo real do {marca} {modelo}",
    "Problemas conhecidos no {marca} {modelo}",
    "Comprar {marca} {modelo} em segunda mao",
    "{modelo} com muitos quilometros ainda compensa?",
    "{marca} {modelo} ou outra alternativa?",
    "Experiencia de utilizador com {marca} {modelo}",
    "Preco dos {marca} {modelo} usados esta a subir?",
    "Vale a pena esperar pelo novo {marca} {modelo}?",
    "{modelo}: qual a motorizacao mais fiavel?",
    "Comparativo {marca} {modelo} vs concorrencia",
]

# ── Templates de comentário por sentimento ───────────────────────────────────
POSITIVE_COMMENTS = [
    "Tenho o {modelo} ha varios meses e ate agora estou satisfeito. Consumos bons e manutencao acessivel.",
    "O {marca} {modelo} parece uma excelente escolha para quem procura fiabilidade e boa relacao preco qualidade.",
    "No meu caso o {modelo} surpreendeu pela positiva. Confortavel, economico e facil de revender.",
    "Tenho visto muita procura por este modelo. No mercado de usados parece estar cada vez mais valorizado.",
    "Para quem quer um carro equilibrado, o {marca} {modelo} continua a ser uma aposta segura.",
    "Ja tenho o segundo {marca} {modelo}. A fiabilidade convenceu-me a repetir a marca.",
    "O {modelo} aguenta bem estrada longa. Fiz Lisboa-Porto sem problemas nenhuns.",
    "Boa relacao preco/desempenho. O {marca} {modelo} surpreende para o valor pedido.",
]

NEGATIVE_COMMENTS = [
    "O {modelo} tem alguns problemas conhecidos e a manutencao pode sair cara se nao houver historico.",
    "Nao comprava esse {marca} sem verificar bem a mecanica. Ha relatos de avarias recorrentes.",
    "O preco pedido pelo {modelo} parece exagerado para o que oferece no mercado atual.",
    "Tenho um conhecido com um {marca} {modelo} e teve varios problemas eletricos.",
    "Acho que ha alternativas melhores. O {modelo} desvaloriza bastante se tiver muitos quilometros.",
    "A assistencia do {marca} em Portugal deixa muito a desejar. Cuidado antes de comprar.",
    "O {modelo} tem um problema conhecido na caixa que o fabricante nunca resolveu a bem.",
    "Custou-me caro. O {marca} {modelo} ficou parado na oficina tres semanas seguidas.",
]

NEUTRAL_COMMENTS = [
    "Depende muito do estado da unidade e do historico de manutencao.",
    "Convem comparar quilometragem, ano, extras e preco antes de decidir.",
    "Esse modelo tem procura, mas e importante ver se o preco esta dentro do mercado.",
    "Para cidade pode fazer sentido, mas para viagens longas depende da motorizacao.",
    "O ideal e fazer test drive e confirmar revisoes antes de comprar.",
    "Ha versoes muito diferentes do {modelo}. Tem que se especificar bem o ano e a motorizacao.",
    "Depende do stand e do historial do carro. Alguns {marca} {modelo} estao otimos.",
    "Nao e mau carro. Mas ha concorrencia forte neste segmento de preco.",
]

GENERIC_FORUM_TEXT = [
    "Ver perfil Responder Citar",
    "Membro desde Mar 2017 892 posts",
    "Senior Member 3401 posts",
    "Utilizador registado desde 2019",
    "Pagina 1 de 2",
    "Pagina 2 de 3",
]

EXTRAS = [
    "Alguem tem experiencia com este motor?",
    "O mercado anda estranho e os precos nao param de mexer.",
    "Vi varios anuncios esta semana e ha muita diferenca entre stands.",
    "Acho importante confirmar garantia e livro de revisoes.",
    "https://www.exemplo-noticias-auto.pt/artigo-mercado-usados",
    "Qual a versao que recomendam — gasolina ou gasoleo?",
    "Alguem sabe quanto custa a revisao dos 60 mil km neste modelo?",
]


# ── Lógica de escolha sazonal ─────────────────────────────────────────────────

def seasonal_model_choice(rng: random.Random, month: int):
    candidatos = list(MODELOS)
    pesos = []
    for marca, modelo, tipo, combustiveis in candidatos:
        peso = 1.0
        if month in {11, 12, 1, 2} and tipo == "SUV":
            peso *= 1.8
        if month in {6, 7, 8} and (
            tipo == "Elétrico" or "100% Elétrico" in combustiveis
        ):
            peso *= 2.0
        if month in {6, 7, 8} and tipo == "Citadino":
            peso *= 1.2
        pesos.append(peso)
    return rng.choices(candidatos, weights=pesos, k=1)[0]


# ── Geração de conteúdo ───────────────────────────────────────────────────────

def gerar_comentario(rng: random.Random, marca: str, modelo: str) -> str:
    sentimento = rng.choices(
        ["positivo", "negativo", "neutro"],
        weights=[0.42, 0.28, 0.30],
        k=1,
    )[0]

    pool = (
        POSITIVE_COMMENTS if sentimento == "positivo"
        else NEGATIVE_COMMENTS if sentimento == "negativo"
        else NEUTRAL_COMMENTS
    )
    comentario = rng.choice(pool).format(marca=marca, modelo=modelo)

    if rng.random() < 0.35:
        comentario += " " + rng.choice(EXTRAS)

    return comentario


def gerar_topico(rng: random.Random, month_start: date) -> str:
    marca, modelo, tipo, combustiveis = seasonal_model_choice(rng, month_start.month)
    prefixo = rng.choice(TOPIC_PREFIXES)
    titulo   = rng.choice(TOPIC_TITLES).format(marca=marca, modelo=modelo)

    partes = [
        f"{prefixo} {titulo}",
        f"Pagina 1 de {rng.randint(1, 4)}",
    ]

    for _ in range(rng.randint(MIN_POSTS_PER_TOPIC, MAX_POSTS_PER_TOPIC)):
        username = rng.choice(USERNAMES)
        posts    = rng.randint(20, 5000)
        mes_reg  = rng.choice(["Jan", "Mar", "Jun", "Set", "Nov"])
        ano_reg  = rng.randint(2012, 2024)

        partes.append(
            f"{username} Membro desde {mes_reg} {ano_reg} {posts} posts"
        )

        if rng.random() < 0.45:
            partes.append(rng.choice(GENERIC_FORUM_TEXT))

        partes.append(gerar_comentario(rng, marca, modelo))

        if rng.random() < 0.65:
            partes.append("Ver perfil Responder Citar")

    return " ".join(partes)


def gerar_dump_mensal(rng: random.Random, month_start: date) -> str:
    partes = list(FORUM_NOISE_HEADER)

    for _ in range(rng.randint(MIN_TOPICS_PER_MONTH, MAX_TOPICS_PER_MONTH)):
        partes.append(gerar_topico(rng, month_start))
        if rng.random() < 0.50:
            partes.append(rng.choice(GENERIC_FORUM_TEXT))

    partes.extend(FORUM_NOISE_FOOTER)
    return " ".join(partes)


# ── Exportação ────────────────────────────────────────────────────────────────

def daterange_months(start: date, end: date) -> Iterable[date]:
    current = date(start.year, start.month, 1)
    while current <= end:
        yield current
        current = (
            date(current.year + 1, 1, 1)
            if current.month == 12
            else date(current.year, current.month + 1, 1)
        )


def exportar_forum() -> dict[str, int]:
    rng = random.Random(SEED)
    OUT_BASE.mkdir(parents=True, exist_ok=True)
    summary: dict[str, int] = {}

    for month_start in daterange_months(START_DATE, END_DATE):
        ano = f"{month_start.year}"
        mes = f"{month_start.month:02d}"

        out_dir = OUT_BASE / ano / mes
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / f"forum_{ano}{mes}.txt"
        dump = gerar_dump_mensal(rng, month_start)

        with out_file.open("w", encoding="utf-8") as f:
            f.write(dump)

        summary[f"{ano}-{mes}"] = len(dump)
        print(f"{out_file}  → {len(dump)} caracteres gerados.")

    return summary


if __name__ == "__main__":
    print("AUTO ESCALA — GERAÇÃO DE DUMPS DO FÓRUM AUTOMÓVEL")
    resumo = exportar_forum()
    print(f"{len(resumo)} ficheiros mensais gerados com sucesso.")