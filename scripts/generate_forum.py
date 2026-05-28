from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable


# ============================================================
# Configuração geral do gerador
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_BASE = BASE_DIR / "data" / "sources" / "forum"

START_DATE = date(2022, 1, 1)
END_DATE = date(2026, 5, 1)

SEED = 42

MIN_TOPICS_PER_MONTH = 8
MAX_TOPICS_PER_MONTH = 16
MIN_POSTS_PER_TOPIC = 2
MAX_POSTS_PER_TOPIC = 5

# De quantos em quantos tópicos se força a escolha de um modelo
# menos representado, para aumentar a diversidade no texto gerado.
DIVERSITY_INTERVAL = 4


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
# Utilizadores fictícios do fórum
# ============================================================

USERNAMES = [
    "nmyguel",
    "LinoMarques",
    "AutoFan92",
    "MiguelCarros",
    "PedroGarage",
    "RuiMotor",
    "BragaDriver",
    "PortoAuto",
    "LisboaCar",
    "DieselPower",
    "EVUser",
    "MecanicoDoNorte",
    "CarSpotter",
    "JotaAuto",
    "TugaDriver",
    "OpelFan",
    "ToyotaNation",
    "KiaNiro_PT",
    "HyundaiPT",
    "AudiTuga",
    "StandWatcher",
    "UsadosPT",
    "MotoristaNorte",
    "AutoCurioso",
]


# ============================================================
# Ruído típico de fórum
# ============================================================
# Este ruído simula o resultado de extrair texto bruto de uma página
# HTML com BeautifulSoup .get_text().

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

GENERIC_FORUM_TEXT = [
    "Ver perfil Responder Citar",
    "Membro desde Mar 2017 892 posts",
    "Senior Member 3401 posts",
    "Utilizador registado desde 2019",
    "Pagina 1 de 2",
    "Pagina 2 de 3",
    "Mensagem editada pelo utilizador",
    "Assinatura: carros usados e manutencao preventiva",
]


# ============================================================
# Títulos e secções do fórum
# ============================================================

TOPIC_PREFIXES = [
    "GERAL",
    "Auto Ajuda",
    "Compra de Usado",
    "Manutencao",
    "Eletricos e Hibridos",
    "Problemas Mecanicos",
    "Mercado Automovel",
    "Opinioes de Donos",
    "Precos de Usados",
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
    "Vale a pena esperar por melhor preco no {marca} {modelo}?",
    "{modelo}: qual a motorizacao mais fiavel?",
    "Comparativo {marca} {modelo} vs concorrencia",
    "O {marca} {modelo} tem boa procura no mercado de usados?",
    "Manutencao do {marca} {modelo}: custos reais",
    "{marca} {modelo} para uso diario, recomendam?",
]


# ============================================================
# Templates de comentário por sentimento
# ============================================================

POSITIVE_COMMENTS = [
    "Tenho o {modelo} ha varios meses e ate agora estou satisfeito. Consumos bons e manutencao acessivel.",
    "O {marca} {modelo} parece uma excelente escolha para quem procura fiabilidade e boa relacao preco qualidade.",
    "No meu caso o {modelo} surpreendeu pela positiva. Confortavel, economico e facil de revender.",
    "Tenho visto muita procura por este modelo. No mercado de usados parece estar cada vez mais valorizado.",
    "Para quem quer um carro equilibrado, o {marca} {modelo} continua a ser uma aposta segura.",
    "Ja tenho o segundo {marca} {modelo}. A fiabilidade convenceu-me a repetir a marca.",
    "O {modelo} aguenta bem estrada longa. Fiz Lisboa-Porto sem problemas nenhuns.",
    "Boa relacao preco desempenho. O {marca} {modelo} surpreende para o valor pedido.",
    "Acho que o {modelo} tem boa liquidez no mercado. Se o preco estiver certo vende-se bem.",
    "Em segunda mao, o {marca} {modelo} parece uma escolha racional e com procura estavel.",
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
    "Antes de comprar esse {modelo}, confirmava muito bem revisoes, garantia e historico.",
    "No mercado atual acho que o {marca} {modelo} esta inflacionado face ao que entrega.",
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
    "Se for para comprar usado, eu dava prioridade a garantia e historico completo.",
    "A decisao depende do preco, quilometros, estado geral e tipo de utilizacao.",
]


EXTRAS = [
    "Alguem tem experiencia com este motor?",
    "O mercado anda estranho e os precos nao param de mexer.",
    "Vi varios anuncios esta semana e ha muita diferenca entre stands.",
    "Acho importante confirmar garantia e livro de revisoes.",
    "https://www.exemplo-noticias-auto.pt/artigo-mercado-usados",
    "Qual a versao que recomendam — gasolina ou gasoleo?",
    "Alguem sabe quanto custa a revisao dos 60 mil km neste modelo?",
    "Tenho visto anuncios com valores muito diferentes para unidades parecidas.",
    "Para quem faz poucos quilometros por ano talvez compense outra motorizacao.",
    "A retoma tambem depende muito da procura pelo modelo naquele momento.",
]


# ============================================================
# Funções auxiliares de datas
# ============================================================

def daterange_months(start: date, end: date) -> Iterable[date]:
    current = date(start.year, start.month, 1)

    while current <= end:
        yield current

        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)


# ============================================================
# Lógica de diversidade e sazonalidade
# ============================================================

def vehicle_key(vehicle: VehicleModel) -> tuple[str, str]:
    return vehicle.marca, vehicle.modelo


def seasonal_weight(month: int, vehicle: VehicleModel) -> float:
    """
    Aplica sazonalidade de forma simples.

    - No inverno aparecem mais SUV.
    - No verão aparecem mais elétricos.
    - Em setembro/outubro os citadinos ganham algum peso.
    """
    weight = 1.0

    if month in {11, 12, 1, 2}:
        if vehicle.tipo == "SUV":
            weight *= 1.8
        if vehicle.tipo == "Elétrico":
            weight *= 0.75

    elif month in {6, 7, 8}:
        if vehicle.tipo == "Elétrico":
            weight *= 2.0
        if vehicle.tipo == "Citadino":
            weight *= 1.15

    elif month in {9, 10}:
        if vehicle.tipo == "Citadino":
            weight *= 1.25

    return weight


def diversity_factor(
    vehicle: VehicleModel,
    model_counts: dict[tuple[str, str], int],
    brand_counts: dict[str, int],
) -> float:
    """
    Penaliza suavemente modelos e marcas que já apareceram muitas vezes.

    Isto garante que o texto gerado menciona uma maior variedade de
    modelos, sem eliminar totalmente a sazonalidade.
    """
    key = vehicle_key(vehicle)

    model_penalty = 1 / (1 + model_counts[key] * 0.40)
    brand_penalty = 1 / (1 + brand_counts[vehicle.marca] * 0.08)

    return model_penalty * brand_penalty


def least_represented_vehicles(
    model_counts: dict[tuple[str, str], int],
) -> list[VehicleModel]:
    """
    Devolve os modelos menos mencionados até ao momento.
    """
    min_count = min(model_counts[vehicle_key(vehicle)] for vehicle in VEHICLES)

    return [
        vehicle
        for vehicle in VEHICLES
        if model_counts[vehicle_key(vehicle)] == min_count
    ]


def seasonal_model_choice(
    rng: random.Random,
    month: int,
    model_counts: dict[tuple[str, str], int],
    brand_counts: dict[str, int],
    force_diversity: bool = False,
) -> VehicleModel:
    """
    Escolhe um modelo para um tópico do fórum.

    A escolha tem em conta:
      - sazonalidade;
      - diversidade de modelos;
      - diversidade de marcas;
      - reforço periódico dos modelos menos mencionados.
    """
    if force_diversity:
        candidates = least_represented_vehicles(model_counts)
    else:
        candidates = list(VEHICLES)

    weights = []

    for vehicle in candidates:
        weight = seasonal_weight(month, vehicle)
        weight *= diversity_factor(vehicle, model_counts, brand_counts)

        weights.append(max(weight, 0.05))

    return rng.choices(candidates, weights=weights, k=1)[0]


# ============================================================
# Geração de conteúdo
# ============================================================

def escolher_sentimento(rng: random.Random, vehicle: VehicleModel) -> str:
    """
    Escolhe o sentimento do comentário.

    A distribuição é ligeiramente ajustada por tipo:
      - elétricos geram mais discussão polarizada;
      - modelos generalistas têm mais comentários neutros/positivos.
    """
    if vehicle.tipo == "Elétrico":
        return rng.choices(
            ["positivo", "negativo", "neutro"],
            weights=[0.44, 0.32, 0.24],
            k=1,
        )[0]

    if vehicle.tipo == "SUV":
        return rng.choices(
            ["positivo", "negativo", "neutro"],
            weights=[0.45, 0.27, 0.28],
            k=1,
        )[0]

    if vehicle.tipo == "Sedan":
        return rng.choices(
            ["positivo", "negativo", "neutro"],
            weights=[0.36, 0.30, 0.34],
            k=1,
        )[0]

    return rng.choices(
        ["positivo", "negativo", "neutro"],
        weights=[0.42, 0.28, 0.30],
        k=1,
    )[0]


def gerar_comentario(
    rng: random.Random,
    vehicle: VehicleModel,
) -> str:
    """
    Gera um comentário textual sobre o modelo escolhido.
    """
    sentimento = escolher_sentimento(rng, vehicle)

    if sentimento == "positivo":
        pool = POSITIVE_COMMENTS
    elif sentimento == "negativo":
        pool = NEGATIVE_COMMENTS
    else:
        pool = NEUTRAL_COMMENTS

    comentario = rng.choice(pool).format(
        marca=vehicle.marca,
        modelo=vehicle.modelo,
    )

    # Algumas mensagens recebem uma frase extra, para tornar o texto
    # menos repetitivo e mais parecido com um fórum real.
    if rng.random() < 0.38:
        comentario += " " + rng.choice(EXTRAS)

    return comentario


def gerar_topico(
    rng: random.Random,
    month_start: date,
    model_counts: dict[tuple[str, str], int],
    brand_counts: dict[str, int],
    topic_counter: int,
) -> str:
    """
    Gera um tópico completo do fórum.

    Cada tópico tem:
      - título;
      - ruído de página;
      - vários posts;
      - comentários sobre o mesmo modelo.
    """
    force_diversity = topic_counter % DIVERSITY_INTERVAL == 0

    vehicle = seasonal_model_choice(
        rng=rng,
        month=month_start.month,
        model_counts=model_counts,
        brand_counts=brand_counts,
        force_diversity=force_diversity,
    )

    model_counts[vehicle_key(vehicle)] += 1
    brand_counts[vehicle.marca] += 1

    prefixo = rng.choice(TOPIC_PREFIXES)
    titulo = rng.choice(TOPIC_TITLES).format(
        marca=vehicle.marca,
        modelo=vehicle.modelo,
    )

    partes = [
        f"{prefixo} {titulo}",
        f"Pagina 1 de {rng.randint(1, 4)}",
        f"Marca {vehicle.marca} Modelo {vehicle.modelo} Segmento {vehicle.tipo}",
    ]

    for _ in range(rng.randint(MIN_POSTS_PER_TOPIC, MAX_POSTS_PER_TOPIC)):
        username = rng.choice(USERNAMES)
        posts = rng.randint(20, 5000)
        mes_reg = rng.choice(["Jan", "Mar", "Jun", "Set", "Nov"])
        ano_reg = rng.randint(2012, 2024)

        partes.append(
            f"{username} Membro desde {mes_reg} {ano_reg} {posts} posts"
        )

        if rng.random() < 0.45:
            partes.append(rng.choice(GENERIC_FORUM_TEXT))

        partes.append(gerar_comentario(rng, vehicle))

        if rng.random() < 0.65:
            partes.append("Ver perfil Responder Citar")

    return " ".join(partes)


def gerar_dump_mensal(
    rng: random.Random,
    month_start: date,
    model_counts: dict[tuple[str, str], int],
    brand_counts: dict[str, int],
    global_topic_counter: int,
) -> tuple[str, int]:
    """
    Gera o dump TXT mensal.

    O resultado simula texto bruto de uma página de fórum, incluindo
    cabeçalho, tópicos, comentários e rodapé.
    """
    partes = list(FORUM_NOISE_HEADER)

    num_topics = rng.randint(MIN_TOPICS_PER_MONTH, MAX_TOPICS_PER_MONTH)

    for i in range(num_topics):
        topic_counter = global_topic_counter + i

        partes.append(
            gerar_topico(
                rng=rng,
                month_start=month_start,
                model_counts=model_counts,
                brand_counts=brand_counts,
                topic_counter=topic_counter,
            )
        )

        if rng.random() < 0.50:
            partes.append(rng.choice(GENERIC_FORUM_TEXT))

    partes.extend(FORUM_NOISE_FOOTER)

    return " ".join(partes), num_topics


# ============================================================
# Exportação
# ============================================================

def exportar_forum() -> dict[str, int]:
    """
    Exporta os dumps TXT mensais.

    Estrutura gerada:
        data/sources/forum/2022/01/forum_202201.txt
        data/sources/forum/2022/02/forum_202202.txt
        ...
    """
    rng = random.Random(SEED)

    OUT_BASE.mkdir(parents=True, exist_ok=True)

    summary: dict[str, int] = {}

    model_counts: dict[tuple[str, str], int] = defaultdict(int)
    brand_counts: dict[str, int] = defaultdict(int)

    global_topic_counter = 1

    for month_start in daterange_months(START_DATE, END_DATE):
        ano = f"{month_start.year}"
        mes = f"{month_start.month:02d}"

        out_dir = OUT_BASE / ano / mes
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / f"forum_{ano}{mes}.txt"

        dump, num_topics = gerar_dump_mensal(
            rng=rng,
            month_start=month_start,
            model_counts=model_counts,
            brand_counts=brand_counts,
            global_topic_counter=global_topic_counter,
        )

        global_topic_counter += num_topics

        with out_file.open("w", encoding="utf-8") as f:
            f.write(dump)

        summary[f"{ano}-{mes}"] = len(dump)

        print(f"{out_file}  -> {len(dump)} caracteres gerados.")

    return summary


# ============================================================
# Execução direta
# ============================================================

if __name__ == "__main__":
    print("AUTO ESCALA — GERAÇÃO DE DUMPS DO FÓRUM AUTOMÓVEL")

    resumo = exportar_forum()

    print(f"{len(resumo)} ficheiros mensais gerados com sucesso.")