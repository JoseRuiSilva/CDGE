import pandas as pd
import json
import calendar
import xml.etree.ElementTree as ET
from deltalake import DeltaTable, write_deltalake
from datetime import datetime, timezone
from pathlib import Path


# ─── CONFIGURAÇÃO ────────────────────────────────────────────────────────────

STANDS_DIR    = Path("data/sources/stands")
TRENDS_DIR    = Path("data/sources/trends")        # YYYY/MM/trends_YYYYMM.json
FORUM_DIR     = Path("data/sources/forum")         # YYYY/MM/forum_YYYYMM.txt
HASHTAGS_DIR  = Path("data/sources/hashtags")      # YYYY/WNN/hashtags_YYYYWNN.xml

BRONZE_INVENTARIO = "data_lake/bronze/inventario_delta"
BRONZE_TRENDS     = "data_lake/bronze/trends_delta"
BRONZE_FORUM      = "data_lake/bronze/forum_delta"
BRONZE_HASHTAGS   = "data_lake/bronze/hashtags_delta"

# Namespace do XML Atom Feed de hashtags (Talkwalker/Mention)
NS_SL = {"sl": "http://www.talkwalker.com/sl"}


# ─── UTILITÁRIOS ─────────────────────────────────────────────────────────────

def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(r"[^\w]", "_", regex=True)
    )
    return df


def converter_para_string(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str)
    return df


def timestamp_simulado(ano: int, mes: int) -> str:
    """Último dia do mês como timestamp UTC — mantém rastreabilidade histórica."""
    ultimo_dia = calendar.monthrange(ano, mes)[1]
    return datetime(ano, mes, ultimo_dia, tzinfo=timezone.utc).isoformat()


def timestamp_semana_simulado(ano: int, semana: int) -> str:
    """Último dia da semana ISO como timestamp UTC — para hashtags semanais."""
    # Domingo da semana ISO (dia 7)
    ultimo_dia = datetime.fromisocalendar(ano, semana, 7)
    return ultimo_dia.replace(tzinfo=timezone.utc).isoformat()


def timestamp_agora() -> str:
    return datetime.now(timezone.utc).isoformat()


def escrever_bronze(df: pd.DataFrame, delta_path: str):
    """Escreve no Bronze — cria tabela se não existir, senão faz append."""
    try:
        DeltaTable(delta_path)
        write_deltalake(delta_path, df, mode="append")
        print(f"    APPEND → {len(df)} registos  [{delta_path}]")
    except Exception:
        Path(delta_path).mkdir(parents=True, exist_ok=True)
        write_deltalake(delta_path, df, mode="overwrite")
        print(f"    CRIADA → {len(df)} registos  [{delta_path}]")


def ficheiros_ja_ingeridos(delta_path: str) -> set:
    """
    Lê a tabela Delta e devolve o conjunto de source_files já ingeridos.
    Se a tabela não existir ainda, devolve conjunto vazio.
    """
    try:
        dt = DeltaTable(delta_path)
        df = dt.to_pandas(columns=["source_file"])
        return set(df["source_file"].unique())
    except Exception:
        return set()


# ─── INGESTÃO ────────────────────────────────────────────────────────────────

def ingerir_inventario(ficheiros: list):
    """Lê os CSVs de inventário fornecidos e escreve no Bronze."""
    print("\n[Bronze] Inventário")
    dataframes = []

    ja_ingeridos = ficheiros_ja_ingeridos(BRONZE_INVENTARIO)

    for filepath in ficheiros:
        if filepath.name in ja_ingeridos:
            print(f"  SKIP  {filepath.name} — já ingerido.")
            continue
        filepath = Path(filepath)
        try:
            partes = filepath.stem.split("_")   # 2022_01_lisboa
            ano, mes = int(partes[0]), int(partes[1])
        except (IndexError, ValueError):
            print(f"  AVISO: nome inesperado '{filepath.name}' — ignorado.")
            continue

        df = pd.read_csv(filepath)
        df = normalizar_colunas(df)
        df["ingestion_timestamp"] = timestamp_simulado(ano, mes)
        df["source_file"]         = filepath.name
        df["source_stand"]        = filepath.parent.name.capitalize()
        df = converter_para_string(df)

        print(f"  {filepath.name} → {len(df)} registos")
        dataframes.append(df)

    if dataframes:
        escrever_bronze(pd.concat(dataframes, ignore_index=True), BRONZE_INVENTARIO)


def ingerir_trends(ficheiros: list = None):
    """
    Lê os JSONs mensais de Google Trends e escreve no Bronze.
    Estrutura esperada: data/sources/trends/YYYY/MM/trends_YYYYMM.json
    Se ficheiros for None, descobre todos os disponíveis em TRENDS_DIR.
    """
    print("\n[Bronze] Google Trends")

    if ficheiros is None:
        ficheiros = sorted(TRENDS_DIR.rglob("trends_*.json"))

    if not ficheiros:
        print(f"  Nenhum ficheiro encontrado em {TRENDS_DIR} — a saltar.")
        return

    ja_ingeridos = ficheiros_ja_ingeridos(BRONZE_TRENDS)
    dataframes = []

    for filepath in ficheiros:
        filepath = Path(filepath)

        if filepath.name in ja_ingeridos:
            print(f"  SKIP  {filepath.name} — já ingerido.")
            continue

        if not filepath.exists():
            print(f"  AVISO: {filepath} não encontrado — ignorado.")
            continue

        if filepath.stat().st_size == 0:
            print(f"  AVISO: {filepath.name} vazio — ignorado.")
            continue

        # Extrair ano e mês do nome: trends_YYYYMM.json
        try:
            stem = filepath.stem  # "trends_202201"
            ano  = int(stem[-6:-2])
            mes  = int(stem[-2:])
        except (ValueError, IndexError):
            print(f"  AVISO: nome inesperado '{filepath.name}' — ignorado.")
            continue

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                dados = json.load(f)
        except json.JSONDecodeError as e:
            print(f"  AVISO: JSON inválido '{filepath.name}' ({e}) — ignorado.")
            continue

        # Estrutura esperada: {"extraction_metadata": {...}, "data": [...]}
        registos = dados.get("data", dados) if isinstance(dados, dict) else dados
        if not registos:
            print(f"  AVISO: '{filepath.name}' sem dados — ignorado.")
            continue

        df = pd.DataFrame(registos)
        df = normalizar_colunas(df)
        df["ingestion_timestamp"] = timestamp_simulado(ano, mes)
        df["source_file"]         = filepath.name
        df = converter_para_string(df)

        print(f"  {filepath.name} → {len(df)} registos")
        dataframes.append(df)

    if dataframes:
        escrever_bronze(pd.concat(dataframes, ignore_index=True), BRONZE_TRENDS)


def ingerir_forum(ficheiros: list = None):
    """
    Lê os TXTs mensais do fórum motorguia.net e escreve no Bronze.
    Estrutura esperada: data/sources/forum/YYYY/MM/forum_YYYYMM.txt
    Bronze: 1 linha por ficheiro — texto bruto preservado como string.
    Se ficheiros for None, descobre todos os disponíveis em FORUM_DIR.
    """
    print("\n[Bronze] Fórum motorguia.net")

    if ficheiros is None:
        ficheiros = sorted(FORUM_DIR.rglob("forum_*.txt"))

    if not ficheiros:
        print(f"  Nenhum ficheiro encontrado em {FORUM_DIR} — a saltar.")
        return

    ja_ingeridos = ficheiros_ja_ingeridos(BRONZE_FORUM)
    registos = []

    for filepath in ficheiros:
        filepath = Path(filepath)

        if filepath.name in ja_ingeridos:
            print(f"  SKIP  {filepath.name} — já ingerido.")
            continue

        if not filepath.exists():
            print(f"  AVISO: {filepath} não encontrado — ignorado.")
            continue

        if filepath.stat().st_size == 0:
            print(f"  AVISO: {filepath.name} vazio — ignorado.")
            continue

        # Extrair ano e mês do nome: forum_YYYYMM.txt
        try:
            stem = filepath.stem  # "forum_202201"
            ano  = int(stem[-6:-2])
            mes  = int(stem[-2:])
        except (ValueError, IndexError):
            print(f"  AVISO: nome inesperado '{filepath.name}' — ignorado.")
            continue

        try:
            texto_bruto = filepath.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"  AVISO: erro ao ler '{filepath.name}' ({e}) — ignorado.")
            continue

        # 1 registo por ficheiro — texto inteiro como string (sem schema)
        registos.append({
            "source_file":         filepath.name,
            "data_extracao":       f"{ano}-{mes:02d}-01",   # início do mês de scraping
            "ingestion_timestamp": timestamp_simulado(ano, mes),
            "texto_bruto":         texto_bruto,
        })
        print(f"  {filepath.name} → 1 registo  ({len(texto_bruto)} chars)")

    if registos:
        df = pd.DataFrame(registos)
        df = converter_para_string(df)
        escrever_bronze(df, BRONZE_FORUM)


def ingerir_hashtags(ficheiros: list = None):
    """
    Lê os XMLs Atom Feed semanais de hashtags e escreve no Bronze.
    Estrutura esperada: data/sources/hashtags/YYYY/WNN/hashtags_YYYYWNN.xml
    Cada <entry> é convertido para um registo flat antes do append.
    Se ficheiros for None, descobre todos os disponíveis em HASHTAGS_DIR.
    """
    print("\n[Bronze] Hashtags Social Listening")

    if ficheiros is None:
        ficheiros = sorted(HASHTAGS_DIR.rglob("hashtags_*.xml"))

    if not ficheiros:
        print(f"  Nenhum ficheiro encontrado em {HASHTAGS_DIR} — a saltar.")
        return

    ja_ingeridos = ficheiros_ja_ingeridos(BRONZE_HASHTAGS)
    dataframes = []

    for filepath in ficheiros:
        filepath = Path(filepath)

        if filepath.name in ja_ingeridos:
            print(f"  SKIP  {filepath.name} — já ingerido.")
            continue

        if not filepath.exists():
            print(f"  AVISO: {filepath} não encontrado — ignorado.")
            continue

        if filepath.stat().st_size == 0:
            print(f"  AVISO: {filepath.name} vazio — ignorado.")
            continue

        # Extrair ano e semana do nome: hashtags_YYYYWNN.xml  (ex: hashtags_2022W03.xml)
        try:
            stem   = filepath.stem            # "hashtags_2022W03"
            partes = stem.split("_")[1]       # "2022W03"
            ano    = int(partes[:4])
            semana = int(partes[5:])          # após o "W"
        except (ValueError, IndexError):
            print(f"  AVISO: nome inesperado '{filepath.name}' — ignorado.")
            continue

        try:
            tree = ET.parse(filepath)
            root = tree.getroot()
        except ET.ParseError as e:
            print(f"  AVISO: XML inválido '{filepath.name}' ({e}) — ignorado.")
            continue

        # Namespace Atom padrão + namespace sl (social listening)
        ns_atom = {"atom": "http://www.w3.org/2005/Atom"}

        registos = []
        for entry in root.findall("atom:entry", ns_atom):
            # Campos principais
            hashtag     = entry.findtext("sl:hashtag",     namespaces=NS_SL, default="")
            data        = entry.findtext("sl:date",         namespaces=NS_SL, default="")
            total_posts = entry.findtext("sl:total_posts",  namespaces=NS_SL, default="0")

            # Breakdown por plataforma: <sl:platform name="instagram">134</sl:platform>
            breakdown_elem = entry.find("sl:breakdown", NS_SL)
            plataformas = {}
            if breakdown_elem is not None:
                for plat in breakdown_elem.findall("sl:platform", NS_SL):
                    nome = plat.get("name", "desconhecido")
                    plataformas[f"posts_{nome}"] = plat.text or "0"

            registo = {
                "hashtag":             hashtag,
                "data":                data,
                "total_posts":         total_posts,
                "source_file":         filepath.name,
                "ingestion_timestamp": timestamp_semana_simulado(ano, semana),
            }
            registo.update(plataformas)
            registos.append(registo)

        if not registos:
            print(f"  AVISO: '{filepath.name}' sem entries — ignorado.")
            continue

        df = pd.DataFrame(registos)
        df = normalizar_colunas(df)
        df = converter_para_string(df)

        print(f"  {filepath.name} → {len(df)} registos")
        dataframes.append(df)

    if dataframes:
        escrever_bronze(pd.concat(dataframes, ignore_index=True), BRONZE_HASHTAGS)


# ─── PONTO DE ENTRADA ─────────────────────────────────────────────────────────

def run_bronze(
    ficheiros_inventario=None,
    ficheiros_trends=None,
    ficheiros_forum=None,
    ficheiros_hashtags=None,
):
    """
    Corre a pipeline Bronze para as 4 fontes.
    Se um argumento for None, descobre automaticamente todos os ficheiros disponíveis.
    Se for uma lista, processa apenas esses (modo incremental via main.py).
    """
    print("\n" + "=" * 60)
    print("  BRONZE PIPELINE")
    print("=" * 60)

    if ficheiros_inventario is None:
        ficheiros_inventario = sorted(STANDS_DIR.rglob("*.csv"))

    ingerir_inventario(ficheiros_inventario)
    ingerir_trends(ficheiros_trends)
    ingerir_forum(ficheiros_forum)
    ingerir_hashtags(ficheiros_hashtags)

    print("\n  Bronze concluído.")
    print("=" * 60)


if __name__ == "__main__":
    run_bronze()