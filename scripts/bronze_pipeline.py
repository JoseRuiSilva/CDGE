import pandas as pd
import json
import calendar
from deltalake import DeltaTable, write_deltalake
from datetime import datetime, timezone
from pathlib import Path


# ─── CONFIGURAÇÃO ────────────────────────────────────────────────────────────

STANDS_DIR    = Path("data/sources/stands")
TRENDS_FILE   = Path("data/sources/trends/google_trends.json")
REVIEWS_FILE  = Path("data/sources/reviews/reviews.csv")

BRONZE_INVENTARIO = "data_lake/bronze/inventario_delta"
BRONZE_TRENDS     = "data_lake/bronze/trends_delta"
BRONZE_REVIEWS    = "data_lake/bronze/reviews_delta"


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


def ingerir_trends():
    """Lê o JSON do Google Trends e escreve no Bronze."""
    print("\n[Bronze] Google Trends")

    if not TRENDS_FILE.exists():
        print(f"  Ficheiro não encontrado: {TRENDS_FILE} — a saltar.")
        return

    if TRENDS_FILE.stat().st_size == 0:
        print(f"  Ficheiro vazio: {TRENDS_FILE} — a saltar.")
        return

    try:
        with open(TRENDS_FILE, "r", encoding="utf-8") as f:
            dados = json.load(f)
    except json.JSONDecodeError as e:
        print(f"  JSON inválido: {TRENDS_FILE} ({e}) — a saltar.")
        return

    if not dados:
        print(f"  JSON sem dados: {TRENDS_FILE} — a saltar.")
        return

    df = pd.DataFrame(dados) if isinstance(dados, list) else pd.DataFrame.from_dict(dados)
    df = normalizar_colunas(df)
    df["ingestion_timestamp"] = timestamp_agora()
    df["source_file"]         = TRENDS_FILE.name
    df = converter_para_string(df)

    print(f"  {TRENDS_FILE.name} → {len(df)} registos")
    escrever_bronze(df, BRONZE_TRENDS)


def ingerir_reviews():
    """Lê o CSV de reviews e escreve no Bronze."""
    print("\n[Bronze] Reviews")

    if not REVIEWS_FILE.exists():
        print(f"  Ficheiro não encontrado: {REVIEWS_FILE} — a saltar.")
        return

    if REVIEWS_FILE.stat().st_size == 0:
        print(f"  Ficheiro vazio: {REVIEWS_FILE} — a saltar.")
        return

    df = pd.read_csv(REVIEWS_FILE)
    df = normalizar_colunas(df)
    df["ingestion_timestamp"] = timestamp_agora()
    df["source_file"]         = REVIEWS_FILE.name
    df = converter_para_string(df)

    print(f"  {REVIEWS_FILE.name} → {len(df)} registos")
    escrever_bronze(df, BRONZE_REVIEWS)


# ─── PONTO DE ENTRADA ─────────────────────────────────────────────────────────

def run_bronze(ficheiros_inventario=None):
    """
    Corre a pipeline Bronze.
    Se ficheiros_inventario for None, lê todos os CSVs disponíveis em STANDS_DIR.
    Se for uma lista de ficheiros, processa apenas esses (modo incremental via main.py).
    """
    print("\n" + "=" * 60)
    print("  BRONZE PIPELINE")
    print("=" * 60)

    if ficheiros_inventario is None:
        ficheiros_inventario = sorted(STANDS_DIR.rglob("*.csv"))

    ingerir_inventario(ficheiros_inventario)
    ingerir_trends()
    ingerir_reviews()

    print("\n  Bronze concluído.")
    print("=" * 60)


if __name__ == "__main__":
    run_bronze()