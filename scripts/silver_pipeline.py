"""
silver_pipeline.py — Camada Silver | Projeto Auto Escala
=========================================================
Lê dados brutos do Bronze (Delta Lake), aplica limpeza, tipagem,
normalização via dicionário PostgreSQL e NLP de sentimento (fórum).
Registos inválidos -> Delta de quarentena por fonte.
Métricas de qualidade -> tabela data_quality_log no PostgreSQL.
Dados limpos -> Silver (Delta Lake, MERGE/UPSERT por Business Key).
=========================================================
"""

import os
import re
import socket
import time
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", message=".*SettingWithCopyWarning.*")

# Fix para conflito de OpenMP (pysentimiento crash)
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from sqlalchemy import create_engine, text

# ─── LOGGING ─────────────────────────────────────────────────────────────────

def _log(msg: str, nivel: str = "INFO") -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"  [{ts}] [{nivel}] {msg}")


# ─── NLP (BATCH PROCESSING) ──────────────────────────────────────────────────
_analisador_nlp = None
NLP_DISPONIVEL  = False
_nlp_ja_tentado = False

def _carregar_nlp() -> None:
    global _analisador_nlp, NLP_DISPONIVEL, _nlp_ja_tentado
    if _nlp_ja_tentado: return
    _nlp_ja_tentado = True
    _log("NLP: a inicializar pysentimiento (lazy load)...")
    try:
        from pysentimiento import create_analyzer
        _analisador_nlp = create_analyzer(task="sentiment", lang="pt")
        NLP_DISPONIVEL  = True
        _log("NLP: pysentimiento carregado.")
    except Exception as e:
        _log(f"AVISO NLP: pysentimiento indisponível ({e}). Score = 0.5.", "WARN")

def _analisar_sentimento_batch(textos: list[str], habilitado: bool = True) -> list[float]:
    """Processa NLP em batch para acelerar massivamente a inferência."""
    if not habilitado or not textos: return [0.5] * len(textos)
    _carregar_nlp()
    if not NLP_DISPONIVEL: return [0.5] * len(textos)
    try:
        # Pysentimiento lida bem com listas
        textos_trunc = [t[:500] for t in textos]
        resultados = _analisador_nlp.predict(textos_trunc)
        mapa = {"POS": 1.0, "NEU": 0.5, "NEG": 0.0}
        
        # Se for só um texto, pysentimiento retorna um objeto em vez de lista
        if not isinstance(resultados, list):
            resultados = [resultados]
            
        return [r.probas["POS"] - r.probas["NEG"] for r in resultados]
    except Exception as e:
        _log(f"Erro no NLP em batch: {e}", "WARN")
        return [0.0] * len(textos)


# ─── CONFIGURAÇÃO ─────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent

# Deltas
BRONZE_DIR = BASE_DIR / "data_lake/bronze"
SILVER_DIR = BASE_DIR / "data_lake/silver"
QUARENTENA_DIR = BASE_DIR / "data_lake/quarantine"

BRONZE_INVENTARIO = str(BRONZE_DIR / "inventario_delta")
BRONZE_TRENDS     = str(BRONZE_DIR / "trends_delta")
BRONZE_FORUM      = str(BRONZE_DIR / "forum_delta")
BRONZE_HASHTAGS   = str(BRONZE_DIR / "hashtags_delta")
BRONZE_CLIENTES   = str(BRONZE_DIR / "clientes_delta")
BRONZE_DEMOGRAFIA = str(BRONZE_DIR / "demografia_delta")

SILVER_INVENTARIO = str(SILVER_DIR / "inventario_delta")
SILVER_TRENDS     = str(SILVER_DIR / "trends_delta")
SILVER_FORUM      = str(SILVER_DIR / "forum_delta")
SILVER_HASHTAGS   = str(SILVER_DIR / "hashtags_delta")
SILVER_CLIENTES   = str(SILVER_DIR / "clientes_delta")
SILVER_DEMOGRAFIA = str(SILVER_DIR / "demografia_delta")

QUARENTENA_INVENTARIO = str(QUARENTENA_DIR / "inventario_delta")
QUARENTENA_TRENDS     = str(QUARENTENA_DIR / "trends_delta")
QUARENTENA_FORUM      = str(QUARENTENA_DIR / "forum_delta")
QUARENTENA_HASHTAGS   = str(QUARENTENA_DIR / "hashtags_delta")
QUARENTENA_CLIENTES   = str(QUARENTENA_DIR / "clientes_delta")
QUARENTENA_DEMOGRAFIA = str(QUARENTENA_DIR / "demografia_delta")

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
DW_URL   = f"postgresql+psycopg2://ae_user:ae_pass_2026@{_PG_HOST}:{_PG_PORT}/auto_escala"
DW_SCHEMA = "auto_escala_dw"

_NULOS_TEXTUAIS = {"", "nan", "none", "null", "n/a", "na", "nd", "s/d", "-"}
_MAPEAMENTO_PLURAL = {"marcas": "marca", "modelos": "modelo", "combustiveis": "combustivel", "tipos": "tipo"}
_CAMPO_PARA_PLURAL = {
    "marca": "marcas",
    "modelo": "modelos",
    "combustivel": "combustiveis",
    "tipo_automovel": "tipos",
}


# ─── UTILITÁRIOS: NORMALIZAÇÃO & DICIONÁRIO OTIMIZADO ────────────────────────

def _normalizar_para_lookup(valor: str) -> str:
    if pd.isna(valor) or not isinstance(valor, str): return ""
    texto = valor.lower().strip()
    texto = re.sub(r"\s+", " ", texto)
    texto = re.sub(r"[,;()'\"\\]", "", texto)
    return texto

def _normalizar_nulos(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].apply(lambda v: pd.NA if (isinstance(v, str) and v.strip().lower() in _NULOS_TEXTUAIS) else v)
    return df

def _cast_seguro(serie: pd.Series, tipo: str) -> pd.Series:
    try:
        if tipo == "int": return pd.to_numeric(serie, errors="coerce").astype("Int64")
        if tipo == "float": return pd.to_numeric(serie, errors="coerce")
        if tipo == "datetime": return pd.to_datetime(serie, errors="coerce", utc=True)
    except: pass
    return serie

def _carregar_dicionario_regex(engine) -> dict:
    """
    Carrega o dicionário e PRÉ-COMPILA as expressões regulares.
    Acelera o processamento de texto em > 10x.
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(f"SELECT campo, valor_original, valor_normalizado FROM {DW_SCHEMA}.dim_dicionario_veiculo WHERE ativo = TRUE", conn)
    except Exception as e:
        _log(f"Erro ao ler dicionário: {e}", "WARN")
        return {}

    mapa_regex = {}
    df["valor_original_norm"] = df["valor_original"].apply(_normalizar_para_lookup)
    
    for campo in df["campo"].unique():
        subset = df[df["campo"] == campo]
        mapa_regex[campo] = {}
        for _, row in subset.iterrows():
            # Pré-compilar com limites de palavra (\b)
            padrao = re.compile(r"\b" + re.escape(row["valor_original_norm"]) + r"\b")
            mapa_regex[campo][padrao] = row["valor_normalizado"]
            
    return mapa_regex

def _lookup_exato(valor: str, campo: str, mapa_regex: dict) -> str | None:
    """Lookup rápido para Inventário (sem iterar regex se possível, testa string exata)."""
    if not mapa_regex or pd.isna(valor): return None
    v_norm = _normalizar_para_lookup(str(valor))
    # Para o inventario a palavra exata costuma bastar. Iteramos pelas chaves regex.
    for padrao, val_norm in mapa_regex.get(campo, {}).items():
        if padrao.search(v_norm): return val_norm
    return None

def _lookup_trends(termo: str, mapa_regex: dict) -> list[dict]:
    """Retorna lista de características individuais isoladas (Granularidade 1-para-1)."""
    if not mapa_regex or pd.isna(termo) or not termo: return []
    chave = _normalizar_para_lookup(str(termo))
    encontrados = []

    # 1. Marca + Modelo (ex: "VW Golf" -> Gera 2 linhas: uma Marca, uma Modelo)
    for padrao, normalizado in mapa_regex.get("marca_modelo", {}).items():
        if padrao.search(chave):
            partes = normalizado.split("|", 1)
            encontrados.append({"marca": partes[0], "modelo": "N/A", "combustivel": "N/A", "tipo": "N/A"})
            encontrados.append({"marca": "N/A", "modelo": partes[1], "combustivel": "N/A", "tipo": "N/A"})

    # 2. Individuais
    for campo in ["marca", "modelo", "combustivel", "tipo_automovel"]:
        for padrao, normalizado in mapa_regex.get(campo, {}).items():
            if padrao.search(chave):
                row = {"marca": "N/A", "modelo": "N/A", "combustivel": "N/A", "tipo": "N/A"}
                c = "tipo" if campo == "tipo_automovel" else campo
                row[c] = normalizado
                encontrados.append(row)
                
    return encontrados

def _extrair_mencoes_detalhadas(texto_normalizado: str, mapa_regex: dict) -> dict:
    """Recebe texto já normalizado — evita chamar _normalizar_para_lookup repetidamente."""
    encontrados = {"marcas": set(), "modelos": set(), "combustiveis": set(), "tipos": set()}

    for padrao, normalizado in mapa_regex.get("marca_modelo", {}).items():
        if padrao.search(texto_normalizado):
            p = normalizado.split("|", 1)
            encontrados["marcas"].add(p[0])
            encontrados["modelos"].add(p[1])

    for campo, plural in _CAMPO_PARA_PLURAL.items():
        for padrao, normalizado in mapa_regex.get(campo, {}).items():
            if padrao.search(texto_normalizado):
                encontrados[plural].add(normalizado)

    return encontrados

def _isolar_texto_por_carro(texto_limpo: str, mapa_regex: dict) -> list[dict]:
    """Isola características para a granularidade do DW (1 caraterística por registo)."""
    frases = [f.strip() for f in texto_limpo.split(".") if len(f.strip()) > 5]
    if not frases: return []

    # Normalizar cada frase uma única vez
    frases_norm = [_normalizar_para_lookup(f) for f in frases]

    # Agrupar menções por categoria e valor
    agrupado = defaultdict(list)

    for frase_orig, frase_norm in zip(frases, frases_norm):
        mencoes = _extrair_mencoes_detalhadas(frase_norm, mapa_regex)
        for k_ext, k_grp in _MAPEAMENTO_PLURAL.items():
            for val in mencoes.get(k_ext, set()):
                # Usar tuplo (cat, val) como chave para agrupar frases relacionadas
                agrupado[(k_grp, val)].append(frase_orig)

    if not agrupado: return []

    res = []
    base_row = {"marca": "N/A", "modelo": "N/A", "tipo": "N/A", "combustivel": "N/A"}

    for (cat, val), frases_list in agrupado.items():
        row = base_row.copy()
        row[cat] = val
        row["texto_completo"] = " . ".join(frases_list)
        row["n_mencoes_modelo"] = len(frases_list)
        res.append(row)

    return res


# ─── ESCRITA & QUALIDADE ─────────────────────────────────────────────────────

def _escrever_quarentena(df_quarentena: pd.DataFrame, delta_path: str):
    if df_quarentena is None or df_quarentena.empty: return
    
    # Preencher nulls para compatibilidade Arrow
    for col in df_quarentena.columns:
        if df_quarentena[col].dtype in ["object", "string"]:
            df_quarentena[col] = df_quarentena[col].fillna("").astype(str)
            
    df_quarentena["data_entrada"] = datetime.now(timezone.utc).isoformat()
    tabela = pa.Table.from_pandas(df_quarentena, preserve_index=False)
    
    try:
        write_deltalake(delta_path, tabela, mode="append", schema_mode="merge")
    except:
        Path(delta_path).parent.mkdir(parents=True, exist_ok=True)
        write_deltalake(delta_path, tabela, mode="overwrite", schema_mode="merge")

def _registar_qualidade(engine, fonte: str, total: int, ok: int, q: int, df_batch: pd.DataFrame = None, notas: str = None):
    try:
        campo_mais_nulo = None
        n_linhas_duplicadas = 0
        n_valores_ausentes = 0
        
        if df_batch is not None and not df_batch.empty:
            n_linhas_duplicadas = df_batch.duplicated().sum()
            n_valores_ausentes = df_batch.isna().sum().sum()
            nulos = df_batch.isna().sum()
            if nulos.max() > 0:
                campo_mais_nulo = nulos.idxmax()

        taxa = round(q/total*100, 2) if total > 0 else 0
        with engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {DW_SCHEMA}.data_quality_log 
                    (fonte, data_run, total_registos, registos_ok, registos_quarentena, taxa_quarentena_pct, n_linhas_duplicadas, n_valores_ausentes, campo_mais_nulo, notas) 
                VALUES (:f, :d, :t, :o, :q, :tx, :nd, :na, :cn, :n)
            """), {
                "f": fonte, "d": datetime.now(timezone.utc), "t": total, "o": ok, "q": q, "tx": taxa, 
                "nd": int(n_linhas_duplicadas), "na": int(n_valores_ausentes), "cn": campo_mais_nulo, "n": notas
            })
    except Exception as e:
        _log(f"Erro ao registar qualidade: {e}", "WARN")

def _merge_silver(df: pd.DataFrame, path: str, bk: list[str]):
    if df.empty: return
    for col in df.columns:
        if df[col].dtype in ["object", "string"]:
            df[col] = df[col].fillna("").astype(str)
            
    t = pa.Table.from_pandas(df, preserve_index=False)
    p = " AND ".join([f"s.{c} = t.{c}" for c in bk])
    
    try:
        dt = DeltaTable(path)
        dt.merge(source=t, predicate=p, source_alias="s", target_alias="t").when_matched_update_all().when_not_matched_insert_all().execute()
    except:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        write_deltalake(path, t, mode="overwrite")


# ─── PIPELINES SILVER ────────────────────────────────────────────────────────

def silver_inventario(files=None, engine=None):
    _log("[Silver] Inventário")
    inicio = time.time()
    try: df = DeltaTable(BRONZE_INVENTARIO).to_pandas()
    except: return
    if files: df = df[df["source_file"].isin([Path(f).name for f in files])]
    if df.empty: return

    mapa_regex = _carregar_dicionario_regex(engine)
    df = _normalizar_nulos(df)

    # Casts
    df["data_entrada_stock"] = _cast_seguro(df["data_entrada_stock"], "datetime")
    df["data_venda"] = _cast_seguro(df["data_venda"], "datetime")
    df["preco_aquisicao"] = _cast_seguro(df["preco_aquisicao"], "float")
    df["preco_venda"] = _cast_seguro(df["preco_venda"], "float")
    df["quilometragem"] = _cast_seguro(df["quilometragem"], "int")

    # Limpeza / Normalização (Vetorizado ou Apply rápido)
    df["marca_normalizada"] = df["marca"].apply(lambda x: _lookup_exato(x, "marca", mapa_regex))
    df["modelo_normalizado"] = df["modelo"].apply(lambda x: _lookup_exato(x, "modelo", mapa_regex))
    df["tipo_automovel"] = df["tipo_automovel"].apply(lambda x: _lookup_exato(x, "tipo_automovel", mapa_regex) or x)
    df["combustivel"] = df["combustivel"].apply(lambda x: _lookup_exato(x, "combustivel", mapa_regex) or x)

    if "stand" in df.columns:
        df["stand"] = df["stand"].astype(str).str.strip().str.title()
    
    # Quarentena (Sem Iterrows!)
    mask_q = df["matricula"].isna() | df["marca_normalizada"].isna() | df["modelo_normalizado"].isna()
    df_q = df[mask_q].copy()
    if not df_q.empty:
        df_q["fonte"] = "inventario"
        df_q["regra_violada"] = "REF_NOT_FOUND"
        df_q["campo_problema"] = "marca/modelo"
        df_q["valor_encontrado"] = df_q["marca"].astype(str) + "/" + df_q["modelo"].astype(str)
        df_q["registo_raw"] = df_q.apply(lambda r: str(r.to_dict()), axis=1) # Conversão segura
        _escrever_quarentena(df_q[["fonte", "source_file", "regra_violada", "campo_problema", "valor_encontrado", "registo_raw"]], QUARENTENA_INVENTARIO)
    
    df_ok = df[~mask_q].copy()
    # CDC-like semantics: manter o histórico guardado em Silver com ingestion_timestamp
    df_ok = df_ok.sort_values(["id_viatura", "ingestion_timestamp", "source_file"])
    _merge_silver(df_ok, SILVER_INVENTARIO, ["id_viatura", "ingestion_timestamp"])
    if engine: _registar_qualidade(engine, "inventario", len(df), len(df_ok), len(df_q), df, f"{time.time()-inicio:.1f}s")


def silver_clientes(source_files=None, engine=None):
    _log("[Silver] Clientes")
    inicio = time.time()
    try: df = DeltaTable(BRONZE_CLIENTES).to_pandas()
    except: return
    if source_files: df = df[df["source_file"].isin([Path(f).name for f in source_files])]
    if df.empty: return

    df = _normalizar_nulos(df.copy())
    
    # Strings e Casts
    if "distrito" in df.columns: df["distrito"] = df["distrito"].astype(str).str.strip().str.title()
    if "nome" in df.columns: df["nome"] = df["nome"].astype(str).str.strip()
    df["idade"] = _cast_seguro(df["idade"], "int")

    # Vectorized faixa_etaria (muito mais rápido que .apply)
    bins = [0, 24, 34, 49, 64, 150]
    labels = ["18-24", "25-34", "35-49", "50-64", "65+"]
    df["faixa_etaria"] = pd.cut(df["idade"].fillna(-1), bins=bins, labels=labels, right=True)
    df["faixa_etaria"] = df["faixa_etaria"].astype(str).replace("nan", "Desconhecido")

    # Quarentena (Sem Iterrows)
    mask_q = df["nif"].isna()
    df_q = df[mask_q].copy()
    if not df_q.empty:
        df_q["fonte"] = "clientes"
        df_q["regra_violada"] = "NULL_BK"
        df_q["campo_problema"] = "nif"
        df_q["valor_encontrado"] = "N/A"
        df_q["registo_raw"] = df_q.apply(lambda r: str(r.to_dict()), axis=1)
        _escrever_quarentena(df_q[["fonte", "source_file", "regra_violada", "campo_problema", "valor_encontrado", "registo_raw"]], QUARENTENA_CLIENTES)

    df_ok = df[~mask_q].copy().drop_duplicates(subset=["nif", "ano_mes"], keep="last")
    _merge_silver(df_ok, SILVER_CLIENTES, ["nif", "ano_mes"])
    if engine: _registar_qualidade(engine, "clientes", len(df), len(df_ok), len(df_q), df, f"{time.time()-inicio:.1f}s")


def silver_demografia(source_files=None, engine=None):
    _log("[Silver] Demografia Regional")
    inicio = time.time()
    try: df = DeltaTable(BRONZE_DEMOGRAFIA).to_pandas()
    except: return
    if source_files: df = df[df["source_file"].isin([Path(f).name for f in source_files])]
    if df.empty: return

    df = _normalizar_nulos(df.copy())
    
    if "distrito" in df.columns: df["distrito"] = df["distrito"].astype(str).str.strip().str.title()
    df["ano_referencia"] = _cast_seguro(df["ano_referencia"], "int")
    df["populacao_total"] = _cast_seguro(df["populacao_total"], "int")
    if "mean_age" in df.columns: df["mean_age"] = _cast_seguro(df["mean_age"], "float")
    for col in [c for c in df.columns if c.startswith("pct_")]:
        df[col] = _cast_seguro(df[col], "float")

    mask_q = df["distrito"].isna() | df["ano_referencia"].isna()
    df_q = df[mask_q].copy()
    if not df_q.empty:
        df_q["fonte"] = "demografia"
        df_q["regra_violada"] = "NULL_BK"
        df_q["campo_problema"] = "distrito|ano_referencia"
        df_q["valor_encontrado"] = df_q["distrito"].astype(str) + "|" + df_q["ano_referencia"].astype(str)
        df_q["registo_raw"] = df_q.apply(lambda r: str(r.to_dict()), axis=1)
        _escrever_quarentena(df_q[["fonte", "source_file", "regra_violada", "campo_problema", "valor_encontrado", "registo_raw"]], QUARENTENA_DEMOGRAFIA)

    df_ok = df[~mask_q].copy().drop_duplicates(subset=["distrito", "ano_referencia"], keep="last")
    _merge_silver(df_ok, SILVER_DEMOGRAFIA, ["distrito", "ano_referencia"])
    if engine: _registar_qualidade(engine, "demografia", len(df), len(df_ok), len(df_q), df, f"{time.time()-inicio:.1f}s")


def silver_forum(files=None, engine=None, nlp=True):
    _log("[Silver] Fórum")
    inicio = time.time()
    try:
        df = DeltaTable(BRONZE_FORUM).to_pandas()
    except Exception as e:
        _log(f"Erro ao ler Bronze Forum: {e}", "WARN")
        return
    if files:
        df = df[df["source_file"].isin({Path(f).name for f in files})]
    if df.empty:
        return

    mapa_regex = _carregar_dicionario_regex(engine)
    res_ok, q_regs = [], []

    # Iterar por colunas em vez de iterrows — evita overhead de Series por linha
    for texto, source_file, data_extracao in zip(
        df["texto_bruto"].fillna(""), df["source_file"], df["data_extracao"]
    ):
        if not texto:
            continue
        blocos = _isolar_texto_por_carro(texto, mapa_regex)
        if not blocos:
            q_regs.append({
                "fonte": "forum", "source_file": source_file,
                "regra_violada": "NO_MENTION", "campo_problema": "texto",
                "valor_encontrado": "N/D", "registo_raw": texto[:500],
            })
            continue
        for b in blocos:
            res_ok.append({
                "source_file": source_file,
                "data_extracao": str(data_extracao),
                "texto_limpo": b["texto_completo"],
                "mencoes_marca": b["marca"],
                "mencoes_modelo": b["modelo"],
                "mencoes_tipo": b["tipo"],
                "mencoes_combustivel": b["combustivel"],
                "n_mencoes_modelo": b["n_mencoes_modelo"],
            })

    # Fix bug: df_ok definido sempre, evita NameError na linha do _registar_qualidade
    df_ok = pd.DataFrame(res_ok) if res_ok else pd.DataFrame()

    if not df_ok.empty:
        textos_unicos = df_ok["texto_limpo"].unique().tolist()
        _log(f"NLP: {len(df_ok)} menções, {len(textos_unicos)} textos únicos. A iniciar...")

        mapa_sentimentos = {}
        batch_size = 50
        for i in range(0, len(textos_unicos), batch_size):
            batch = textos_unicos[i : i + batch_size]
            scores = _analisar_sentimento_batch(batch, nlp)
            mapa_sentimentos.update(zip(batch, scores))  # substituiu o loop manual
            _log(f"  > NLP: {min(i + batch_size, len(textos_unicos))}/{len(textos_unicos)}...")

        df_ok["score_sentimento"] = df_ok["texto_limpo"].map(mapa_sentimentos)
        _merge_silver(df_ok, SILVER_FORUM, [
            "source_file", "mencoes_marca", "mencoes_modelo",
            "mencoes_tipo", "mencoes_combustivel",
        ])

    if q_regs:
        _escrever_quarentena(pd.DataFrame(q_regs), QUARENTENA_FORUM)
    if engine:
        _registar_qualidade(
            engine, "forum", len(df), len(res_ok), len(q_regs),
            df_ok if not df_ok.empty else None,
            f"{time.time()-inicio:.1f}s",
        )


def silver_trends(files=None, engine=None):
    _log("[Silver] Trends")
    inicio = time.time()
    try: df = DeltaTable(BRONZE_TRENDS).to_pandas()
    except: return
    if files: df = df[df["source_file"].isin([Path(f).name for f in files])]
    if df.empty: return

    mapa_regex = _carregar_dicionario_regex(engine)
    rows = []
    
    # Extração de múltiplas características (granularidade ajustada)
    for _, row in df.iterrows():
        matches = _lookup_trends(row["termo"], mapa_regex)
        for m in matches:
            new_row = row.to_dict()
            new_row["marca_normalizada"] = m["marca"]
            new_row["modelo_normalizado"] = m["modelo"]
            new_row["combustivel_normalizado"] = m["combustivel"]
            new_row["tipo_normalizado"] = m["tipo"]
            rows.append(new_row)
            
    if not rows: return
    df_new = pd.DataFrame(rows)
    df_new["mes"] = pd.to_datetime(df_new["mes"]).dt.date
    if "regiao" in df_new.columns: df_new["regiao"] = df_new["regiao"].astype(str).str.strip().str.title()
    
    _merge_silver(df_new, SILVER_TRENDS, ["termo", "mes", "regiao", "marca_normalizada", "modelo_normalizado", "tipo_normalizado", "combustivel_normalizado"])
    if engine: _registar_qualidade(engine, "trends", len(df), len(df_new), 0, df_new, f"{time.time()-inicio:.1f}s")


def silver_hashtags(files=None, engine=None):
    _log("[Silver] Hashtags")
    inicio = time.time()
    try: df = DeltaTable(BRONZE_HASHTAGS).to_pandas()
    except: return
    if files: df = df[df["source_file"].isin([Path(f).name for f in files])]
    if df.empty: return

    mapa_regex = _carregar_dicionario_regex(engine)

    # Mapa modelo -> marca derivado do catálogo de veículos (para inferir marca quando so modelo é conhecido)
    try:
        from generate_hashtags import VEHICLES as _HASH_VEHICLES
        _modelo_para_marca = {v.modelo: v.marca for v in _HASH_VEHICLES}
    except Exception:
        _modelo_para_marca = {}

    rows = []
    for _, row in df.iterrows():
        matches = _lookup_trends(row["categoria"], mapa_regex)
        for m in matches:
            new_row = row.to_dict()
            marca = m["marca"]
            modelo = m["modelo"]
            # Inferir marca se só o modelo é conhecido
            if (marca in ("N/A", "", None)) and modelo not in ("N/A", "", None) and modelo in _modelo_para_marca:
                marca = _modelo_para_marca[modelo]
            new_row["marca_normalizada"] = marca
            new_row["modelo_normalizado"] = modelo
            new_row["combustivel_normalizado"] = m["combustivel"]
            new_row["tipo_normalizado"] = m["tipo"]
            rows.append(new_row)

    if not rows: return
    df_new = pd.DataFrame(rows)
    bk_hashtags = [
    "hashtag", "data", "marca_normalizada", "modelo_normalizado",
    "tipo_normalizado", "combustivel_normalizado"
    ]
    _merge_silver(df_new, SILVER_HASHTAGS, bk_hashtags)

    if engine: _registar_qualidade(engine, "hashtags", len(df), len(df_new), 0, df_new, f"{time.time()-inicio:.1f}s")



def run_silver(nlp_habilitado=True, **kwargs):
    _log("=" * 60)
    _log("SILVER PIPELINE INICIADO")
    _log("=" * 60)
    
    try:
        with socket.create_connection((_PG_HOST, int(_PG_PORT)), timeout=3):
            engine = create_engine(DW_URL)
    except: engine = None
    
    silver_inventario(files=kwargs.get("ficheiros_inventario"), engine=engine)
    silver_trends(files=kwargs.get("ficheiros_trends"), engine=engine)
    silver_forum(files=kwargs.get("ficheiros_forum"), engine=engine, nlp=nlp_habilitado)
    silver_hashtags(files=kwargs.get("ficheiros_hashtags"), engine=engine)
    silver_clientes(source_files=kwargs.get("ficheiros_clientes"), engine=engine)
    silver_demografia(source_files=kwargs.get("ficheiros_demografia"), engine=engine)
    
    if engine: engine.dispose()
    _log("=" * 60)

if __name__ == "__main__":
    run_silver()