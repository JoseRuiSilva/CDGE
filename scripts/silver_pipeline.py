"""
silver_pipeline.py — Camada Silver | Projeto Auto Escala
=========================================================
Lê dados brutos do Bronze (Delta Lake), aplica limpeza, tipagem,
normalização via dicionário PostgreSQL e NLP de sentimento (fórum).
Registos inválidos → Delta de quarentena por fonte.
Métricas de qualidade → tabela data_quality_log no PostgreSQL.
Dados limpos → Silver (Delta Lake, MERGE/UPSERT por Business Key).

Fontes: inventário (CSV), Google Trends (JSON), fórum (TXT), hashtags (XML).
Decisões de desenho: ver contexto_auto_escala_llm.txt e diálogos de decisão.
"""

import re
import sys
import time
import socket
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy import create_engine, text

# ─── LOGGING ─────────────────────────────────────────────────────────────────

def _log(msg: str, nivel: str = "INFO") -> None:
    """Print com timestamp ISO usado em toda a pipeline Silver."""
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"  [{ts}] [{nivel}] {msg}")


# ─── NLP com lazy loading ─────────────────────────────────────────────────────
# O modelo BERT so e carregado quando silver_forum for chamado com NLP activo.
# Evita atrasos em runs de debug ou batches sem ficheiros de forum novos.
# Na 1a execucao pode demorar 10-30s (download HF Hub) -- use --no-nlp para saltar.

_analisador_nlp = None
NLP_DISPONIVEL  = False
_nlp_ja_tentado = False


def _carregar_nlp() -> None:
    """Carrega pysentimiento na primeira chamada (lazy). No-op nas seguintes."""
    global _analisador_nlp, NLP_DISPONIVEL, _nlp_ja_tentado
    if _nlp_ja_tentado:
        return
    _nlp_ja_tentado = True
    _log("NLP: a inicializar pysentimiento (1a execucao pode demorar)...")
    try:
        from pysentimiento import create_analyzer as _criar_analisador
        _analisador_nlp = _criar_analisador(task="sentiment", lang="pt")
        NLP_DISPONIVEL  = True
        _log("NLP: pysentimiento carregado com sucesso.")
    except Exception as _err_nlp:
        _analisador_nlp = None
        NLP_DISPONIVEL  = False
        _log(f"AVISO NLP: pysentimiento indisponivel ({_err_nlp}). Score = 0.0.", "WARN")


# ─── CONFIGURAÇÃO ─────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent.parent

# Caminhos Bronze (leitura)
BRONZE_INVENTARIO = str(BASE_DIR / "data_lake/bronze/inventario_delta")
BRONZE_TRENDS     = str(BASE_DIR / "data_lake/bronze/trends_delta")
BRONZE_FORUM      = str(BASE_DIR / "data_lake/bronze/forum_delta")
BRONZE_HASHTAGS   = str(BASE_DIR / "data_lake/bronze/hashtags_delta")

# Caminhos Silver (escrita via MERGE)
SILVER_INVENTARIO = str(BASE_DIR / "data_lake/silver/inventario_delta")
SILVER_TRENDS     = str(BASE_DIR / "data_lake/silver/trends_delta")
SILVER_FORUM      = str(BASE_DIR / "data_lake/silver/forum_delta")
SILVER_HASHTAGS   = str(BASE_DIR / "data_lake/silver/hashtags_delta")

# Caminhos Quarentena (append — registos rejeitados)
QUARENTENA_INVENTARIO = str(BASE_DIR / "data_lake/quarantine/inventario_delta")
QUARENTENA_TRENDS     = str(BASE_DIR / "data_lake/quarantine/trends_delta")
QUARENTENA_FORUM      = str(BASE_DIR / "data_lake/quarantine/forum_delta")
QUARENTENA_HASHTAGS   = str(BASE_DIR / "data_lake/quarantine/hashtags_delta")

# PostgreSQL -- dicionario de normalizacao e data_quality_log
_PG_HOST  = __import__("os").environ.get("PG_HOST", "localhost")
_PG_PORT  = __import__("os").environ.get("PG_PORT", "5432")
DW_URL    = f"postgresql+psycopg://ae_user:ae_pass_2026@{_PG_HOST}:{_PG_PORT}/auto_escala"
DW_SCHEMA = "auto_escala_dw"

# Limiares de deteção de tendências (documentados na secção 5.3 do relatório)
LIMIAR_CRESCIMENTO_MOM   = 30.0   # crescimento_mom_pct ≥ 30% para trending_flag
LIMIAR_DELTA_SENTIMENTO  = 0.3    # delta_sentimento ≥ 0.3 para trending_flag

# Representações textuais de nulo a normalizar para NA real
_NULOS_TEXTUAIS = {"", "nan", "none", "null", "n/a", "na", "nd", "s/d", "-"}

# Padrões de ruído nos ficheiros de fórum (navegação, paginação, cabeçalho do site)
_REGEX_RUIDO_FORUM = re.compile(
    r"(motorguia\.net|Início\s+Marcas|Elétricos|SUV|Contacto|Login"
    r"|»\s*Página\s*(anterior|seguinte)"
    r"|^\d+\s*$)",
    re.IGNORECASE,
)
# Linha de cabeçalho de post: "username  |  YYYY-MM"
_REGEX_CABECALHO_POST = re.compile(r"^[\w_\-]+\s{2,}\|\s{2,}\d{4}-\d{2}$")

# Frases de ruído literais do generate_forum.py (header, footer, metadata de utilizador).
# Usadas quando o texto chega como bloco contínuo (sem '\n') em vez de linhas separadas.
_RUIDO_LITERAIS_FORUM = [
    "motorguia.net Forum Automovel Portugues Registo Login Pesquisar",
    "Bem-vindo convidado Entrar Registar Topicos Recentes Atividade",
    "Novos Posts Ajuda Calendario Comunidade Forum Regras Utilizadores",
    "Topicos Recentes 1 2 3 ... 24 Proxima Pagina Anterior Ir para o topo",
    "Contactos Arquivo Politica de Privacidade Termos de Utilizacao",
    "motorguia.net 2005-2026 Todos os direitos reservados",
    "Ver perfil Responder Citar",
    "Pagina 1 de 2", "Pagina 2 de 3", "Pagina 1 de 3", "Pagina 1 de 4",
    "Pagina 2 de 2",
]
# Metadata de utilizador: "Membro desde Mar 2017 892 posts" / "Senior Member 3401 posts"
_REGEX_METADATA_UTILIZADOR = re.compile(
    r"(?:Membro|Senior\s+Member|Utilizador\s+registado)[^.]{0,80}posts",
    re.IGNORECASE,
)


# ─── UTILITÁRIOS GERAIS ───────────────────────────────────────────────────────

def _normalizar_para_lookup(valor: str) -> str:
    """
    Pré-processa um valor antes de o comparar com o dicionário:
    lowercase + trim + colapsa espaços múltiplos + remove caracteres especiais.
    Assim o dicionário só precisa de ter entradas simples (ex: 'vw', não 'VW' e ' VW ').
    """
    if pd.isna(valor) or not isinstance(valor, str):
        return ""
    texto = valor.lower().strip()
    texto = re.sub(r"\s+", " ", texto)
    texto = re.sub(r"[^\w\s]", "", texto)
    return texto


def _normalizar_nulos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Substitui representações textuais de nulo (nan, none, null, etc.)
    por pd.NA real em todas as colunas de texto.
    """
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].apply(
            lambda v: pd.NA if (isinstance(v, str) and v.strip().lower() in _NULOS_TEXTUAIS) else v
        )
    return df


def _cast_seguro(serie: pd.Series, tipo: str, fallback=pd.NA) -> pd.Series:
    """
    Tenta converter uma série para o tipo pedido ('int', 'float', 'datetime').
    Valores não conversíveis ficam como fallback (pd.NA por omissão).
    """
    try:
        if tipo == "int":
            return pd.to_numeric(serie, errors="coerce").astype("Int64")
        if tipo == "float":
            return pd.to_numeric(serie, errors="coerce")
        if tipo == "datetime":
            return pd.to_datetime(serie, errors="coerce", utc=True)
    except Exception:
        pass
    return serie


# ─── DICIONÁRIO DE NORMALIZAÇÃO ───────────────────────────────────────────────

def _carregar_dicionario(engine) -> pd.DataFrame:
    """
    Lê dim_dicionario_veiculo do PostgreSQL e devolve DataFrame com
    valor_original pré-normalizado (lowercase+trim) para lookup direto.
    Se o PostgreSQL não estiver acessível, devolve DataFrame vazio (pipeline continua).
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                f"SELECT campo, valor_original, valor_normalizado "
                f"FROM {DW_SCHEMA}.dim_dicionario_veiculo WHERE ativo = TRUE",
                conn,
            )
        df["valor_original_norm"] = df["valor_original"].apply(_normalizar_para_lookup)
        print(f"  Dicionário carregado: {len(df)} entradas.")
        return df
    except Exception as e:
        print(f"  AVISO: dicionário indisponível ({e}). Normalização desativada.")
        return pd.DataFrame(columns=["campo", "valor_original", "valor_normalizado", "valor_original_norm"])


def _lookup(valor: str, campo: str, dicionario: pd.DataFrame) -> str | None:
    """
    Devolve o valor normalizado para (campo, valor) no dicionário.
    Devolve None se não encontrado.
    """
    if dicionario.empty or pd.isna(valor):
        return None
    chave = _normalizar_para_lookup(str(valor))
    subset = dicionario[dicionario["campo"] == campo]
    match = subset[subset["valor_original_norm"] == chave]
    if not match.empty:
        return match.iloc[0]["valor_normalizado"]
    return None


def _lookup_trends(termo: str, dicionario: pd.DataFrame) -> tuple:
    """
    Para termos de Trends (frases como "VW Golf usado"):
    procura marcas e modelos por substring/word-boundary dentro do termo,
    em vez de correspondência exacta.
    Devolve (marca_normalizada, modelo_normalizado) — cada um pode ser None.
    """
    if dicionario.empty or pd.isna(termo) or not termo:
        return None, None
    chave = _normalizar_para_lookup(str(termo))
    marca_encontrada, modelo_encontrado = None, None
    for _, row in dicionario.iterrows():
        padrao = row["valor_original_norm"]
        if not padrao:
            continue
        if re.search(r"\b" + re.escape(padrao) + r"\b", chave):
            if row["campo"] == "marca" and not marca_encontrada:
                marca_encontrada = row["valor_normalizado"]
            elif row["campo"] == "modelo" and not modelo_encontrado:
                modelo_encontrado = row["valor_normalizado"]
    return marca_encontrada, modelo_encontrado

def _extrair_mencoes(texto: str, dicionario: pd.DataFrame) -> tuple[list[str], list[str]]:
    """
    Percorre o texto e devolve (marcas_encontradas, modelos_encontrados)
    como listas de valores normalizados únicos.
    Estratégia defensiva para fonte não estruturada: procura por substring/word boundary,
    não assume estrutura do texto.
    """
    if dicionario.empty or not texto:
        return [], []

    texto_norm = _normalizar_para_lookup(texto)
    marcas, modelos = set(), set()

    for _, row in dicionario.iterrows():
        padrao = re.escape(row["valor_original_norm"])
        if not padrao:
            continue
        if re.search(r"\b" + padrao + r"\b", texto_norm):
            if row["campo"] == "marca":
                marcas.add(row["valor_normalizado"])
            elif row["campo"] == "modelo":
                modelos.add(row["valor_normalizado"])

    return sorted(marcas), sorted(modelos)


# ─── NLP — SENTIMENTO ─────────────────────────────────────────────────────────

def _analisar_sentimento(texto: str, nlp_habilitado: bool = True) -> float:
    """
    Devolve score de sentimento em [-1.0, +1.0] usando pysentimiento.
    Devolve 0.0 se NLP desabilitado, indisponivel ou texto sem conteudo.

    Estrategia de chunking (respeita o limite de ~512 tokens do modelo):
    - Formato multi-linha: divide por paragrafos (linhas com >15 chars).
    - Formato continuo (bloco unico): divide em janelas de 400 chars com overlap
      por fronteira de palavra, cobrindo todo o documento em vez de apenas os
      primeiros 500 chars.
    """
    if not nlp_habilitado or not texto or not texto.strip():
        return 0.0
    _carregar_nlp()
    if not NLP_DISPONIVEL:
        return 0.0

    # Tentar dividir por paragrafos (formato multi-linha)
    paragrafos = [p.strip() for p in texto.split("\n") if len(p.strip()) > 15]

    # Se houver poucos paragrafos (formato continuo), dividir em janelas de palavras
    if len(paragrafos) <= 1:
        palavras = texto.split()
        tamanho_janela = 80   # ~400 chars em portugues medio
        passo = 60            # overlap de 20 palavras entre janelas
        paragrafos = [
            " ".join(palavras[i: i + tamanho_janela])
            for i in range(0, len(palavras), passo)
            if len(" ".join(palavras[i: i + tamanho_janela])) > 15
        ]

    if not paragrafos:
        return 0.0

    scores = []
    for paragrafo in paragrafos[:30]:   # maximo 30 chunks por ficheiro
        try:
            resultado = _analisador_nlp.predict(paragrafo[:500])
            proba = resultado.probas
            score = proba.get("POS", 0.0) - proba.get("NEG", 0.0)
            scores.append(score)
        except Exception:
            continue

    return round(sum(scores) / len(scores), 4) if scores else 0.0


# ─── QUARENTENA ───────────────────────────────────────────────────────────────

def _escrever_quarentena(registos: list[dict], delta_path: str):
    """
    Faz append dos registos rejeitados na tabela de quarentena Delta da fonte.
    Campos obrigatórios em cada registo:
      - fonte, source_file, regra_violada, campo_problema, valor_encontrado
      - data_entrada, registo_raw (dict com o registo original)
    """
    if not registos:
        return
    df = pd.DataFrame(registos)
    df["data_entrada"] = datetime.now(timezone.utc).isoformat()
    # Serializar registo_raw como string JSON para compatibilidade com Delta
    if "registo_raw" in df.columns:
        import json as _json
        df["registo_raw"] = df["registo_raw"].apply(
            lambda v: _json.dumps(v, ensure_ascii=False, default=str) if isinstance(v, dict) else str(v)
        )
    tabela = pa.Table.from_pandas(df, preserve_index=False)
    try:
        DeltaTable(delta_path)
        write_deltalake(delta_path, tabela, mode="append", schema_mode="merge")
    except Exception:
        Path(delta_path).mkdir(parents=True, exist_ok=True)
        write_deltalake(delta_path, tabela, mode="overwrite", schema_mode="merge")
    print(f"    Quarentena → {len(registos)} registos  [{delta_path}]")


# ─── DATA QUALITY LOG ─────────────────────────────────────────────────────────

def _registar_qualidade(engine, fonte: str, total: int, ok: int, quarentena: int, notas: str = None):
    """
    Insere uma linha em data_quality_log no PostgreSQL.
    Se o PostgreSQL não estiver acessível, avisa mas não interrompe a pipeline.
    """
    try:
        taxa = round(quarentena / total * 100, 2) if total > 0 else 0.0
        with engine.begin() as conn:
            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.data_quality_log
                        (fonte, data_run, total_registos, registos_ok,
                         registos_quarentena, taxa_quarentena_pct, notas)
                    VALUES
                        (:fonte, :data_run, :total, :ok, :q, :taxa, :notas)
                """),
                {
                    "fonte":    fonte,
                    "data_run": datetime.now(timezone.utc),
                    "total":    total,
                    "ok":       ok,
                    "q":        quarentena,
                    "taxa":     taxa,
                    "notas":    notas,
                },
            )
        print(f"    Quality log → {ok} ok  |  {quarentena} quarentena  ({taxa}%)  [{fonte}]")
    except Exception as e:
        print(f"    AVISO: data_quality_log indisponível ({e}).")


# ─── MERGE SILVER ─────────────────────────────────────────────────────────────

def _merge_silver(df: pd.DataFrame, delta_path: str, bk_cols: list[str]):
    """
    Faz MERGE/UPSERT na tabela Silver Delta por Business Key (bk_cols).
    Estratégia: update_all para todos os campos quando BK coincide.
    Se a tabela não existir, cria com overwrite.

    API delta-rs 1.5.0:
        dt.merge(...).when_matched_update_all().when_not_matched_insert_all().execute()
    """
    if df.empty:
        print(f"    SKIP MERGE — DataFrame vazio  [{delta_path}]")
        return

    tabela = pa.Table.from_pandas(df, preserve_index=False)
    predicado = " AND ".join(f"s.{c} = t.{c}" for c in bk_cols)

    # Verificar se a tabela Delta já existe antes de tentar abrir
    # (evita depender do texto da exceção que varia com o SO e versão da lib)
    log_path = Path(delta_path) / "_delta_log"
    tabela_existe = log_path.exists()

    if tabela_existe:
        dt = DeltaTable(delta_path)
        (
            dt.merge(
                source=tabela,
                predicate=predicado,
                source_alias="s",
                target_alias="t",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        print(f"    MERGE → {len(df)} registos  [{delta_path}]")
    else:
        # Primeira vez — cria a tabela com overwrite
        Path(delta_path).mkdir(parents=True, exist_ok=True)
        write_deltalake(delta_path, tabela, mode="overwrite", schema_mode="merge")
        print(f"    CRIADA → {len(df)} registos  [{delta_path}]")


# ─── SILVER: INVENTÁRIO ───────────────────────────────────────────────────────

def silver_inventario(source_files: list[str] | None = None, engine=None):
    """
    Transforma os dados de inventário do Bronze para o Silver.

    Transformações aplicadas:
      - Trim + normalização de nulos textuais → NA real
      - Cast: datas → datetime UTC, preços → float, km → int
      - Normalização de marca e modelo via dicionário PostgreSQL
      - Quarentena: matricula nula, cast irrecuperável, marca/modelo não reconhecida,
                    km negativo, data_venda < data_entrada_stock

    BK para MERGE: matricula
    """
    print("\n[Silver] Inventário")
    inicio = time.time()

    # Abrir Bronze
    try:
        dt_bronze = DeltaTable(BRONZE_INVENTARIO)
    except Exception:
        print(f"  Bronze não encontrado em {BRONZE_INVENTARIO} — a saltar.")
        return

    df_bronze = dt_bronze.to_pandas()

    # Filtrar apenas os ficheiros pedidos (modo incremental)
    if source_files is not None:
        nomes = {Path(f).name for f in source_files}
        df_bronze = df_bronze[df_bronze["source_file"].isin(nomes)]

    if df_bronze.empty:
        print("  Nenhum registo novo no Bronze — a saltar.")
        return

    print(f"  Bronze lido: {len(df_bronze)} registos")

    dicionario = _carregar_dicionario(engine) if engine else pd.DataFrame(
        columns=["campo", "valor_original", "valor_normalizado", "valor_original_norm"]
    )

    # 1. Normalizar nulos textuais
    df = _normalizar_nulos(df_bronze.copy())

    # 2. Cast de datas e valores numéricos
    for col_data in ["data_entrada_stock", "data_venda"]:
        if col_data in df.columns:
            df[col_data] = _cast_seguro(df[col_data], "datetime")

    for col_float in ["preco_aquisicao", "preco_venda"]:
        if col_float in df.columns:
            df[col_float] = _cast_seguro(df[col_float], "float")

    if "quilometragem" in df.columns:
        # Guardar máscara de valores originalmente não-nulos antes do cast
        # para detetar "85000 km" → NULL (INVALID_TYPE) vs genuinamente ausente
        _km_tinha_valor = df["quilometragem"].notna()
        df["quilometragem"] = _cast_seguro(df["quilometragem"], "int")
        mask_km_invalido = _km_tinha_valor & df["quilometragem"].isna()
    else:
        mask_km_invalido = pd.Series(False, index=df.index)

    # 3. Normalizar marca e modelo via dicionário
    df["marca_normalizada"]  = df["marca"].apply(lambda v: _lookup(v, "marca", dicionario))
    df["modelo_normalizado"] = df["modelo"].apply(lambda v: _lookup(v, "modelo", dicionario))

    # 4. Identificar registos para quarentena
    # Cada máscara identifica uma regra. A union decide quem sai.
    # Cada registo rejeitado vai para quarentena UMA VEZ, com a primeira regra violada.

    # BK nula
    mask_bk = df["matricula"].isna() if "matricula" in df.columns else pd.Series(False, index=df.index)

    # Marca não resolvida (só para registos com BK válida)
    mask_marca = ~mask_bk & df["marca"].notna() & df["marca_normalizada"].isna()

    # Modelo não resolvido (só para registos com BK válida e marca ok)
    mask_modelo = ~mask_bk & ~mask_marca & df["modelo"].notna() & df["modelo_normalizado"].isna()

    # Km negativo
    mask_km = mask_km_invalido | (df["quilometragem"].notna() & (df["quilometragem"] < 0))

    # Data impossível: data_venda < data_entrada_stock
    mask_datas = pd.Series(False, index=df.index)
    if "data_venda" in df.columns and "data_entrada_stock" in df.columns:
        mask_datas = (
            df["data_venda"].notna()
            & df["data_entrada_stock"].notna()
            & (df["data_venda"] < df["data_entrada_stock"])
        )

    # Máscara total — union de todas as condições
    mask_rejeitar = mask_bk | mask_marca | mask_modelo | mask_km | mask_datas

    # Construir lista de quarentena: cada registo rejeitado aparece UMA vez
    # com a primeira regra que violou (ordem de prioridade: BK > marca > modelo > km > data)
    regras = [
        (mask_bk,     "NULL_BK",        "matricula"),
        (mask_marca,  "REF_NOT_FOUND",  "marca"),
        (mask_modelo, "REF_NOT_FOUND",  "modelo"),
        (mask_km,     "INVALID_TYPE",   "quilometragem"),  # cobre cast inválido ("85000 km") e km negativo
        (mask_datas,  "DATE_IMPOSSIBLE","data_venda"),
    ]
    ja_quarentenado = pd.Series(False, index=df.index)
    quarentena_registos = []

    for mask, regra, campo in regras:
        novos = mask & ~ja_quarentenado
        for _, row in df[novos].iterrows():
            quarentena_registos.append({
                "fonte":            "inventario",
                "source_file":      row.get("source_file", ""),
                "regra_violada":    regra,
                "campo_problema":   campo,
                "valor_encontrado": str(row.get(campo, "")),
                "registo_raw":      row.to_dict(),
            })
        ja_quarentenado = ja_quarentenado | novos

    df_ok = df[~mask_rejeitar].copy()

    # Converter colunas object para string (compatibilidade Arrow/Delta)
    for col in df_ok.select_dtypes(include="object").columns:
        df_ok[col] = df_ok[col].astype("string")

    # 5. Escrever resultados
    total = len(df)
    n_quarentena = len(quarentena_registos)
    n_ok = len(df_ok)

    _escrever_quarentena(quarentena_registos, QUARENTENA_INVENTARIO)

    if not df_ok.empty:
        _merge_silver(df_ok, SILVER_INVENTARIO, bk_cols=["matricula"])

    if engine:
        _registar_qualidade(engine, "inventario", total, n_ok, n_quarentena,
                            f"Duração: {round(time.time()-inicio, 1)}s")

    print(f"  Inventário Silver concluído  [{n_ok} ok | {n_quarentena} quarentena]")


# ─── SILVER: TRENDS ───────────────────────────────────────────────────────────

def silver_trends(source_files: list[str] | None = None, engine=None):
    """
    Transforma os dados de Google Trends do Bronze para o Silver.

    Transformações aplicadas:
      - Cast: valor_interesse → int (nulo → 0); mes → date
      - Normalização de termo para marca/modelo via dicionário
      - Quarentena: BK nula (termo, mes, regiao), valor_interesse fora de [0, 100]

    BK para MERGE: termo + mes + regiao
    """
    print("\n[Silver] Google Trends")
    inicio = time.time()

    try:
        dt_bronze = DeltaTable(BRONZE_TRENDS)
    except Exception:
        print(f"  Bronze não encontrado em {BRONZE_TRENDS} — a saltar.")
        return

    df_bronze = dt_bronze.to_pandas()

    if source_files is not None:
        nomes = {Path(f).name for f in source_files}
        df_bronze = df_bronze[df_bronze["source_file"].isin(nomes)]

    if df_bronze.empty:
        print("  Nenhum registo novo no Bronze — a saltar.")
        return

    print(f"  Bronze lido: {len(df_bronze)} registos")

    dicionario = _carregar_dicionario(engine) if engine else pd.DataFrame(
        columns=["campo", "valor_original", "valor_normalizado", "valor_original_norm"]
    )

    df = _normalizar_nulos(df_bronze.copy())

    # Cast
    df["mes"] = pd.to_datetime(df["mes"], format="%Y-%m", errors="coerce").dt.date
    df["valor_interesse"] = pd.to_numeric(df["valor_interesse"], errors="coerce")
    df["valor_interesse"] = df["valor_interesse"].fillna(0).astype(float).round().clip(0, 100).astype("Int64")

    # Normalização do termo → marca/modelo
    # Trends têm termos como "VW Golf usado" — usa lookup por substring, não exacto
    lookup_results = df["termo"].apply(lambda v: _lookup_trends(v, dicionario))
    df["marca_normalizada"]  = lookup_results.apply(lambda t: t[0])
    df["modelo_normalizado"] = lookup_results.apply(lambda t: t[1])

    # Quarentena
    quarentena_registos = []

    bk_nula = df["termo"].isna() | df["mes"].isna() | df["regiao"].isna()
    for _, row in df[bk_nula].iterrows():
        quarentena_registos.append({
            "fonte": "trends", "source_file": row.get("source_file", ""),
            "regra_violada": "NULL_BK", "campo_problema": "termo|mes|regiao",
            "valor_encontrado": f"{row.get('termo')}|{row.get('mes')}|{row.get('regiao')}",
            "registo_raw": row.to_dict(),
        })

    df_ok = df[~bk_nula].copy()

    for col in df_ok.select_dtypes(include="object").columns:
        df_ok[col] = df_ok[col].astype("string")

    total, n_quarentena, n_ok = len(df), len(quarentena_registos), len(df_ok)

    _escrever_quarentena(quarentena_registos, QUARENTENA_TRENDS)
    if not df_ok.empty:
        _merge_silver(df_ok, SILVER_TRENDS, bk_cols=["termo", "mes", "regiao"])

    if engine:
        _registar_qualidade(engine, "trends", total, n_ok, n_quarentena,
                            f"Duração: {round(time.time()-inicio, 1)}s")

    print(f"  Trends Silver concluído  [{n_ok} ok | {n_quarentena} quarentena]")


# ─── SILVER: FÓRUM ────────────────────────────────────────────────────────────

def _limpar_texto_forum(texto: str) -> str:
    """
    Remove ruido estrutural do texto bruto do forum.

    Suporta dois formatos de input:
    - Formato multi-linha (generate_samples.py): separado por newline - filtra linha a linha.
    - Formato continuo (generate_forum.py): um unico bloco separado por espacos.
      A abordagem linha-a-linha descartaria o documento inteiro quando a unica linha
      contiver 'motorguia.net'. Em vez disso, substitui frases de ruido literais
      e devolve o texto restante para NLP.
    """
    linhas = texto.split("\n")

    # Formato multi-linha
    if len(linhas) > 3:
        linhas_limpas = []
        for linha in linhas:
            linha_strip = linha.strip()
            if not linha_strip:
                continue
            if _REGEX_RUIDO_FORUM.search(linha_strip):
                continue
            if _REGEX_CABECALHO_POST.match(linha_strip):
                continue
            linhas_limpas.append(linha_strip)
        return "\n".join(linhas_limpas)

    # Formato continuo (bloco unico sem newlines)
    texto_limpo = texto
    for frase in _RUIDO_LITERAIS_FORUM:
        texto_limpo = texto_limpo.replace(frase, " ")
    texto_limpo = _REGEX_METADATA_UTILIZADOR.sub(" ", texto_limpo)
    return re.sub(r"\s+", " ", texto_limpo).strip()


def silver_forum(source_files: list[str] | None = None, engine=None, nlp_habilitado: bool = True):
    """
    Processa os ficheiros TXT do fórum do Bronze para o Silver.

    Abordagem para fonte não estruturada:
      - Não tenta reconstruir a estrutura de posts
      - Remove ruído de navegação de forma defensiva
      - Extrai sinais: menções a marcas/modelos + score de sentimento
      - 1 linha por ficheiro no Silver (grain = source_file)

    Quarentena: ficheiro sem nenhuma menção reconhecível E sem sentimento extraível.

    BK para MERGE: source_file
    """
    print("\n[Silver] Fórum motorguia.net")
    inicio = time.time()

    try:
        dt_bronze = DeltaTable(BRONZE_FORUM)
    except Exception:
        print(f"  Bronze não encontrado em {BRONZE_FORUM} — a saltar.")
        return

    df_bronze = dt_bronze.to_pandas()

    if source_files is not None:
        nomes = {Path(f).name for f in source_files}
        df_bronze = df_bronze[df_bronze["source_file"].isin(nomes)]

    if df_bronze.empty:
        print("  Nenhum registo novo no Bronze — a saltar.")
        return

    print(f"  Bronze lido: {len(df_bronze)} ficheiros")

    dicionario = _carregar_dicionario(engine) if engine else pd.DataFrame(
        columns=["campo", "valor_original", "valor_normalizado", "valor_original_norm"]
    )

    registos_ok, quarentena_registos = [], []

    for _, row in df_bronze.iterrows():
        source_file = row.get("source_file", "desconhecido")
        texto_bruto = row.get("texto_bruto", "")

        if pd.isna(texto_bruto) or not str(texto_bruto).strip():
            quarentena_registos.append({
                "fonte": "forum", "source_file": source_file,
                "regra_violada": "EMPTY_TEXT", "campo_problema": "texto_bruto",
                "valor_encontrado": "", "registo_raw": row.to_dict(),
            })
            continue

        # Limpar ruído estrutural
        texto_limpo = _limpar_texto_forum(str(texto_bruto))

        # Extrair menções a marcas e modelos (abordagem defensiva por substring/regex)
        marcas, modelos = _extrair_mencoes(texto_limpo, dicionario)
        n_mencoes = len(marcas) + len(modelos)

        # Análise de sentimento
        score_sentimento = _analisar_sentimento(texto_limpo, nlp_habilitado=nlp_habilitado)

        # Quarentena: sem menções E sentimento neutro sem extração (provável texto inútil)
        if n_mencoes == 0 and score_sentimento == 0.0:
            quarentena_registos.append({
                "fonte": "forum", "source_file": source_file,
                "regra_violada": "NO_SIGNAL",
                "campo_problema": "mencoes+sentimento",
                "valor_encontrado": f"chars={len(texto_limpo)}",
                "registo_raw": {"source_file": source_file, "texto_len": len(texto_bruto)},
            })
            continue

        registos_ok.append({
            "source_file":        source_file,
            "data_extracao":      str(row.get("data_extracao", "")),
            "ingestion_timestamp": str(row.get("ingestion_timestamp", "")),
            "texto_limpo":        texto_limpo,
            "mencoes_marca":      "|".join(marcas),   # pipe-separated (Delta não suporta arrays)
            "mencoes_modelo":     "|".join(modelos),
            "score_sentimento":   score_sentimento,
            "n_mencoes_total":    n_mencoes,
            "n_chars_texto_limpo": len(texto_limpo),
        })

        print(f"  {source_file} → marcas={marcas} modelos={modelos} sentimento={score_sentimento}")

    total = len(df_bronze)
    n_quarentena = len(quarentena_registos)
    n_ok = len(registos_ok)

    _escrever_quarentena(quarentena_registos, QUARENTENA_FORUM)

    if registos_ok:
        df_ok = pd.DataFrame(registos_ok)
        for col in df_ok.select_dtypes(include="object").columns:
            df_ok[col] = df_ok[col].astype("string")
        _merge_silver(df_ok, SILVER_FORUM, bk_cols=["source_file"])

    if engine:
        _registar_qualidade(engine, "forum", total, n_ok, n_quarentena,
                            f"NLP={'pysentimiento' if NLP_DISPONIVEL else 'off'} | "
                            f"Duração: {round(time.time()-inicio, 1)}s")

    print(f"  Fórum Silver concluído  [{n_ok} ok | {n_quarentena} quarentena]")


# ─── SILVER: HASHTAGS ─────────────────────────────────────────────────────────

def silver_hashtags(source_files: list[str] | None = None, engine=None):
    """
    Transforma os dados de hashtags do Bronze para o Silver.

    Transformações aplicadas:
      - Cast: total_posts e colunas de plataforma → int
      - Cálculo de variacao_semanal: % variação face à semana anterior (LAG por hashtag)
        Calculado sobre histórico Silver completo + novos registos Bronze
      - Extração de modelo a partir do nome da hashtag via dicionário (regex substring)
      - Quarentena: hashtag ou data nulos, total_posts não conversível

    BK para MERGE: hashtag + data
    """
    print("\n[Silver] Hashtags Social Listening")
    inicio = time.time()

    try:
        dt_bronze = DeltaTable(BRONZE_HASHTAGS)
    except Exception:
        print(f"  Bronze não encontrado em {BRONZE_HASHTAGS} — a saltar.")
        return

    df_bronze = dt_bronze.to_pandas()

    if source_files is not None:
        nomes = {Path(f).name for f in source_files}
        df_bronze = df_bronze[df_bronze["source_file"].isin(nomes)]

    if df_bronze.empty:
        print("  Nenhum registo novo no Bronze — a saltar.")
        return

    print(f"  Bronze lido: {len(df_bronze)} registos")

    dicionario = _carregar_dicionario(engine) if engine else pd.DataFrame(
        columns=["campo", "valor_original", "valor_normalizado", "valor_original_norm"]
    )

    df = _normalizar_nulos(df_bronze.copy())

    # Cast de totais para int
    colunas_posts = [c for c in df.columns if c.startswith("posts_") or c == "total_posts"]
    for col in colunas_posts:
        df[col] = _cast_seguro(df[col], "int")

    # Quarentena: BK nula ou total_posts não conversível
    quarentena_registos = []
    bk_nula = df["hashtag"].isna() | df["data"].isna()
    posts_invalidos = ~bk_nula & df["total_posts"].isna()

    for _, row in df[bk_nula | posts_invalidos].iterrows():
        regra = "NULL_BK" if (pd.isna(row.get("hashtag")) or pd.isna(row.get("data"))) else "INVALID_TYPE"
        quarentena_registos.append({
            "fonte": "hashtags", "source_file": row.get("source_file", ""),
            "regra_violada": regra, "campo_problema": "hashtag|data|total_posts",
            "valor_encontrado": f"{row.get('hashtag')}|{row.get('data')}|{row.get('total_posts')}",
            "registo_raw": row.to_dict(),
        })

    df_ok = df[~(bk_nula | posts_invalidos)].copy()

    # Extração de modelo a partir da hashtag (ex: "#volkswagengolf" → "Golf")
    # Remove o '#' e faz lookup no campo 'hashtag' do dicionário
    def _modelo_de_hashtag(hashtag: str) -> str | None:
        if pd.isna(hashtag):
            return None
        tag_limpa = re.sub(r"^#", "", str(hashtag).lower().strip())
        return _lookup(tag_limpa, "hashtag", dicionario)

    df_ok["modelo_normalizado"] = df_ok["hashtag"].apply(_modelo_de_hashtag)

    # Calcular variacao_semanal sobre histórico completo Silver + novos registos
    # Carrega Silver existente para ter o histórico de semanas anteriores
    try:
        df_silver_hist = DeltaTable(SILVER_HASHTAGS).to_pandas()[["hashtag", "data", "total_posts"]]
        df_combinado = pd.concat([df_silver_hist, df_ok[["hashtag", "data", "total_posts"]]], ignore_index=True)
        df_combinado = df_combinado.drop_duplicates(subset=["hashtag", "data"], keep="last")
    except Exception:
        df_combinado = df_ok[["hashtag", "data", "total_posts"]].copy()

    df_combinado["data"] = pd.to_datetime(df_combinado["data"], errors="coerce")
    df_combinado = df_combinado.sort_values(["hashtag", "data"])
    df_combinado["total_posts_lag"] = df_combinado.groupby("hashtag")["total_posts"].shift(1)
    df_combinado["variacao_semanal"] = (
        (df_combinado["total_posts"] - df_combinado["total_posts_lag"])
        / df_combinado["total_posts_lag"].replace(0, pd.NA)
        * 100
    ).round(4)

    # Juntar variacao_semanal de volta aos novos registos
    df_ok["data_dt"] = pd.to_datetime(df_ok["data"], errors="coerce")
    df_ok = df_ok.merge(
        df_combinado[["hashtag", "data", "variacao_semanal"]].rename(columns={"data": "data_dt"}),
        on=["hashtag", "data_dt"],
        how="left",
    ).drop(columns=["data_dt"], errors="ignore")

    # Converter object para string
    for col in df_ok.select_dtypes(include="object").columns:
        df_ok[col] = df_ok[col].astype("string")

    total = len(df)
    n_quarentena = len(quarentena_registos)
    n_ok = len(df_ok)

    _escrever_quarentena(quarentena_registos, QUARENTENA_HASHTAGS)
    if not df_ok.empty:
        _merge_silver(df_ok, SILVER_HASHTAGS, bk_cols=["hashtag", "data"])

    if engine:
        _registar_qualidade(engine, "hashtags", total, n_ok, n_quarentena,
                            f"Duração: {round(time.time()-inicio, 1)}s")

    print(f"  Hashtags Silver concluído  [{n_ok} ok | {n_quarentena} quarentena]")


# ─── PONTO DE ENTRADA ─────────────────────────────────────────────────────────

def run_silver(
    ficheiros_inventario: list[str] | None = None,
    ficheiros_trends:     list[str] | None = None,
    ficheiros_forum:      list[str] | None = None,
    ficheiros_hashtags:   list[str] | None = None,
    nlp_habilitado:       bool = True,
):
    """
    Corre a pipeline Silver para as 4 fontes.

    Se os argumentos forem None, processa todos os registos disponíveis no Bronze.
    Se forem listas de caminhos, processa apenas esses ficheiros (modo incremental via main.py).

    A ligação ao PostgreSQL é criada uma vez e partilhada entre as 4 fontes,
    evitando abrir/fechar a ligação por cada lookup de dicionário.
    """
    print("\n" + "=" * 60)
    print("  SILVER PIPELINE")
    print("=" * 60)

    # Tentar criar ligacao ao PostgreSQL (dicionario + data_quality_log)
    # Pre-check TCP rapido para nao ficar pendurado se o Docker nao estiver a correr
    pg_disponivel = False
    try:
        with socket.create_connection(("localhost", 5432), timeout=3.0):
            pg_disponivel = True
    except (OSError, ConnectionRefusedError):
        pass

    if not pg_disponivel:
        print("  AVISO: PostgreSQL nao acessivel na porta 5432. Dicionario e quality log desativados.")
        pg_engine = None
    else:
        try:
            pg_engine = create_engine(DW_URL, echo=False, connect_args={"connect_timeout": 5})
            with pg_engine.connect():
                pass  # teste de conectividade
            print("  PostgreSQL: ligacao estabelecida.")
        except Exception as e:
            print(f"  AVISO: PostgreSQL indisponivel ({e}). Dicionario e quality log desativados.")
            pg_engine = None

    t0 = time.time()
    if not nlp_habilitado:
        _log("NLP desabilitado (--no-nlp). Score sentimento = 0.0 para todos os ficheiros de forum.", "WARN")

    _log("Iniciando silver_inventario...")
    silver_inventario(ficheiros_inventario, pg_engine)
    _log(f"silver_inventario concluido em {time.time()-t0:.1f}s")

    t1 = time.time()
    _log("Iniciando silver_trends...")
    silver_trends(ficheiros_trends, pg_engine)
    _log(f"silver_trends concluido em {time.time()-t1:.1f}s")

    t2 = time.time()
    _log(f"Iniciando silver_forum (NLP={'activo' if nlp_habilitado else 'desabilitado'})...")
    silver_forum(ficheiros_forum, pg_engine, nlp_habilitado=nlp_habilitado)
    _log(f"silver_forum concluido em {time.time()-t2:.1f}s")

    t3 = time.time()
    _log("Iniciando silver_hashtags...")
    silver_hashtags(ficheiros_hashtags, pg_engine)
    _log(f"silver_hashtags concluido em {time.time()-t3:.1f}s")
    _log(f"Silver total: {time.time()-t0:.1f}s")

    if pg_engine:
        pg_engine.dispose()

    print("\n  Silver concluído.")
    print("=" * 60)


if __name__ == "__main__":
    run_silver()