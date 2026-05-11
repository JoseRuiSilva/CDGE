"""
silver_pipeline.py — Camada Silver | Projeto Auto Escala
=========================================================
Lê dados brutos do Bronze (Delta Lake), aplica limpeza, tipagem,
normalização via dicionário PostgreSQL e NLP de sentimento (fórum).
Registos inválidos -> Delta de quarentena por fonte.
Métricas de qualidade -> tabela data_quality_log no PostgreSQL.
Dados limpos -> Silver (Delta Lake, MERGE/UPSERT por Business Key).

Fontes: inventário (CSV), Google Trends (JSON), fórum (TXT), hashtags (XML).
Decisões de desenho: ver contexto_auto_escala_llm.txt e diálogos de decisão.
"""

import os
import re
import sys
import time
import socket
import pyarrow.compute as pc

# Fix para conflito de OpenMP (libiomp5md.dll) que faz o pysentimiento crashar/devolver 0.0
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"

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
BRONZE_CLIENTES   = str(BASE_DIR / "data_lake/bronze/clientes_delta")
BRONZE_DEMOGRAFIA = str(BASE_DIR / "data_lake/bronze/demografia_delta")

# Caminhos Silver (escrita via MERGE)
SILVER_INVENTARIO = str(BASE_DIR / "data_lake/silver/inventario_delta")
SILVER_TRENDS     = str(BASE_DIR / "data_lake/silver/trends_delta")
SILVER_FORUM      = str(BASE_DIR / "data_lake/silver/forum_delta")
SILVER_HASHTAGS   = str(BASE_DIR / "data_lake/silver/hashtags_delta")
SILVER_CLIENTES   = str(BASE_DIR / "data_lake/silver/clientes_delta")
SILVER_DEMOGRAFIA = str(BASE_DIR / "data_lake/silver/demografia_delta")

# Caminhos Quarentena (append — registos rejeitados)
QUARENTENA_INVENTARIO = str(BASE_DIR / "data_lake/quarantine/inventario_delta")
QUARENTENA_TRENDS     = str(BASE_DIR / "data_lake/quarantine/trends_delta")
QUARENTENA_FORUM      = str(BASE_DIR / "data_lake/quarantine/forum_delta")
QUARENTENA_HASHTAGS   = str(BASE_DIR / "data_lake/quarantine/hashtags_delta")
QUARENTENA_CLIENTES   = str(BASE_DIR / "data_lake/quarantine/clientes_delta")
QUARENTENA_DEMOGRAFIA = str(BASE_DIR / "data_lake/quarantine/demografia_delta")

# PostgreSQL -- dicionario de normalizacao e data_quality_log
_PG_HOST  = __import__("os").environ.get("PG_HOST", "localhost")
_PG_PORT  = __import__("os").environ.get("PG_PORT", "5432")
DW_URL    = f"postgresql+psycopg2://ae_user:ae_pass_2026@{_PG_HOST}:{_PG_PORT}/auto_escala"
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

_REGEX_PAGINACAO = re.compile(r"Pagina \d+ de \d+", re.IGNORECASE)

# Linha de cabeçalho de post: "username  |  YYYY-MM"
_REGEX_CABECALHO_POST = re.compile(r"^[\w_\-]+\s{2,}\|\s{2,}\d{4}-\d{2}$")

# Frases de ruído literais do generate_forum.py (header, footer, metadata de utilizador).
# Usadas quando o texto chega como bloco contínuo (sem '\n') em vez de linhas separadas.
_RUIDO_LITERAIS_FORUM = [
    "motorguia.net Forum Automovel Portugues Registo Login Pesquisar",
    "Bem-vindo convidado Entrar Registar Topicos Recentes Atividade",
    "Novos Posts Ajuda Calendario Comunidade Forum Regras Utilizadores",
    "Contactos Arquivo Politica de Privacidade Termos de Utilizacao",
    "motorguia.net 2005-2026 Todos os direitos reservados",
    "Ver perfil Responder Citar",
    "Ir para o topo",
    "Topicos Recentes",
    "Proxima Pagina Anterior"
]
# Metadata de utilizador: "Membro desde Mar 2017 892 posts" / "Senior Member 3401 posts"
_REGEX_METADATA_UTILIZADOR = re.compile(
    r"("
    r"([\w_]+\s+)?Membro desde [a-zA-Z]+ \d{4} \d+ posts|"
    r"Senior Member \d+ posts|"
    r"Utilizador registado desde \d{4}"
    r")",
    re.IGNORECASE
)

_REGEX_URLS = re.compile(r"https?://[^\s]+", re.IGNORECASE)


# ─── UTILITÁRIOS GERAIS ───────────────────────────────────────────────────────

def _normalizar_para_lookup(valor: str) -> str:
    """
    Pré-processa um valor antes de o comparar com o dicionário:
    lowercase + trim + colapsa espaços múltiplos.

    Não remove '.' nem '%' para preservar "ID.4" e "100% Elétrico".
    Remove apenas caracteres que nunca aparecem em chaves do dicionário
    (vírgulas, ponto-e-vírgulas, parênteses, aspas).
    """
    if pd.isna(valor) or not isinstance(valor, str):
        return ""
    texto = valor.lower().strip()
    texto = re.sub(r"\s+", " ", texto)
    texto = re.sub(r"[,;()'\"\\]", "", texto)
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


def _criar_mapa_dicionario(df_dic: pd.DataFrame) -> dict:
    """Transforma o DataFrame do dicionário num mapa {campo: {original: normalizado}} para O(1) lookup."""
    mapa = {}
    if df_dic.empty:
        return mapa
    for campo in df_dic["campo"].unique():
        subset = df_dic[df_dic["campo"] == campo]
        mapa[campo] = dict(zip(subset["valor_original_norm"], subset["valor_normalizado"]))
    return mapa


def _lookup(valor: str, campo: str, dicionario_map: dict) -> str | None:
    """Devolve o valor normalizado usando o mapa (O(1))."""
    if not dicionario_map or pd.isna(valor):
        return None
    chave = _normalizar_para_lookup(str(valor))
    return dicionario_map.get(campo, {}).get(chave)


def _lookup_trends(termo: str, dicionario_map: dict) -> tuple:
    """
    Lookup por substring para Google Trends.
    Tenta primeiro marca_modelo (ex: 'vw golf' -> 'Volkswagen|Golf'),
    depois marca e modelo individualmente.
    Devolve (marca_normalizada, modelo_normalizado, combustivel_normalizado, tipo_normalizado).
    """
    if not dicionario_map or pd.isna(termo) or not termo:
        return None, None, None, None
    chave = _normalizar_para_lookup(str(termo))
    marca_encontrada, modelo_encontrado, combustivel_encontrado, tipo_encontrado = None, None, None, None

    # 1. Tentar marca_modelo por substring — cobre "VW Golf usado", etc.
    for original, normalizado in dicionario_map.get("marca_modelo", {}).items():
        if re.search(r"\b" + re.escape(original) + r"\b", chave):
            partes = normalizado.split("|", 1)
            if len(partes) == 2:
                marca_encontrada, modelo_encontrado = partes[0], partes[1]
            return marca_encontrada, modelo_encontrado, combustivel_encontrado, tipo_encontrado

    # 2. Fallback: marca, modelo, combustivel e tipo em separado
    for campo in ["marca", "modelo", "combustivel", "tipo_automovel"]:
        for original, normalizado in dicionario_map.get(campo, {}).items():
            if re.search(r"\b" + re.escape(original) + r"\b", chave):
                if campo == "marca":
                    marca_encontrada = normalizado
                elif campo == "modelo":
                    modelo_encontrado = normalizado
                elif campo == "combustivel":
                    combustivel_encontrado = normalizado
                elif campo == "tipo_automovel":
                    tipo_encontrado = normalizado
                break  # primeira correspondência por campo

    return marca_encontrada, modelo_encontrado, combustivel_encontrado, tipo_encontrado

def _extrair_mencoes(texto: str, dicionario_map: dict) -> tuple[list[str], list[str]]:
    """
    Extração de menções de marcas e modelos no texto do fórum.

    Estratégia:
    1. Tenta pares marca_modelo (ex: 'vw golf') — mais preciso, evita falsos positivos
       em que "clio" apanha "Renault" por engano.
    2. Tenta marca e modelo individualmente para o que não foi apanhado por par.
    """
    if not dicionario_map or not texto:
        return [], []

    texto_norm = _normalizar_para_lookup(texto)
    marcas, modelos = set(), set()

    # 1. Pares marca_modelo
    for original, normalizado in dicionario_map.get("marca_modelo", {}).items():
        if original in texto_norm:
            if re.search(r"\b" + re.escape(original) + r"\b", texto_norm):
                partes = normalizado.split("|", 1)
                if len(partes) == 2:
                    marcas.add(partes[0])
                    modelos.add(partes[1])

    # 2. Marcas e modelos individuais (para os que não vieram em par)
    for campo, items in dicionario_map.items():
        if campo not in ["marca", "modelo"]:
            continue
        for original, normalizado in items.items():
            if original in texto_norm:
                if re.search(r"\b" + re.escape(original) + r"\b", texto_norm):
                    if campo == "marca":
                        marcas.add(normalizado)
                    else:
                        modelos.add(normalizado)

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

def _isolar_texto_por_carro(texto_limpo: str, dicionario_map: dict) -> list[dict]:
    """
    Divide o texto em frases. Agrupa as frases por carro mencionado.
    Se uma frase não tiver menções, herda o contexto do último carro mencionado.
    """
    # Dividir por pontos finais (ignorando partes vazias ou muito curtas)
    frases = [f.strip() for f in texto_limpo.split(".") if len(f.strip()) > 5]
    
    # Usamos um dicionário com a chave (marca, modelo) para agrupar facilmente as frases
    blocos_temporarios = {}
    
    ultima_marca = "N/A"
    ultimo_modelo = "N/A"
    
    for frase in frases:
        marcas, modelos = _extrair_mencoes(frase, dicionario_map)
        
        if marcas or modelos:
            ultima_marca = marcas[0] if marcas else ultima_marca
            ultimo_modelo = modelos[0] if modelos else ultimo_modelo
            
        chave = (ultima_marca, ultimo_modelo)
        
        if chave not in blocos_temporarios:
            blocos_temporarios[chave] = {"frases": [], "contagem_mencoes": 0}            
        blocos_temporarios[chave]["frases"].append(frase)

        if ultimo_modelo in modelos:
            blocos_temporarios[chave]["contagem_mencoes"] += 1
        
    # Converter para o formato final de fácil leitura
    resultados = []
    for (marca, modelo), dados in blocos_temporarios.items():
        # Ignorar se marca/modelo não foram identificados ou se não houve menções reais
        if marca == "N/A" or modelo == "N/A" or dados["contagem_mencoes"] == 0:
            continue
            
        resultados.append({
            "marca": marca,
            "modelo": modelo,
            "frases": dados["frases"],
            "texto_completo": " . ".join(dados["frases"]) + ".",
            "n_mencoes_modelo": dados["contagem_mencoes"]
        })
        
    return resultados

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
    print(f"    Quarentena -> {len(registos)} registos  [{delta_path}]")


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
        print(f"    Quality log -> {ok} ok  |  {quarentena} quarentena  ({taxa}%)  [{fonte}]")
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
        print(f"    MERGE -> {len(df)} registos  [{delta_path}]")
    else:
        # Primeira vez — cria a tabela com overwrite
        Path(delta_path).mkdir(parents=True, exist_ok=True)
        write_deltalake(delta_path, tabela, mode="overwrite", schema_mode="merge")
        print(f"    CRIADA -> {len(df)} registos  [{delta_path}]")


# ─── SILVER: INVENTÁRIO ───────────────────────────────────────────────────────

def silver_inventario(source_files: list[str] | None = None, engine=None):
    """
    Transforma os dados de inventário do Bronze para o Silver.

    Transformações aplicadas:
      - Trim + normalização de nulos textuais -> NA real
      - Cast: datas -> datetime UTC, preços -> float, km -> int
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

    df_dic = _carregar_dicionario(engine) if engine else pd.DataFrame()
    dic_map = _criar_mapa_dicionario(df_dic)

    # 1. Normalizar nulos textuais e nomes de stands
    df = _normalizar_nulos(df_bronze.copy())
    if "stand" in df.columns:
        df["stand"] = df["stand"].str.strip().str.title()

    # 2. Cast de datas e valores numéricos
    for col_data in ["data_entrada_stock", "data_venda"]:
        if col_data in df.columns:
            df[col_data] = _cast_seguro(df[col_data], "datetime")

    for col_float in ["preco_aquisicao", "preco_venda"]:
        if col_float in df.columns:
            df[col_float] = _cast_seguro(df[col_float], "float")

    # 3. Cast quilometragem — guardar máscara antes para detetar "85000 km" -> NULL
    if "quilometragem" in df.columns:
        _km_tinha_valor = df["quilometragem"].notna()
        df["quilometragem"] = _cast_seguro(df["quilometragem"], "int")
        mask_km_invalido = _km_tinha_valor & df["quilometragem"].isna()
    else:
        mask_km_invalido = pd.Series(False, index=df.index)

    # 4. Normalizar marca, modelo, combustivel e tipo via dicionário
    #
    # Para marca e modelo, a estratégia tem dois níveis:
    #   a) Lookup direto no campo individual ('marca', 'modelo')
    #   b) Fallback para 'marca_modelo' (ex: "VW Golf" -> "Volkswagen|Golf")
    #      — cobre casos em que o CSV tem os dois campos juntos numa só célula
    #      — o resultado é split por '|' e distribuído pelas colunas certas

    def _resolver_marca_modelo(row) -> tuple[str | None, str | None]:
        """Devolve (marca_norm, modelo_norm) tentando lookup individual e depois par."""
        marca_raw  = row.get("marca",  "")
        modelo_raw = row.get("modelo", "")

        # Lookup individual
        m_marca  = _lookup(marca_raw,  "marca",  dic_map)
        m_modelo = _lookup(modelo_raw, "modelo", dic_map)

        # Se algum falhou, tentar o par "marca modelo" em marca_modelo
        if m_marca is None or m_modelo is None:
            par = f"{marca_raw} {modelo_raw}".strip()
            m_par = _lookup(par, "marca_modelo", dic_map)
            if m_par and "|" in m_par:
                partes = m_par.split("|", 1)
                m_marca  = m_marca  or partes[0]
                m_modelo = m_modelo or partes[1]

        return m_marca, m_modelo

    resultados_mm = df.apply(_resolver_marca_modelo, axis=1)
    df["marca_normalizada"]  = resultados_mm.apply(lambda t: t[0])
    df["modelo_normalizado"] = resultados_mm.apply(lambda t: t[1])

    # Normalização de Tipo e Combustível (fallback para Title case se não no dicionário)
    df["tipo_automovel"] = df["tipo_automovel"].apply(
        lambda x: _lookup(x, "tipo_automovel", dic_map) or (str(x).strip().title() if pd.notna(x) else "N/A")
    )
    df["combustivel"] = df["combustivel"].apply(
        lambda x: _lookup(x, "combustivel", dic_map) or (str(x).strip().title() if pd.notna(x) else "N/A")
    )

    # Garantir que siglas fiquem em Upper Case (SUV, GPL)
    df["tipo_automovel"] = df["tipo_automovel"].replace({"Suv": "SUV"})
    df["combustivel"]    = df["combustivel"].replace({"Gpl": "GPL"})

    # 4. Identificar registos para quarentena
    # Cada máscara identifica uma regra. A union decide quem sai.
    # Cada registo rejeitado vai para quarentena UMA VEZ, com a primeira regra violada.

    # BK nula
    mask_bk = df["matricula"].isna() if "matricula" in df.columns else pd.Series(False, index=df.index)
    mask_nl = df["num_lugares"].isna() if "num_lugares" in df.columns else pd.Series(False, index=df.index)

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
    mask_rejeitar = mask_bk | mask_marca | mask_modelo | mask_km | mask_datas | mask_nl

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
        # Deduplicar por matricula para evitar erro de MERGE do Delta Lake
        # Se houver varias versoes do mesmo carro no mesmo batch, ficamos com a ultima.
        df_ok = df_ok.drop_duplicates(subset=["matricula"], keep="last")
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
      - Cast: valor_interesse -> int (nulo -> 0); mes -> date
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

    df_dic = _carregar_dicionario(engine) if engine else pd.DataFrame()
    dicionario_map = _criar_mapa_dicionario(df_dic)

    df = _normalizar_nulos(df_bronze.copy())

    # Cast
    df["mes"] = pd.to_datetime(df["mes"], format="%Y-%m", errors="coerce").dt.date
    df["valor_interesse"] = pd.to_numeric(df["valor_interesse"], errors="coerce")
    df["valor_interesse"] = df["valor_interesse"].fillna(0).astype(float).round().clip(0, 100).astype("Int64")

    # Normalização do termo -> marca/modelo
    # Trends têm termos como "VW Golf usado" — usa lookup por substring, não exacto
    lookup_results = df["termo"].apply(lambda v: _lookup_trends(v, dicionario_map))
    df["marca_normalizada"]  = lookup_results.apply(lambda t: t[0])
    df["modelo_normalizado"] = lookup_results.apply(lambda t: t[1])
    df["combustivel_normalizado"] = lookup_results.apply(lambda t: t[2])
    df["tipo_normalizado"] = lookup_results.apply(lambda t: t[3])

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
        # Deduplicar por termo+mes+regiao para evitar erro de MERGE
        df_ok = df_ok.drop_duplicates(subset=["termo", "mes", "regiao"], keep="last")
        _merge_silver(df_ok, SILVER_TRENDS, bk_cols=["termo", "mes", "regiao"])

    if engine:
        _registar_qualidade(engine, "trends", total, n_ok, n_quarentena,
                            f"Duração: {round(time.time()-inicio, 1)}s")

    print(f"  Trends Silver concluído  [{n_ok} ok | {n_quarentena} quarentena]")


# ─── SILVER: FÓRUM ────────────────────────────────────────────────────────────

def _limpar_texto_forum(texto: str) -> str:
    """
    Remove ruído estrutural do texto bruto do fórum de forma robusta.
    """
    texto_limpo = str(texto)
    
    for frase in _RUIDO_LITERAIS_FORUM:
        texto_limpo = texto_limpo.replace(frase, " ")
        
    texto_limpo = _REGEX_URLS.sub(" ", texto_limpo)
    texto_limpo = _REGEX_PAGINACAO.sub(" ", texto_limpo)
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

    if source_files is not None:
        nomes = [Path(f).name for f in source_files]
        df_bronze = dt_bronze.to_pyarrow_dataset().to_table(
            filter=pc.field("source_file").isin(nomes)
        ).to_pandas()
    else:
        df_bronze = dt_bronze.to_pandas()

    if df_bronze.empty:
        print("  Nenhum registo novo no Bronze — a saltar.")
        return

    print(f"  Bronze lido: {len(df_bronze)} ficheiros")

    df_dic = _carregar_dicionario(engine) if engine else pd.DataFrame()
    dicionario_map = _criar_mapa_dicionario(df_dic)

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

        blocos_por_carro = _isolar_texto_por_carro(texto_limpo, dicionario_map)

        if not blocos_por_carro:
            quarentena_registos.append({
                "fonte": "forum", "source_file": source_file,
                "regra_violada": "NO_SIGNAL",
                "campo_problema": "mencoes+sentimento",
                "valor_encontrado": f"chars={len(texto_limpo)}",
                "registo_raw": {"source_file": source_file, "texto_len": len(texto_bruto)},
            })
            continue

        for bloco in blocos_por_carro:
            texto_para_nlp = bloco["texto_completo"]
            score_sentimento = _analisar_sentimento(texto_para_nlp, nlp_habilitado=nlp_habilitado)

            registos_ok.append({
                "source_file":         source_file,
                "data_extracao":       str(row.get("data_extracao", "")),
                "ingestion_timestamp": str(row.get("ingestion_timestamp", "")),
                "texto_limpo":         texto_para_nlp, 
                "mencoes_marca":       bloco["marca"], 
                "mencoes_modelo":      bloco["modelo"],
                "score_sentimento":    score_sentimento,
                "n_mencoes_modelo":    bloco["n_mencoes_modelo"],
            })


    total = len(df_bronze)
    n_quarentena = len(quarentena_registos)
    n_ok = len(registos_ok)

    _escrever_quarentena(quarentena_registos, QUARENTENA_FORUM)

    if registos_ok:
        df_ok = pd.DataFrame(registos_ok)
        for col in df_ok.select_dtypes(include="object").columns:
            df_ok[col] = df_ok[col].astype("string")
        _merge_silver(df_ok, SILVER_FORUM, bk_cols=["source_file", "mencoes_marca", "mencoes_modelo"])

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
      - Cast: total_posts e colunas de plataforma -> int
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

    df_dic = _carregar_dicionario(engine) if engine else pd.DataFrame()
    dicionario_map = _criar_mapa_dicionario(df_dic)

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

    # Analise de categorias
    analise_tags = df_ok["categoria"].apply(lambda v: _lookup_trends(v, dicionario_map))

    df_ok["marca_normalizada"]       = analise_tags.apply(lambda t: t[0])
    df_ok["modelo_normalizado"]      = analise_tags.apply(lambda t: t[1])
    df_ok["combustivel_normalizado"] = analise_tags.apply(lambda t: t[2])
    df_ok["tipo_normalizado"]        = analise_tags.apply(lambda t: t[3])

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
        # Deduplicar por hashtag+data para evitar erro de MERGE
        df_ok = df_ok.drop_duplicates(subset=["hashtag", "data"], keep="last")
        _merge_silver(df_ok, SILVER_HASHTAGS, bk_cols=["hashtag", "data"])

    if engine:
        _registar_qualidade(engine, "hashtags", total, n_ok, n_quarentena,
                            f"Duração: {round(time.time()-inicio, 1)}s")

    print(f"  Hashtags Silver concluído  [{n_ok} ok | {n_quarentena} quarentena]")


# ─── SILVER: CLIENTES ─────────────────────────────────────────────────────────

def silver_clientes(source_files: list[str] | None = None, engine=None):
    """
    Transforma os dados de clientes do Bronze para o Silver.
    BK: nif
    """
    print("\n[Silver] Clientes")
    inicio = time.time()
    try:
        dt_bronze = DeltaTable(BRONZE_CLIENTES)
    except Exception:
        print(f"  Bronze não encontrado em {BRONZE_CLIENTES} — a saltar.")
        return

    df_bronze = dt_bronze.to_pandas()
    if source_files is not None:
        nomes = {Path(f).name for f in source_files}
        df_bronze = df_bronze[df_bronze["source_file"].isin(nomes)]
    if df_bronze.empty:
        print("  Nenhum registo novo no Bronze — a saltar.")
        return

    df = _normalizar_nulos(df_bronze.copy())
    
    # Cast idade
    df["idade"] = _cast_seguro(df["idade"], "int")

    # Calcular faixa etaria
    def calcular_faixa(idade):
        if pd.isna(idade): return "Desconhecido"
        if idade < 25: return "18-24"
        if idade < 35: return "25-34"
        if idade < 50: return "35-49"
        if idade < 65: return "50-64"
        return "65+"
        
    df["faixa_etaria"] = df["idade"].apply(calcular_faixa)

    # Quarentena: BK nula
    quarentena_registos = []
    bk_nula = df["nif"].isna()

    for _, row in df[bk_nula].iterrows():
        quarentena_registos.append({
            "fonte": "clientes", "source_file": row.get("source_file", ""),
            "regra_violada": "NULL_BK", "campo_problema": "nif",
            "valor_encontrado": str(row.get("nif")), "registo_raw": row.to_dict(),
        })

    df_ok = df[~bk_nula].copy()
    for col in df_ok.select_dtypes(include="object").columns:
        df_ok[col] = df_ok[col].astype("string")

    total, n_quarentena, n_ok = len(df), len(quarentena_registos), len(df_ok)
    _escrever_quarentena(quarentena_registos, QUARENTENA_CLIENTES)

    if not df_ok.empty:
        df_ok = df_ok.drop_duplicates(subset=["nif"], keep="last")
        _merge_silver(df_ok, SILVER_CLIENTES, bk_cols=["nif"])

    if engine:
        _registar_qualidade(engine, "clientes", total, n_ok, n_quarentena, f"Duração: {round(time.time()-inicio, 1)}s")
    print(f"  Clientes Silver concluído  [{n_ok} ok | {n_quarentena} quarentena]")


# ─── SILVER: DEMOGRAFIA ───────────────────────────────────────────────────────

def silver_demografia(source_files: list[str] | None = None, engine=None):
    """
    Transforma os dados de demografia do Bronze para o Silver.
    BK: distrito + ano_referencia
    """
    print("\n[Silver] Demografia Regional")
    inicio = time.time()
    try:
        dt_bronze = DeltaTable(BRONZE_DEMOGRAFIA)
    except Exception:
        print(f"  Bronze não encontrado em {BRONZE_DEMOGRAFIA} — a saltar.")
        return

    df_bronze = dt_bronze.to_pandas()
    if source_files is not None:
        nomes = {Path(f).name for f in source_files}
        df_bronze = df_bronze[df_bronze["source_file"].isin(nomes)]
    if df_bronze.empty:
        print("  Nenhum registo novo no Bronze — a saltar.")
        return

    df = _normalizar_nulos(df_bronze.copy())
    
    # Cast
    df["ano_referencia"] = _cast_seguro(df["ano_referencia"], "int")
    df["populacao_total"] = _cast_seguro(df["populacao_total"], "int")
    for pct in ["pct_18_24", "pct_25_34", "pct_35_49", "pct_50_64", "pct_65_mais", "pct_masculino", "pct_feminino"]:
        if pct in df.columns:
            df[pct] = _cast_seguro(df[pct], "float")

    # Quarentena: BK nula
    quarentena_registos = []
    bk_nula = df["distrito"].isna() | df["ano_referencia"].isna()
    
    for _, row in df[bk_nula].iterrows():
        quarentena_registos.append({
            "fonte": "demografia", "source_file": row.get("source_file", ""),
            "regra_violada": "NULL_BK", "campo_problema": "distrito|ano_referencia",
            "valor_encontrado": f"{row.get('distrito')}|{row.get('ano_referencia')}", "registo_raw": row.to_dict(),
        })

    df_ok = df[~bk_nula].copy()
    for col in df_ok.select_dtypes(include="object").columns:
        df_ok[col] = df_ok[col].astype("string")

    total, n_quarentena, n_ok = len(df), len(quarentena_registos), len(df_ok)
    _escrever_quarentena(quarentena_registos, QUARENTENA_DEMOGRAFIA)
    
    if not df_ok.empty:
        df_ok = df_ok.drop_duplicates(subset=["distrito", "ano_referencia"], keep="last")
        _merge_silver(df_ok, SILVER_DEMOGRAFIA, bk_cols=["distrito", "ano_referencia"])

    if engine:
        _registar_qualidade(engine, "demografia", total, n_ok, n_quarentena, f"Duração: {round(time.time()-inicio, 1)}s")
    print(f"  Demografia Silver concluído  [{n_ok} ok | {n_quarentena} quarentena]")


# ─── PONTO DE ENTRADA ─────────────────────────────────────────────────────────

def run_silver(
    ficheiros_inventario: list[str] | None = None,
    ficheiros_trends:     list[str] | None = None,
    ficheiros_forum:      list[str] | None = None,
    ficheiros_hashtags:   list[str] | None = None,
    ficheiros_clientes:   list[str] | None = None,
    ficheiros_demografia: list[str] | None = None,
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
        with socket.create_connection((_PG_HOST, int(_PG_PORT)), timeout=3.0):
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
    
    t4 = time.time()
    _log("Iniciando silver_clientes...")
    silver_clientes(ficheiros_clientes, pg_engine)
    _log(f"silver_clientes concluido em {time.time()-t4:.1f}s")
    
    t5 = time.time()
    _log("Iniciando silver_demografia...")
    silver_demografia(ficheiros_demografia, pg_engine)
    _log(f"silver_demografia concluido em {time.time()-t5:.1f}s")
    
    _log(f"Silver total: {time.time()-t0:.1f}s")

    if pg_engine:
        pg_engine.dispose()

    print("\n  Silver concluído.")
    print("=" * 60)


if __name__ == "__main__":
    run_silver()