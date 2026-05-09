"""
load_to_postgres.py — Silver -> PostgreSQL (Star Schema)
=========================================================
Carrega as 4 tabelas Silver (Delta Lake) para o Star Schema PostgreSQL.

Schema Silver real (colunas disponiveis):
  inventario_delta : id_viatura, matricula, marca, modelo, tipo_automovel,
                     num_lugares, ano_viatura, combustivel, quilometragem,
                     preco_aquisicao, data_entrada_stock, preco_venda,
                     data_venda, stand, ingestion_timestamp, source_file,
                     source_stand, marca_normalizada, modelo_normalizado
  trends_delta     : termo, marca, modelo, regiao, mes, valor_interesse,
                     ingestion_timestamp, source_file, marca_normalizada,
                     modelo_normalizado
  forum_delta      : source_file, data_extracao, ingestion_timestamp,
                     texto_limpo, mencoes_marca, mencoes_modelo,
                     score_sentimento, n_mencoes_total, n_chars_texto_limpo
  hashtags_delta   : hashtag, data, total_posts, source_file,
                     ingestion_timestamp, posts_instagram, posts_twitter,
                     posts_youtube, modelo_normalizado, variacao_semanal

Projeto Auto Escala — CDGE 2025/2026
"""
import pandas as pd
import numpy as np
import socket
from sqlalchemy import create_engine, text
from deltalake import DeltaTable
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SILVER_INVENTARIO = str(BASE_DIR / "data_lake/silver/inventario_delta")
SILVER_TRENDS     = str(BASE_DIR / "data_lake/silver/trends_delta")
SILVER_FORUM      = str(BASE_DIR / "data_lake/silver/forum_delta")
SILVER_HASHTAGS   = str(BASE_DIR / "data_lake/silver/hashtags_delta")

_PG_HOST  = __import__("os").environ.get("PG_HOST", "localhost")
_PG_PORT  = __import__("os").environ.get("PG_PORT", "5432")
DW_URL    = f"postgresql+psycopg2://ae_user:ae_pass_2026@{_PG_HOST}:{_PG_PORT}/auto_escala"
DW_SCHEMA = "auto_escala_dw"


def _safe_col(df: pd.DataFrame, col: str, default=None) -> pd.Series:
    """Devolve a coluna se existir, caso contrario uma Serie com o valor default."""
    return df[col] if col in df.columns else pd.Series([default] * len(df), index=df.index)


def run_load_to_postgres():
    print("\n" + "=" * 60)
    print("  LOAD TO POSTGRESQL (STAR SCHEMA)")
    print("=" * 60)

    # Pre-check TCP rapido — falha imediatamente se o PostgreSQL nao estiver acessivel
    try:
        with socket.create_connection(("localhost", 5432), timeout=3.0):
            pass
    except (OSError, ConnectionRefusedError):
        print("  AVISO: PostgreSQL nao acessivel na porta 5432. Load ignorado.")
        return

    engine = create_engine(DW_URL, connect_args={"connect_timeout": 5})

    # ==========================================================================
    # 1. Carregar tabelas Silver para DataFrames
    # ==========================================================================
    print("  A ler camadas Silver...")

    def _ler_silver(path):
        try:
            return DeltaTable(path).to_pandas()
        except Exception:
            return pd.DataFrame()

    df_inv    = _ler_silver(SILVER_INVENTARIO)
    df_trends = _ler_silver(SILVER_TRENDS)
    df_forum  = _ler_silver(SILVER_FORUM)
    df_hash   = _ler_silver(SILVER_HASHTAGS)

    with engine.begin() as conn:

        # ======================================================================
        # DIM_TEMPO
        # ======================================================================
        print("  A processar dim_tempo...")
        series_datas = []

        if not df_inv.empty:
            for col in ["data_entrada_stock", "data_venda"]:
                if col in df_inv.columns:
                    s = pd.to_datetime(df_inv[col], errors="coerce").dt.normalize()
                    series_datas.append(s)
                    s_eom = s + pd.offsets.MonthEnd(0)
                    series_datas.append(s_eom)

        if not df_trends.empty and "mes" in df_trends.columns:
            # mes esta no formato YYYY-MM (ex: "2024-01")
            series_datas.append(pd.to_datetime(df_trends["mes"].astype(str) + "-01", errors="coerce"))

        if not df_forum.empty and "data_extracao" in df_forum.columns:
            series_datas.append(pd.to_datetime(df_forum["data_extracao"], errors="coerce").dt.normalize())

        if not df_hash.empty and "data" in df_hash.columns:
            series_datas.append(pd.to_datetime(df_hash["data"], errors="coerce").dt.normalize())

        if series_datas:
            datas_todas = pd.concat(series_datas)
            datas_todas = pd.to_datetime(datas_todas, errors="coerce").dropna().drop_duplicates()

            if not datas_todas.empty:
                dim_tempo = pd.DataFrame({"data": datas_todas.dt.date})
                dim_tempo["ano"]       = pd.to_datetime(dim_tempo["data"]).dt.year
                dim_tempo["mes"]       = pd.to_datetime(dim_tempo["data"]).dt.month
                dim_tempo["dia"]       = pd.to_datetime(dim_tempo["data"]).dt.day
                dim_tempo["trimestre"] = pd.to_datetime(dim_tempo["data"]).dt.quarter
                dim_tempo["nome_mes"]  = pd.to_datetime(dim_tempo["data"]).dt.month_name()
                dim_tempo["semana_ano"]= pd.to_datetime(dim_tempo["data"]).dt.isocalendar().week.astype(int)

                conn.execute(
                    text(f"""
                        INSERT INTO {DW_SCHEMA}.dim_tempo
                            (data, ano, mes, dia, trimestre, nome_mes, semana_ano)
                        VALUES (:data, :ano, :mes, :dia, :trimestre, :nome_mes, :semana_ano)
                        ON CONFLICT (data) DO NOTHING
                    """),
                    dim_tempo.to_dict(orient="records"),
                )

        # ======================================================================
        # DIM_STAND
        # Silver inventario tem coluna 'stand' (nome) e 'source_stand'.
        # Nao tem cidade/distrito/pais — inserir com valores NULL.
        # ======================================================================
        print("  A processar dim_stand...")
        if not df_inv.empty and "stand" in df_inv.columns:
            dim_stand = (
                df_inv[["stand"]]
                .drop_duplicates(subset=["stand"])
                .dropna(subset=["stand"])
                .copy()
            )
            dim_stand["cidade"]   = None
            dim_stand["distrito"] = None
            dim_stand["pais"]     = "Portugal"

            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.dim_stand (nome_stand, cidade, distrito, pais)
                    VALUES (:stand, :cidade, :distrito, :pais)
                    ON CONFLICT (nome_stand) DO NOTHING
                """),
                dim_stand.to_dict(orient="records"),
            )

        # ======================================================================
        # DIM_MODELO
        # Usa marca_normalizada/modelo_normalizado quando disponiveis.
        # ======================================================================
        print("  A processar dim_modelo...")
        modelos = []

        if not df_inv.empty:
            tmp = df_inv.copy()
            # Preferir versoes normalizadas
            if "marca_normalizada" in tmp.columns:
                tmp["marca"] = tmp["marca_normalizada"].fillna(tmp["marca"])
            if "modelo_normalizado" in tmp.columns:
                tmp["modelo"] = tmp["modelo_normalizado"].fillna(tmp["modelo"])
            tmp["tipo_automovel"] = _safe_col(tmp, "tipo_automovel", "N/A")
            tmp["combustivel"]    = _safe_col(tmp, "combustivel",    "N/A")
            modelos.append(tmp[["marca", "modelo", "tipo_automovel", "combustivel"]])

        if not df_trends.empty:
            tmp = df_trends.copy()
            if "marca_normalizada" in tmp.columns:
                tmp["marca"] = tmp["marca_normalizada"].fillna(tmp.get("marca", pd.NA))
            if "modelo_normalizado" in tmp.columns:
                tmp["modelo"] = tmp["modelo_normalizado"].fillna(tmp.get("modelo", pd.NA))
            tmp["tipo_automovel"] = "N/A"
            tmp["combustivel"]    = "N/A"
            modelos.append(tmp[["marca", "modelo", "tipo_automovel", "combustivel"]])

        if not df_forum.empty:
            # Forum tem mencoes_marca e mencoes_modelo (pipe-separated)
            # Expandir: cada mencao e um par marca|modelo potencial
            linhas = []
            for _, row in df_forum.iterrows():
                marcas  = [m.strip() for m in str(row.get("mencoes_marca", "") or "").split("|") if m.strip()]
                modelos_f = [m.strip() for m in str(row.get("mencoes_modelo", "") or "").split("|") if m.strip()]
                for m in marcas:
                    linhas.append({"marca": m, "modelo": None, "tipo_automovel": "N/A", "combustivel": "N/A"})
                for m in modelos_f:
                    linhas.append({"marca": None, "modelo": m, "tipo_automovel": "N/A", "combustivel": "N/A"})
            if linhas:
                modelos.append(pd.DataFrame(linhas))

        if modelos:
            dim_modelo = (
                pd.concat(modelos)
                .drop_duplicates(subset=["marca", "modelo"])
                .dropna(subset=["marca", "modelo"])
                .copy()
            )
            dim_modelo["tipo_automovel"] = dim_modelo["tipo_automovel"].fillna("N/A")
            dim_modelo["combustivel"]    = dim_modelo["combustivel"].fillna("N/A")

            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.dim_modelo (marca, modelo, tipo_automovel, combustivel)
                    VALUES (:marca, :modelo, :tipo_automovel, :combustivel)
                    ON CONFLICT (marca, modelo) DO UPDATE SET
                        tipo_automovel = CASE
                            WHEN EXCLUDED.tipo_automovel != 'N/A'
                            THEN EXCLUDED.tipo_automovel
                            ELSE dim_modelo.tipo_automovel END,
                        combustivel = CASE
                            WHEN EXCLUDED.combustivel != 'N/A'
                            THEN EXCLUDED.combustivel
                            ELSE dim_modelo.combustivel END
                """),
                dim_modelo.to_dict(orient="records"),
            )

        # ======================================================================
        # DIM_VEICULO
        # ======================================================================
        print("  A processar dim_veiculo...")
        if not df_inv.empty and "id_viatura" in df_inv.columns:
            map_modelos = pd.read_sql(
                f"SELECT modelo_key, marca, modelo FROM {DW_SCHEMA}.dim_modelo", conn
            )
            tmp = df_inv.copy()
            if "marca_normalizada" in tmp.columns:
                tmp["marca"] = tmp["marca_normalizada"].fillna(tmp["marca"])
            if "modelo_normalizado" in tmp.columns:
                tmp["modelo"] = tmp["modelo_normalizado"].fillna(tmp["modelo"])

            dim_veic = tmp.merge(map_modelos, on=["marca", "modelo"], how="left")
            dim_veic = (
                dim_veic[["id_viatura", "matricula", "modelo_key", "num_lugares", "ano_viatura"]]
                .drop_duplicates(subset=["id_viatura"])
                .dropna(subset=["id_viatura"])
            )

            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.dim_veiculo
                        (id_viatura, matricula, modelo_key, num_lugares, ano_viatura)
                    VALUES (:id_viatura, :matricula, :modelo_key, :num_lugares, :ano_viatura)
                    ON CONFLICT (id_viatura) DO UPDATE SET
                        matricula  = EXCLUDED.matricula,
                        modelo_key = EXCLUDED.modelo_key,
                        num_lugares = EXCLUDED.num_lugares,
                        ano_viatura = EXCLUDED.ano_viatura
                """),
                dim_veic.replace({np.nan: None}).to_dict(orient="records"),
            )

        # ======================================================================
        # DIM_HASHTAG
        # Silver hashtags nao tem coluna 'categoria' — inserir com NULL.
        # ======================================================================
        print("  A processar dim_hashtag...")
        if not df_hash.empty and "hashtag" in df_hash.columns:
            dim_hashtag = (
                df_hash[["hashtag"]]
                .drop_duplicates(subset=["hashtag"])
                .dropna(subset=["hashtag"])
                .copy()
            )
            dim_hashtag["categoria"] = None

            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.dim_hashtag (hashtag, categoria)
                    VALUES (:hashtag, :categoria)
                    ON CONFLICT (hashtag) DO NOTHING
                """),
                dim_hashtag.to_dict(orient="records"),
            )

        # ======================================================================
        # MAPAS DE SURROGATE KEYS
        # ======================================================================
        map_tempo   = pd.read_sql(f"SELECT tempo_key, data FROM {DW_SCHEMA}.dim_tempo",   conn)
        map_tempo["data"] = pd.to_datetime(map_tempo["data"])
        map_stand   = pd.read_sql(f"SELECT stand_key, nome_stand AS stand FROM {DW_SCHEMA}.dim_stand", conn)
        map_fonte   = pd.read_sql(f"SELECT fonte_key, nome_fonte FROM {DW_SCHEMA}.dim_fonte", conn)
        map_veiculo = pd.read_sql(f"SELECT veiculo_key, id_viatura FROM {DW_SCHEMA}.dim_veiculo", conn)
        map_modelo  = pd.read_sql(f"SELECT modelo_key, marca, modelo FROM {DW_SCHEMA}.dim_modelo", conn)
        map_hashtag = pd.read_sql(f"SELECT hashtag_key, hashtag FROM {DW_SCHEMA}.dim_hashtag", conn)

        def get_fonte_key(nome):
            res = map_fonte[map_fonte["nome_fonte"] == nome]
            return int(res.iloc[0]["fonte_key"]) if not res.empty else None

        # ======================================================================
        # FCT_VENDA
        # Silver inventario nao tem: margem, dias_em_stock, vendido
        # Esses campos sao calculados aqui ou enviados como NULL.
        # ======================================================================
        print("  A processar fct_venda...")
        if not df_inv.empty:
            fct = df_inv.copy()
            if "marca_normalizada" in fct.columns:
                fct["marca"] = fct["marca_normalizada"].fillna(fct["marca"])
            if "modelo_normalizado" in fct.columns:
                fct["modelo"] = fct["modelo_normalizado"].fillna(fct["modelo"])

            # Normalizar para datetime64[ns] sem timezone (igual ao que vem do PostgreSQL via map_tempo)
            fct["data_entrada_stock"] = (
                pd.to_datetime(fct["data_entrada_stock"], errors="coerce", utc=True)
                .dt.tz_convert(None).dt.normalize()
            )
            fct["data_venda"] = (
                pd.to_datetime(fct["data_venda"], errors="coerce", utc=True)
                .dt.tz_convert(None).dt.normalize()
            )
            # Garantir que map_tempo tambem e naive datetime64[ns]
            map_tempo["data"] = pd.to_datetime(map_tempo["data"]).dt.tz_localize(None)

            fct = fct.merge(map_veiculo, on="id_viatura", how="left")
            fct = fct.merge(map_stand,   on="stand",      how="left")
            fct = fct.merge(
                map_tempo.rename(columns={"data": "data_entrada_stock", "tempo_key": "tempo_entrada_key"}),
                on="data_entrada_stock", how="left",
            )
            fct = fct.merge(
                map_tempo.rename(columns={"data": "data_venda", "tempo_key": "tempo_venda_key"}),
                on="data_venda", how="left",
            )

            # Calcular campos derivados que o Silver nao tem
            fct["margem"] = None
            if "preco_venda" in fct.columns and "preco_aquisicao" in fct.columns:
                fct["margem"] = (
                    pd.to_numeric(fct["preco_venda"],    errors="coerce") -
                    pd.to_numeric(fct["preco_aquisicao"], errors="coerce")
                )

            fct["dias_em_stock"] = None
            if "data_venda" in fct.columns and "data_entrada_stock" in fct.columns:
                delta = fct["data_venda"] - fct["data_entrada_stock"]
                fct["dias_em_stock"] = delta.dt.days.where(delta.notna(), other=None)

            fct["vendido"] = fct["data_venda"].notna()

            cols = ["veiculo_key", "stand_key", "tempo_entrada_key", "tempo_venda_key",
                    "quilometragem", "preco_aquisicao", "preco_venda",
                    "margem", "dias_em_stock", "vendido"]
            fct = fct[cols].dropna(subset=["veiculo_key", "stand_key", "tempo_entrada_key"])
            fct = fct.replace({np.nan: None})

            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.fct_venda
                        (veiculo_key, stand_key, tempo_entrada_key, tempo_venda_key,
                         quilometragem, preco_aquisicao, preco_venda,
                         margem, dias_em_stock, vendido)
                    VALUES
                        (:veiculo_key, :stand_key, :tempo_entrada_key, :tempo_venda_key,
                         :quilometragem, :preco_aquisicao, :preco_venda,
                         :margem, :dias_em_stock, :vendido)
                    ON CONFLICT (veiculo_key, stand_key, tempo_entrada_key) DO UPDATE SET
                        tempo_venda_key = EXCLUDED.tempo_venda_key,
                        quilometragem   = EXCLUDED.quilometragem,
                        preco_venda     = EXCLUDED.preco_venda,
                        margem          = EXCLUDED.margem,
                        dias_em_stock   = EXCLUDED.dias_em_stock,
                        vendido         = EXCLUDED.vendido
                """),
                fct.to_dict(orient="records"),
            )

        # ======================================================================
        # FCT_TENDENCIA (Trends + Forum)
        # ======================================================================
        print("  A processar fct_tendencia...")
        fct_tendencias_list = []

        fonte_key_trends = get_fonte_key("Google Trends")
        if not df_trends.empty and fonte_key_trends:
            ft = df_trends.copy()
            if "marca_normalizada" in ft.columns:
                ft["marca"] = ft["marca_normalizada"].fillna(ft.get("marca", pd.NA))
            if "modelo_normalizado" in ft.columns:
                ft["modelo"] = ft["modelo_normalizado"].fillna(ft.get("modelo", pd.NA))
            # mes no formato YYYY-MM -> data de primeiro dia do mes
            ft["data"] = pd.to_datetime(ft["mes"].astype(str) + "-01", errors="coerce")
            ft = ft.merge(map_tempo, on="data", how="left")
            ft = ft.merge(map_modelo, on=["marca", "modelo"], how="left")
            ft["fonte_key"]       = fonte_key_trends
            ft["score_sentimento"] = None
            ft["delta_sentimento"] = None
            ft["crescimento_mom_pct"] = _safe_col(ft, "crescimento_mom_pct", None)
            fct_tendencias_list.append(
                ft[["tempo_key", "fonte_key", "modelo_key", "valor_interesse",
                    "crescimento_mom_pct", "score_sentimento", "delta_sentimento"]]
            )

        fonte_key_forum = get_fonte_key("Fórum motorguia.net")
        if not df_forum.empty and fonte_key_forum:
            # Forum: 1 linha por ficheiro — expandir mencoes_marca x mencoes_modelo
            linhas = []
            for _, row in df_forum.iterrows():
                data_str = str(row.get("data_extracao", "") or "")
                data_dt  = pd.to_datetime(data_str, errors="coerce")
                if pd.isna(data_dt):
                    continue

                # Mapear tempo_key
                data_norm = data_dt.normalize()
                match = map_tempo[map_tempo["data"] == data_norm]
                tempo_key = int(match.iloc[0]["tempo_key"]) if not match.empty else None
                if tempo_key is None:
                    continue

                marcas   = [m.strip() for m in str(row.get("mencoes_marca", "") or "").split("|") if m.strip()]
                modelos_f= [m.strip() for m in str(row.get("mencoes_modelo", "") or "").split("|") if m.strip()]

                for marca in marcas:
                    match_mod = map_modelo[map_modelo["marca"] == marca]
                    modelo_key = int(match_mod.iloc[0]["modelo_key"]) if not match_mod.empty else None
                    linhas.append({
                        "tempo_key":         tempo_key,
                        "fonte_key":         fonte_key_forum,
                        "modelo_key":        modelo_key,
                        "valor_interesse":   None,
                        "crescimento_mom_pct": None,
                        "score_sentimento":  float(row.get("score_sentimento", 0.0) or 0.0),
                        "delta_sentimento":  None,
                    })

                if not marcas and not modelos_f:
                    linhas.append({
                        "tempo_key":         tempo_key,
                        "fonte_key":         fonte_key_forum,
                        "modelo_key":        None,
                        "valor_interesse":   None,
                        "crescimento_mom_pct": None,
                        "score_sentimento":  float(row.get("score_sentimento", 0.0) or 0.0),
                        "delta_sentimento":  None,
                    })

            if linhas:
                fct_tendencias_list.append(pd.DataFrame(linhas))

        if fct_tendencias_list:
            fct_tend = pd.concat(fct_tendencias_list).replace({np.nan: None})
            fct_tend = fct_tend.dropna(subset=["tempo_key", "fonte_key"])

            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.fct_tendencia
                        (tempo_key, fonte_key, modelo_key, valor_interesse,
                         crescimento_mom_pct, score_sentimento, delta_sentimento)
                    VALUES
                        (:tempo_key, :fonte_key, :modelo_key, :valor_interesse,
                         :crescimento_mom_pct, :score_sentimento, :delta_sentimento)
                    ON CONFLICT (tempo_key, fonte_key, modelo_key) DO UPDATE SET
                        valor_interesse     = EXCLUDED.valor_interesse,
                        crescimento_mom_pct = EXCLUDED.crescimento_mom_pct,
                        score_sentimento    = EXCLUDED.score_sentimento,
                        delta_sentimento    = EXCLUDED.delta_sentimento
                """),
                fct_tend.to_dict(orient="records"),
            )

        # ======================================================================
        # FCT_HASHTAG_VOLUME
        # ======================================================================
        print("  A processar fct_hashtag_volume...")
        fonte_key_hash = get_fonte_key("Hashtags Sociais")
        if not df_hash.empty and fonte_key_hash:
            fh = df_hash.copy()
            fh["data"] = pd.to_datetime(fh["data"], errors="coerce").dt.normalize()
            fh = fh.merge(map_tempo,   on="data",    how="left")
            fh = fh.merge(map_hashtag, on="hashtag", how="left")
            
            # Mapear modelo_normalizado para modelo_key
            if "modelo_normalizado" in fh.columns:
                fh = fh.merge(map_modelo.rename(columns={"modelo": "modelo_normalizado"}), 
                             on="modelo_normalizado", how="left")
            else:
                fh["modelo_key"] = None

            fh["fonte_key"] = fonte_key_hash
            fh = fh.replace({np.nan: None})

            cols = ["tempo_key", "fonte_key", "hashtag_key", "modelo_key", "total_posts",
                    "posts_instagram", "posts_twitter", "posts_youtube", "variacao_semanal"]
            fh = fh[cols].dropna(subset=["tempo_key", "fonte_key", "hashtag_key"])
            fh = fh.rename(columns={"total_posts": "volume"})

            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.fct_hashtag_volume
                        (tempo_key, fonte_key, hashtag_key, modelo_key, volume,
                         posts_instagram, posts_twitter, posts_youtube, variacao_semanal)
                    VALUES
                        (:tempo_key, :fonte_key, :hashtag_key, :modelo_key, :volume,
                         :posts_instagram, :posts_twitter, :posts_youtube, :variacao_semanal)
                    ON CONFLICT (tempo_key, fonte_key, hashtag_key, modelo_key) DO UPDATE SET
                        volume           = EXCLUDED.volume,
                        posts_instagram  = EXCLUDED.posts_instagram,
                        posts_twitter    = EXCLUDED.posts_twitter,
                        posts_youtube    = EXCLUDED.posts_youtube,
                        variacao_semanal = EXCLUDED.variacao_semanal
                """),
                fh.to_dict(orient="records"),
            )

        # ======================================================================
        # FCT_INVENTARIO_MENSAL (ELT)
        # ======================================================================
        print("  A processar fct_inventario_mensal (Snapshot Mensal via ELT)...")
        if not df_inv.empty:
            conn.execute(
                text(f"""
                    WITH meses_afetados AS (
                        -- Obtemos todas as datas de fim de mês relevantes para o run atual
                        SELECT DISTINCT date_trunc('month', data)::date + interval '1 month' - interval '1 day' AS data_fim_mes
                        FROM {DW_SCHEMA}.dim_tempo
                        WHERE data IN (
                            SELECT t.data FROM {DW_SCHEMA}.fct_venda v JOIN {DW_SCHEMA}.dim_tempo t ON v.tempo_entrada_key = t.tempo_key
                            UNION
                            SELECT t.data FROM {DW_SCHEMA}.fct_venda v JOIN {DW_SCHEMA}.dim_tempo t ON v.tempo_venda_key = t.tempo_key
                        )
                    ),
                    meses_keys AS (
                        SELECT t.tempo_key, m.data_fim_mes
                        FROM {DW_SCHEMA}.dim_tempo t
                        JOIN meses_afetados m ON t.data = m.data_fim_mes::date
                    )
                    INSERT INTO {DW_SCHEMA}.fct_inventario_mensal
                        (tempo_key, stand_key, veiculo_key, valor_em_stock, dias_em_parque)
                    SELECT
                        mk.tempo_key,
                        v.stand_key,
                        v.veiculo_key,
                        v.preco_aquisicao AS valor_em_stock,
                        (mk.data_fim_mes::date - t_entrada.data) AS dias_em_parque
                    FROM
                        {DW_SCHEMA}.fct_venda v
                    JOIN {DW_SCHEMA}.dim_tempo t_entrada ON v.tempo_entrada_key = t_entrada.tempo_key
                    LEFT JOIN {DW_SCHEMA}.dim_tempo t_venda ON v.tempo_venda_key = t_venda.tempo_key
                    CROSS JOIN meses_keys mk
                    WHERE
                        t_entrada.data <= mk.data_fim_mes
                        AND (t_venda.data IS NULL OR t_venda.data > mk.data_fim_mes)
                    ON CONFLICT (tempo_key, stand_key, veiculo_key) DO UPDATE SET
                        valor_em_stock = EXCLUDED.valor_em_stock,
                        dias_em_parque = EXCLUDED.dias_em_parque
                """)
            )

    engine.dispose()
    print("\n  Carga para o PostgreSQL concluida com sucesso.")
    print("=" * 60)


if __name__ == "__main__":
    run_load_to_postgres()
