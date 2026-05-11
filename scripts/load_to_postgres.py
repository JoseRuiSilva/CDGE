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

  trends_delta     : termo, regiao, mes, valor_interesse,
                     ingestion_timestamp, source_file, marca_normalizada,
                     modelo_normalizado, combustivel_normalizado, tipo_normalizado

  forum_delta      : source_file, data_extracao, ingestion_timestamp,
                     texto_limpo, mencoes_marca, mencoes_modelo,
                     score_sentimento, n_mencoes_modelo

  hashtags_delta   : hashtag, data, categoria, total_posts, source_file,
                     ingestion_timestamp, posts_instagram, posts_twitter,
                     posts_youtube, marca_normalizada, modelo_normalizado,
                     combustivel_normalizado, tipo_normalizado, variacao_semanal

  clientes_delta   : nif, nome, idade, genero, distrito, ingestion_timestamp,
                     source_file, faixa_etaria

  demografia_delta : distrito, ano_referencia, populacao_total, pct_18_24,
                     pct_25_34, pct_35_44, pct_45_54, pct_55_64, pct_65_mais,
                     pct_masculino, pct_feminino, ingestion_timestamp, source_file

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
SILVER_CLIENTES   = str(BASE_DIR / "data_lake/silver/clientes_delta")
SILVER_DEMOGRAFIA = str(BASE_DIR / "data_lake/silver/demografia_delta")

_PG_HOST  = __import__("os").environ.get("PG_HOST", "localhost")
_PG_PORT  = __import__("os").environ.get("PG_PORT", "5432")
DW_URL    = f"postgresql+psycopg2://ae_user:ae_pass_2026@{_PG_HOST}:{_PG_PORT}/auto_escala"
DW_SCHEMA = "auto_escala_dw"


def _safe_col(df: pd.DataFrame, col: str, default=None) -> pd.Series:
    """Devolve a coluna se existir, caso contrario uma Serie com o valor default."""
    return df[col] if col in df.columns else pd.Series([default] * len(df), index=df.index)


def run_load_to_postgres(mode: str = "full_load"):
    print("\n" + "=" * 60)
    print(f"  LOAD TO POSTGRESQL (MODE: {mode.upper()})")
    print("=" * 60)

    # Pre-check TCP rapido — falha imediatamente se o PostgreSQL nao estiver acessivel
    try:
        with socket.create_connection((_PG_HOST, int(_PG_PORT)), timeout=3.0):
            pass
    except (OSError, ConnectionRefusedError):
        print(f"  AVISO: PostgreSQL nao acessivel em {_PG_HOST}:{_PG_PORT}. Load ignorado.")
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
    df_cli    = _ler_silver(SILVER_CLIENTES)
    df_demo   = _ler_silver(SILVER_DEMOGRAFIA)

    with engine.begin() as conn:

        # ======================================================================
        # DIM_TEMPO
        # ======================================================================
        print("  A inicializar dim_tempo (geração estática)...")
        
        ano_inicio = 2020
        ano_fim = 2030

        datas = pd.date_range(start=f'{ano_inicio}-01-01', end=f'{ano_fim}-12-31', freq='D')
        
        dim_tempo = pd.DataFrame({'data': datas.date})
        dim_tempo['ano'] = datas.year
        dim_tempo['mes'] = datas.month
        dim_tempo['dia'] = datas.day
        dim_tempo['trimestre'] = datas.quarter
        dim_tempo['nome_mes'] = datas.month_name() 
        dim_tempo['semana_ano'] = datas.isocalendar().week.values.astype(int)

        query = text(f"""
            INSERT INTO {DW_SCHEMA}.dim_tempo
                (data, ano, mes, dia, trimestre, nome_mes, semana_ano)
            VALUES (:data, :ano, :mes, :dia, :trimestre, :nome_mes, :semana_ano)
            ON CONFLICT (data) DO NOTHING
        """)
        conn.execute(query, dim_tempo.to_dict(orient="records"))
        print(f"  dim_tempo preenchida até {ano_fim}.")

        # ======================================================================
        # DIM_LOCALIZACAO
        # ======================================================================
        print("  A processar dim_localizacao...")
        distritos = set()
        if not df_inv.empty:
            distritos.update(df_inv["stand"].unique())
        if not df_cli.empty:
            distritos.update(df_cli["distrito"].unique())
        if not df_demo.empty:
            distritos.update(df_demo["distrito"].unique())
        if not df_trends.empty:
            distritos.update(df_trends["regiao"].unique())
        
        distritos = {d for d in distritos if pd.notna(d) and str(d).strip()}
        if distritos:
            df_loc = pd.DataFrame([{"distrito": d, "pais": "Portugal"} for d in distritos])
            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.dim_localizacao (distrito, pais)
                    VALUES (:distrito, :pais)
                    ON CONFLICT (distrito) DO NOTHING
                """),
                df_loc.to_dict(orient="records"),
            )
        
        map_loc = pd.read_sql(f"SELECT localizacao_key, distrito FROM {DW_SCHEMA}.dim_localizacao", conn)

        # ======================================================================
        # DIM_STAND
        # ======================================================================
        print("  A processar dim_stand...")
        if not df_inv.empty and "stand" in df_inv.columns:
            df_stand = (
                df_inv[["stand"]]
                .drop_duplicates(subset=["stand"])
                .rename(columns={"stand": "nome_stand"})
                .merge(map_loc.rename(columns={"distrito": "nome_stand"}), on="nome_stand", how="left")
            )
            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.dim_stand (nome_stand, localizacao_key)
                    VALUES (:nome_stand, :localizacao_key)
                    ON CONFLICT (nome_stand) DO NOTHING
                """),
                df_stand.replace({np.nan: None}).to_dict(orient="records"),
            )

        # ======================================================================
        # DIM_MODELO (Dynamic Discovery)
        # ======================================================================
        print("  A processar dim_modelo (Descoberta Dinâmica)...")
        modelos_list = []

        # 1. De Inventário (Geralmente modelos completos)
        if not df_inv.empty:
            tmp = df_inv.copy()
            tmp["marca"] = tmp["marca_normalizada"].fillna(tmp.get("marca", "N/A"))
            tmp["modelo"] = tmp["modelo_normalizado"].fillna(tmp.get("modelo", "N/A"))
            tmp["tipo_automovel"] = _safe_col(tmp, "tipo_automovel", "N/A").fillna("N/A")
            tmp["combustivel"]    = _safe_col(tmp, "combustivel",    "N/A").fillna("N/A")
            modelos_list.append(tmp[["marca", "modelo", "tipo_automovel", "combustivel"]])

        # 2. De Trends (Pode ser apenas Marca ou apenas Modelo)
        if not df_trends.empty:
            tmp = df_trends.copy()
            tmp["marca"] = tmp["marca_normalizada"].fillna("N/A")
            tmp["modelo"] = tmp["modelo_normalizado"].fillna("N/A")
            tmp["tipo_automovel"] = tmp["tipo_normalizado"].fillna("N/A")
            tmp["combustivel"]    = tmp["combustivel_normalizado"].fillna("N/A")
            modelos_list.append(tmp[["marca", "modelo", "tipo_automovel", "combustivel"]])

        # 3. De Forum (Menções soltas)
        if not df_forum.empty:
            df_temp = df_forum[["mencoes_marca", "mencoes_modelo"]].copy()
            df_temp = df_temp.rename(columns={
                "mencoes_marca": "marca", 
                "mencoes_modelo": "modelo"
            })
            df_temp["marca"] = df_temp["marca"].replace(["", "Desconhecida", "Desconhecido", "SemMarca", None], "N/A")
            df_temp["modelo"] = df_temp["modelo"].replace(["", "Desconhecido", "Desconhecida", "SemModelo", None], "N/A")
            df_temp["tipo_automovel"] = "N/A"
            df_temp["combustivel"] = "N/A"
            df_temp = df_temp.drop_duplicates()
            modelos_list.append(df_temp)

        # 4. De Hashtags (Melhorado via Silver analysis)
        if not df_hash.empty:
            tmp = df_hash.copy()
            tmp["marca"]          = _safe_col(tmp, "marca_normalizada", "N/A").fillna("N/A")
            tmp["modelo"]         = _safe_col(tmp, "modelo_normalizado", "N/A").fillna("N/A")
            tmp["tipo_automovel"] = _safe_col(tmp, "tipo_normalizado", "N/A").fillna("N/A")
            tmp["combustivel"]    = _safe_col(tmp, "combustivel_normalizado", "N/A").fillna("N/A")
            modelos_list.append(tmp[["marca", "modelo", "tipo_automovel", "combustivel"]])

        if modelos_list:
            dim_modelo = pd.concat(modelos_list).replace({pd.NA: "N/A", np.nan: "N/A"})
            
            modelos_conhecidos = dim_modelo[dim_modelo["marca"] != "N/A"].drop_duplicates(subset=["modelo"])
            mapa_marcas = modelos_conhecidos.set_index("modelo")["marca"].to_dict()
            
            mask_na = dim_modelo["marca"] == "N/A"
            dim_modelo.loc[mask_na, "marca"] = dim_modelo.loc[mask_na, "modelo"].map(mapa_marcas).fillna("N/A")
            
            dim_modelo = dim_modelo[["marca", "modelo", "tipo_automovel", "combustivel"]].drop_duplicates()

            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.dim_modelo (marca, modelo, tipo_automovel, combustivel)
                    VALUES (:marca, :modelo, :tipo_automovel, :combustivel)
                    ON CONFLICT (marca, modelo, tipo_automovel, combustivel) DO NOTHING
                """),
                dim_modelo.to_dict(orient="records"),
            )

        # ======================================================================
        # DIM_VEICULO
        # ======================================================================
        print("  A processar dim_veiculo...")
        if not df_inv.empty and "id_viatura" in df_inv.columns:
            map_modelos = pd.read_sql(
                f"SELECT modelo_key, marca, modelo, tipo_automovel, combustivel FROM {DW_SCHEMA}.dim_modelo", 
                conn
            )
            tmp = df_inv.copy()
            tmp["marca"] = tmp["marca_normalizada"].fillna(tmp["marca"]).fillna("N/A")
            tmp["modelo"] = tmp["modelo_normalizado"].fillna(tmp["modelo"]).fillna("N/A")
            tmp["tipo_automovel"] = _safe_col(tmp, "tipo_automovel", "N/A").fillna("N/A")
            tmp["combustivel"]    = _safe_col(tmp, "combustivel",    "N/A").fillna("N/A")

            dim_veic = tmp.merge(
                map_modelos, 
                on=["marca", "modelo", "tipo_automovel", "combustivel"], 
                how="left"
            )

            dim_veic = (
                dim_veic[[
                    "id_viatura", "matricula", "modelo_key", "num_lugares", "ano_viatura"
                ]]
                .drop_duplicates(subset=["id_viatura"])
                .dropna(subset=["id_viatura"])
            )

            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.dim_veiculo
                        (id_viatura, matricula, modelo_key, num_lugares, ano_viatura)
                    VALUES (:id_viatura, :matricula, :modelo_key, :num_lugares, :ano_viatura)
                    ON CONFLICT (id_viatura) DO NOTHING
                """),
                dim_veic.replace({np.nan: None}).to_dict(orient="records"),
            )

        # ======================================================================
        # DIM_CLIENTE
        # ======================================================================
        print("  A processar dim_cliente...")
        if not df_cli.empty:
            dim_cli = df_cli.merge(map_loc, on="distrito", how="left")
            if "ano_mes" in dim_cli.columns:
                dim_cli = dim_cli.sort_values("ano_mes", ascending=False)
            
            dim_cli = (
                        dim_cli
                        .drop_duplicates(subset=["nif"], keep="first")   
                        .copy()
                    )
            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.dim_cliente
                        (nif, nome, idade, faixa_etaria, genero, localizacao_key)
                    VALUES (:nif, :nome, :idade, :faixa_etaria, :genero, :localizacao_key)
                    ON CONFLICT (nif) DO UPDATE SET
                        nome = EXCLUDED.nome,
                        idade = EXCLUDED.idade,
                        faixa_etaria = EXCLUDED.faixa_etaria,
                        genero = EXCLUDED.genero,
                        localizacao_key = EXCLUDED.localizacao_key
                """),
                dim_cli.replace({np.nan: None}).to_dict(orient="records"),
            )

        # ======================================================================
        # DIM_DEMOGRAFIA_REGIONAL
        # ======================================================================
        print("  A processar dim_demografia_regional...")
        if not df_demo.empty:
            dim_demo_dw = df_demo.merge(map_loc, on="distrito", how="left")
            dim_demo_dw = dim_demo_dw.drop_duplicates(subset=["localizacao_key", "ano_referencia"]).copy()
            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.dim_demografia_regional
                        (localizacao_key, ano_referencia, populacao_total, pct_18_24, pct_25_34,
                         pct_35_49, pct_50_64, pct_65_mais, pct_masculino, pct_feminino)
                    VALUES (:localizacao_key, :ano_referencia, :populacao_total, :pct_18_24, :pct_25_34,
                            :pct_35_49, :pct_50_64, :pct_65_mais, :pct_masculino, :pct_feminino)
                    ON CONFLICT (localizacao_key, ano_referencia) DO UPDATE SET
                        populacao_total = EXCLUDED.populacao_total,
                        pct_18_24 = EXCLUDED.pct_18_24,
                        pct_25_34 = EXCLUDED.pct_25_34,
                        pct_35_49 = EXCLUDED.pct_35_49,
                        pct_50_64 = EXCLUDED.pct_50_64,
                        pct_65_mais = EXCLUDED.pct_65_mais,
                        pct_masculino = EXCLUDED.pct_masculino,
                        pct_feminino = EXCLUDED.pct_feminino
                """),
                dim_demo_dw.replace({np.nan: None}).to_dict(orient="records"),
            )

        # ======================================================================
        # MAPAS DE SURROGATE KEYS
        # ======================================================================
        map_tempo   = pd.read_sql(f"SELECT tempo_key, data FROM {DW_SCHEMA}.dim_tempo",   conn)
        map_tempo["data"] = pd.to_datetime(map_tempo["data"])
        map_stand   = pd.read_sql(f"SELECT stand_key, nome_stand AS stand FROM {DW_SCHEMA}.dim_stand", conn)
        map_fonte   = pd.read_sql(f"SELECT fonte_key, nome_fonte FROM {DW_SCHEMA}.dim_fonte", conn)
        map_veiculo = pd.read_sql(f"SELECT veiculo_key, id_viatura FROM {DW_SCHEMA}.dim_veiculo", conn)
        map_modelo  = pd.read_sql(f"SELECT modelo_key, marca, modelo, tipo_automovel, combustivel FROM {DW_SCHEMA}.dim_modelo", conn)
        map_cliente = pd.read_sql(f"SELECT cliente_key, nif FROM {DW_SCHEMA}.dim_cliente", conn)

        def get_fonte_key(nome):
            res = map_fonte[map_fonte["nome_fonte"] == nome]
            return int(res.iloc[0]["fonte_key"]) if not res.empty else None

        # ======================================================================
        # FCT_VENDA
        # Silver inventario nao tem: margem, dias_em_stock
        # Esses campos sao calculados aqui.
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
            
            if "nif_cliente" in fct.columns:
                fct = fct.merge(map_cliente.rename(columns={"nif": "nif_cliente"}), on="nif_cliente", how="left")
            else:
                fct["cliente_key"] = None

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

            cols = ["veiculo_key", "stand_key", "tempo_entrada_key", "tempo_venda_key", "cliente_key",
                    "quilometragem", "preco_aquisicao", "preco_venda",
                    "margem", "dias_em_stock"]
            fct = fct[cols].dropna(subset=["veiculo_key", "stand_key", "tempo_entrada_key"])
            fct = fct.replace({np.nan: None})

            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.fct_venda
                        (veiculo_key, stand_key, tempo_entrada_key, tempo_venda_key, cliente_key,
                         quilometragem, preco_aquisicao, preco_venda,
                         margem, dias_em_stock)
                    VALUES
                        (:veiculo_key, :stand_key, :tempo_entrada_key, :tempo_venda_key, :cliente_key,
                         :quilometragem, :preco_aquisicao, :preco_venda,
                         :margem, :dias_em_stock)
                    ON CONFLICT (veiculo_key, stand_key, tempo_entrada_key) DO UPDATE SET
                        tempo_venda_key = EXCLUDED.tempo_venda_key,
                        cliente_key     = EXCLUDED.cliente_key,
                        quilometragem   = EXCLUDED.quilometragem,
                        preco_venda     = EXCLUDED.preco_venda,
                        margem          = EXCLUDED.margem,
                        dias_em_stock   = EXCLUDED.dias_em_stock
                """),
                fct.to_dict(orient="records"),
            )

        # ======================================================================
        # FACT_TRENDS
        # ======================================================================
        print("  A processar fact_trends...")
        if not df_trends.empty:
            ft = df_trends.copy()
            ft["data"] = pd.to_datetime(ft["mes"], errors="coerce").dt.normalize()
            ft = ft.merge(map_tempo, on="data", how="left")
            
            # 1. Match completo (marca, modelo, tipo, combustivel)
            ft["marca"]  = ft["marca_normalizada"].fillna("N/A")
            ft["modelo"] = ft["modelo_normalizado"].fillna("N/A")
            ft["tipo_automovel"] = ft["tipo_normalizado"].fillna("N/A")
            ft["combustivel"]    = ft["combustivel_normalizado"].fillna("N/A")
            
            ft = ft.merge(map_modelo, on=["marca", "modelo", "tipo_automovel", "combustivel"], how="left")
            
            # 2. Fallback (marca, modelo) para as linhas que não tiveram match total
            mask_fallback = ft["modelo_key"].isna()
            if mask_fallback.any():
                map_fallback = map_modelo.drop_duplicates(subset=["marca", "modelo"])[["marca", "modelo", "modelo_key"]]
                ft_fallback = ft[mask_fallback].drop(columns="modelo_key").merge(
                    map_fallback, on=["marca", "modelo"], how="left"
                )
                ft.loc[mask_fallback, "modelo_key"] = ft_fallback["modelo_key"].values

            # 3. Match Localização
            ft = ft.rename(columns={"regiao": "distrito"})
            ft = ft.merge(map_loc, on="distrito", how="left")

            ft = ft.dropna(subset=["tempo_key", "modelo_key", "localizacao_key"])
            ft["tempo_key"]  = ft["tempo_key"].astype(int)
            ft["modelo_key"] = ft["modelo_key"].astype(int)
            ft["localizacao_key"] = ft["localizacao_key"].astype(int)

            # Cálculo de crescimento_mom_pct
            ft = ft.sort_values(["modelo_key", "localizacao_key", "data"])
            
            if mode == "incremental":
                # Buscar valores do mês anterior na DB para modelos no batch
                mod_ids = tuple(ft["modelo_key"].unique())
                loc_ids = tuple(ft["localizacao_key"].unique())
                if len(mod_ids) == 1: mod_ids_sql = f"({mod_ids[0]})"
                else: mod_ids_sql = str(mod_ids)
                if len(loc_ids) == 1: loc_ids_sql = f"({loc_ids[0]})"
                else: loc_ids_sql = str(loc_ids)
                
                query_prev = text(f"""
                    SELECT f.modelo_key, f.localizacao_key, f.valor_interesse, t.data
                    FROM {DW_SCHEMA}.fact_trends f
                    JOIN {DW_SCHEMA}.dim_tempo t ON f.tempo_key = t.tempo_key
                    WHERE f.modelo_key IN {mod_ids_sql} AND f.localizacao_key IN {loc_ids_sql}
                    AND t.data < :min_data
                    ORDER BY t.data DESC
                """)
                prev_data = pd.read_sql(query_prev, conn, params={"min_data": ft["data"].min()})
                if not prev_data.empty:
                    # Manter apenas o último registo de cada (modelo, localizacao)
                    prev_data = prev_data.sort_values("data").groupby(["modelo_key", "localizacao_key"]).tail(1)
                    ft = pd.concat([prev_data, ft], ignore_index=True).sort_values(["modelo_key", "localizacao_key", "data"])

            ft["crescimento_mom_pct"] = (
                ft.groupby(["modelo_key", "localizacao_key"])["valor_interesse"]
                .pct_change()
                .mul(100)
                .round(4)
            )
            
            # Limpar linhas auxiliares do incremental
            if mode == "incremental":
                ft = ft.dropna(subset=["tempo_key"])

            ft["trending_flag"] = ft["crescimento_mom_pct"].fillna(0) >= 30.0
            
            ft_final = ft[["tempo_key", "modelo_key", "localizacao_key", "valor_interesse", "crescimento_mom_pct", "trending_flag"]]
            ft_final = ft_final.replace({np.nan: None})

            if not ft_final.empty:
                conn.execute(
                    text(f"""
                        INSERT INTO {DW_SCHEMA}.fact_trends
                            (tempo_key, modelo_key, localizacao_key, valor_interesse, crescimento_mom_pct, trending_flag)
                        VALUES
                            (:tempo_key, :modelo_key, :localizacao_key, :valor_interesse, :crescimento_mom_pct, :trending_flag)
                        ON CONFLICT (tempo_key, modelo_key, localizacao_key) DO UPDATE SET
                            valor_interesse     = EXCLUDED.valor_interesse,
                            crescimento_mom_pct = EXCLUDED.crescimento_mom_pct,
                            trending_flag       = EXCLUDED.trending_flag
                    """),
                    ft_final.to_dict(orient="records"),
                )
                print(f"    -> {len(ft_final)} registos em fact_trends.")
            else:
                print("    -> fact_trends: nenhum registo válido após filtros.")

        # ======================================================================
        # FACT_FORUM_SENTIMENT
        # ======================================================================
        print("  A processar fact_forum_sentiment...")
        if not df_forum.empty:
            linhas = []
            for _, row in df_forum.iterrows():
                data_dt = pd.to_datetime(str(row.get("data_extracao", "") or ""), errors="coerce")
                if pd.isna(data_dt): continue

                match_tempo = map_tempo[map_tempo["data"] == data_dt.normalize()]
                if match_tempo.empty: continue
                tempo_key = int(match_tempo.iloc[0]["tempo_key"])

                score     = float(row.get("score_sentimento",  0.0) or 0.0)
                n_mencoes = int(row.get("n_mencoes_modelo", 0) or 0)

                marcas  = [m.strip() for m in str(row.get("mencoes_marca",  "") or "").split("|") if m.strip()]
                modelos = [m.strip() for m in str(row.get("mencoes_modelo", "") or "").split("|") if m.strip()]

                keys_encontrados = set()
                # Tenta match (marca, modelo)
                if marcas and modelos and len(marcas) == len(modelos):
                    for ma, mo in zip(marcas, modelos):
                        res = map_modelo[(map_modelo["marca"] == ma) & (map_modelo["modelo"] == mo)]
                        if not res.empty: keys_encontrados.add(int(res.iloc[0]["modelo_key"]))

                # Fallback só modelo
                if not keys_encontrados:
                    for mo in modelos:
                        res = map_modelo[map_modelo["modelo"] == mo]
                        if not res.empty: keys_encontrados.add(int(res.iloc[0]["modelo_key"]))

                for mk in (keys_encontrados if keys_encontrados else [None]):
                    linhas.append({
                        "tempo_key": tempo_key,
                        "modelo_key": mk,
                        "n_mencoes": n_mencoes,
                        "score_sentimento": score,
                        "data": data_dt.normalize()
                    })

            if linhas:
                ff = pd.DataFrame(linhas)
                ff = ff.dropna(subset=["tempo_key", "modelo_key"])
                # Adicionar filtro explicito para evitar n_mencoes == 0
                ff = ff[ff["n_mencoes"] > 0]
                ff["tempo_key"]  = ff["tempo_key"].astype(int)
                ff["modelo_key"] = ff["modelo_key"].astype(int)

                ff = ff.sort_values(["modelo_key", "data"])
                
                if mode == "incremental" and not ff.empty:
                    mod_ids = tuple(ff["modelo_key"].unique())
                    if len(mod_ids) == 1: mod_ids_sql = f"({mod_ids[0]})"
                    else: mod_ids_sql = str(tuple(mod_ids))
                    
                    query_prev = text(f"""
                        SELECT f.modelo_key, f.score_sentimento, t.data
                        FROM {DW_SCHEMA}.fact_forum_sentiment f
                        JOIN {DW_SCHEMA}.dim_tempo t ON f.tempo_key = t.tempo_key
                        WHERE f.modelo_key IN {mod_ids_sql} AND t.data < :min_data
                        ORDER BY t.data DESC
                    """)
                    prev_f = pd.read_sql(query_prev, conn, params={"min_data": ff["data"].min()})
                    if not prev_f.empty:
                        prev_f = prev_f.sort_values("data").groupby("modelo_key").tail(1)
                        ff = pd.concat([prev_f, ff], ignore_index=True).sort_values(["modelo_key", "data"])

                ff["delta_sentimento"] = (
                    ff.groupby("modelo_key")["score_sentimento"]
                    .diff()
                    .round(4)
                )

                if mode == "incremental":
                    ff = ff.dropna(subset=["tempo_key"])

                ff_final = ff[["tempo_key", "modelo_key", "n_mencoes", "score_sentimento", "delta_sentimento"]]
                ff_final = ff_final.replace({np.nan: None})

                if not ff_final.empty:
                    conn.execute(
                        text(f"""
                            INSERT INTO {DW_SCHEMA}.fact_forum_sentiment
                                (tempo_key, modelo_key, n_mencoes, score_sentimento, delta_sentimento)
                            VALUES
                                (:tempo_key, :modelo_key, :n_mencoes, :score_sentimento, :delta_sentimento)
                            ON CONFLICT (tempo_key, modelo_key) DO UPDATE SET
                                n_mencoes        = EXCLUDED.n_mencoes,
                                score_sentimento = EXCLUDED.score_sentimento,
                                delta_sentimento = EXCLUDED.delta_sentimento
                        """),
                        ff_final.to_dict(orient="records"),
                    )
                    print(f"    -> {len(ff_final)} registos em fact_forum_sentiment.")
                else:
                    print("    -> fact_forum_sentiment: nenhum registo válido após filtros.")

        # ======================================================================
        # FCT_HASHTAG_VOLUME
        # ======================================================================
        print("  A processar fct_hashtag_volume...")
        fonte_key_hash = get_fonte_key("Hashtags Sociais")
        if not df_hash.empty and fonte_key_hash:
            fh = df_hash.copy()
            fh["data"] = pd.to_datetime(fh["data"], errors="coerce").dt.normalize()
            fh = fh.merge(map_tempo,   on="data",    how="left")

            map_full = pd.read_sql(f"SELECT modelo_key, marca, modelo, tipo_automovel, combustivel FROM {DW_SCHEMA}.dim_modelo", conn)
            
            fh["marca"]  = _safe_col(fh, "marca_normalizada", "N/A").fillna("N/A")
            fh["modelo"] = _safe_col(fh, "modelo_normalizado", "N/A").fillna("N/A")
            fh["tipo_automovel"] = _safe_col(fh, "tipo_automovel_normalizado", "N/A").fillna("N/A")
            fh["combustivel"] = _safe_col(fh, "combustivel_normalizado", "N/A").fillna("N/A")
            
            fh = fh.merge(map_full, on=["marca", "modelo", "tipo_automovel", "combustivel"], how="left")

            fh["fonte_key"] = fonte_key_hash
            fh["variacao_semanal"] = fh["variacao_semanal"].fillna(0.0)
            fh = fh.replace({np.nan: None, pd.NA: None})

            cols = ["tempo_key", "fonte_key", "modelo_key", "total_posts",
                    "posts_instagram", "posts_twitter", "posts_youtube", "variacao_semanal"]
            fh = fh[cols].dropna(subset=["tempo_key", "fonte_key", "modelo_key"])
            fh = fh.rename(columns={"total_posts": "volume"})

            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.fct_hashtag_volume
                        (tempo_key, fonte_key, modelo_key, volume,
                         posts_instagram, posts_twitter, posts_youtube, variacao_semanal)
                    VALUES
                        (:tempo_key, :fonte_key, :modelo_key, :volume,
                         :posts_instagram, :posts_twitter, :posts_youtube, :variacao_semanal)
                    ON CONFLICT (tempo_key, fonte_key, modelo_key) DO UPDATE SET
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
                    SELECT DISTINCT ON (mk.tempo_key, v.stand_key, v.veiculo_key)
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
                    ORDER BY mk.tempo_key, v.stand_key, v.veiculo_key, t_entrada.data DESC
                    ON CONFLICT (tempo_key, stand_key, veiculo_key) DO UPDATE SET
                        valor_em_stock = EXCLUDED.valor_em_stock,
                        dias_em_parque = EXCLUDED.dias_em_parque
                """)
            )
        pass

    engine.dispose()
    print("\n  Carga para o PostgreSQL concluida com sucesso.")
    print("=" * 60)


if __name__ == "__main__":
    run_load_to_postgres()