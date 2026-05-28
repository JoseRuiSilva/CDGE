"""
load_to_postgres.py — Silver -> PostgreSQL (Star Schema)
=========================================================
Carrega as 4 tabelas Silver (Delta Lake) para o Star Schema PostgreSQL.
Suporta CDC-like incremental no inventario/vendas, com upserts idempotentes e merge por business key.

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

  demografia_delta : distrito, ano_referencia, populacao_total, mean_age,
                     pct_masculino, pct_feminino, ingestion_timestamp, source_file

Projeto Auto Escala — CDGE 2025/2026
"""
from datetime import timedelta
from datetime import date
from datetime import timezone
from datetime import datetime
from silver_pipeline import _log
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


def run_load_to_postgres(mode: str = "full_load", data_limite: date = None):
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

        # dim_tempo é agora populada pelo generate_dw.py para garantir consistência.
        pass

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

        print("  A processar dim_stand...")
        if not df_inv.empty and "stand" in df_inv.columns:
            # Limpeza e Mapping de Localização
            stands_raw = df_inv["stand"].unique()
            df_stand = pd.DataFrame([{"nome_stand": str(s).strip()} for s in stands_raw if pd.notna(s)])
            
            # Heurística: se o nome do stand contiver o nome de um distrito, associar.
            def inferir_loc(nome):
                for _, row in map_loc.iterrows():
                    if row["distrito"].lower() in nome.lower():
                        return int(row["localizacao_key"])
                return None
                
            df_stand["localizacao_key"] = df_stand["nome_stand"].apply(inferir_loc)
            
            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.dim_stand (nome_stand, localizacao_key)
                    VALUES (:nome_stand, :localizacao_key)
                    ON CONFLICT (nome_stand) DO UPDATE SET localizacao_key = EXCLUDED.localizacao_key
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
            dim_geral = pd.concat(modelos_list).replace({pd.NA: "Unknown", np.nan: "Unknown", "N/A": "Unknown"})
            
            # Inserir dinamicamente para Marca, Modelo, Tipo e Combustível
            for tbl, col in [("dim_marca", "marca"), ("dim_modelo", "modelo"), 
                             ("dim_tipo", "tipo_automovel"), ("dim_combustivel", "combustivel")]:
                df_dim = dim_geral[[col]].drop_duplicates().dropna()
                df_dim = df_dim[df_dim[col] != "Unknown"]
                if not df_dim.empty:
                    conn.execute(
                        text(f"INSERT INTO {DW_SCHEMA}.{tbl} ({col}) VALUES (:{col}) ON CONFLICT DO NOTHING"),
                        df_dim.to_dict(orient="records")
                    )

        # Garantir Unknowns de fallback
        conn.execute(text(f"INSERT INTO {DW_SCHEMA}.dim_modelo (modelo) VALUES ('Unknown') ON CONFLICT DO NOTHING"))

        # ======================================================================
        # DIM_VEICULO
        # ======================================================================
        print("  A processar dim_veiculo...")
        if not df_inv.empty and "id_viatura" in df_inv.columns:
            map_marca = pd.read_sql(f"SELECT marca_key, marca FROM {DW_SCHEMA}.dim_marca", conn)
            map_modelo = pd.read_sql(f"SELECT modelo_key, modelo FROM {DW_SCHEMA}.dim_modelo", conn)
            map_tipo = pd.read_sql(f"SELECT tipo_key, tipo_automovel FROM {DW_SCHEMA}.dim_tipo", conn)
            map_comb = pd.read_sql(f"SELECT combustivel_key, combustivel FROM {DW_SCHEMA}.dim_combustivel", conn)
            
            tmp = df_inv.copy()
            tmp["marca"] = tmp["marca_normalizada"].fillna("Unknown")
            tmp["modelo"] = tmp["modelo_normalizado"].fillna("Unknown")
            tmp["tipo_automovel"] = _safe_col(tmp, "tipo_automovel", "Unknown").fillna("Unknown")
            tmp["combustivel"]    = _safe_col(tmp, "combustivel", "Unknown").fillna("Unknown")
            tmp = tmp.sort_values(["id_viatura", "ingestion_timestamp", "source_file"], ascending=[True, True, True])

            dim_veic = tmp.merge(map_marca, on="marca", how="left")
            dim_veic = dim_veic.merge(map_modelo, on="modelo", how="left")
            dim_veic = dim_veic.merge(map_tipo, on="tipo_automovel", how="left")
            dim_veic = dim_veic.merge(map_comb, on="combustivel", how="left")

            # Preencher keys ausentes com -1 (Unknown)
            for k in ["marca_key", "modelo_key", "tipo_key", "combustivel_key"]:
                dim_veic[k] = dim_veic[k].fillna(-1).astype(int)

            dim_veic = (
                dim_veic[[
                    "id_viatura", "matricula", "marca_key", "modelo_key", "tipo_key", "combustivel_key", "num_lugares", "ano_viatura"
                ]]
                .dropna(subset=["id_viatura"])
            )

            # Normalizar colunas numéricas que podem vir como strings vazias do Silver.
            dim_veic["num_lugares"] = pd.to_numeric(dim_veic["num_lugares"], errors="coerce").astype("Int64")
            dim_veic["ano_viatura"] = pd.to_numeric(dim_veic["ano_viatura"], errors="coerce").astype("Int64")
            dim_veic = dim_veic.where(pd.notna(dim_veic), None)
            
            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.dim_veiculo 
                        (id_viatura, matricula, marca_key, modelo_key, tipo_key, combustivel_key, num_lugares, ano_viatura)
                    VALUES (:id_viatura, :matricula, :marca_key, :modelo_key, :tipo_key, :combustivel_key, :num_lugares, :ano_viatura)
                    ON CONFLICT (id_viatura) DO UPDATE SET
                        matricula = EXCLUDED.matricula,
                        marca_key = EXCLUDED.marca_key,
                        modelo_key = EXCLUDED.modelo_key,
                        tipo_key = EXCLUDED.tipo_key,
                        combustivel_key = EXCLUDED.combustivel_key,
                        num_lugares = EXCLUDED.num_lugares,
                        ano_viatura = EXCLUDED.ano_viatura
                """),
                dim_veic.to_dict(orient="records"),
            )

        # ======================================================================
        # DIM_CLIENTE (SCD TIPO 2 - VERSÃO FINAL 100% FUNCIONAL)
        # ======================================================================
        print("  A processar dim_cliente (SCD Type 2)...")

        if not df_cli.empty:
            df_cli_sorted = df_cli.sort_values(["ano_mes", "ingestion_timestamp"])
            map_loc_cli = pd.read_sql(f"SELECT localizacao_key, distrito FROM {DW_SCHEMA}.dim_localizacao", conn)

            for mes_str, df_month in df_cli_sorted.groupby("ano_mes"):
                if mode == "full_load":
                    ano, mes = map(int, mes_str.split("-"))
                    hoje = date(ano, mes, 1)
                else:
                    hoje = datetime.now(timezone.utc).date()
                
                ontem = hoje - timedelta(days=1)
                
                query_ativos = text(f"""
                    SELECT c.cliente_key, c.nif, c.idade, c.faixa_etaria, l.distrito 
                    FROM {DW_SCHEMA}.dim_cliente c
                    LEFT JOIN {DW_SCHEMA}.dim_localizacao l ON c.localizacao_key = l.localizacao_key
                    WHERE c.is_ativo = TRUE
                """)
                df_ativos = pd.read_sql(query_ativos, conn)
                
                df_merge = df_month.merge(
                    df_ativos[['nif', 'cliente_key', 'distrito', 'idade', 'faixa_etaria']], 
                    on='nif', how='left', suffixes=('', '_old')
                )
                
                novos_clientes = df_merge[df_merge['cliente_key'].isna()].copy()
                mudancas = df_merge[
                    df_merge['cliente_key'].notna() & 
                    ((df_merge['distrito'] != df_merge['distrito_old']) | 
                     (df_merge['idade'] != df_merge['idade_old']))
                ].copy()
                
                if not mudancas.empty:
                    keys_para_fechar = mudancas['cliente_key'].tolist()
                    conn.execute(
                        text(f"""
                            UPDATE {DW_SCHEMA}.dim_cliente 
                            SET is_ativo = FALSE, data_fim = :ontem 
                            WHERE cliente_key IN :keys
                        """),
                        {"ontem": ontem, "keys": tuple(keys_para_fechar)}
                    )
                    print(f"    SCD2: {len(mudancas)} versões antigas terminadas em {ontem}")
                
                registos_para_inserir = pd.concat([novos_clientes, mudancas], ignore_index=True)
                
                if not registos_para_inserir.empty:
                    registos_para_inserir = registos_para_inserir.merge(map_loc_cli, on='distrito', how='left')
                    df_final = registos_para_inserir[[
                        'nif', 'nome', 'idade', 'faixa_etaria', 'genero', 'localizacao_key'
                    ]].copy()
                    
                    df_final['data_inicio'] = hoje
                    df_final['data_fim']    = date(9999, 12, 31)
                    df_final['is_ativo']    = True
                    df_final = df_final.replace({np.nan: None})

                    # Usar INSERT explícito para que os triggers de auditoria disparem corretamente
                    conn.execute(
                        text(f"""
                            INSERT INTO {DW_SCHEMA}.dim_cliente
                                (nif, nome, idade, faixa_etaria, genero, localizacao_key, data_inicio, data_fim, is_ativo)
                            VALUES
                                (:nif, :nome, :idade, :faixa_etaria, :genero, :localizacao_key, :data_inicio, :data_fim, :is_ativo)
                            ON CONFLICT DO NOTHING
                        """),
                        df_final.to_dict(orient='records')
                    )
                    print(f"    SCD2: {len(df_final)} registos inseridos com início em {hoje}")
            
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
                        (localizacao_key, ano_referencia, populacao_total, mean_age, pct_masculino, pct_feminino)
                    VALUES (:localizacao_key, :ano_referencia, :populacao_total, :mean_age, :pct_masculino, :pct_feminino)
                    ON CONFLICT (localizacao_key, ano_referencia) DO UPDATE SET
                        populacao_total = EXCLUDED.populacao_total,
                        mean_age = EXCLUDED.mean_age,
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
        map_cliente = pd.read_sql(f"SELECT cliente_key, nif FROM {DW_SCHEMA}.dim_cliente", conn)
        map_marca = pd.read_sql(f"SELECT marca_key, marca FROM {DW_SCHEMA}.dim_marca", conn)
        map_modelo = pd.read_sql(f"SELECT modelo_key, modelo FROM {DW_SCHEMA}.dim_modelo", conn)
        map_tipo = pd.read_sql(f"SELECT tipo_key, tipo_automovel FROM {DW_SCHEMA}.dim_tipo", conn)
        map_comb = pd.read_sql(f"SELECT combustivel_key, combustivel FROM {DW_SCHEMA}.dim_combustivel", conn)

        def get_fonte_key(nome):
            res = map_fonte[map_fonte["nome_fonte"] == nome]
            return int(res.iloc[0]["fonte_key"]) if not res.empty else None

        # ======================================================================
        # FACT_VENDA (Com verificação de histórico SCD2)
        # ======================================================================
        print("  A processar fact_venda...")
        if not df_inv.empty:
            fct = df_inv.copy()
            fct = fct.dropna(subset=["preco_venda", "data_venda"]) # Só carros vendidos
            fct["data_venda_dt"] = pd.to_datetime(fct["data_venda"]).dt.date

            # Garantir que ficamos com a linha que TEM nif_cliente preenchido.
            # Ordenar: primeiro por nif_cliente não vazio (desc string, vazio vai para o fim),
            # depois por data_venda mais recente e ingestion mais recente.
            fct["_tem_nif"] = fct["nif_cliente"].fillna("").apply(lambda x: 0 if x == "" else 1)
            fct = fct.sort_values(
                ["id_viatura", "_tem_nif", "data_venda_dt", "ingestion_timestamp"],
                ascending=[True, False, False, False]
            )
            fct = fct.drop_duplicates(subset=["id_viatura"])  # Mantém a linha com nif se existir
            fct = fct.drop(columns=["_tem_nif"])

            # Converter datas para facilitar o cruzamento SCD2
            fct["data_entrada_stock"] = pd.to_datetime(fct["data_entrada_stock"]).dt.tz_localize(None).dt.normalize()
            fct["data_venda"] = pd.to_datetime(fct["data_venda"]).dt.tz_localize(None).dt.normalize()

            # Mapear Chaves Básicas
            fct = fct.merge(map_veiculo, on="id_viatura", how="left")
            fct = fct.merge(map_stand, on="stand", how="left")
            fct = fct.merge(map_tempo.rename(columns={"data": "data_entrada_stock", "tempo_key": "tempo_entrada_key"}), on="data_entrada_stock", how="left")
            fct = fct.merge(map_tempo.rename(columns={"data": "data_venda", "tempo_key": "tempo_venda_key"}), on="data_venda", how="left")

            # Mapear Cliente (Lógica SCD Tipo 2)
            query_cli = text(f"SELECT cliente_key, nif, data_inicio, data_fim, is_ativo FROM {DW_SCHEMA}.dim_cliente")
            map_cli = pd.read_sql(query_cli, conn)
            map_cli["data_inicio"] = pd.to_datetime(map_cli["data_inicio"], errors="coerce").dt.date

            # data_fim = 9999-12-31 causa OverflowError no pandas.
            # Para registos activos (is_ativo=True), substituir por uma data segura muito no futuro.
            from datetime import date as _date
            def _safe_date_fim(row):
                if row["is_ativo"]:
                    return _date(2099, 12, 31)
                try:
                    d = pd.to_datetime(row["data_fim"])
                    return d.date() if not pd.isna(d) else None
                except Exception:
                    return _date(2099, 12, 31)
            map_cli["data_fim"] = map_cli.apply(_safe_date_fim, axis=1)

            fct["nif_cliente"] = fct["nif_cliente"].fillna("").replace("", None)
            fct = fct.merge(map_cli, left_on="nif_cliente", right_on="nif", how="left")

            # FILTRO CRÍTICO: Manter o cliente apenas se a data da venda estiver dentro da sua data de validade (SCD2)
            mask_cliente_valido = (
                fct["cliente_key"].isna() |
                (
                    fct["data_inicio"].notna() &
                    fct["data_fim"].notna() &
                    (fct["data_venda_dt"] >= fct["data_inicio"]) &
                    (fct["data_venda_dt"] <= fct["data_fim"])
                )
            )
            fct = fct[mask_cliente_valido]

            # Cálculos Finais
            fct["quilometragem"] = pd.to_numeric(fct["quilometragem"], errors="coerce")
            fct["preco_aquisicao"] = pd.to_numeric(fct["preco_aquisicao"], errors="coerce")
            fct["preco_venda"] = pd.to_numeric(fct["preco_venda"], errors="coerce")
            fct["margem"] = fct["preco_venda"] - fct["preco_aquisicao"]
            fct["dias_em_stock"] = (pd.to_datetime(fct["data_venda"]) - pd.to_datetime(fct["data_entrada_stock"])).dt.days

            cols = ["veiculo_key", "stand_key", "tempo_entrada_key", "tempo_venda_key", "cliente_key", 
                    "quilometragem", "preco_aquisicao", "preco_venda", "margem", "dias_em_stock"]
            
            fct_final = fct[cols].dropna(subset=["veiculo_key", "stand_key", "tempo_entrada_key", "tempo_venda_key"]).replace({np.nan: None})
            
            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.fact_venda (veiculo_key, stand_key, tempo_entrada_key, tempo_venda_key, cliente_key, quilometragem, preco_aquisicao, preco_venda, margem, dias_em_stock)
                    VALUES (:veiculo_key, :stand_key, :tempo_entrada_key, :tempo_venda_key, :cliente_key, :quilometragem, :preco_aquisicao, :preco_venda, :margem, :dias_em_stock)
                    ON CONFLICT (veiculo_key, stand_key, tempo_entrada_key) DO UPDATE SET
                        tempo_venda_key = EXCLUDED.tempo_venda_key,
                        cliente_key     = EXCLUDED.cliente_key,
                        quilometragem   = EXCLUDED.quilometragem,
                        preco_aquisicao = EXCLUDED.preco_aquisicao,
                        preco_venda     = EXCLUDED.preco_venda,
                        margem          = EXCLUDED.margem,
                        dias_em_stock   = EXCLUDED.dias_em_stock
                """),
                fct_final.to_dict(orient="records")
            )

        # ======================================================================
        # FACT_TRENDS
        # ======================================================================
        print("  A processar fact_trends...")
        if not df_trends.empty:
            ft = df_trends.copy()
            ft["data"] = pd.to_datetime(ft["mes"], errors="coerce").dt.normalize()
            ft["data"] = ft["data"].apply(lambda x: x.replace(day=1) if pd.notnull(x) else x)
            ft = ft.merge(map_tempo, on="data", how="left")
            
            # Match robusto considerando "N/A" -> ID -1 (Unknown)
            ft = ft.replace({"N/A": "Unknown"})
            
            ft = ft.merge(map_marca, left_on="marca_normalizada", right_on="marca", how="left")
            ft = ft.merge(map_modelo, left_on="modelo_normalizado", right_on="modelo", how="left")
            ft = ft.merge(map_tipo, left_on="tipo_normalizado", right_on="tipo_automovel", how="left")
            ft = ft.merge(map_comb, left_on="combustivel_normalizado", right_on="combustivel", how="left")

            for k in ["marca_key", "modelo_key", "tipo_key", "combustivel_key"]:
                ft[k] = ft[k].fillna(-1).astype(int)
            
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
            
            ft_final = ft[["tempo_key", "marca_key", "modelo_key", "tipo_key", "combustivel_key", "localizacao_key", "valor_interesse", "crescimento_mom_pct", "trending_flag"]]
            ft_final = ft_final.replace({np.nan: None})

            if not ft_final.empty:
                # Mapeamento para fact_trends (usando todas as chaves)
                conn.execute(
                    text(f"""
                        INSERT INTO {DW_SCHEMA}.fact_trends
                            (tempo_key, marca_key, modelo_key, tipo_key, combustivel_key, localizacao_key, valor_interesse, crescimento_mom_pct, trending_flag)
                        VALUES
                            (:tempo_key, :marca_key, :modelo_key, :tipo_key, :combustivel_key, :localizacao_key, :valor_interesse, :crescimento_mom_pct, :trending_flag)
                        ON CONFLICT (tempo_key, marca_key, modelo_key, tipo_key, combustivel_key, localizacao_key) DO UPDATE SET
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

                data_dt = data_dt.replace(day=1)

                match_tempo = map_tempo[map_tempo["data"] == data_dt.normalize()]
                if match_tempo.empty: continue
                tempo_key = int(match_tempo.iloc[0]["tempo_key"])

                score     = float(row.get("score_sentimento",  0.0) or 0.0)
                n_mencoes = int(row.get("n_mencoes_modelo", 0) or 0)

                # Mapeamento individual (granularidade agora é por característica)
                # O Silver garante que apenas uma característica é enviada por linha
                m_ma = row.get("mencoes_marca", "N/A")
                m_mo = row.get("mencoes_modelo", "N/A")
                m_ti = row.get("mencoes_tipo", "N/A")
                m_co = row.get("mencoes_combustivel", "N/A")
                
                # Mapear N/A para Unknown
                m_ma = "Unknown" if m_ma in ["N/A", "N/D", ""] else m_ma
                m_mo = "Unknown" if m_mo in ["N/A", "N/D", ""] else m_mo
                m_ti = "Unknown" if m_ti in ["N/A", "N/D", ""] else m_ti
                m_co = "Unknown" if m_co in ["N/A", "N/D", ""] else m_co
                
                res_ma = map_marca[map_marca["marca"] == m_ma]
                res_mo = map_modelo[map_modelo["modelo"] == m_mo]
                res_ti = map_tipo[map_tipo["tipo_automovel"] == m_ti]
                res_co = map_comb[map_comb["combustivel"] == m_co]
                
                ma_k = int(res_ma.iloc[0]["marca_key"]) if not res_ma.empty else -1
                mo_k = int(res_mo.iloc[0]["modelo_key"]) if not res_mo.empty else -1
                ti_k = int(res_ti.iloc[0]["tipo_key"]) if not res_ti.empty else -1
                co_k = int(res_co.iloc[0]["combustivel_key"]) if not res_co.empty else -1
                
                linhas.append({
                    "tempo_key": tempo_key,
                    "marca_key": ma_k,
                    "modelo_key": mo_k,
                    "tipo_key": ti_k,
                    "combustivel_key": co_k,
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

                ff_final = ff[["tempo_key", "marca_key", "modelo_key", "tipo_key", "combustivel_key", "n_mencoes", "score_sentimento", "delta_sentimento"]]
                ff_final = ff_final.replace({np.nan: None})

                if not ff_final.empty:
                    conn.execute(
                        text(f"""
                            INSERT INTO {DW_SCHEMA}.fact_forum_sentiment
                                (tempo_key, marca_key, modelo_key, tipo_key, combustivel_key, n_mencoes, score_sentimento, delta_sentimento)
                            VALUES
                                (:tempo_key, :marca_key, :modelo_key, :tipo_key, :combustivel_key, :n_mencoes, :score_sentimento, :delta_sentimento)
                            ON CONFLICT (tempo_key, marca_key, modelo_key, tipo_key, combustivel_key) DO UPDATE SET
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
        # FACT_HASHTAG_VOLUME
        # ======================================================================
        print("  A processar fact_hashtag_volume...")
        fonte_key_hash = get_fonte_key("Hashtags Sociais")
        if not df_hash.empty and fonte_key_hash:
            fh = df_hash.copy()
            fh["data"] = pd.to_datetime(fh["data"], errors="coerce").dt.normalize()
            fh["data"] = fh["data"].apply(lambda x: x.replace(day=1) if pd.notnull(x) else x)
            fh = fh.merge(map_tempo,   on="data",    how="left")
            
            fh["marca"]  = _safe_col(fh, "marca_normalizada", "Unknown").fillna("Unknown").replace("N/A", "Unknown")
            fh["modelo"] = _safe_col(fh, "modelo_normalizado", "Unknown").fillna("Unknown").replace("N/A", "Unknown")
            fh["tipo_automovel"] = _safe_col(fh, "tipo_normalizado", "Unknown").fillna("Unknown").replace("N/A", "Unknown")
            fh["combustivel"] = _safe_col(fh, "combustivel_normalizado", "Unknown").fillna("Unknown").replace("N/A", "Unknown")
            
            fh = fh.merge(map_marca, left_on="marca_normalizada", right_on="marca", how="left")
            fh = fh.merge(map_modelo, left_on="modelo_normalizado", right_on="modelo", how="left")
            fh = fh.merge(map_tipo, left_on="tipo_normalizado", right_on="tipo_automovel", how="left")
            fh = fh.merge(map_comb, left_on="combustivel_normalizado", right_on="combustivel", how="left")

            for k in ["marca_key", "modelo_key", "tipo_key", "combustivel_key"]:
                fh[k] = fh[k].fillna(-1).astype(int)

            # Cast de plataformas
            for p in ["posts_instagram", "posts_twitter", "posts_youtube"]:
                if p in fh.columns:
                    fh[p] = pd.to_numeric(fh[p], errors="coerce").fillna(0).astype(int)

            fh["fonte_key"] = fonte_key_hash
            fh["variacao_semanal"] = _safe_col(fh, "variacao_semanal", 0.0).fillna(0.0)
            fh["volume"] = pd.to_numeric(_safe_col(fh, "total_posts", 0), errors="coerce").fillna(0).astype(int)
            fh = fh.replace({np.nan: None, pd.NA: None})

            cols = ["tempo_key", "fonte_key", "marca_key", "modelo_key", "tipo_key", "combustivel_key", 
                    "volume", "posts_instagram", "posts_twitter", "posts_youtube", "variacao_semanal"]
            fh = fh[cols].dropna(subset=["tempo_key", "fonte_key", "modelo_key"])

            conn.execute(
                text(f"""
                    INSERT INTO {DW_SCHEMA}.fact_hashtag_volume
                        (tempo_key, fonte_key, marca_key, modelo_key, tipo_key, combustivel_key,
                         volume, posts_instagram, posts_twitter, posts_youtube, variacao_semanal)
                    VALUES
                        (:tempo_key, :fonte_key, :marca_key, :modelo_key, :tipo_key, :combustivel_key,
                         :volume, :posts_instagram, :posts_twitter, :posts_youtube, :variacao_semanal)
                    ON CONFLICT (tempo_key, fonte_key, marca_key, modelo_key, tipo_key, combustivel_key) DO UPDATE SET
                        volume           = EXCLUDED.volume,
                        posts_instagram  = EXCLUDED.posts_instagram,
                        posts_twitter    = EXCLUDED.posts_twitter,
                        posts_youtube    = EXCLUDED.posts_youtube,
                        variacao_semanal = EXCLUDED.variacao_semanal
                """),
                fh.to_dict(orient="records"),
            )

        # ======================================================================
        # FACT_INVENTARIO_MENSAL (Lógica SQL de Expansão Histórica)
        # ======================================================================
        print("  A processar fact_inventario_mensal...")
        if not df_inv.empty:
            # 1. Preparar uma tabela de Staging rápida no PostgreSQL
            df_stg = df_inv[["id_viatura", "stand", "preco_aquisicao", "data_entrada_stock", "data_venda"]].copy()
            df_stg["data_entrada_stock"] = pd.to_datetime(df_stg["data_entrada_stock"]).dt.date
            df_stg["data_venda"] = pd.to_datetime(df_stg["data_venda"]).dt.date
            df_stg = df_stg.sort_values(["id_viatura", "data_entrada_stock", "data_venda"], ascending=[True, False, False])
            df_stg = df_stg.drop_duplicates(subset=["id_viatura"])
            
            df_stg.to_sql("stg_inv", conn, schema=DW_SCHEMA, if_exists="replace", index=False)

            # 2. Executar a magia da Expansão Temporal no Postgres
            # A query cruza o carro com o último dia de cada mês. Se o carro entrou antes desse dia 
            # e ainda não foi vendido, gera uma linha de inventário para esse mês!
            # Se data_limite não for fornecida, usamos a data atual como fallback.
            data_limite_sql = f"'{data_limite}'::date" if data_limite else "CURRENT_DATE"

            conn.execute(text(f"""
                INSERT INTO {DW_SCHEMA}.fact_inventario_mensal
                    (tempo_key, stand_key, veiculo_key, valor_em_stock, dias_em_parque)
                SELECT tempo_key, stand_key, veiculo_key, valor_em_stock, dias_em_parque
                FROM (
                    -- Caso 1: Carro em stock no último dia do mês (não vendido ou vendido depois)
                    SELECT
                        dt.tempo_key,
                        ds.stand_key,
                        dv.veiculo_key,
                        si.preco_aquisicao::numeric AS valor_em_stock,
                        (dt.data - si.data_entrada_stock::date) AS dias_em_parque
                    FROM {DW_SCHEMA}.stg_inv si
                    JOIN {DW_SCHEMA}.dim_veiculo dv ON dv.id_viatura = si.id_viatura
                    JOIN {DW_SCHEMA}.dim_stand ds ON ds.nome_stand = si.stand
                    JOIN {DW_SCHEMA}.dim_tempo dt
                      ON dt.data = (date_trunc('month', dt.data) + interval '1 month - 1 day')::date
                    WHERE
                        si.data_entrada_stock::date <= dt.data
                        AND (si.data_venda IS NULL OR si.data_venda::date > dt.data)
                        AND dt.data <= {data_limite_sql}

                    UNION ALL

                    -- Caso 2: Carro vendido no mesmo mês em que entrou (não aparece no caso 1)
                    SELECT
                        dt2.tempo_key,
                        ds2.stand_key,
                        dv2.veiculo_key,
                        si2.preco_aquisicao::numeric AS valor_em_stock,
                        (si2.data_venda::date - si2.data_entrada_stock::date) AS dias_em_parque
                    FROM {DW_SCHEMA}.stg_inv si2
                    JOIN {DW_SCHEMA}.dim_veiculo dv2 ON dv2.id_viatura = si2.id_viatura
                    JOIN {DW_SCHEMA}.dim_stand ds2 ON ds2.nome_stand = si2.stand
                    JOIN {DW_SCHEMA}.dim_tempo dt2
                      ON dt2.data = (date_trunc('month', si2.data_entrada_stock::date) + interval '1 month - 1 day')::date
                    WHERE
                        si2.data_venda IS NOT NULL
                        AND date_trunc('month', si2.data_entrada_stock::date) = date_trunc('month', si2.data_venda::date)
                        AND dt2.data <= {data_limite_sql}
                ) sub
                ON CONFLICT (tempo_key, stand_key, veiculo_key) DO UPDATE SET
                    valor_em_stock = EXCLUDED.valor_em_stock,
                    dias_em_parque = EXCLUDED.dias_em_parque;
            """))

            # Limpar a staging
            conn.execute(text(f"DROP TABLE {DW_SCHEMA}.stg_inv;"))

        pass

    engine.dispose()
    print("\n  Carga para o PostgreSQL concluida com sucesso.")
    print("=" * 60)


if __name__ == "__main__":
    run_load_to_postgres()