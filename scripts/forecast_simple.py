import os
import pandas as pd
import socket
from sqlalchemy import create_engine, text

# Host configurável via env var
_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
DW_URL   = f"postgresql+psycopg2://ae_user:ae_pass_2026@{_PG_HOST}:{_PG_PORT}/auto_escala"

def _criar_engine():
    """Cria engine com pre-check TCP."""
    try:
        with socket.create_connection((_PG_HOST, int(_PG_PORT)), timeout=3.0):
            pass
    except (OSError, ConnectionRefusedError):
        return None
    return create_engine(DW_URL, connect_args={"connect_timeout": 5})

def run_simple_forecast():
    print("\n" + "=" * 60)
    print("  COMPOSITE HEURISTIC FORECAST (All Metrics)")
    print("=" * 60)

    engine = _criar_engine()
    if engine is None:
        print("  [AVISO] PostgreSQL não acessível -- Forecast ignorado.")
        return

    # Query consolidada: cruza todas as métricas por modelo/mês
    query = """
        WITH mensal_vendas AS (
            SELECT 
                dv.modelo_key, 
                fv.tempo_venda_key AS tempo_key, 
                COUNT(*) AS volume_vendas
            FROM auto_escala_dw.fct_venda fv
            JOIN auto_escala_dw.dim_veiculo dv ON fv.veiculo_key = dv.veiculo_key
            WHERE fv.tempo_venda_key IS NOT NULL
            GROUP BY 1, 2
        ),
        mensal_hashtags AS (
            SELECT 
                modelo_key, 
                tempo_key, 
                SUM(volume) AS volume_hashtags
            FROM auto_escala_dw.fct_hashtag_volume
            GROUP BY 1, 2
        )
        SELECT
            dm.modelo_key,
            dt.data AS ds,
            dt.tempo_key,
            COALESCE(ft.valor_interesse, 0) AS trends_y,
            COALESCE(ft.crescimento_mom_pct, 0) AS trends_growth,
            COALESCE(ffs.score_sentimento, 0.5) AS sentiment_y,
            COALESCE(ffs.delta_sentimento, 0) AS sentiment_growth,
            COALESCE(mh.volume_hashtags, 0) AS hashtags_y,
            COALESCE(mv.volume_vendas, 0) AS sales_y
        FROM auto_escala_dw.dim_modelo dm
        CROSS JOIN (SELECT DISTINCT tempo_key, data FROM auto_escala_dw.dim_tempo WHERE dia = 1) dt
        LEFT JOIN auto_escala_dw.fact_trends ft ON dm.modelo_key = ft.modelo_key AND dt.tempo_key = ft.tempo_key
        LEFT JOIN auto_escala_dw.fact_forum_sentiment ffs ON dm.modelo_key = ffs.modelo_key AND dt.tempo_key = ffs.tempo_key
        LEFT JOIN mensal_hashtags mh ON dm.modelo_key = mh.modelo_key AND dt.tempo_key = mh.tempo_key
        LEFT JOIN mensal_vendas mv ON dm.modelo_key = mv.modelo_key AND dt.tempo_key = mv.tempo_key
        ORDER BY dm.modelo_key, dt.data ASC
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
    except Exception as e:
        print(f"  [ERRO] Falha ao ler DW: {repr(e)}")
        return

    if df.empty:
        print("  [AVISO] Sem dados suficientes para o forecast.")
        return

    df['ds'] = pd.to_datetime(df['ds'])
    modelos = df['modelo_key'].unique()
    
    print(f"  A analisar correlações e a gerar previsões para {len(modelos)} modelos...")
    
    # Pré-carregar mapeamento de tempo
    try:
        with engine.connect() as conn:
            map_tempo = pd.read_sql("SELECT data, tempo_key FROM auto_escala_dw.dim_tempo WHERE dia = 1", conn)
        map_tempo['data'] = pd.to_datetime(map_tempo['data'])
        tempo_dict = map_tempo.set_index('data')['tempo_key'].to_dict()
    except Exception as e:
        print(f"  [ERRO] Falha ao carregar dim_tempo: {repr(e)}")
        return

    previsoes = []
    for mod in modelos:
        df_mod = df[df['modelo_key'] == mod].sort_values('ds')
        if len(df_mod) < 2: continue
            
        # Pegamos no último registo que tenha dados reais de Trends
        df_real = df_mod[df_mod['trends_y'] > 0]
        if df_real.empty: continue
        
        last_row = df_real.iloc[-1]
        last_idx = df_mod.index.get_loc(last_row.name)
        if last_idx == 0: continue
        prev_row = df_mod.iloc[last_idx - 1]
        
        # Pesos da Heurística
        w_trends, w_sent, w_sales = 0.40, 0.25, 0.20
        
        # Cálculo de crescimento
        sales_growth = (last_row['sales_y'] - prev_row['sales_y']) / max(1, prev_row['sales_y'])
        
        composite_growth = (
            (float(last_row['trends_growth'] or 0) * w_trends) + 
            (float(last_row['sentiment_growth'] or 0) * w_sent) + 
            (float(sales_growth) * w_sales)
        )
        
        base_value = float(last_row['trends_y'])
        y_pred = base_value * (1 + (composite_growth / 100.0))
        
        dt_ref = last_row['ds']
        mes_seguinte = dt_ref + pd.offsets.MonthBegin(1)
        
        tk_ref = tempo_dict.get(dt_ref)
        tk_alv = tempo_dict.get(mes_seguinte)

        if tk_ref and tk_alv:
            previsoes.append({
                'modelo_key':     int(mod),
                'tempo_ref_key':  int(tk_ref),
                'tempo_alvo_key': int(tk_alv),
                'valor_previsto': float(max(0, y_pred)),
                'yhat_lower':     float(max(0, y_pred * 0.85)),
                'yhat_upper':     float(max(0, y_pred * 1.15)),
                'mae':            0.0,
                'mape':           0.0
            })

    if previsoes:
        print(f"  A inserir {len(previsoes)} previsões em fact_previsao...")
        sql_insert = text("""
            INSERT INTO auto_escala_dw.fact_previsao
                (modelo_key, tempo_ref_key, tempo_alvo_key, valor_previsto, yhat_lower, yhat_upper, mae, mape)
            VALUES
                (:modelo_key, :tempo_ref_key, :tempo_alvo_key, :valor_previsto, :yhat_lower, :yhat_upper, :mae, :mape)
            ON CONFLICT (modelo_key, tempo_ref_key, tempo_alvo_key) DO UPDATE SET
                valor_previsto = EXCLUDED.valor_previsto,
                yhat_lower     = EXCLUDED.yhat_lower,
                yhat_upper     = EXCLUDED.yhat_upper
        """)
        try:
            with engine.begin() as conn:
                conn.execute(sql_insert, previsoes)
            print("  Previsões guardadas com sucesso!")
        except Exception as e:
            print(f"  [ERRO] Ao inserir previsões: {repr(e)}")
    else:
        print("  Nenhuma previsão foi gerada.")

    engine.dispose()
    print("=" * 60)

if __name__ == "__main__":
    run_simple_forecast()
