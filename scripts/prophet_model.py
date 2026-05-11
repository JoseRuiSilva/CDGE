import os
import pandas as pd
import warnings
import socket
from sqlalchemy import create_engine, text
from pathlib import Path

# Suprimir avisos verbosos do Prophet
import logging
logging.getLogger("cmdstanpy").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

# Host configuravel via env var: 'localhost' local, 'postgres' no Docker Airflow
_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
DW_URL   = f"postgresql+psycopg2://ae_user:ae_pass_2026@{_PG_HOST}:{_PG_PORT}/auto_escala"


def _criar_engine_prophet():
    """Cria engine com pre-check TCP para nao ficar pendurado."""
    try:
        with socket.create_connection((_PG_HOST, int(_PG_PORT)), timeout=3.0):
            pass
    except (OSError, ConnectionRefusedError):
        return None
    return create_engine(DW_URL, connect_args={"connect_timeout": 5})


def run_prophet():
    print("\n" + "=" * 60)
    print("  FORECASTING (FACEBOOK PROPHET)")
    print("=" * 60)

    if not PROPHET_AVAILABLE:
        print("  [ERRO] A biblioteca 'prophet' nao esta instalada.")
        print("  Executa: pip install prophet")
        return

    engine = _criar_engine_prophet()
    if engine is None:
        print("  [AVISO] PostgreSQL nao acessivel -- Prophet ignorado.")
        return

    # Ler dados das trends
    query = """
        SELECT
            t.data AS ds,
            t.tempo_key,
            f.tendencia_key,
            f.modelo_key,
            f.valor_interesse AS y
        FROM auto_escala_dw.fact_trends f
        JOIN auto_escala_dw.dim_tempo t ON f.tempo_key = t.tempo_key
        WHERE f.valor_interesse IS NOT NULL
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
    except Exception as e:
        print(f"  [ERRO] Falha ao ler DW: {e}")
        engine.dispose()
        return

    if df.empty:
        print("  [AVISO] Sem dados suficientes para o Prophet.")
        engine.dispose()
        return

    df['ds'] = pd.to_datetime(df['ds'])
    modelos = df['modelo_key'].unique()

    print(f"  A treinar modelos para {len(modelos)} chaves de modelo...")
    atualizacoes = []

    # Expanding window natural: usamos toda a info historica disponivel
    for mod in modelos:
        df_mod = df[df['modelo_key'] == mod].sort_values('ds').copy()

        # Prophet requer pelo menos 2 pontos
        if len(df_mod) < 2:
            continue

        m = Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False)
        m.fit(df_mod[['ds', 'y']])

        # Prever o proximo mes
        future   = m.make_future_dataframe(periods=1, freq='MS')
        forecast = m.predict(future)

        # Identificar o ultimo mes disponivel (ref) e o mes previsto (alvo)
        dt_ref  = df_mod.iloc[-1]['ds']
        dt_alvo = forecast.iloc[-1]['ds']
        yhat_prox = forecast.iloc[-1]['yhat']
        
        # Obter as chaves de tempo correspondentes
        with engine.connect() as conn:
            res_ref = conn.execute(text("SELECT tempo_key FROM auto_escala_dw.dim_tempo WHERE data = :d"), {"d": dt_ref.date()}).fetchone()
            res_alv = conn.execute(text("SELECT tempo_key FROM auto_escala_dw.dim_tempo WHERE data = :d"), {"d": dt_alvo.date()}).fetchone()
        
        if res_ref and res_alv:
            atualizacoes.append({
                'modelo_key':     int(mod),
                'tempo_ref_key':  int(res_ref[0]),
                'tempo_alvo_key': int(res_alv[0]),
                'valor_previsto': float(max(0, yhat_prox)),
                'yhat_lower':     float(max(0, forecast.iloc[-1]['yhat_lower'])),
                'yhat_upper':     float(max(0, forecast.iloc[-1]['yhat_upper'])),
            })

    if atualizacoes:
        print(f"  A inserir {len(atualizacoes)} previsoes em fact_previsao...")
        sql_insert = text("""
            INSERT INTO auto_escala_dw.fact_previsao
                (modelo_key, tempo_ref_key, tempo_alvo_key, valor_previsto, yhat_lower, yhat_upper)
            VALUES
                (:modelo_key, :tempo_ref_key, :tempo_alvo_key, :valor_previsto, :yhat_lower, :yhat_upper)
            ON CONFLICT (modelo_key, tempo_ref_key, tempo_alvo_key) DO UPDATE SET
                valor_previsto = EXCLUDED.valor_previsto,
                yhat_lower     = EXCLUDED.yhat_lower,
                yhat_upper     = EXCLUDED.yhat_upper
        """)
        try:
            with engine.begin() as conn:
                conn.execute(sql_insert, atualizacoes)
            print("  Previsoes guardadas com sucesso!")
        except Exception as e:
            print(f"  [ERRO] Ao fazer o update: {e}")
    else:
        print("  Nenhuma previsao foi gerada.")

    engine.dispose()
    print("=" * 60)


if __name__ == "__main__":
    run_prophet()
