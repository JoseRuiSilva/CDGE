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
DW_URL   = f"postgresql+psycopg://ae_user:ae_pass_2026@{_PG_HOST}:{_PG_PORT}/auto_escala"


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
        FROM auto_escala_dw.fct_tendencia f
        JOIN auto_escala_dw.dim_tempo t ON f.tempo_key = t.tempo_key
        JOIN auto_escala_dw.dim_fonte src ON f.fonte_key = src.fonte_key
        WHERE src.nome_fonte = 'Google Trends' AND f.valor_interesse IS NOT NULL
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

        # Identificar o ultimo mes disponivel (N) e a previsao para (N+1)
        tend_key_atual = int(df_mod.iloc[-1]['tendencia_key'])
        yhat_prox      = forecast.iloc[-1]['yhat']

        atualizacoes.append({
            'tendencia_key': tend_key_atual,
            'previsao':      float(max(0, yhat_prox)),
        })

    if atualizacoes:
        print(f"  A atualizar {len(atualizacoes)} previsoes no PostgreSQL...")
        sql_update = text("""
            UPDATE auto_escala_dw.fct_tendencia
            SET previsao_prox_mes = :previsao
            WHERE tendencia_key = :tendencia_key
        """)
        try:
            with engine.begin() as conn:
                conn.execute(sql_update, atualizacoes)
            print("  Previsoes escritas com sucesso na fct_tendencia!")
        except Exception as e:
            print(f"  [ERRO] Ao fazer o update: {e}")
    else:
        print("  Nenhuma previsao foi gerada.")

    engine.dispose()
    print("=" * 60)


if __name__ == "__main__":
    run_prophet()
