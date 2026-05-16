import os
import pandas as pd
from sqlalchemy import create_engine, text

# Correct credentials from load_to_postgres.py
DB_USER = os.environ.get("DB_USER", "ae_user")
DB_PASS = os.environ.get("DB_PASS", "ae_pass_2026")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "auto_escala")

URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(URL)

tables = [
    "dim_marca", "dim_modelo", "dim_tipo", "dim_combustivel", "dim_localizacao", "dim_stand",
    "dim_veiculo", "dim_cliente", "dim_demografia_regional",
    "fct_venda", "fct_inventario_mensal", "fact_trends", "fact_forum_sentiment", "fct_hashtag_volume",
    "data_quality_log"
]

print("=== CONTAGEM DE REGISTOS DW ===")
with engine.connect() as conn:
    for t in tables:
        try:
            res = conn.execute(text(f"SELECT COUNT(*) FROM auto_escala_dw.{t}")).scalar()
            print(f"{t:25}: {res}")
        except Exception as e:
            print(f"{t:25}: ERRO ({e})")

print("\n=== QUALIDADE DE DADOS (TOP 10) ===")
try:
    df_dq = pd.read_sql("SELECT * FROM auto_escala_dw.data_quality_log ORDER BY data_run DESC LIMIT 10", engine)
    print(df_dq[["fonte", "total_registos", "registos_ok", "registos_quarentena", "campo_mais_nulo"]])
except:
    print("Erro ao ler data_quality_log")
