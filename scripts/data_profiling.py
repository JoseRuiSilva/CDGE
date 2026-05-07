import os
import time
from pathlib import Path
from deltalake import DeltaTable

# ydata_profiling might not be installed in all environments.
# We will catch the import error and warn the user.
try:
    from ydata_profiling import ProfileReport
    YDATA_AVAILABLE = True
except ImportError:
    YDATA_AVAILABLE = False

BASE_DIR = Path(__file__).resolve().parent.parent

# Caminhos Bronze
BRONZE_INVENTARIO = str(BASE_DIR / "data_lake/bronze/inventario_delta")
BRONZE_TRENDS     = str(BASE_DIR / "data_lake/bronze/trends_delta")
BRONZE_FORUM      = str(BASE_DIR / "data_lake/bronze/forum_delta")
BRONZE_HASHTAGS   = str(BASE_DIR / "data_lake/bronze/hashtags_delta")

# Pasta de destino dos relatórios
REPORTS_DIR = BASE_DIR / "data" / "profiling_reports"

def generate_profile(delta_path: str, report_name: str, title: str):
    """
    Lê uma tabela Delta e gera um report HTML usando ydata-profiling.
    """
    if not YDATA_AVAILABLE:
        print("  [AVISO] ydata-profiling não está instalado. Não é possível gerar os relatórios.")
        print("  Executa: pip install ydata-profiling")
        return

    print(f"  A ler a tabela {delta_path}...")
    try:
        dt = DeltaTable(delta_path)
        df = dt.to_pandas()
    except Exception as e:
        print(f"  [ERRO] Não foi possível ler {delta_path}: {e}")
        return

    if df.empty:
        print(f"  [AVISO] Tabela {delta_path} está vazia. Profiling cancelado.")
        return

    print(f"  A gerar report '{title}' (pode demorar alguns minutos)...")
    profile = ProfileReport(df, title=title, minimal=True)
    
    out_file = REPORTS_DIR / f"{report_name}.html"
    profile.to_file(str(out_file))
    print(f"  Report guardado em: {out_file}")

def run_profiling():
    print("\n" + "=" * 60)
    print("  DATA PROFILING (ydata-profiling)")
    print("=" * 60)
    
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    
    inicio = time.time()
    
    generate_profile(BRONZE_INVENTARIO, "profiling_bronze_inventario", "Inventário Bronze")
    generate_profile(BRONZE_TRENDS, "profiling_bronze_trends", "Google Trends Bronze")
    generate_profile(BRONZE_FORUM, "profiling_bronze_forum", "Fórum Bronze")
    generate_profile(BRONZE_HASHTAGS, "profiling_bronze_hashtags", "Hashtags Bronze")
    
    print(f"\n  Profiling total concluído em {time.time()-inicio:.1f}s")
    print("=" * 60)

if __name__ == "__main__":
    run_profiling()
