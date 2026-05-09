import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_ROOT = BASE_DIR / "data" / "sources" / "demografia"

DISTRITOS = [
    "Aveiro", "Beja", "Braga", "Bragança", "Castelo Branco", "Coimbra",
    "Évora", "Faro", "Guarda", "Leiria", "Lisboa", "Portalegre", "Porto",
    "Santarém", "Setúbal", "Viana do Castelo", "Vila Real", "Viseu"
]

# Distribuição aproximada da população em Portugal por distrito
POPULACAO_BASE = {
    "Aveiro": 700000, "Beja": 144000, "Braga": 846000, "Bragança": 122000, 
    "Castelo Branco": 177000, "Coimbra": 408000, "Évora": 152000, "Faro": 467000, 
    "Guarda": 142000, "Leiria": 458000, "Lisboa": 2871000, "Portalegre": 104000, 
    "Porto": 1785000, "Santarém": 425000, "Setúbal": 877000, "Viana do Castelo": 231000, 
    "Vila Real": 185000, "Viseu": 351000
}

def generate_demografia():
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_ROOT / "demografia_regional.csv"
    
    anos = [2022, 2023, 2024]
    registos = []
    
    for distrito in DISTRITOS:
        base_pop = POPULACAO_BASE[distrito]
        
        # Simular diferenças regionais
        if distrito in ["Lisboa", "Porto", "Braga"]:
            # Distritos mais jovens
            p18_24, p25_34, p35_49, p50_64, p65m = 10.5, 14.5, 24.0, 26.0, 25.0
        elif distrito in ["Faro", "Aveiro", "Leiria", "Setúbal"]:
            # Distritos intermédios
            p18_24, p25_34, p35_49, p50_64, p65m = 9.0, 12.0, 22.0, 27.0, 30.0
        else:
            # Interior mais envelhecido
            p18_24, p25_34, p35_49, p50_64, p65m = 7.5, 10.0, 19.0, 25.5, 38.0
            
        for ano in anos:
            # Ligeira variação populacional por ano (+- 1%)
            variacao = 1 + ((ano - 2022) * 0.005)
            if distrito in ["Bragança", "Guarda", "Portalegre"]:
                variacao = 1 - ((ano - 2022) * 0.008) # Interior perde população
                
            pop_ano = int(base_pop * variacao)
            
            registos.append({
                "distrito": distrito,
                "ano_referencia": ano,
                "populacao_total": pop_ano,
                "pct_18_24": p18_24,
                "pct_25_34": p25_34,
                "pct_35_49": p35_49,
                "pct_50_64": p50_64,
                "pct_65_mais": p65m,
                "pct_masculino": 47.5, # Média PT
                "pct_feminino": 52.5
            })
            
    with output_file.open("w", newline="", encoding="utf-8") as f:
        fields = ["distrito", "ano_referencia", "populacao_total", "pct_18_24", "pct_25_34", "pct_35_49", "pct_50_64", "pct_65_mais", "pct_masculino", "pct_feminino"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(registos)
        
    print(f"Gerados {len(registos)} registos demográficos em {output_file}")

if __name__ == "__main__":
    generate_demografia()
