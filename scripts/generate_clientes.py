import csv
import random
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_ROOT = BASE_DIR / "data" / "sources" / "clientes"
SEED = 42
NUM_CLIENTES = 5000

DISTRITOS = [
    "Aveiro", "Beja", "Braga", "Bragança", "Castelo Branco", "Coimbra",
    "Évora", "Faro", "Guarda", "Leiria", "Lisboa", "Portalegre", "Porto",
    "Santarém", "Setúbal", "Viana do Castelo", "Vila Real", "Viseu"
]

NOMES_MASC = ["João", "Tiago", "Rui", "José", "António", "Manuel", "Carlos", "Pedro", "Luís", "Miguel", "Nuno", "Ricardo", "Hugo", "Bruno", "Diogo"]
NOMES_FEM = ["Maria", "Ana", "Margarida", "Sofia", "Catarina", "Inês", "Joana", "Marta", "Diana", "Sara", "Beatriz", "Teresa", "Patrícia", "Cláudia", "Rita"]
APELIDOS = ["Silva", "Santos", "Ferreira", "Pereira", "Oliveira", "Costa", "Rodrigues", "Martins", "Jesus", "Sousa", "Fernandes", "Gomes", "Marques", "Almeida", "Ribeiro"]

def generate_nif(rng: random.Random) -> str:
    # NIFs de pessoas singulares começam por 1, 2 ou 3
    first = rng.choice([1, 2, 3])
    rest = [rng.randint(0, 9) for _ in range(7)]
    base = [first] + rest
    
    # Cálculo do dígito de controlo
    check_sum = sum(digit * (9 - i) for i, digit in enumerate(base))
    remainder = check_sum % 11
    check_digit = 0 if remainder in [0, 1] else 11 - remainder
    
    return "".join(map(str, base)) + str(check_digit)

def generate_clientes():
    rng = random.Random(SEED)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_ROOT / "clientes_ativos.csv"
    
    used_nifs = set()
    
    clientes = []
    
    for _ in range(NUM_CLIENTES):
        while True:
            nif = generate_nif(rng)
            if nif not in used_nifs:
                used_nifs.add(nif)
                break
        
        genero = rng.choice(["M", "F"])
        if genero == "M":
            nome = f"{rng.choice(NOMES_MASC)} {rng.choice(APELIDOS)} {rng.choice(APELIDOS)}"
        else:
            nome = f"{rng.choice(NOMES_FEM)} {rng.choice(APELIDOS)} {rng.choice(APELIDOS)}"
            
        idade = int(rng.gauss(42, 12))
        idade = max(18, min(idade, 85))
        
        distrito = rng.choices(DISTRITOS, weights=[
            5, 2, 10, 2, 2, 6,
            2, 5, 2, 6, 25, 1, 18,
            4, 8, 3, 3, 4
        ])[0] # Pesos aproximados à população real
        
        clientes.append({
            "nif": nif,
            "nome": nome,
            "idade": idade,
            "genero": genero,
            "distrito": distrito
        })
        
    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["nif", "nome", "idade", "genero", "distrito"])
        writer.writeheader()
        writer.writerows(clientes)
        
    print(f"Gerados {NUM_CLIENTES} clientes em {output_file}")

if __name__ == "__main__":
    generate_clientes()
