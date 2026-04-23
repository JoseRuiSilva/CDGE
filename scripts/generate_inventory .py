from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable


BASE_DIR=Path(__file__).resolve().parent.parent
OUTPUT_ROOT=BASE_DIR/"data"/"sources"/"stands"
START_DATE=date(2022,1,1)
END_DATE=date(2024,12,31)
SEED=42
ERROR_PROBABILITY=0.01

COLUMNS=[
    "id_viatura",
    "matricula",
    "marca",
    "modelo",
    "tipo_automovel",
    "num_lugares",
    "ano_viatura",
    "combustivel",
    "quilometragem",
    "preco_aquisicao",
    "data_entrada_stock",
    "preco_venda",
    "data_venda",
    "stand",
]

STANDS=["lisboa","porto","braga"]

@dataclass(frozen=True)
class VehicleModel:
    marca: str
    modelo: str
    tipo: str
    num_lugares: int
    combustiveis: tuple[str,...]
    base_price: int
    age_target: int
    km_range: tuple[int,int]

VEHICLES:tuple[VehicleModel,...]=(
    VehicleModel("Mercedes","GLA","SUV",5,("Gasolina","Híbrido a Gasolina","Gasóleo"),28500,3,(18000,95000)),
    VehicleModel("BMW","X1","SUV",5,("Gasóleo","Gasolina","Híbrido a Gasolina"),27600,4,(22000,110000)),
    VehicleModel("Volkswagen","Tiguan","SUV",5,("Gasóleo","Gasolina","Híbrido a Gasolina"),24800,5,(35000,125000)),
    VehicleModel("Peugeot","3008","SUV",5,("Gasóleo","Gasolina","Híbrido a Gasolina"),22100,4,(26000,118000)),
    VehicleModel("Nissan","Qashqai","SUV",5,("Gasolina","Gasóleo","Híbrido a Gasolina"),21500,5,(30000,130000)),
    VehicleModel("Seat","Arona","SUV",5,("Gasolina",),16800,3,(12000,82000)),
    VehicleModel("Volkswagen","Golf","Hatchback",5,("Gasolina","Gasóleo","Híbrido a Gasolina"),18500,5,(30000,140000)),
    VehicleModel("Renault","Clio","Citadino",5,("Gasolina","Gasóleo","GPL"),12400,4,(25000,135000)),
    VehicleModel("Seat","Ibiza","Citadino",5,("Gasolina","Gasóleo","GPL"),11800,5,(32000,150000)),
    VehicleModel("Citroën","C3","Citadino",5,("Gasolina","Gasóleo"),11200,5,(30000,145000)),
    VehicleModel("Fiat","500","Citadino",4,("Gasolina","Híbrido a Gasolina"),13200,3,(15000,90000)),
    VehicleModel("Peugeot","208","Citadino",5,("Gasolina","Gasóleo","100% Elétrico"),14800,3,(14000,95000)),
    VehicleModel("BMW","Série 1","Hatchback",5,("Gasóleo","Gasolina","Híbrido a Gasolina"),20900,4,(18000,105000)),
    VehicleModel("Mercedes","Classe A","Hatchback",5,("Gasóleo","Gasolina","Híbrido a Gasolina"),21800,4,(20000,108000)),
    VehicleModel("Audi","A3","Sedan",5,("Gasóleo","Gasolina","Híbrido a Gasolina"),22500,4,(22000,112000)),
    VehicleModel("Tesla","Model 3","Elétrico",5,("100% Elétrico",),31800,2,(12000,85000)),
    VehicleModel("Renault","Zoe","Elétrico",5,("100% Elétrico",),16900,3,(10000,70000)),
    VehicleModel("Nissan","Leaf","Elétrico",5,("100% Elétrico",),17400,4,(18000,90000)),
    VehicleModel("Hyundai","Kona","SUV",5,("100% Elétrico","Gasolina","Híbrido a Gasolina"),22900,3,(14000,98000)),
    VehicleModel("Kia","Niro","SUV",5,("100% Elétrico","Híbrido a Gasolina"),23300,3,(16000,96000)),
)

PRICE_FACTOR={
    "Gasolina":1.00,
    "Gasóleo":1.03,
    "Híbrido a Gasolina":1.08,
    "Híbrido a Gasóleo":1.10,
    "100% Elétrico":1.18,
    "GPL":0.95,
}

MONTHLY_VOLUME={
    "lisboa":14,
    "porto":12,
    "braga":9,
}


def daterange_months(start:date,end:date)->Iterable[date]:
    current=date(start.year,start.month,1)
    while current<=end:
        yield current
        if current.month==12:
            current=date(current.year+1,1,1)
        else:
            current=date(current.year,current.month+1,1)


def seasonal_weights(month:int)->dict[str,float]:
    weights={
        "SUV":1.0,
        "Citadino":1.0,
        "Hatchback":1.0,
        "Sedan":0.9,
        "Elétrico":0.8,
    }
    if month in {11,12,1,2}:
        weights["SUV"]=1.8
        weights["Elétrico"]=0.7
    elif month in {6,7,8}:
        weights["Elétrico"]=1.8
        weights["SUV"]=0.95
        weights["Citadino"]=1.1
    elif month in {3,4,5,9,10}:
        weights["SUV"]=1.1
        weights["Elétrico"]=1.0
    return weights


def weighted_vehicle_choice(rng:random.Random,month:int)->VehicleModel:
    weights_by_type=seasonal_weights(month)
    candidates=list(VEHICLES)
    weights=[weights_by_type.get(vehicle.tipo,1.0) for vehicle in candidates]
    return rng.choices(candidates,weights=weights,k=1)[0]

def choose_fuel(rng:random.Random,vehicle:VehicleModel,month:int)->str:
    options=list(vehicle.combustiveis)
    if len(options)==1:
        return options[0]

    weights=[]
    for fuel in options:
        weight=1.0
        if fuel=="100% Elétrico" and month in {6,7,8}:
            weight=2.1
        elif fuel=="100% Elétrico":
            weight=0.8
        elif fuel=="Gasóleo" and month in {11,12,1,2}:
            weight=1.2
        elif fuel.startswith("Híbrido"):
            weight=1.15
        weights.append(weight)
    return rng.choices(options,weights=weights,k=1)[0]


def random_entry_date(rng:random.Random, month_start:date)->date:
    if month_start.month==12:
        next_month=date(month_start.year+1,1,1)
    else:
        next_month=date(month_start.year,month_start.month+1,1)
    last_day=(next_month-timedelta(days=1)).day
    return date(month_start.year,month_start.month,rng.randint(1,last_day))

def vehicle_year(rng:random.Random,entry_date:date,target_age:int)->int:
    max_year=entry_date.year-1
    min_year=max(2013,entry_date.year-target_age-4)
    if min_year>max_year:
        min_year=max_year
    likely_year=max(min_year,min(max_year,entry_date.year-max(1,target_age+rng.choice([-2,-1,0,1,2]))))
    if rng.random()<0.25:
        year=rng.randint(min_year,likely_year)
    else:
        year=likely_year
    return min(year,max_year)

def estimate_km(rng:random.Random,model:VehicleModel,year:int,entry_date:date)->int:
    age=max(1,entry_date.year-year)
    base_min,base_max=model.km_range
    expected=age*rng.randint(11000,19000)
    km=int((base_min+base_max)/2 *0.25+expected*0.75)
    km+=rng.randint(-12000,12000)
    km=max(base_min,min(base_max,km))
    return int(round(km/1000.0)*1000)

def acquisition_price(rng:random.Random,model:VehicleModel,fuel:str,year:int,entry_date:date,km:int)->int:
    age=max(1,entry_date.year-year)
    price=model.base_price*PRICE_FACTOR[fuel]
    price*=(0.92**age)
    price*=max(0.72,1-(km/280000))
    price*=rng.uniform(0.94,1.06)
    return int(round(price/100)*100)

def sale_info(rng:random.Random,entry_date:date,purchase_price:int)->tuple[str,str]:
    sold_probability=0.82
    if entry_date>date(2024,9,1):
        sold_probability=0.60
    elif entry_date>date(2024,6,1):
        sold_probability=0.72
    if rng.random()>sold_probability:
        return "",""
    days_to_sell=rng.randint(12,140)
    sale_date=entry_date+timedelta(days=days_to_sell)
    if sale_date>END_DATE:
        return "",""
    margin=rng.uniform(1.06,1.18)
    sale_price=int(round((purchase_price*margin)/100)*100)
    return str(sale_price),sale_date.isoformat()

def generate_plate(rng:random.Random,used:set[str])->str:
    while True:
        plate=f"{rng.randint(10,99)}-{chr(rng.randint(65,90))}{chr(rng.randint(65,90))}-{rng.randint(10,99)}"
        if plate not in used:
            used.add(plate)
            return plate

def inject_quality_issue(rng:random.Random,row:dict[str,object])->dict[str,object]:
    if rng.random()>=ERROR_PROBABILITY:
        return row
    broken=dict(row)
    error_type=rng.choice(
        [
            "marca_variation",
            "combustivel_case_space",
            "stand_case_space",
            "km_with_unit",
            "missing_num_lugares",
            "tipo_case_space",
        ]
    )

    if error_type=="marca_variation":
        replacements={
            "Volkswagen":"vw",
            "Mercedes":"mercedes-benz",
            "BMW":"bmw",
            "Citroën":"citroen",
        }
        broken["marca"]=replacements.get(str(broken["marca"]),str(broken["marca"]).lower())
    elif error_type=="combustivel_case_space":
        broken["combustivel"]=f"{str(broken['combustivel']).lower()} "
    elif error_type=="stand_case_space":
        broken["stand"]=f"{str(broken['stand']).upper()} "
    elif error_type=="km_with_unit":
        broken["quilometragem"]=f"{broken['quilometragem']} km"
    elif error_type=="missing_num_lugares":
        broken["num_lugares"]=""
    elif error_type=="tipo_case_space":
        broken["tipo_automovel"]=f"{str(broken['tipo_automovel']).lower()}"
    return broken

def generate_inventory()->dict[str,int]:
    rng=random.Random(SEED)
    OUTPUT_ROOT.mkdir(parents=True,exist_ok=True)
    used_plates:set[str]=set()
    vehicle_counter=1
    counts:dict[str,int]={}

    for stand in STANDS:
        stand_dir=OUTPUT_ROOT/stand
        stand_dir.mkdir(parents=True,exist_ok=True)
        row_count=0
        for month_start in daterange_months(START_DATE,END_DATE):
            file_name=f"{month_start.year}_{month_start.month:02d}_{stand}.csv"
            output_file=stand_dir/file_name

            with output_file.open("w",newline="",encoding="utf-8") as f:
                writer=csv.DictWriter(f,fieldnames=COLUMNS)
                writer.writeheader()
                monthly_target=MONTHLY_VOLUME[stand]+rng.randint(-2,3)
                monthly_target=max(6,monthly_target)

                for _ in range(monthly_target):
                        vehicle=weighted_vehicle_choice(rng,month_start.month)
                        fuel=choose_fuel(rng,vehicle,month_start.month)
                        entry_date=random_entry_date(rng,month_start)
                        year=vehicle_year(rng,entry_date,vehicle.age_target)
                        km=estimate_km(rng,vehicle,year,entry_date)
                        purchase=acquisition_price(rng,vehicle,fuel,year,entry_date,km)
                        sale_price,sale_date=sale_info(rng,entry_date,purchase)
                        row={
                                "id_viatura":f"V{vehicle_counter:06d}",
                                "matricula":generate_plate(rng,used_plates),
                                "marca":vehicle.marca,
                                "modelo":vehicle.modelo,
                                "tipo_automovel":vehicle.tipo,
                                "num_lugares":vehicle.num_lugares,
                                "ano_viatura":year,
                                "combustivel":fuel,
                                "quilometragem":km,
                                "preco_aquisicao":purchase,
                                "data_entrada_stock":entry_date.isoformat(),
                                "preco_venda":sale_price,
                                "data_venda":sale_date,
                                "stand":stand.capitalize(),
                            }
                        row=inject_quality_issue(rng,row)
                        writer.writerow(row)
                        vehicle_counter+=1
                        row_count+=1
        counts[stand]=row_count
    return counts

if __name__=="__main__":
    summary=generate_inventory()
    print("Ficheiros mensais gerados com sucesso:")
    for stand,total in summary.items():
        print(f"-{stand.capitalize()}:{total} registos")