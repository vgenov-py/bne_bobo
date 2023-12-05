from os import system
from constants import datasets
import sqlite3
import csv

con = sqlite3.connect("bne.db")
cur = con.cursor()
def human_fields(dataset) -> list:
    result = ""
    fields = tuple(filter(lambda f: not f.startswith("t_"), (row[1] for row in cur.execute(f"pragma table_info({dataset});"))))
    for field in fields:
        result += f"{field}, "
    return result[:-2]

def marc_fields(dataset) -> list:
    result = ""
    fields = tuple(filter(lambda f: f.startswith("t_"), (row[1] for row in cur.execute(f"pragma table_info({dataset});"))))
    for field in fields:
        result += f"{field}, "
    return result[:-2]
'''
sqlite3 bne.db -header -csv -separator ";" " {query} " > {file_name}.csv
sqlite3 bne.db -json " {self.query(count=False, limit=False)} " > {file_name}.json
'''

def export_csv(dataset:str) -> None:
    print(f"Exportando {dataset} en CSV-UTF8")
    file_name = f"{datasets[dataset].lower()}-UTF8.csv"
    query = f"SELECT {human_fields(dataset)} FROM {dataset};"
    command = f'''sqlite3 bne.db -header -csv -separator ";" " {query} " > {file_name}'''
    system(command)
    print(f"Exportando {dataset} en CSV-CP1252")
    system(f"cp {file_name} ./{file_name[:-8]}CP1252.csv")
    print(f"Exportando {dataset} en ODS")
    system(f"cp {file_name} ./{file_name[:-8]}ODS.ods")

def export_txt(dataset:str) -> None:
    print(f"Exportando {dataset} en TXT-UTF8")
    file_name = f"{datasets[dataset].lower()}-TXT.txt"
    query = f"SELECT {human_fields(dataset)} FROM {dataset};"
    command = f'''sqlite3 bne.db -header -csv -separator "|" " {query} " > {file_name}'''
    system(command)

def export_json(dataset:str) -> None:
    print(f"Exportando {dataset} en JSON")
    file_name = f"{datasets[dataset].lower()}-JSON.json"
    query = f"SELECT {human_fields(dataset)} FROM {dataset};"
    command = f'''sqlite3 bne.db -json " {query} " > {file_name}'''
    system(command)

def export_xml(dataset:str) -> None:
    print(f"Exportando {dataset} en XML")
    file = open(f"{datasets[dataset].lower()}-UTF8.csv")
    csv_reader = csv.reader(file, delimiter=";")
    headers = next(csv_reader)
    data = '''<?xml version="1.0" encoding="UTF-8"?>\n<list>\n'''
    for row in csv_reader:
        to_add = "    <item>\n"
        for i, dc in enumerate(row):
            header = headers[i]
            if dc:
                to_add += f"            <{header}>{dc}</{header}>\n"
            else:
                continue
        to_add += "    </item>\n"
        data += to_add
    data += "</list>\n"
    file.close()
    with open(f"{datasets[dataset].lower()}-XML.xml", "w", encoding="utf-8") as file:
        file.write(data)
    
def export_mrc_xml(dataset:str) -> None:
    print(f"Exportando {dataset} en MARC XML")
    headers = marc_fields(dataset)
    query = f"SELECT {headers} FROM {dataset};"
    headers = headers.split(",")
    data = cur.execute(query)
    result = '''<?xml version="1.0" encoding="UTF-8"?>\n<list>\n'''
    for row in data:
        to_add = "    <item>\n"
        for i, dc in enumerate(row):
            header = headers[i]
            if dc:
                to_add += f"            <{header}>{dc}</{header}>\n"
            else:
                continue
        to_add += "    </item>\n"
        result += to_add
    result += "</list>\n"
    with open(f"{datasets[dataset].lower()}-MARCXML.xml", "w", encoding="utf-8") as file:
        file.write(result)

if __name__ == "__main__":
    from time import perf_counter
    s = perf_counter()
    export_csv("geo")
    export_txt("geo")
    export_json("geo")
    export_xml("geo")
    export_mrc_xml("geo")
    print(perf_counter() - s)