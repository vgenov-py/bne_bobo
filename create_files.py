from os import system
from constants import datasets
import sqlite3
import csv
from zipfile import ZipFile
import zipfile

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
    with ZipFile(file_name.replace("csv", "zip"), 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(file_name)
    with ZipFile(f"{file_name[:-8]}CP1252.csv".replace("csv", "zip"), 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(f"{file_name[:-8]}CP1252.csv")
    with ZipFile(f"{file_name[:-8]}ODS.ods".replace("ods", "zip"), 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(f"{file_name[:-8]}ODS.ods")

def export_txt(dataset:str) -> None:
    print(f"Exportando {dataset} en TXT-UTF8")
    file_name = f"{datasets[dataset].lower()}-TXT.txt"
    query = f"SELECT {human_fields(dataset)} FROM {dataset};"
    command = f'''sqlite3 bne.db -header -csv -separator "|" " {query} " > {file_name}'''
    system(command)
    with ZipFile(file_name.replace("txt", "zip"), 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(file_name)

def export_json(dataset:str) -> None:
    print(f"Exportando {dataset} en JSON")
    file_name = f"{datasets[dataset].lower()}-JSON.json"
    query = f"SELECT {human_fields(dataset)} FROM {dataset};"
    command = f'''sqlite3 bne.db -json " {query} " > {file_name}'''
    system(command)
    with ZipFile(file_name.replace("json", "zip"), 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(file_name)

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
    file_name = f"{datasets[dataset].lower()}-XML.xml"
    with open(file_name, "w", encoding="utf-8") as file:
        file.write(data)
    with ZipFile(file_name.replace("xml", "zip"), 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(file_name)

def export_xml_2(dataset: str) -> None:
    print(f"Exportando {dataset} en XML")
    cap_record = 200000
    file_name = f"{datasets[dataset].lower()}-XML.xml"
    system(f"rm {file_name}")
    headers = human_fields(dataset)
    query = f"SELECT {headers} FROM {dataset};"
    cur.execute(query)
    with open(file_name, "w", encoding="utf-8") as file:
        file.write('''<?xml version="1.0" encoding="UTF-8"?>\n<list>\n''')
    headers = headers.split(",")
    while True:
        try:
            data = cur.fetchmany(cap_record)
            if not data:
                break
            else:
                for row in data:
                    to_add = "    <item>\n"
                    for i, dc in enumerate(row):
                        header = headers[i].strip()
                        if dc:
                            to_add += f"            <{header}>{dc}</{header}>\n"
                        else:
                            continue
                    to_add += "    </item>\n"
                    with open(file_name, "a", encoding="utf-8") as file:
                        file.write(to_add)
        except:
            raise
    with open(file_name, "a", encoding="utf-8") as file:
        file.write("</list>\n")
    with ZipFile(file_name.replace("xml", "zip"), 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(file_name)

def export_mrc_xml_2(dataset: str) -> None:
    print(f"Exportando {dataset} en MRC XML")
    cap_record = 200000
    file_name = f"{datasets[dataset].lower()}-MARCXML.xml"
    system(f"rm {file_name}")
    headers = marc_fields(dataset)
    query = f"SELECT {headers} FROM {dataset};"
    cur.execute(query)
    with open(file_name, "w", encoding="utf-8") as file:
        file.write('''<?xml version="1.0" encoding="UTF-8"?>\n<list>\n''')
    headers = headers.split(",")
    while True:
        try:
            data = cur.fetchmany(cap_record)
            if not data:
                break
            else:
                for row in data:
                    to_add = "    <item>\n"
                    for i, dc in enumerate(row):
                        header = headers[i].strip()
                        if dc:
                            to_add += f"            <{header}>{dc}</{header}>\n"
                        else:
                            continue
                    to_add += "    </item>\n"
                    with open(file_name, "a", encoding="utf-8") as file:
                        file.write(to_add)
        except:
            raise
    with open(file_name, "a", encoding="utf-8") as file:
        file.write("</list>\n")
    with ZipFile(file_name.replace("xml", "zip"), 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(file_name)

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
    file_name = f"{datasets[dataset].lower()}-MARCXML.xml"
    with open(f"{datasets[dataset].lower()}-MARCXML.xml", "w", encoding="utf-8") as file:
        file.write(result)
    with ZipFile(file_name.replace("xml", "zip"), 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(file_name)

if __name__ == "__main__":
    from time import perf_counter
    s = perf_counter()
    dataset = input("DATASET: ")
    export_csv(dataset)
    export_txt(dataset)
    export_json(dataset)
    export_xml_2(dataset)
    export_mrc_xml_2(dataset) # \\bne.local\bns02\SGA\BNELAB\Proyectos\Datos-Reutilización
    print(perf_counter() - s)