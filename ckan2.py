import sqlite3
import re
import time
import json
import csv
from pyexcel_ods3 import save_data
import xml.etree.ElementTree as ET
import tqdm

DBFILE = "bne.db"

# cur = ""   

def available_fields(dataset:str) -> list:
    cur = sqlite3.connect(DBFILE).cursor()
    return [row[1] for row in cur.execute(f"pragma table_info({dataset}_fts);")]

def get_all(dataset):
    cur = sqlite3.connect(DBFILE).cursor()
    print(cur.connection)
    return cur.execute(f"SELECT * FROM {dataset}_fts;")

def write_csv(dataset, encoding:str="utf-8"):
    with open(f"files/{dataset}-{encoding}.csv", "w", encoding=encoding, errors="ignore") as file:
        fields = available_fields(dataset)
        csv_writer = csv.writer(file, delimiter=";")
        csv_writer.writerow(fields)
        tuple(map(lambda record: csv_writer.writerow(record), get_all(dataset)))

def write_xml(dataset:str):
    data = tuple(get_all(dataset))
    lists = ET.Element("list")
    for record in data:
        item = ET.SubElement(lists,"item")
        for k,v in zip(available_fields(dataset),record):
            key = ET.SubElement(item, k)
            key.text = v
    tree = ET.ElementTree(lists)
    tree.write(f'files/{dataset}.xml')

# write_xml("geo")

def write_all(dataset:str):
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor() as executor:
        a = executor.submit(write_csv, dataset)
        b = executor.submit(write_xml, dataset)
        a.result()
        b.result()

datasets = ["geo", "ent"]
import concurrent.futures
with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(write_all, datasets)
    # pass