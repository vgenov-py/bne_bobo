from pymarc import MARCReader
import zipfile
import sqlite3
import requests as req
from models import create_statements
import time
from humanizer import *
import concurrent.futures
from os import system

s = time.perf_counter()
real_s = s
datasets = {
    "vid": "VIDEO",
    "par": "MUSICAESC",
    "geo": "GEOGRAFICO",
    "per": "PERSONA", 
    "mon": "MONOMODERN", 
    "moa": "MONOANTIGU", 
    "ent": "ENTIDAD", 
    "ser": "SERIADA", 
    "mss": "MANUSCRITO"
}

urls = (
    "https://www.bne.es/redBNE/SuministroRegistros/Bibliograficos/VIDEO.zip",
    "https://www.bne.es/redBNE/SuministroRegistros/Bibliograficos/MUSICAESC.zip",
    "https://www.bne.es/redBNE/SuministroRegistros/Autoridades/GEOGRAFICO.zip",
    "https://www.bne.es/redBNE/SuministroRegistros/Bibliograficos/MONOMODERN.zip",
    "https://www.bne.es/redBNE/SuministroRegistros/Bibliograficos/MANUSCRITO.zip",
    "https://www.bne.es/redBNE/SuministroRegistros/Autoridades/PERSONA.zip",
    "https://www.bne.es/redBNE/SuministroRegistros/Autoridades/ENTIDAD.zip",
    "https://www.bne.es/redBNE/SuministroRegistros/Bibliograficos/SERIADA.zip",
    "https://www.bne.es/redBNE/SuministroRegistros/Bibliograficos/MONOANTIGU.zip"
)

with concurrent.futures.ThreadPoolExecutor() as executor:
    def a(url):
        print(f"Accediendo a {url.split('/')[-1]}")
        res = req.get(url)
        z_file_name = re.findall("\w{1,}\.zip", res.url)[0]
        z_file = open(z_file_name, "wb")
        print(f"Escribiendo {z_file_name}")
        z_file.write(res.content)
        z_file.close()
        z_file = zipfile.ZipFile(z_file_name, "r")
        z_file.extractall()
        z_file.close()
    tuple(executor.map(a,urls))


print(time.perf_counter()-s)
con = sqlite3.connect("bne.db")
cur.execute(create_statements["queries"])
cur = con.cursor()

for dataset, mrc_file in datasets.items():
    # if dataset not in ("vid", "geo"):
    #     continue
    with open(f"{mrc_file}.mrc", "rb") as file:
        reader = MARCReader(file, force_utf8=True)
        cur.execute(create_statements[f"{dataset}"])
        con.commit()
        mf = marc_fields(dataset)
        def insert(data):
            counter = create_statements[f"{dataset}"].count("\n") -3
            query = f'''insert or ignore into {dataset} values ({'?, '*counter})'''
            query = query.replace(", )", ")")
            print(f"Insertando datos en {dataset}...")
            try:
                cur.executemany(query,filter(lambda d:d,data))
            except ValueError:
                print(data)
            con.commit()
        def mapper(record):
            if not record:
                return
            to_extract = {}
            old_t = None
            fields = record.as_dict()["fields"]
            for f in fields:
                t,v = tuple(f.items())[0]
                try:
                    subfields = v.get("subfields")
                except:
                    pass
                if type(v) == dict and subfields:
                    v = ""
                    for sf in subfields:
                        t_sf, v_sf = tuple(sf.items())[0]
                        v += f"|{t_sf} {v_sf}"
                    to_extract[t] = f'{to_extract[t]} /**/ {v}' if old_t == t else v 
                else:
                    to_extract[t] = f"{to_extract[t]} |a {v}" if old_t == t else f"|a {v}"
                old_t = t
            return extract_values(dataset,to_extract)

        data = map(lambda a:mapper(a), reader)
        insert(data)



# system("rm -r *.mrc && rm -r *.zip")
print(time.perf_counter() - real_s)

