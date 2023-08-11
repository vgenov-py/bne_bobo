from pymarc import MARCReader
import zipfile
import sqlite3
import requests as req
from models import create_statements
import time
from humanizer import *
import concurrent.futures
s = time.perf_counter()

datasets = {
    "geo": "GEOGRAFICO", "per": "PERSONA", "mon": "MONOMODERN", "moa": "MONOANTIGU", "ent": "ENTIDAD"
}
    
urls = (
    "https://www.bne.es/redBNE/SuministroRegistros/Bibliograficos/MONOMODERN.zip",
    "https://www.bne.es/redBNE/SuministroRegistros/Autoridades/PERSONA.zip",
        "https://www.bne.es/redBNE/SuministroRegistros/Autoridades/GEOGRAFICO.zip",
        "https://www.bne.es/redBNE/SuministroRegistros/Autoridades/ENTIDAD.zip",
        "https://www.bne.es/redBNE/SuministroRegistros/Bibliograficos/MONOANTIGU.zip"
        )

with concurrent.futures.ThreadPoolExecutor() as executor:
    def a(url):
        print(url)
        res = req.get(url)
        z_file_name = re.findall("\w{1,}\.zip", res.url)[0]
        z_file = open(z_file_name, "wb")
        z_file.write(res.content)
        z_file.close()
        z_file = zipfile.ZipFile(z_file_name, "r")
        z_file.extractall()
        z_file.close()
    tuple(executor.map(a,urls))

print(time.perf_counter()-s)
con = sqlite3.connect("bne.db")
cur = con.cursor()

dataset = "geo"

for dataset, mrc_file in datasets.items():
    with open(f"{mrc_file}.mrc", "rb") as file:
        reader = MARCReader(file, force_utf8=True)
        cur.execute(create_statements[f"{dataset}_fts"])
        con.commit()
        mf = marc_fields(dataset)
        def insert(data):
            query = f"insert or ignore into {dataset}_fts values ({'?, '*len(available_fields(dataset))})"
            query = query.replace(", )", ")")
            print(query.center(50 + len(query), "#"))

            # for x in data:
            #     try:
            #         if x:
            #             cur.execute(query, x)
            #     except:
            #         print("ERROR:")
            #         print(x)
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
                # if t not in mf:
                #     continue
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

print(time.perf_counter() - s)