from pymarc import MARCReader
import zipfile
import re
import sqlite3
import requests as req
from models import create_statements
import time
import pstats
from pstats import SortKey
import cProfile
from humanizer import *

urls = ("https://www.bne.es/redBNE/SuministroRegistros/Bibliograficos/MONOMODERN.zip", "https://www.bne.es/redBNE/SuministroRegistros/Autoridades/PERSONA.zip", "https://www.bne.es/redBNE/SuministroRegistros/Autoridades/GEOGRAFICO.zip", "https://www.bne.es/redBNE/SuministroRegistros/Autoridades/ENTIDAD.zip", "https://www.bne.es/redBNE/SuministroRegistros/Bibliograficos/MONOANTIGU.zip")

datasets = {
    "geo": "GEOGRAFICO", "per": "PERSONA", "mon": "MONOMODERN", "moa": "MONOANTIGU", "ent": "ENTIDAD"
}

s = time.perf_counter()
# res = req.get(urls[0])

# with open("q.zip", "wb") as file:
#     file.write(res.content)

# with zipfile.ZipFile("q.zip", "r") as f:
#     f.extractall()

con = sqlite3.connect("bne.db")
cur = con.cursor()

dataset = "mon"


# with cProfile.Profile() as pr:
with open(f"{datasets[dataset]}.mrc", "rb") as file:
    reader = MARCReader(file, force_utf8=True)
    cur.execute(create_statements[f"{dataset}_fts"])
    con.commit()
    mf = marc_fields(dataset)
    def insert(data):
        query = f"insert or ignore into {dataset}_fts values ({'?, '*len(available_fields(dataset))})"
        query = query.replace(", )", ")")
        print(query.center(50 + len(query), "#"))

        for x in data:
            try:
                if x:
                    cur.execute(query, x)
            except:
                print("ERROR:")
                print(x)
        # try:
        #     cur.executemany(query,data)
        # except ValueError:
        #     print(data)
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
        # for a in extract_values("geo",to_extract):
        #     print(a)

        #     input()
# p = pstats.Stats(pr)
# p.strip_dirs().sort_stats(-1).print_stats()
# p.sort_stats(SortKey.TIME)
# p.print_stats()

print(time.perf_counter() - s)