### Tutorial uso de la aplicación

La aplicación **BNE converter**, permite generar la **DB** en base a los ficheros de suministros que puedes encontrar [aquí](https://www.bne.es/es/servicios/servicios-para-bibliotecarios/suministro-registros/descarga-ficheros). Desde el momento que se haya generado la **DB**, se podrán crear también los datasets en todas las extensiones disponibles en [datos.gob](https://datos.gob.es/es/catalogo/ea0019768-catalogo-de-autoridades-geografico)

#### Para éste tutorial, vamos a imaginarnos que queremos agregar una etíqueta/campo MARC al conjunto geo

Ésta información debería ser extraida de los excels suministrados por la **BNE**

1. Ver etiqueta y campo humano a agregar, normalmente aparecerá de la siguiente manera:
```
tag|tag_public|notas
610|nombre_entidad_coorporativa|$a($b)
```
* Si **$a**: Mondelez y **$b**: RRHH, entonces la conversión será: $aModelez$bRRHH **->** **Mondelez(RRHH)**

2. Agregar el tag y el campo humano a **models.py**
```python
query_create_geo ='''
    CREATE VIRTUAL TABLE IF NOT EXISTS geo USING FTS5(
        id,
        t_001,
        t_024,
        t_034,
        t_080,
        t_151,
        t_451,
        t_510,
        t_550,
        t_551,
        --t_610--,
        t_667,
        t_670,
        t_781,
        otros_identificadores,
        coordenadas_lat_lng,
        CDU,
        nombre_de_lugar,
        otros_nombres_de_lugar,
        entidad_relacionada,
        materia_relacionada,
        lugar_relacionado,
        nota_general,
        --nombre_entidad_coorporativa--,
        fuentes_de_informacion,
        lugar_jerarquico,
        obras_relacionadas_en_el_catalogo_BNE
    );
'''
```
* No colocar los **--** en los nuevos campos

3. De ser necesario, crear una nueva función para extraer los valores de la manera correcta en **humanizer.py**
```python
def coorporative_entity(value_610: str) -> str:
    '''
    610: $a($b)
    '''
    if not value: # Es muy común que el campo no exista en el registro consultado
        return 
    # Tenemos disponible la función get_single_dollar para éste tipo de casos:
    result = get_single_dollar(value, "a") # Si hay valor, siempre tendrán $a
    d_b = get_single_dollar(value, "b")  # short for dollar_b
    if d_b:
        result += f"({d_b})"
    return result
```

4. Modificar **extract_values** en **humanizer.py**
```python
def extract_values(dataset:str ,record:dict) -> tuple:
    result = []
    if dataset == "geo":
        result.append(record.get("001")[3:])
        result.append(record.get("001"))
        result.append(record.get("024"))
        result.append(record.get("034"))
        result.append(record.get("080"))
        result.append(record.get("151"))
        result.append(record.get("451"))
        result.append(record.get("510"))
        result.append(record.get("550"))
        result.append(record.get("551"))
        # Nuevo campo 610:
        result.append(record.get("610"))
        result.append(record.get("667"))
        result.append(record.get("670"))
        result.append(record.get("781"))
        humans = []
        humans.append(other_identifiers(record.get("024")))
        humans.append(f_lat_lng(record.get("034")) if f_lat_lng(record.get("034")) else None)
        #CDU:
        humans.append(dollar_parser(record.get("080"))
        #nombre de lugar:
        humans.append(dollar_parser(record.get("151")))
        #otros nombres de lugar
        humans.append(dollar_parser(record.get("451")))
        #entidad relacionada
        humans.append(dollar_parser(record.get("510")))
        #materia relacionada
        humans.append(geo_related_subject(record.get("550")))
        #lugar relacionado
        humans.append(related_place(record.get("551")))
        #nota general
        humans.append(dollar_parser(record.get("667")))
        # NUEVO CAMPO nombre_entidad_coorporativa
        humans.append(coorporative_entity(record.get("610")))
        #fuentes de información
        humans.append(sources(record.get("670")))
        #lugar jerárquico
        humans.append(dollar_parser(record.get("781")))
        #obras relacionadas en el catálogo BNE
        humans.append(gen_url(record.get("001")))
        result.extend(humans)
```