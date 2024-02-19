# Aplicación de conversión

### La aplicación de conversión permite crear el fichero bne.db

### Instalación Windows

1. Descargar repositorio
2. Crear entorno virtual
```
python -m venv venv
```
3. Activar entorno virtual
```
venv/Scripts/activate
```
* Verás sobre el path de la terminal **(venv)**

4. Instalar dependencias 
```
pip install -r requirements.txt
```

5. Ejecutar **main.py**
```
python main.py
```

### Consideraciones

1. Para descargar los conjuntos activar la siguiente línea en **main** --> **tuple(executor.map(a,urls))**

### Crear ficheros para datosabiertos

* Todos los ficheros con sus respectivas extensiones, son generados a partir de la base de datos **bne.db**, la misma debe ser generada **previamente** a la ejecucción de **create_files.py**

1. Ejecutar **create_files.py**

* Todos los ficheros serán generados correctamente en bucle con la siguiente denominación:

```
{fichero}-{ENCODING||FILE_EXTENSION}.zip
```
* **Ejemplo:** geografico-TXT.zip - geografico-UTF8.zip
* El nivel de compresión se ha mantenido constante

### Actualización CKAN

* Para actualizar la última fecha de modificación de un **paquete***, debemos seguir los siguientes pasos
* *paquete/package: Entidad que contiene todos los ficheros asociados de un determinado dataset
* Se utilizará como ejemplo el paquete vito_test

1. Obtener id del paquete a modificar

```
GET https://datosabiertos.bne.es/api/3/action/package_show?id=vito_test
```

```json
//Respuesta:
{
  "help": "https://datosabiertos.bne.es/api/3/action/help_show?name=package_patch",
  "success": true,
  "result": {
    "author": "",
    "author_email": "",
    "creator_user_id": "d754bf30-75a8-428d-8ffb-deb8c28bcec2",
    "id": "8dbc5bfb-e141-4de6-b1dd-18503eda2629",
    "isopen": false,
    "license_id": "",
    "license_title": "",
    "maintainer": "",
    "maintainer_email": "",
    "metadata_created": "2023-12-12T09:08:12.163990",
    "metadata_modified": "2023-12-12T09:29:35.852447"
  }
}
```
2. Ejecutar la acción **package_patch** con método **POST** y agregar al body el identificador

```
POST https://datosabiertos.bne.es/api/3/action/package_patch?id=vito_test
```
```json
//body
{
  "id":"8405ee20-c145-4cd7-bdd4-539b39cb1a86"
}
```


El parámetro **metadata_modified**, se actualizará automáticamente.