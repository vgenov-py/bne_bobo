# Aplicación de conversión

### La aplicación de conversión permite crear el fichero bne.db

### Instalación Windows

1. Abrir terminal git bash

2. Descargar repositorio
```
git clone https://github.com/vgenov-py/bne_converter
```
3. Cambiar directorio **bne_converter**
```
cd bne_converter
```
4. Crear entorno virtual
```
python -m venv venv
```
5. Activar entorno virtual
```
venv/Scripts/activate
```
* Verás sobre el path de la terminal **(venv)**

6. Instalar dependencias 
```
pip install -r requirements.txt
```

7. Ejecutar **main.py**
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
