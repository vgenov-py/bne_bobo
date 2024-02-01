### Generar bne.db

1. Abrir **Powershell**
2. Ejecutar el comando **wsl** y presionar **Enter**
```
$ wsl
```
3. Una vez abierta la terminal de Ubuntu, ejecutar:
```
$ cd /home/bnelab/bne_converter
```
* Si la ruta no existe, seguir los siguientes **pasos alternativos**:
```
1. cd /home
2. ls
# del usuario listado hacer cd al mismo 
4. cd bnelab
5. cd bne_converter
```
4. Activar entorno virtual
```
$ source venv/bin/activate
```
* Debería ver **(venv)** sobre la ruta
5. Ejecutar la aplicación principal (creación del fichero **bne.db**)
```
$ python3 main.py
```
Verá:
1. Todos
2. Dataset
3. Salir

Si elige todos, se crearán los 12 conjuntos en una misma db. Si elige dataset, se creará un solo conjunto en una db aislada, ejemplo **geo** creará **geo.db**

### Generar ficheros para datosgob

#### ¡ATENCIÓN! Para que la aplicación funcione debe ser generado el fichero bne.db PREVIAMENTE

1. Ejecutar **create_files**
```
$ python3 create_files.py
```
2. Indicar dataset a generar (geo, per, ent) etc.
* Ejemplo con **geo**:
```DATASET: geo
Exportando geo en CSV-UTF8
Exportando geo en CSV-CP1252
Exportando geo en ODS
Exportando geo en TXT-UTF8
Exportando geo en JSON
Exportando geo en XML
Exportando geo en MRC XML
rm: cannot remove 'geografico-MARCXML.xml': No such file or directory
9.92277484200008
```
* No preocuparse por el mensaje **rm**, es normal
3. Una vez terminado el fichero tendrá todos los ficheros con su correcta denominación en el directorio :)
