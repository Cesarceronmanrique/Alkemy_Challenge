# Challenge Data Analytics - Python
_Este proyecto desarrollado en Python presenta la soluci贸n al Challenge Data Analytics de Alkemy._

## Comenzando 馃殌
_Para ejecutar el proyecto es importante trabajar en un ambiente virtual que contenga los paquetes y/o librerias necesarias._

### Pre-requisitos e Instalaci贸n 馃敡馃搵
En primer lugar, se instala el paquete virtualenv y se activa el ambiente virtual:

    virtualenv -p python env

    .\env\Scripts\activate

Finalmente, se instalan los paquetes contenidos en el archivo "requirements.txt":

    python -m pip install -r requirements.txt

## Ejecutando las pruebas 鈿欙笍

_Para realizar la ejecuci贸n del programa es importante contar con todos los archivos .py, .sql y crear un archivo .env que contiene las credenciales de la conexi贸n a la base de datos (Por seguridad este archivo se incluye en el .gitignore)._

Los archivos "Dataframes.py", "DB.py" y "Peticion.py" son m贸dulos que contienen funciones para ser ejecutadas desde el archivo principal "Alkemy.py" que contiene la funci贸n main. Por otro lado los archivos con extensi贸n .sql contienen los comandos para la creaci贸n de la base de datos.

Se debe ejecutar el archivo principal de la siguiente manera:

    python .\src\Alkemy.py

**Recuerda cambiar los valores del dia, fecha y a帽o.**

### 驴Logs? 馃摝

_Los log son generados en el archivo Main.log_

---
鈱笍 con 鉂わ笍 por [C茅sar Cer贸n Manrique](https://github.com/Cesarceronmanrique) 馃槉