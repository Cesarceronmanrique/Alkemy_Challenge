# Challenge Data Analytics - Python
_Este proyecto desarrollado en Python presenta la solución al Challenge Data Analytics de Alkemy._

## Comenzando 🚀
_Para ejecutar el proyecto es importante trabajar en un ambiente virtual que contenga los paquetes y/o librerias necesarias._

### Pre-requisitos e Instalación 🔧📋
En primer lugar, se instala el paquete virtualenv y se activa el ambiente virtual:

    virtualenv -p python env

    .\env\Scripts\activate

Finalmente, se instalan los paquetes contenidos en el archivo "requirements.txt":

    python -m pip install -r requirements.txt

## Ejecutando las pruebas ⚙️

_Para realizar la ejecución del programa es importante contar con todos los archivos .py, .sql y crear un archivo .env que contiene las credenciales de la conexión a la base de datos (Por seguridad este archivo se incluye en el .gitignore)._

Los archivos "Dataframes.py", "DB.py" y "Peticion.py" son módulos que contienen funciones para ser ejecutadas desde el archivo principal "Alkemy.py" que contiene la función main. Por otro lado los archivos con extensión .sql contienen los comandos para la creación de la base de datos.

Se debe ejecutar el archivo principal de la siguiente manera:

    python .\src\Alkemy.py

**Recuerda cambiar los valores del dia, fecha y año.**

### ¿Logs? 📦

_Los log son generados en el archivo Main.log_

---
⌨️ con ❤️ por [César Cerón Manrique](https://github.com/Cesarceronmanrique) 😊