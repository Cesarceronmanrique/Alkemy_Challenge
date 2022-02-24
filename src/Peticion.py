import requests as req
import os
import errno
import logging

#formato_logging = "%(levelname)-s %(asctime)s %(message)s"
#logging.basicConfig(filename="Request.log", level="DEBUG", format=formato_logging, filemode="w")

def Crear_Directorios(Nombre):
    """
    Esta funcion permite crear un directorio mediante un unico parametro de entrada:
        1. Nombre: Que contiene el nombre o ruta del directorio a crear.
    """
    try:
        os.makedirs(Nombre)
        logging.info("Se creo el directorio {}.".format(Nombre))
    except OSError as e:
        if e.errno != errno.EEXIST:
            logging.info("El directorio {} ya existe.".format(Nombre))
            raise

def Peticion_Descargas(URL, Nombre_Archivo):
    """
    Esta funcion permite descargar archivos mediante peticiones con 2 parametros de entrada:
        1. URL: Que contiene la URL donde se realizará la peticion.
        2. Nombre_Archivo: Que contiene el nombre con el que se guardará el archivo.
    """
    with req.get(URL) as rq:
        with open(Nombre_Archivo, "wb") as file:
            file.write(rq.content)
            logging.info("Se ha descargado el dataset.")