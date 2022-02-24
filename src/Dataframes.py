import pandas as pd
import logging

def Lectura_CSV(Nombre):
    """
    Esta funcion permite generar un Dataframe auxiliar como variable de salida apartir de:
        1. Nombre: Que contiene el nombre o ruta del Dataframe de entrada.
    """
    try:
        Datos = pd.read_csv(Nombre)
        logging.info("Se ha leido el Dataframe.")
        return Datos
    except OSError as Error:
        logging.error(str(Error))

def Concatenacion(Dataframe1, Dataframe2, Dataframe3=None):
    """
    Esta funcion permite generar un Dataframe auxiliar como variable de salida apartir de la concatenacion de hasta 3 Dataframes:
        1. Dataframe1: Que contiene el Dataframe1.
        2. Dataframe2: Que contiene el Dataframe2.
        3. Dataframe3: Que contiene el Dataframe3 (Por defecto este parámetro es None).
    """
    try:
        Dataset_Final = pd.concat([ Dataframe1, 
                                    Dataframe2, 
                                    Dataframe3])
        logging.info("Se ha generado el Dataset concatenado.")
        return Dataset_Final
    except:
        logging.error("Se ha generado un error al concatenar.")

def Slicing(Dataframe, Columnas):
    """
    Esta funcion permite generar un Dataframe auxiliar como variable de salida apartir de la extracción de columnas de interes ingresadas en una lista:
        1. Dataframe: Que contiene el Dataframe original.
        2. Columnas: Que es una lista que contiene las columnas a extraer del Dataframe.
    """
    try:
        Dataframe_salida = Dataframe[Columnas]
        logging.info("Se ha generado el Dataset de salida.")
        return Dataframe_salida
    except:
        logging.error("Se ha generado un error al generar el Dataframe de salida.")

def Cambio_Columnas(Dataframe, Columnas_nuevas):
    """
    Esta funcion permite generar un Dataframe auxiliar como variable de salida apartir del cambio de nombre de las columnas iniciales (Permitiendo que todas las columnas de los Dataframe posean un mismo nombre):
        1. Dataframe: Que contiene el Dataframe original.
        2. Columnas_nuevas: Que es una lista que contiene los nombres de las columnas.
    """
    try:
        Dataframe.columns = Columnas_nuevas
        logging.info("Se ha cambiado el nombre de las columnas.")
        return Dataframe
    except:
        logging.error("Se ha generado un error al cambiar el nombre de las columnas.")