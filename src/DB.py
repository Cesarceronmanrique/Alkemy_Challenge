from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
import logging

def Conexion(Usuario, Contra, Host, Puerto, NombreDB):
    """
    Esta funcion permite retornar la conexión a la Base de datos mediante 5 parámetros de entrada:
        1. Usuario: Nombre de usuario.
        2. Contra: Contraseña del usuario.
        3. Host: Host a usar.
        4. Puerto: Puerto a usar.
        5. NombreDB: Nombre de la base de datos.
    """
    URL = f"postgresql://{Usuario}:{Contra}@{Host}:{Puerto}/{NombreDB}"
    print(URL)
    try:
        if not database_exists(URL):
            create_database(URL)
        engine = create_engine(URL, pool_size = 50, echo = False)
        logging.info("Se ha conectado a la base de datos.")
        return engine
    except Exception as E:
        logging.error(E)

def Crear_Sesion(Valor):
    """
    Esta funcion permite retornar la sesion mediante la conexion como parametro de entrada:
        1. Valor: Conexion.
    """
    try:
        Sesion = sessionmaker(bind=Valor)
        return Sesion
    except Exception as E:
        logging.error(E)

def Quitar_Indice(Dataframe):
    """
    Esta funcion permite suprimir la columna de indices del Dataframe de entrada (Evitando duplicar indice al momento de actualizar la base de datos en PostgreSQL):
        1. Dataframe: Dataframe a suprimir columna indice.
    """
    Valores = Dataframe.columns
    DataFrame = Dataframe.set_index(Valores[0])
    return DataFrame

def Crear_Tabla(Conector, Rutas):
    """
    Esta funcion permite crear las tablas mediante 2 parametros de entrada:
        1. Conector: Conector a la base de datos.
        2. Rutas: Lista con los directorios de los archivos .sql.
    """
    try:
        for i in range(0, 3):
            with open(Rutas[i], 'r') as Archivo:
                Datos = Archivo.read()
                Conector.execute(Datos)
                logging.info("Archivo {} se ha ejecutado".format(Rutas[i]))
    except Exception as E:
        logging.error(E)

def Actualizar_DB(Dataframe, Nombre, Valor):
    """
    Esta funcion permite actualizar las tablas mediante 3 parametros de entrada:
        1. Dataframe: Dataframe enviado para actualizar la tabla.
        2. Nombre: Nombre de la tabla a actualizar.
        3. Valor: Conector a la base de datos.
    """
    try:
        Dataframe.to_sql(Nombre, Valor, if_exists="replace")
        logging.info("Tabla {} se ha creado".format(Nombre))
    except Exception as E:
        logging.error(E)
