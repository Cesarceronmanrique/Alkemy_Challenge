from Peticion import Crear_Directorios, Peticion_Descargas
from Dataframes import Lectura_CSV, Concatenacion, Slicing, Cambio_Columnas
from DB import Conexion, Crear_Sesion, Actualizar_DB, Quitar_Indice, Crear_Tabla
from decouple import config as configuracion
import logging

if __name__ == "__main__":
    #Definición del loggin
    formato_logging = "%(levelname)-s %(asctime)s %(message)s"
    logging.basicConfig(filename="Main.log", level="DEBUG", format=formato_logging, filemode="w")

    #Declaración de las variables y directorios
    año, mes, año = "22", "Febrero", "2022"
    Directorios = [ "./src/museos/" + año + "-" + mes, 
                    "./src/salas-cine/" + año + "-" + mes, 
                    "./src/bibliotecas/" + año + "-" + mes]

    #Llamado a la función de creación de directorios
    for i in range(0, 3):
        Crear_Directorios(Directorios[i])

    #Declaración de los nombres de los archivos y URLs para realizar peticiones (3 valores por lista)
    Nombres = [
    ".\\src\\museos\\" + año + "-" + mes + "\\" + "museos-" + año + "-" + mes + "-" + año + ".csv", 
    ".\\src\\salas-cine\\" + año + "-" + mes + "\\" + "salas-cine-" + año + "-" + mes + "-" + año + ".csv", 
    ".\\src\\bibliotecas\\" + año + "-" + mes + "\\" + "bibliotecas-" + año + "-" + mes + "-" + año + ".csv"]

    URLs = [
    "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museo.csv",
    "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv",
    "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv"]

    #Llamado a la función de descarga de archivos
    for i in range(0, 3):
        Peticion_Descargas(URLs[i], Nombres[i])

    #Declaración de la columna normalizada y llamado a la función de normalización de Dataframes
    Columna_Final = ["Cod_Loc", "IdProvincia", "IdDepartamento", "Categoría", "Provincia", "Localidad", "Nombre", "Domicilio", "Cod_tel", "Teléfono", "Mail", "Web"]
    for i in range(0, 3):
        if i == 0:
            Columna = ["Cod_Loc", "IdProvincia", "IdDepartamento", "categoria", "provincia", "localidad", "nombre", "direccion", "cod_area", "telefono", "Mail", "Web"]
            Datos_Museos = Lectura_CSV(Nombres[i])
            Datos_Normalizados_Museos = Slicing(Datos_Museos, Columna)
            Datos_Normalizados_Museos = Cambio_Columnas(Datos_Normalizados_Museos, Columna_Final)
        if i == 1:
            Columna = ["Cod_Loc", "IdProvincia", "IdDepartamento", "Categoría", "Provincia", "Localidad", "Nombre", "Dirección", "cod_area", "Teléfono", "Mail", "Web"]
            Datos_Salas_Cine = Lectura_CSV(Nombres[i])
            Datos_Normalizados_Salas_Cine = Slicing(Datos_Salas_Cine, Columna)
            Datos_Normalizados_Salas_Cine = Cambio_Columnas(Datos_Normalizados_Salas_Cine, Columna_Final)
        if i == 2:
            Columna = ["Cod_Loc", "IdProvincia", "IdDepartamento", "Categoría", "Provincia", "Localidad", "Nombre", "Domicilio", "Cod_tel", "Teléfono", "Mail", "Web"]
            Datos_Bibliotecas = Lectura_CSV(Nombres[i])
            Datos_Normalizados_Bibliotecas = Slicing(Datos_Bibliotecas, Columna)
            Datos_Normalizados_Bibliotecas = Cambio_Columnas(Datos_Normalizados_Bibliotecas, Columna_Final)
    
    #Creación del Dataframe que contiene la tabla #1
    Tabla1 = Concatenacion(Datos_Normalizados_Museos, Datos_Normalizados_Salas_Cine, Datos_Normalizados_Bibliotecas)
    Tabla1 = Quitar_Indice(Tabla1)

    #Creación del Dataframe de los registros por categoría
    DataFrame2_1 = Slicing(Tabla1, ["Categoría"]).value_counts(dropna=False).to_frame()
    #Creación del Dataframe de registros por fuente
    DataFrame2_2 = Slicing(Datos_Museos, ["fuente"]).value_counts(dropna=False).to_frame()
    DataFrame2_3 = Slicing(Datos_Salas_Cine, ["Fuente"]).value_counts(dropna=False).to_frame()
    DataFrame2_4 = Slicing(Datos_Bibliotecas, ["Fuente"]).value_counts(dropna=False).to_frame()
    #Creación del Dataframe de registros por Provincia - Categoría
    DataFrame2_5 = Slicing(Tabla1, ["Provincia", "Categoría"]).value_counts(dropna=False).to_frame()
    #Concatenacion de los Dataframe por fuente
    DataFrame_Aux = Concatenacion(DataFrame2_2, DataFrame2_3, DataFrame2_4)
    #Creación del Dataframe que contiene la tabla #2
    Tabla2 = Concatenacion(DataFrame2_5, DataFrame2_1, DataFrame_Aux)
    Tabla2 = Tabla2.rename_axis("Clasificacion").reset_index()
    Tabla2.columns = ["Clasificacion", "Cantidad"]
    Tabla2 = Quitar_Indice(Tabla2)

    #Creación del Dataframe que contiene la tabla #3
    DataFrame3 = Lectura_CSV(".\\src\\salas-cine\\" + año + "-" + mes + "\\" + "salas-cine-" + año + "-" + mes + "-" + año + ".csv")
    Tabla3 = Slicing(DataFrame3, ["Provincia", "Pantallas", "Butacas", "espacio_INCAA"])
    Tabla3 = Quitar_Indice(Tabla3)
    logging.info("Se han generado las tablas.")

    #Cracion del conector y de la sesion de la base de datos (LAS VARIABLES DE ENTRADA DE LA FUNCION SE DEBEN DEFINIR EN UN ARCHIVO .env)
    Conector = Conexion(configuracion("pguser"),
                        configuracion("pgpass"),
                        configuracion("pghost"),
                        configuracion("pgport"),
                        configuracion("pgdb"))
    Sesion = Crear_Sesion(Conector)

    #Creación de las tablas
    RutasDB = ["./src/DB1.sql", "./src/DB2.sql", "./src/DB3.sql"]
    Crear_Tabla(Conector, RutasDB)
    
    #actualizacion de las tablas
    Actualizar_DB(Tabla1, "datos_normalizados", Conector)
    Actualizar_DB(Tabla2, "registros_totales", Conector)
    Actualizar_DB(Tabla3, "info_cines", Conector)