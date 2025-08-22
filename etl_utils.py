import os
import logging
import pandas as pd
from sqlalchemy import Engine, create_engine
from sqlalchemy.exc import SQLAlchemyError

#########################################################
# Configuración del logging
#########################################################
def configurar_logging(nombre_archivo: str = 'etl.log'):
    """
    Configura el sistema de logging para registrar eventos del proceso ETL.

    Parámetros:
    - nombre_archivo (str): Nombre del archivo donde se guardará el log. 
    Por defecto, 'etl.log'.

    Detalles:
    - El nivel de logging se establece en INFO.
    - El formato incluye fecha, hora, nivel y mensaje.
    - Los mensajes se guardan en un archivo con codificación de fecha y hora.

    Esta función debe ejecutarse al comienzo del script principal para registrar todos los eventos del proceso ETL.
    """
    logging.basicConfig(
        filename=nombre_archivo,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info("----------------------Inicio del LOG----------------------")

#########################################################
# Creamos una funcion para la CONEXION a la Base de Datos
# Función: crear_conexion
#########################################################
def crear_conexion(nombre_bbdd: str) -> Engine:
    """
    Establece una conexión con una base de datos SQL Server mediante SQLAlchemy.

    Parámetros:
    - nombre_bbdd (str): Nombre de la base de datos a la que se desea conectar.

    Retorna:
    - engine (sqlalchemy.Engine): Objeto Engine que representa la conexión.

    Detalles:
    - Utiliza autenticación integrada de Windows (trusted_connection).
    - El servidor se obtiene automáticamente con os.getlogin().
    - Usa el driver ODBC 17 para SQL Server.

    Lanza:
    - SQLAlchemyError: Si ocurre un error al crear la conexión.

    Este engine se puede reutilizar para leer y escribir datos desde y hacia la base de datos.
    """
    server = os.getlogin()
    driver = 'ODBC+Driver+17+for+SQL+Server'
    conn_str = f"mssql+pyodbc://@{server}/{nombre_bbdd}?trusted_connection=yes&driver={driver}"
    try:
        engine = create_engine(conn_str)
        logging.info("Conexion establecida con %s", nombre_bbdd)
        return engine
    except SQLAlchemyError as err:
        logging.error("ERROR: error al conectar a la BBDD: %s", err, exc_info=True)
        raise

#########################################################
# Leer CSV y exportar a CSV
#########################################################
def leer_datos(ruta_csv: str, convertir_fecha: bool = False, columna_fecha: str = 'Date') -> pd.DataFrame:
    """
    Lee un archivo CSV y lo convierte en un DataFrame de pandas.
    Puede convertir una columna a tipo datetime si se especifica.

    Parámetros:
    - ruta_csv (str): Ruta del archivo CSV a leer.
    - convertir_fecha (bool): Indica si debe intentar convertir una columna a datetime. 
    Por defecto es False.
    - columna_fecha (str): Nombre de la columna a convertir si convertir_fecha es True. 
    Por defecto es 'Date'.

    Retorna:
    - DataFrame con los datos del archivo CSV.

    Detalles:
    - Si la columna indicada no existe, no se realiza la conversión.
    - Si hay errores en el formato de fecha, se asignará NaT (Not a Time) en esos casos.
    - Registra en el log si la carga es exitosa y si la conversión fue aplicada.

    Lanza:
    - Exception: Si ocurre un error al leer el archivo.
    """
    try:
        df = pd.read_csv(ruta_csv)
        logging.info("CSV cargado correctamente: %s (filas=%d)", ruta_csv, len(df))

        if convertir_fecha and columna_fecha in df.columns:
            df[columna_fecha] = pd.to_datetime(df[columna_fecha], format='%Y-%m-%d', errors='coerce')
            logging.info("Columna '%s' convertida a datetime", columna_fecha)

        return df

    except Exception as e:
        logging.error("ERROR: error al leer el CSV '%s': %s", ruta_csv, e, exc_info=True)
        raise

def exportar_a_csv(df: pd.DataFrame, ruta_csv: str) -> None:
    """
    Exporta un DataFrame a un archivo CSV.

    Parámetros:
    - df (pd.DataFrame): DataFrame que se desea exportar.
    - ruta_csv (str): Ruta y nombre del archivo CSV de salida.

    Detalles:
    - El archivo se guarda sin índice (`index=False`).
    - Usa codificación 'utf-8-sig' para que sea compatible con Excel.
    - Registra en el log si la exportación fue exitosa.

    Lanza:
    - Exception: Si ocurre un error durante la exportación.
    """
    try:
        df.to_csv(ruta_csv, index=False, encoding='utf-8-sig')
        logging.info("Exportado a CSV: %s (filas=%d)", ruta_csv, len(df))
    except Exception as e:
        logging.error("ERROR: error al exportar CSV '%s': %s", ruta_csv, e, exc_info=True)
        raise

#########################################################
# CARGAR A BDD
#########################################################
def cargar_en_bdd(df: pd.DataFrame, nombre_tabla: str, engine: Engine, modo: str = 'replace') -> None:
    """
    Inserta un DataFrame en una tabla de SQL Server utilizando SQLAlchemy.

    Parámetros:
    - df (pd.DataFrame): DataFrame con los datos a insertar.
    - nombre_tabla (str): Nombre de la tabla de destino en la base de datos.
    - engine (sqlalchemy.Engine): Objeto de conexión SQLAlchemy.
    - modo (str): Modo de inserción:
        - 'replace': Elimina la tabla si existe y la crea de nuevo.
        - 'append': Añade los datos al final de la tabla existente.
    Por defecto, 'replace'.

    Detalles:
    - No inserta el índice del DataFrame.
    - Utiliza `to_sql()` de pandas con el motor SQLAlchemy.
    - Registra en el log la operación realizada y el número de filas insertadas.

    Lanza:
    - Exception: Si ocurre un error durante la inserción en la base de datos.
    """
    try:
        df.to_sql(name=nombre_tabla, con=engine, if_exists=modo, index=False)
        logging.info("Insertado en BDD: tabla=%s, filas=%d, modo=%s", nombre_tabla, len(df), modo)
    except Exception as e:
        logging.error("ERROR: error al insertar en la BBDD '%s': %s", nombre_tabla, e, exc_info=True)
        raise

#########################################################
# LEER VARIOS CSV
#########################################################

def cargar_ficheros_en_dataframe(ruta_directorio: str) -> pd.DataFrame:
    """
    Carga todos los CSV de la carpeta, añade columna 'Audit_Date' con la fecha
    extraída del nombre del archivo (sin extensión), y concatena todos los DataFrames.

    Parámetros:
    - ruta_directorio (str): Ruta de la carpeta que contiene los archivos CSV.

    Retorna:
    - Un único DataFrame con todos los datos y columna 'Audit_Date' añadida.

    Logging:
    - Registra errores y exito en la lectura.
    """
    dataframes = []

    try:
        for archivo in os.listdir(ruta_directorio):
            if archivo.endswith('.csv'):
                ruta_completa = os.path.join(ruta_directorio, archivo)
                nombre_archivo = os.path.splitext(archivo)[0]

                try:
                    df = pd.read_csv(ruta_completa)
                    df['Audit_Date'] = pd.to_datetime(nombre_archivo, errors='coerce')
                    dataframes.append(df)
                    logging.info("Archivo cargado correctamente: %s", archivo)
                except Exception as e:
                    logging.warning("Error al leer %s: %s", archivo, e)

        if dataframes:
            df_final = pd.concat(dataframes, ignore_index=True)
            logging.info("Todos los archivos fueron combinados en un unico DataFrame")
            return df_final
        else:
            logging.warning("No se encontró ningún archivo CSV")
            return pd.DataFrame()

    except Exception as e:
        logging.exception("Fallo general al procesar la carpeta de CSV: %s", e)
        return pd.DataFrame()