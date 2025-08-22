# Como ya tengo un archivo utils con funciones para extracción, carga, logging, etc., las reutilizo
import os
import logging
import pandas as pd
import numpy as np

from etl_utils import (
    configurar_logging,
    crear_conexion,
    exportar_a_csv,
    cargar_en_bdd,
    cargar_ficheros_en_dataframe
)

################################################
# INICIALIZACIÓN DEL LOGGING
################################################

# Inicializamos el logging para registrar eventos y errores del proceso ETL
configurar_logging('logfile.log')

################################################
# VARIABLES DE CONFIGURACIÓN
################################################

# Ruta a la carpeta que contiene los CSV de entrada
ruta_csv = r"C:\\path\\to\\project\\csv"

# Nombre de la base de datos de destino
nombre_bbdd = "MyDatabase"

# Establecemos la conexión a SQL Server
engine = crear_conexion(nombre_bbdd)

################################################
# FASE DE EXTRACCIÓN (E)
################################################

# Cargamos todos los CSV (en utils, ya añadimos la columna Audit_Date)
df_principal = cargar_ficheros_en_dataframe(ruta_csv)

# Mostramos las primeras filas para verificar la carga
# print(df_principal.head(20))

################################################
# FASE DE TRANSFORMACIÓN (T) – LIMPIEZA VALIDAS
################################################

# Primero hacemos una copia del df original para trabajar con los arreglos para Vetas_validas
df_validas = df_principal.copy()

# Difinimos la funcion
def limpiar_ventas_validas(df_principal: pd.DataFrame) -> pd.DataFrame:
    # reservamos el original
    df = df_principal.copy()  

####### ARREGLOS SALE_ID #######
    # Ponemos en mayus
    df['Sale_ID'] = df['Sale_ID'].str.upper() 
    # Quitamos nulos
    df = df[df['Sale_ID'].notna()] 
    # Quitamos duplicados
    df = df.drop_duplicates(subset=['Sale_ID'], keep='first') 

####### ARREGLOS PRODUCT #######
    # Nos quedamos solo con la letra al final de la cadena str que esta separada de la cadena por un (-)
    df['Product'] = (df['Product'].str.upper().str.strip().str.split('-').str[-1]) 
    # Igual que en Sales, quitamos los nulos
    df = df[df['Product'].notna()]

####### ARREGLOS AMOUNT #######

# Eliminamos los símbolos de divisas (USD, EUR) y los montos que estén en euros convertirlos a dólares, teniendo en cuenta que el cambio de dólar se puede hacer multiplicando el valor por 0.85.
    # Guard el valor de amount para mas adelante
    df_amount_original = df['Amount'] 
    # Quitamos las siglas del final de la cadena para poder trabajar con los valores
    df['Amount_arreglado'] = (df['Amount'].str.replace('USD', '').str.replace('EUR', '')) 
    # Lo convertimos en float para poder hacer calculos de decimales
    df['Amount'] = df['Amount_arreglado'].astype(float) 
    # Comprobamos del valor original de amount si acaba en EUR, si devuelve FALSE el amount lo multimplicamos por 1.12
    df.loc[df_amount_original.str.endswith('EUR', na=False), 'Amount'] *= 0.85
    # Redondeamos el resultado a 2 decimales
    df['Amount'] = df['Amount'].round(2) 
    # Quitamos nulos
    df = df[df['Amount'].notna()] 
    # Elimino la tabla temporal que ya no necesitaremos
    df.drop(columns=['Amount_arreglado'], inplace=True) 

####### ARREGLOS DATE #######

# Convertimos a datetime 
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['Audit_Date'] = pd.to_datetime(df['Audit_Date'], errors='coerce')

    # Quitamos los nulos
    df = df[df['Date'].notna()]
    df = df[df['Audit_Date'].notna()]

    logging.info("Ventas validas: Sale_ID todo mayusculas, hemos eliminado nulos y duplicados, Product y Amount arreglados (filas=%d)",len(df))
    return df

################################################
# FASE DE TRANSFORMACIÓN – LIMPIEZA INVALIDAS
################################################

def limpiar_ventas_invalidas(df_principal: pd.DataFrame) -> pd.DataFrame:
    try:
        df = df_principal.copy()
        logging.info("Iniciando deteccion de ventas invalidas")

        # Limpieza Sale_ID y Product
        df['Sale_ID'] = df['Sale_ID'].astype(str).str.upper()
        df['Product'] = df['Product'].astype(str).str.split('-').str[-1].str.upper()

        df_invalidas = pd.DataFrame()

        # REASON N: NULOS
        #Sacamos las filas donde hay al menos un valor nulo con una mascara y le añado una columna Reason con valor N 
        mask_nulos = df.isnull().any(axis=1)
        df_nulos = df[mask_nulos].copy()
        df_nulos['Reason'] = 'N'

        # REASON A: MONTO INVÁLIDA
        # Ahora me quedo solo con las filas que no tenían nulos, reviso su columna Amount, si no pone 'USD' o 'EUR', entonces es un monto inválido.
        df_no_nulos = df[~mask_nulos].copy()
        # COnvertimos Amount en mayúsculas para facilitar la detección de la moneda (USD o EUR)
        df_no_nulos['Amount_str'] = df_no_nulos['Amount'].astype(str).str.upper()
        # Creamos una máscara para identificar filas cuyo Amount no contiene una monedaa válida (ni USD ni EUR
        mask_monto_invalido = ~df_no_nulos['Amount_str'].str.contains('USD|EUR', na=False)
        df_monto_invalido = df_no_nulos[mask_monto_invalido].copy()
        df_monto_invalido['Reason'] = 'A'

        # REASON D: DUPLICADOS
        df_restantes = df_no_nulos[~mask_monto_invalido].copy()
        # COgemos solo las filas que tiene  en Sale_ID duplicados y ponemos en Reason 'D'
        duplicados = df_restantes[df_restantes.duplicated(subset='Sale_ID', keep=False)]
        df_duplicados = duplicados.copy()
        df_duplicados['Reason'] = 'D'

        # UNIMOS TODAS LAS INVÁLIDAS
        df_invalidas = pd.concat([df_nulos, df_monto_invalido, df_duplicados], ignore_index=True)

        # Eliminamos la columna extra que utilizamos antes para el arreglo
        if 'Amount_str' in df_invalidas.columns:
            df_invalidas.drop(columns=['Amount_str'], inplace=True)
        logging.info("Ventas invalidas detectadas: %d filas", len(df_invalidas))
        return df_invalidas
    
    except Exception as e:
        logging.error("Error durante limpieza de ventas inválidas: %s", e, exc_info=True)
        return pd.DataFrame()
    
################################################
# CREAR VENTAS RESUMEN MENSUAL
################################################
def generar_ventas_resumen_mensual(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # Creamos una copia del DataFrame original
        df = df.copy()
        # Registramos en el log que comienza la generación del resumen mensual
        logging.info("Generando informe agregado mensual")

        # Creamos una nueva columna 'Mes' extrayendo el mes y año en formato MM/YYYY desde la columna 'Date'
        df['Mes'] = df['Date'].dt.strftime('%m/%Y')

        # Agrupamos los datos por 'Mes' y 'Product' y calculamos la suma, el conteo y el mínimo del campo 'Amount'
        resumen = df.groupby(['Mes', 'Product'], as_index=False).agg({
            'Amount': ['sum', 'count', 'min']
        })

        # Renombramos columnas
        resumen.columns = ['Mes', 'Producto', 'Ventas_Totales', 'Numero_Transacciones', 'Venta_Minima']

        logging.info("Informe mensual generado correctamente: %d filas", len(resumen))
        return resumen

    except Exception as e:
        logging.error("Error al generar el informe mensual: %s", e, exc_info=True)
        return pd.DataFrame()
#############################################################################################################################################
###############################################
##USO DE LAS FUNCIONES DE LIMPIEZA
###############################################
df_validas = limpiar_ventas_validas(df_principal)
df_invalidas = limpiar_ventas_invalidas(df_principal)
df_resumen = generar_ventas_resumen_mensual(df_validas)

###############################################
## DATA FRAMES FINALES PARA RECORRER CON BUCLE FOR
###############################################

dataframes_finales = [
    ('Ventas_Validas_M', df_validas),
    ('Ventas_Invalidas_M', df_invalidas),
    ('Ventas_Resumen_Mensual', df_resumen)
]

###############################################
# FORATEAMOS FECHAS PARA QUE NO APAREZCN HORAS
###############################################
def formatear_fechas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Recorremos las columnas 'Date' y 'Audit_Date'
    for col in ['Date', 'Audit_Date']:
        if col in df.columns:
            # Aseguramos que estén en formato datetime
            df[col] = pd.to_datetime(df[col], errors='coerce') 
            # Convertimos la fecha al formato 'yyyy-mm-dd' (sin horas)
            df[col] = df[col].dt.strftime('%Y-%m-%d')
    return df

###############################################
## CARGA EN BDD (con el arreglo de fechas)
###############################################
for nombre_tabla, df in dataframes_finales:
    df_formateado = formatear_fechas(df)
    cargar_en_bdd(df_formateado, nombre_tabla, engine)

###############################################
## Exportar a CSV (con el arreglo de fechas)
###############################################
for nombre_archivo, df in dataframes_finales:
    df_formateado = formatear_fechas(df)
    exportar_a_csv(df_formateado, f'Resultados/{nombre_archivo}.csv')

################################################
# CERRAMOS CONEXIÓN
################################################
engine.dispose()
logging.info("------------------------Conexion a la base de datos cerrada correctamente.------------------------")