# MODIFICADO: 2025-06-21
# ACAPTADO a Nuevas Tablas y Estructura de Datos en POSTGRES
# Se hace con Ejecución Remota 
# Depende de procesos de estración Previo
# Se mantiene por Compatibilidad con el resto de la aplicación las funciones _OLD
"""
funciones_forecast.py
Versión 1.3.3

Este módulo contiene todas las funciones necesarias para:
- conexión a bases de datos
- carga de datos históricos
- ejecución de algoritmos de pronóstico (ALGO_01 a ALGO_06)
- operaciones sobre ejecuciones de forecast
- Nuevas Funciona JSON y Diccionarios
- Nuevos JSON Refacctorizado
- CONFIGURACIÓN DINAMICA desde .env
- NUEVA Generación Masiva de Datos

"""

# Librerías necesarias
import base64
from io import BytesIO
import os
import shutil
import pandas as pd
import matplotlib.pyplot as plt 
import numpy as np
import time
import getpass
import uuid
import warnings
import traceback
from datetime import datetime, timedelta

# Acceso a Datos
from dotenv import dotenv_values
import psycopg2 as pg2
import pyodbc
import sqlalchemy
from sqlalchemy import text

# Graficos y Paralelismo
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from io import BytesIO
import base64
from multiprocessing import Pool, cpu_count
import json
from statsmodels.tsa.holtwinters import ExponentialSmoothing, Holt
import ace_tools_open as tools

# Configuración global
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

# Cargar configuración DINAMIDA de acuerdo al entorno
from dotenv import dotenv_values
import os
import sys
ENV_PATH = os.environ.get("FORECAST_ENV_PATH", "E:/ETL/FORECAST/.env")  # Toma Producción si está definido, o la ruta por defecto
# Verificar si el archivo .env existe
if not os.path.exists(ENV_PATH):
    print(f"El archivo .env no existe en la ruta: {ENV_PATH}")
    print(f"Directorio actual: {os.getcwd()}")
    sys.exit(1)
    
secrets = dotenv_values(ENV_PATH)
folder = f"{secrets['BASE_DIR']}/{secrets['FOLDER_DATOS']}"

# ---------------------------------------------------------------------
# A continuación se deben pegar todas las funciones previamente definidas:
# - Open_Connection, Open_Diarco_Data, Open_Conn_Postgres, Close_Connection
# - generar_datos, Exportar_Pronostico
# - Calcular_Demanda_ALGO_01 a ALGO_06
# - Procesar_ALGO_01 a ALGO_06
# - get_forecast
# - get_execution, update_execution, get_execution_execute_by_status
# - get_execution_parameter
# ---------------------------------------------------------------------


# FUNCIONES OPTIMIZADA PARA EL PRONOSTICO DE DEMANDA
# Trataremos de Estandarizar las salidas y optimizar el proceso
# Generaremos datos para regenerar graficos simples al vuelo y grabaremos un gráfico ya precalculado
# En esta primera etapa en un blob64, luego en un servidor de archivos con un link.

###----------------------------------------------------------------
#     DATOS y CONEXIONES A DATOS
###----------------------------------------------------------------
def Open_Connection():
    conn_str = f'DRIVER={secrets["SQL_DRIVER"]};SERVER={secrets["SQL_SERVER"]};PORT={secrets["SQL_PORT"]};DATABASE={secrets["SQL_DATABASE"]};UID={secrets["SQL_USER"]};PWD={secrets["SQL_PASSWORD"]}'
    # print (conn_str) 
    try:    
        conn = pyodbc.connect(conn_str)
        return conn
    except:
        print('Error en la Conexión')
        return None

def Open_Diarco_Data(): 
    conn_str = f"dbname={secrets['PG_DB']} user={secrets['PG_USER']} password={secrets['PG_PASSWORD']} host={secrets['PG_HOST']} port={secrets['PG_PORT']}"
    #print (conn_str)
    for i in range(5):
        try:    
            conn = pg2.connect(conn_str)
            return conn
        except Exception as e:
            print(f'Error en la conexión: {e}')
            time.sleep(5)
    return None  # Retorna None si todos los intentos fallan


def Open_Conn_Postgres():
    conn_str = f"dbname={secrets['PGP_DB']} user={secrets['PGP_USER']} password={secrets['PGP_PASSWORD']} host={secrets['PGP_HOST']} port={secrets['PGP_PORT']}"
    for i in range(5):
        try:
            conn = pg2.connect(conn_str)
            return conn 
        except Exception as e:
            print(f"Error en la conexión, intento {i+1}/{5}: {e}")
            time.sleep(5)
    return None  # Retorna None si todos los intentos fallan

def Open_Postgres_retry(max_retries=5, wait_seconds=5):  
    conn_str = f"dbname={secrets['PGP_DB']} user={secrets['PGP_USER']} password={secrets['PGP_PASSWORD']} host={secrets['PGP_HOST']} port={secrets['PGP_PORT']}"
    for i in range(max_retries):
        try:
            conn = pg2.connect(conn_str)
            return conn 
        except Exception as e:
            print(f"Error en la conexión, intento {i+1}/{max_retries}: {e}")
            time.sleep(wait_seconds)
    return None  # Retorna None si todos los intentos fallan

def Open_Connexa_Alquemy():
    DB_TYPE = "postgresql"
    DB_USER = secrets['PGP_USER']
    DB_PASS = secrets['PGP_PASSWORD']  # ⚠️ Reemplazar por la contraseña real o usar variables de entorno
    DB_HOST = secrets['PGP_HOST']
    DB_PORT = secrets['PGP_PORT']
    DB_NAME = secrets['PGP_DB']

    # Crear engine de conexión
    try:
        engine = sqlalchemy.create_engine(
        f"{DB_TYPE}://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        conn = engine.connect()
        return conn
    except Exception as e:
        print(f'Error en la conexión: {e}')
        return None   

def Close_Connection(conn): 
    if conn is not None:
        conn.close()
        # print("✅ Conexión cerrada.")    
    return True

def id_aleatorio():       # Helper para generar identificadores únicos
    return str(uuid.uuid4())

##  ----------------------------------------------------------------
# Rutina que Evita Errores de NULL y NAN

def preparar_dataframe_forecast(df: pd.DataFrame, columnas_tipo: dict, label: str = "") -> pd.DataFrame:
    """
    Prepara un DataFrame para forecasting:
    - Reemplaza NaN, inf y -inf por 0 en columnas numéricas.
    - Convierte los tipos de datos según el diccionario provisto.
    - Loguea columnas con datos faltantes.

    Args:
        df (pd.DataFrame): DataFrame original.
        columnas_tipo (dict): Diccionario con columnas y tipos deseados. Ej: {"COL1": "Int64", "COL2": "Float64"}
        label (str): Etiqueta para logging (opcional).

    Returns:
        pd.DataFrame: DataFrame preparado.
    """
    print(f"🧪 Validando y preparando DataFrame para {label}...")

    # Estandarizar nombres
    df.columns = df.columns.str.upper()

    # Detectar nulos o infinitos antes del reemplazo
    for col in df.columns:
        if df[col].dtype in ["float64", "float32", "int64", "int32", "Int64", "Float64"]:
            if df[col].isna().any() or np.isinf(df[col]).any():
                print(f"⚠️ [{label}] Valores no finitos en columna '{col}' — serán reemplazados por 0")

    # Reemplazo en todas las columnas numéricas
    df = df.replace([np.inf, -np.inf], np.nan)
    df[df.select_dtypes(include=["number"]).columns] = df.select_dtypes(include=["number"]).fillna(0)

    # Conversión de tipos según especificación
    for col, tipo in columnas_tipo.items():
        if col in df.columns:
            try:
                if tipo.startswith("Float"):
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(tipo)
                elif tipo.startswith("Int"):
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(tipo)
                elif tipo == "int":
                    df[col] = df[col].astype(int)
                else:
                    df[col] = df[col].astype(tipo)
            except Exception as e:
                print(f"❌ Error al convertir la columna '{col}' a tipo {tipo}: {e}")
        else:
            print(f"⚠️ [{label}] Columna esperada '{col}' no encontrada en el DataFrame.")

    print(f"✅ DataFrame listo para forecasting: {label}")
    return df
# MODO DE USO
# Diccionario de columnas y sus tipos esperados
# tipos_stock = {
#     "CODIGO_SUCURSAL": "int",
#     "CODIGO_ARTICULO": "int",
#     "CODIGO_PROVEEDOR": "int",
#     "PEDIDO_SGM": "Float64",
#     "STOCK": "Float64",
#     "PEDIDO_PENDIENTE": "Float64",
#     "I_LISTA_CALCULADO": "Float64",
#     "FACTOR_VENTA": "Int64",
#     "PRECIO_VENTA": "Float64",
#     "PRECIO_COSTO": "Float64",
#     "Q_DIAS_STOCK": "Int64",
#     "TRANSFER_PENDIENTE": "Float64",
#     "VENTA_UNIDADES_1Q": "Float64",
#     "VENTA_UNIDADES_2Q": "Float64"
# }
# stock_sucursal = preparar_dataframe_forecast(stock_sucursal, tipos_stock, label="Stock_Sucursal")


# Nueva Rutina al MIGRAR a PostgreSQL y Ejecución REMOTA
# 2025-05-16 Se agrega c_comprador
def generar_datos(id_proveedor, etiqueta, ventana):
    folder = secrets["FOLDER_DATOS"]
    archivo_datos = f'{folder}/{etiqueta}.csv'
    archivo_articulos = f'{folder}/{etiqueta}_articulos.csv'
    archivo_stock = f'{folder}/{etiqueta}_stock_sucursal.csv'
    
    # Verificar la fecha de modificación del archivo
    if os.path.exists(archivo_datos):
        fecha_modificacion = datetime.fromtimestamp(os.path.getmtime(archivo_datos))
        if fecha_modificacion.date() == datetime.today().date():
            try:
                data = pd.read_csv(archivo_datos)
                data['Codigo_Articulo'] = data['Codigo_Articulo'].astype(int)
                data['Sucursal'] = data['Sucursal'].astype(int)
                data['Fecha'] = pd.to_datetime(data['Fecha'])

                articulos = pd.read_csv(archivo_articulos)

                print(f"-> Datos Recuperados del CACHE: {id_proveedor}, Label: {etiqueta}")
                return data, articulos
            except Exception as e:
                print(f"Error al leer los archivos cacheados: {e}")
        else:
            # Eliminar archivos si la fecha no es la de hoy
            os.remove(archivo_datos)
            if os.path.exists(archivo_articulos):
                os.remove(archivo_articulos)
            print(f"-> Archivos eliminados por ser obsoletos: {archivo_datos}, {archivo_articulos}")
    else:
        print(f"-> Generando datos para ID: {id_proveedor}, Label: {etiqueta}")
        # Aquí puedes incluir el código para generar los datos si no EXISTE el Archivo en el CACHE
        conn = Open_Diarco_Data()

        # --- ARTÍCULOS --- NUEVA FUENTE GLOBAL 06/25 -- SP_BASE_PRODUCTOS_VIGENTES
        query_articulos = f"""
            SELECT DISTINCT c_sucu_empr, c_articulo, c_proveedor_primario, abastecimiento, cod_cd, habilitado,  
                    cod_comprador AS c_comprador, 
                    q_factor_compra, full_capacity_pallet, number_of_layers, number_of_boxes_per_layer
            FROM src.base_productos_vigentes
            WHERE c_proveedor_primario = {id_proveedor}
            ORDER BY c_articulo, c_proveedor_primario;
        """
        articulos = pd.read_sql(query_articulos, conn) # type: ignore
        if articulos.empty:
            print(f"❗ No se encontraron artículos para el proveedor {id_proveedor}.")
            Close_Connection(conn)
            return None, None

        articulos.columns = articulos.columns.str.upper()
        articulos.rename(columns={'COD_COMPRADOR': 'C_COMPRADOR'}, inplace=True)   # En la base nueva se llama distinto
        articulos['C_SUCU_EMPR'] = articulos['C_SUCU_EMPR'].astype(int)
        articulos['C_ARTICULO'] = articulos['C_ARTICULO'].astype(int)
        articulos['C_PROVEEDOR_PRIMARIO'] = articulos['C_PROVEEDOR_PRIMARIO'].astype(int)
        articulos['ABASTECIMIENTO'] = articulos['ABASTECIMIENTO'].astype(int)
        articulos['HABILITADO'] = articulos['HABILITADO'].astype(int)
        articulos['C_COMPRADOR'] = articulos['C_COMPRADOR'].astype(int)
        articulos['Q_FACTOR_COMPRA'] = articulos['Q_FACTOR_COMPRA'].astype(int)
        articulos['FULL_CAPACITY_PALLET'] = articulos['FULL_CAPACITY_PALLET'].astype(int)
        articulos['NUMBER_OF_LAYERS'] = articulos['NUMBER_OF_LAYERS'].astype(int)
        articulos['NUMBER_OF_BOXES_PER_LAYER'] = articulos['NUMBER_OF_BOXES_PER_LAYER'].astype(int)
        articulos.to_csv(archivo_articulos, index=False, encoding='utf-8')
        print(f"---> Datos de Artículos guardados")        
        
        # --- BASE STOCK --- NUEVA FUENTE GLOBAL 06/25 -- SP_BASE_STOCK_SUCURSAL
        query_stock_sucursal = f"""
            SELECT codigo_articulo, codigo_sucursal, codigo_proveedor, pedido_sgm, stock, 
                pedido_pendiente, i_lista_calculado, factor_venta, precio_venta, precio_costo, 
                q_dias_stock, transfer_pendiente, venta_unidades_1q, venta_unidades_2q
            FROM src.base_stock_sucursal
            WHERE codigo_proveedor = {id_proveedor}
            ORDER BY codigo_articulo, codigo_articulo;
        """
        stock_sucursal = pd.read_sql(query_stock_sucursal, conn) # type: ignore
        if stock_sucursal.empty:
            print(f"❗ No se encontraron artículos de Stock_Sucursal para el proveedor {id_proveedor}.")
            Close_Connection(conn)
            return None, None
        
        # Limpieza general antes de conversión
        stock_sucursal = stock_sucursal.replace([np.inf, -np.inf], np.nan)
        stock_sucursal = stock_sucursal.fillna(0)

        stock_sucursal.columns = stock_sucursal.columns.str.upper()
        #  Cambiar tipos de datos
        stock_sucursal['CODIGO_SUCURSAL'] = stock_sucursal['CODIGO_SUCURSAL'].astype(int)
        stock_sucursal['CODIGO_ARTICULO'] = stock_sucursal['CODIGO_ARTICULO'].astype(int)
        stock_sucursal['CODIGO_PROVEEDOR'] = stock_sucursal['CODIGO_PROVEEDOR'].astype(int)
        stock_sucursal["PEDIDO_SGM"] = pd.to_numeric(stock_sucursal["PEDIDO_SGM"], errors="coerce").astype("Float64")
        stock_sucursal["STOCK"] = pd.to_numeric(stock_sucursal["STOCK"], errors="coerce").astype("Float64")
        stock_sucursal["PEDIDO_PENDIENTE"] = pd.to_numeric(stock_sucursal["PEDIDO_PENDIENTE"], errors="coerce").astype("Float64")
        stock_sucursal["I_LISTA_CALCULADO"] = pd.to_numeric(stock_sucursal["I_LISTA_CALCULADO"], errors="coerce").astype("Float64")
        stock_sucursal['FACTOR_VENTA'] = stock_sucursal['FACTOR_VENTA'].astype(int)
        stock_sucursal['PRECIO_VENTA'] = pd.to_numeric(stock_sucursal['PRECIO_VENTA'], errors='coerce').astype('Float64')
        stock_sucursal['PRECIO_COSTO'] = pd.to_numeric(stock_sucursal['PRECIO_COSTO'], errors='coerce').astype('Float64')
        stock_sucursal['Q_DIAS_STOCK'] = pd.to_numeric(stock_sucursal['Q_DIAS_STOCK'], errors='coerce').astype('Int64')
        stock_sucursal['TRANSFER_PENDIENTE'] = pd.to_numeric(stock_sucursal['TRANSFER_PENDIENTE'], errors='coerce').astype('Float64')
        stock_sucursal['VENTA_UNIDADES_1Q'] = pd.to_numeric(stock_sucursal['VENTA_UNIDADES_1Q'], errors='coerce').astype('Float64')
        stock_sucursal['VENTA_UNIDADES_2Q'] = pd.to_numeric(stock_sucursal['VENTA_UNIDADES_2Q'], errors='coerce').astype('Float64')

        stock_sucursal.to_csv(archivo_stock, index=False, encoding='utf-8')
        print(f"---> Datos de Stock Sucursal guardados")
        
        # -- COMBINAR ARTÍCULOS y STOCK --
        articulos = pd.merge(articulos, stock_sucursal, left_on=['C_ARTICULO', 'C_SUCU_EMPR'], right_on=['CODIGO_ARTICULO', 'CODIGO_SUCURSAL'], how='inner')  

        # --- VENTAS --- DIARCO + BARRIO ( En 2 Bases de Datos distintas )
        query_ventas_diarco = f"""
            SELECT 
                v.f_venta AS Fecha, 
                v.c_articulo as Codigo_Articulo, 
                v.c_sucu_empr as Sucursal, 
                v.q_unidades_vendidas as Unidades
            FROM src.t702_est_vtas_por_articulo v
            JOIN src.base_productos_vigentes a 
                ON a.c_articulo = v.c_articulo
                AND a.c_sucu_empr = v.c_sucu_empr
                AND a.c_proveedor_primario = {id_proveedor}
            WHERE v.f_venta >= '2024-06-01'  
            ORDER BY fecha;
        """
        ventas_d = pd.read_sql(query_ventas_diarco, conn) # type: ignore
        if ventas_d.empty:
            print(f"⚠️ No se encontraron ventas DIARCO para el proveedor {id_proveedor}.")
        
        # --- VENTAS --- BARRIO ( En 2 Bases de Datos distintas )
        query_ventas_barrio = f"""
            SELECT 
                v.f_venta AS Fecha, 
                v.c_articulo as Codigo_Articulo, 
                v.c_sucu_empr as Sucursal, 
                v.q_unidades_vendidas as Unidades
            FROM src.t702_est_vtas_por_articulo_dbarrio v
            JOIN src.base_productos_vigentes a 
                ON a.c_articulo = v.c_articulo
                AND a.c_sucu_empr = v.c_sucu_empr
                AND a.c_proveedor_primario = {id_proveedor}
            WHERE v.f_venta >= '2024-06-01'  
            ORDER BY fecha;
        """
        ventas_b = pd.read_sql(query_ventas_barrio, conn) # type: ignore
        if ventas_b.empty:
            print(f"⚠️ No se encontraron ventas DIARCO BARRIO para el proveedor {id_proveedor}.")
        
        # Convertir columnas a minúsculas si hay datos
        if not ventas_d.empty:
            ventas_d.columns = ventas_d.columns.str.lower()
        if not ventas_b.empty:
            ventas_b.columns = ventas_b.columns.str.lower()
    
        # Transformar tipos de datos si hay datos
        if not ventas_d.empty:
            ventas_d['sucursal'] = ventas_d['sucursal'].astype(int)
            ventas_d['codigo_articulo'] = ventas_d['codigo_articulo'].astype(int)
            ventas_d['fecha'] = pd.to_datetime(ventas_d['fecha'])

        if not ventas_b.empty:
            ventas_b['sucursal'] = ventas_b['sucursal'].astype(int)
            ventas_b['codigo_articulo'] = ventas_b['codigo_articulo'].astype(int)
            ventas_b['fecha'] = pd.to_datetime(ventas_b['fecha'])

        # Concatenar los datos
        if not ventas_d.empty and not ventas_b.empty:
            demanda = pd.concat([ventas_d, ventas_b], ignore_index=True)
        elif not ventas_d.empty:
            demanda = ventas_d.copy()
        elif not ventas_b.empty:
            demanda = ventas_b.copy()
        else:
            print(f"⚠️ No se encontraron ventas para el proveedor {id_proveedor} ni en DIARCO ni en BARRIO.")
            demanda = pd.DataFrame(columns=['fecha', 'codigo_articulo', 'sucursal', 'unidades'])  # DataFrame vacío con columnas esperadas

        demanda = demanda.rename(columns={
            "fecha": "Fecha",
            "codigo_articulo": "Codigo_Articulo",
            "sucursal": "Sucursal",
            "unidades": "Unidades"
        })

        # Guardar solo VENTAS 
        demanda.to_csv(f'{folder}/{etiqueta}_Demanda.csv', index=False, encoding='utf-8')
        print(f"---> Datos de Ventas guardados")

        # --- MERGE ---
        data = pd.merge(
            articulos,
            demanda,  
            left_on=['C_ARTICULO', 'C_SUCU_EMPR'],          
            right_on=['Codigo_Articulo', 'Sucursal'],            
            how='inner'  # Solo traer los productos que están en AMBOS ARCHIVOS
        )

        if data.empty:
            print(f"⚠️ No hay coincidencias entre artículos y ventas para el proveedor {id_proveedor}.")
            Close_Connection(conn)
            return None, articulos

        # Guardado
        data['C_ARTICULO'] = data['C_ARTICULO'].astype(int)
        data['C_SUCU_EMPR'] = data['C_SUCU_EMPR'].astype(int)
        data['Codigo_Articulo'] = data['Codigo_Articulo'].astype(int)
        data['Sucursal'] = data['Sucursal'].astype(int)
        data.to_csv(archivo_datos, index=False, encoding='utf-8')
        print(f"---> Datos de RECUPERACIÓN guardados")
            
        
        # Guardar los datos Compactos de VENTAS en un archivo CSV con el nombre del Proveedor y sufijo _Ventas
        file_path = f'{folder}/{etiqueta}_Ventas.csv'
        print(f"[DEBUG] Ruta destino definida en .env: {folder}")

        # Eliminar Columnas Innecesarias
        data = data[['Fecha', 'Codigo_Articulo', 'Sucursal', 'Unidades']]
        data.to_csv(file_path, index=False, encoding='utf-8')
        print(f"---> Datos de Ventas guardados: {file_path}")  

        Close_Connection(conn)
        return data, articulos

def dividir_dataframe(data, fecha_corte):
    """
    Divide un DataFrame en dos partes: data_train y data_test según la fecha_corte.
    
    :param data: DataFrame con la columna 'Fecha'
    :param fecha_corte: Fecha límite para dividir el DataFrame (tipo datetime o string con formato 'YYYY-MM-DD')
    :return: data_train, data_test
    """
    # Asegurarse de que la columna 'Fecha' sea de tipo datetime
    data['Fecha'] = pd.to_datetime(data['Fecha'])
    
    # Filtrar los datos
    data_train = data[data['Fecha'] < pd.to_datetime(fecha_corte)]
    data_test = data[data['Fecha'] >= pd.to_datetime(fecha_corte)]
    
    return data_train, data_test

def obtener_datos_stock(id_proveedor, etiqueta):
    folder = f"{secrets['BASE_DIR']}/{secrets['FOLDER_DATOS']}"
    
    #  Intento recuperar datos cacheados
    try:         
        print(f"-> Generando datos para ID: {id_proveedor}, Label: {etiqueta}")
        # Configuración de conexión (AHORA EN FORMA LOCAL)
        conn = Open_Diarco_Data()
        
        # ----------------------------------------------------------------
        # FILTRA solo PRODUCTOS HABILITADOS y Traer datos de STOCK y PENDIENTES desde PRODUCCIÓN
        # ----------------------------------------------------------------
        query = f"""              
            SELECT codigo_proveedor, codigo_articulo, codigo_sucursal, precio_venta, precio_costo, factor_venta, m_vende_por_peso, stock,             
            venta_unidades_30_dias, stock_valorizado, venta_valorizada, dias_stock, f_ultima_vta, venta_unidades_1q, venta_unidades_2q
            
            FROM src.base_forecast_stock
            WHERE codigo_proveedor = {id_proveedor}
            ORDER BY codigo_articulo, codigo_sucursal;
        """
        # Ejecutar la consulta SQL
        df_stock = pd.read_sql(query, conn) # type: ignore
        # Renombrar columnas para estandarizar
        df_stock = df_stock.rename(columns={
            "codigo_proveedor": "Codigo_Proveedor",
            "codigo_articulo": "Codigo_Articulo",
            "codigo_sucursal": "Codigo_Sucursal",
            "precio_venta": "Precio_Venta",
            "precio_costo": "Precio_Costo",
            "factor_venta": "Factor_Venta",
            "m_vende_por_peso": "M_Vende_Por_Peso",
            "stock": "Stock_Unidades",
            "venta_unidades_30_dias": "Venta_Unidades_30_Dias",
            "stock_valorizado": "Stock_Valorizado",
            "venta_valorizada": "Venta_Valorizada",
            "dias_stock": "Dias_Stock",
            "f_ultima_vta": "F_ULTIMA_VTA",
            "venta_unidades_1q": "VENTA_UNIDADES_1Q",
            "venta_unidades_2q": "VENTA_UNIDADES_2Q"            
        })
        
        file_path = f'{folder}/{etiqueta}_Stock.csv'
        df_stock['Codigo_Proveedor']= df_stock['Codigo_Proveedor'].astype(int)
        df_stock['Codigo_Articulo']= df_stock['Codigo_Articulo'].astype(int)
        df_stock['Codigo_Sucursal']= df_stock['Codigo_Sucursal'].astype(int)
        df_stock.fillna(0, inplace= True)
        # df_stock.to_csv(file_path, index=False, encoding='utf-8')        
        print(f"---> Datos de STOCK guardados: {file_path}")
        return df_stock
    except Exception as e:
        print(f"Error en get_execution: {e}")
        return None
    finally:
        Close_Connection(conn)
        
def obtener_datos_stock_OLD (id_proveedor, etiqueta):
    folder = f"{secrets['BASE_DIR']}/{secrets['FOLDER_DATOS']}"
    
    #  Intento recuperar datos cacheados
    try:         
        print(f"-> Generando datos para ID: {id_proveedor}, Label: {etiqueta}")
        # Configuración de conexión
        conn = Open_Connection()
        
        # ----------------------------------------------------------------
        # FILTRA solo PRODUCTOS HABILITADOS y Traer datos de STOCK y PENDIENTES desde PRODUCCIÓN
        # OJO: hay que cambiar la consulta para que tome los datos de la tabla de stock por M_VENDE_POR_PESO cambiar StOCK por Peso
        # ----------------------------------------------------------------
        query = f"""              
            SELECT 
                A.[C_PROVEEDOR_PRIMARIO] AS Codigo_Proveedor,
                A.[C_COMPRADOR] AS Codigo_Comprador,
                S.[C_ARTICULO] AS Codigo_Articulo,
                S.[C_SUCU_EMPR] AS Codigo_Sucursal,
                S.[I_PRECIO_VTA] AS Precio_Venta,
                S.[I_COSTO_ESTADISTICO] AS Precio_Costo,
                S.[Q_FACTOR_VTA_SUCU] AS Factor_Venta,
                A.[M_VENDE_POR_PESO] AS M_Vende_Por_Peso,
                ST.Q_UNID_ARTICULO + ST.Q_PESO_ARTICULO AS Stock_Unidades, -- Stock Cierre Día Anterior
                
                (R.[Q_VENTA_30_DIAS] + R.[Q_VENTA_15_DIAS]) * S.[Q_FACTOR_VTA_SUCU] AS Venta_Unidades_30_Dias, -- OJO convertida desde BULTOS DIARCO
                
                (ST.Q_UNID_ARTICULO + ST.Q_PESO_ARTICULO) * S.[I_COSTO_ESTADISTICO] AS Stock_Valorizado, -- Stock Cierre Día Anterior
                
                (R.[Q_VENTA_30_DIAS] + R.[Q_VENTA_15_DIAS]) * S.[Q_FACTOR_VTA_SUCU] * S.[I_COSTO_ESTADISTICO] AS Venta_Valorizada,
                
                CASE 
                    WHEN (ISNULL(R.[Q_VENTA_30_DIAS], 0) + ISNULL(R.[Q_VENTA_15_DIAS], 0)) * ISNULL(S.[Q_FACTOR_VTA_SUCU], 0) * ISNULL(S.[I_COSTO_ESTADISTICO], 0) = 0 THEN NULL
                    ELSE 
                        ROUND(
                            ((ISNULL(ST.Q_UNID_ARTICULO,0) + ISNULL(ST.Q_PESO_ARTICULO,0)) * ISNULL(S.[I_COSTO_ESTADISTICO],0)) / 
                            NULLIF(
                                (ISNULL(R.[Q_VENTA_30_DIAS],0) + ISNULL(R.[Q_VENTA_15_DIAS],0)) * ISNULL(S.[Q_FACTOR_VTA_SUCU],0) * ISNULL(S.[I_COSTO_ESTADISTICO],0),
                                0
                            ), 
                            0
                        ) * 30
                END AS Dias_Stock,
                
                S.[F_ULTIMA_VTA],
                
                S.[Q_VTA_ULTIMOS_15DIAS] * S.[Q_FACTOR_VTA_SUCU] AS VENTA_UNIDADES_1Q, -- OJO esto está en BULTOS DIARCO
                S.[Q_VTA_ULTIMOS_30DIAS] * S.[Q_FACTOR_VTA_SUCU] AS VENTA_UNIDADES_2Q -- OJO esto está en BULTOS DIARCO

            FROM [DIARCOP001].[DiarcoP].[dbo].[T051_ARTICULOS_SUCURSAL] S
            INNER JOIN [DIARCOP001].[DiarcoP].[dbo].[T050_ARTICULOS] A
                ON A.[C_ARTICULO] = S.[C_ARTICULO]
            LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T060_STOCK] ST
                ON ST.C_ARTICULO = S.[C_ARTICULO] 
                AND ST.C_SUCU_EMPR = S.[C_SUCU_EMPR]
            LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T710_ESTADIS_REPOSICION] R
                ON R.[C_ARTICULO] = S.[C_ARTICULO]
                AND R.[C_SUCU_EMPR] = S.[C_SUCU_EMPR]

            WHERE 
                S.[M_HABILITADO_SUCU] = 'S' -- Permitido Reponer
                AND A.M_BAJA = 'N'          -- Activo en Maestro Artículos
                AND A.[C_PROVEEDOR_PRIMARIO] = {id_proveedor} -- Solo del Proveedor

            ORDER BY 
                S.[C_ARTICULO],
                S.[C_SUCU_EMPR];
        """
        # Ejecutar la consulta SQL
        df_stock = pd.read_sql(query, conn) # type: ignore
        file_path = f'{folder}/{etiqueta}_Stock.csv'
        df_stock['Codigo_Proveedor']= df_stock['Codigo_Proveedor'].astype(int)
        df_stock['Codigo_Articulo']= df_stock['Codigo_Articulo'].astype(int)
        df_stock['Codigo_Sucursal']= df_stock['Codigo_Sucursal'].astype(int)
        df_stock.fillna(0, inplace= True)
        # df_stock.to_csv(file_path, index=False, encoding='utf-8')        
        print(f"---> Datos de STOCK guardados: {file_path}")
        return df_stock
    except Exception as e:
        print(f"Error en get_execution: {e}")
        return None
    finally:
        Close_Connection(conn)

def obtener_demora_oc(id_proveedor, etiqueta):
    folder = f"{secrets['BASE_DIR']}/{secrets['FOLDER_DATOS']}"
    
    #  Intento recuperar datos cacheados
    try:         
        print(f"-> Generando datos para ID: {id_proveedor}, Label: {etiqueta}")
        # Configuración de conexión
        conn = Open_Diarco_Data()
        
        # ----------------------------------------------------------------
        # FILTRA solo PRODUCTOS HABILITADOS y Traer datos de STOCK y PENDIENTES desde PRODUCCIÓN
        # ----------------------------------------------------------------
        query = f""" 
        SELECT c_oc, u_prefijo_oc, u_sufijo_oc, u_dias_limite_entrega, fecha_limite, demora, codigo_proveedor, 
        codigo_sucursal, c_sucu_destino, c_sucu_destino_alt, c_situac, f_situac, f_alta_sist, f_emision, f_entrega, c_usuario_operador
        FROM src.base_forecast_oc_demoradas
        WHERE codigo_proveedor = {id_proveedor};
        """
        # Ejecutar la consulta SQL
        df_demoras = pd.read_sql(query, conn) # type: ignore
        # Renombrar columnas para estandarizar
        df_demoras = df_demoras.rename(columns={
            "c_oc": "C_OC",
            "u_prefijo_oc": "U_PREFIJO_OC",
            "u_sufijo_oc": "U_SUFIJO_OC",
            "u_dias_limite_entrega": "U_DIAS_LIMITE_ENTREGA",
            "fecha_limite": "FECHA_LIMITE",
            "demora": "Demora",
            "codigo_proveedor": "Codigo_Proveedor",
            "codigo_sucursal": "Codigo_Sucursal",
            "c_sucu_destino": "C_SUC_DESTINO",
            "c_sucu_destino_alt": "C_SUC_DESTINO_ALT",
            "c_situac": "C_SITUAC",
            "f_situac": "F_SITUAC",
            "f_alta_sist": "F_ALTA_SIST",
            "f_emision": "F_EMISION",
            "f_entrega": "F_ENTREGA",
            "c_usuario_operador": "C_USUARIO_OPERADOR"
        })
        
        df_demoras['Codigo_Proveedor']= df_demoras['Codigo_Proveedor'].astype(int)
        df_demoras['Codigo_Sucursal']= df_demoras['Codigo_Sucursal'].astype(int)
        df_demoras['Demora']= df_demoras['Demora'].astype(int)
        df_demoras.fillna(0, inplace= True)         
        print(f"---> Datos de OC DEMORADAS Recuperados: {etiqueta}")
        return df_demoras
    except Exception as e:
        print(f"Error en get_execution: {e}")
        return None
    finally:
        Close_Connection(conn)

def obtener_demora_oc_OLD(id_proveedor, etiqueta):
    folder = f"{secrets['BASE_DIR']}/{secrets['FOLDER_DATOS']}"
    
    #  Intento recuperar datos cacheados
    try:         
        print(f"-> Generando datos para ID: {id_proveedor}, Label: {etiqueta}")
        # Configuración de conexión
        conn = Open_Connection()
        
        # ----------------------------------------------------------------
        # FILTRA solo PRODUCTOS HABILITADOS y Traer datos de STOCK y PENDIENTES desde PRODUCCIÓN
        # ----------------------------------------------------------------
        query = f"""              
        SELECT  [C_OC]
            ,[U_PREFIJO_OC]
            ,[U_SUFIJO_OC]      
            ,[U_DIAS_LIMITE_ENTREGA]
            , DATEADD(DAY, [U_DIAS_LIMITE_ENTREGA], [F_ENTREGA]) as FECHA_LIMITE
            , DATEDIFF (DAY, DATEADD(DAY, [U_DIAS_LIMITE_ENTREGA], [F_ENTREGA]), GETDATE()) as Demora
            ,[C_PROVEEDOR] as Codigo_Proveedor
            ,[C_SUCU_COMPRA] as Codigo_Sucursal
            ,[C_SUCU_DESTINO]
            ,[C_SUCU_DESTINO_ALT]
            ,[C_SITUAC]
            ,[F_SITUAC]
            ,[F_ALTA_SIST]
            ,[F_EMISION]
            ,[F_ENTREGA]    
            ,[C_USUARIO_OPERADOR]    
            
        FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]  
        WHERE [C_SITUAC] = 1
        AND C_PROVEEDOR = {id_proveedor} 
        AND DATEADD(DAY, [U_DIAS_LIMITE_ENTREGA], [F_ENTREGA]) < GETDATE();
        """
        # Ejecutar la consulta SQL
        df_demoras = pd.read_sql(query, conn) # type: ignore
        df_demoras['Codigo_Proveedor']= df_demoras['Codigo_Proveedor'].astype(int)
        df_demoras['Codigo_Sucursal']= df_demoras['Codigo_Sucursal'].astype(int)
        df_demoras['Demora']= df_demoras['Demora'].astype(int)
        df_demoras.fillna(0, inplace= True)         
        print(f"---> Datos de OC DEMORADAS Recuperados: {etiqueta}")
        return df_demoras
    except Exception as e:
        print(f"Error en get_execution: {e}")
        return None
    finally:
        Close_Connection(conn)

def Exportar_Pronostico(df_forecast, proveedor, etiqueta, algoritmo):
    df_forecast['Codigo_Articulo']= df_forecast['Codigo_Articulo'].astype(int)
    df_forecast['Sucursal']= df_forecast['Sucursal'].astype(int)
    
    # tools.display_dataframe_to_user(name="SET de Datos del Proveedor", dataframe=df_forecast)
    # df_forecast.info()
    #print(f'-> ** Pronostico Guardado en: {folder}/{etiqueta}_{algoritmo}_Pronostico.csv **')
    #df_forecast.to_csv(f'{folder}/{etiqueta}_{algoritmo}_Pronostico.csv', index=False)
    
    ## GUARDAR TABLA EN POSTGRES
    usuario = getpass.getuser()  # Obtiene el usuario del sistema operativo
    fecha_actual = datetime.today().strftime('%Y-%m-%d')  # Obtiene la fecha de hoy en formato 'YYYY-MM-DD'
    conn = Open_Diarco_Data()
    
    # Query de inserción
    insert_query = """
    INSERT INTO public.f_oc_precarga_connexa (
        c_proveedor, c_articulo, c_sucu_empr, q_forecast_unidades, f_alta_forecast, c_usuario_forecast, create_date
    ) VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    # Convertir el DataFrame a una lista de tuplas para la inserción en bloque
    data_to_insert = [
        (proveedor, row['Codigo_Articulo'], row['Sucursal'], row['Forecast'], fecha_actual, usuario, fecha_actual)
        for _, row in df_forecast.iterrows()
    ]

    try:
        with conn.cursor() as cur: # type: ignore
            cur.executemany(insert_query, data_to_insert)
        conn.commit() # type: ignore
        print(f"✅ Inserción completada: {len(data_to_insert)} registros insertados.")
    except Exception as e:
        conn.rollback() # type: ignore # type: ignore # type: ignore
        print(f"❌ Error en la inserción: {e}")
    finally:
        Close_Connection(conn)

def get_precios(id_proveedor):
    conn = Open_Connection()
    query = f"""
        SELECT c_proveedor_primario, c_articulo, c_sucu_empr, i_precio_vta, i_costo_estadistico
        FROM src.base_forecast_precios
        WHERE c_proveedor_primario = {id_proveedor};
    """
    # Ejecutar la consulta SQL
    precios = pd.read_sql(query, conn) # type: ignore
    
    # Renombrar columnas para estandarizar
    precios = precios.rename(columns={
        "c_proveedor_primario": "C_PROVEEDOR_PRIMARIO",
        "c_articulo": "C_ARTICULO",
        "c_sucu_empr": "C_SUCU_EMPR",
        "i_precio_vta": "PRECIO_VENTA",
        "i_costo_estadistico": "PRECIO_COSTO"
        })
    
    precios['C_PROVEEDOR_PRIMARIO']= precios['C_PROVEEDOR_PRIMARIO'].astype(int)
    precios['C_ARTICULO']= precios['C_ARTICULO'].astype(int)
    precios['C_SUCU_EMPR']= precios['C_SUCU_EMPR'].astype(int)
    return precios

def get_precios_OLD (id_proveedor):
    conn = Open_Connection()
    query = f"""
        SELECT 
        A.[C_PROVEEDOR_PRIMARIO],
        S.[C_ARTICULO]
        ,S.[C_SUCU_EMPR]
        ,S.[I_PRECIO_VTA]
        ,S.[I_COSTO_ESTADISTICO]
        --,S.[M_HABILITADO_SUCU]
        --,A.M_BAJA                   
        FROM [DIARCOP001].[DiarcoP].[dbo].[T051_ARTICULOS_SUCURSAL] S
        LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T050_ARTICULOS] A
            ON A.[C_ARTICULO] = S.[C_ARTICULO]
        
        WHERE S.[M_HABILITADO_SUCU] = 'S' -- Permitido Reponer
            AND A.M_BAJA = 'N'  -- Activo en Maestro Artículos
            AND A.[C_PROVEEDOR_PRIMARIO] = {id_proveedor} -- Solo del Proveedor        
        ORDER BY S.[C_ARTICULO],S.[C_SUCU_EMPR];
    """
    # Ejecutar la consulta SQL
    precios = pd.read_sql(query, conn) # type: ignore
    precios['C_PROVEEDOR_PRIMARIO']= precios['C_PROVEEDOR_PRIMARIO'].astype(int)
    precios['C_ARTICULO']= precios['C_ARTICULO'].astype(int)
    precios['C_SUCU_EMPR']= precios['C_SUCU_EMPR'].astype(int)
    return precios

def actualizar_site_ids(df_forecast_ext, conn, name):
    """
    Reemplaza site_id en df_forecast_ext con datos válidos desde fnd_site.
    Asegura que no haya conflictos de columnas durante el merge.
    """
    query = """
    SELECT code, name, id FROM public.fnd_site
    WHERE company_id = 'e7498b2e-2669-473f-ab73-e2c8b4dcc585'
    ORDER BY code
    """
    stores = pd.read_sql(query, conn)

    # Asegurar que el campo 'code' sea numérico y entero
    stores = stores[pd.to_numeric(stores['code'], errors='coerce').notna()].copy()
    stores['code'] = stores['code'].astype(int)

    # Eliminar columna 'site_id' si ya existe
    df_forecast_ext = df_forecast_ext.drop(columns=['site_id'], errors='ignore')

    # Eliminar columna 'code' si ya existe en df_forecast_ext para evitar colisión en el merge
    if 'code' in df_forecast_ext.columns:
        df_forecast_ext = df_forecast_ext.drop(columns=['code'])

    # Realizar el merge con stores (fnd_site) para traer el site_id
    df_forecast_ext = df_forecast_ext.merge(
        stores[['code', 'id']],
        left_on='Sucursal',
        right_on='code',
        how='left'
    ).rename(columns={'id': 'site_id'})

    # Validar valores faltantes de site_id
    missing = df_forecast_ext[df_forecast_ext['site_id'].isna()]
    if not missing.empty:
        print(f"⚠️ Faltan site_id en {len(missing)} registros")
        missing.to_csv(f"{folder}/{name}_Missing_Site_IDs.csv", index=False)
    else:
        print("✅ Todos los registros tienen site_id válido")

    return df_forecast_ext


def mover_archivos_procesados(algoritmo, folder):    # Movel a procesado los archivos.
    destino = os.path.join(folder, "procesado")
    os.makedirs(destino, exist_ok=True)  # Crea la carpeta si no existe

    for archivo in os.listdir(folder):
        if archivo.startswith(algoritmo):
            origen = os.path.join(folder, archivo)
            destino_final = os.path.join(destino, archivo)
            shutil.move(origen, destino_final)
            print(f"📁 Archivo movido: {archivo} → {destino_final}")
            
            
import time
from functools import wraps

def medir_tiempo(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(f"\n🕒 Iniciando ejecución de {func.__name__}...")
        inicio = time.time()
        resultado = func(*args, **kwargs)
        fin = time.time()
        duracion = fin - inicio
        print(f"✅ Finalizó {func.__name__} en {duracion:.2f} segundos.\n")
        return resultado
    return wrapper

###----------------------------------------------------------------
#     ALGORITMOS
###----------------------------------------------------------------
# ALGO_01 Promedio de Ventas Ponderado 
###---------------------------------------------------------------- 
def Calcular_Demanda_ALGO_01(df, id_proveedor, etiqueta, period_length, current_date, factor_last, factor_previous, factor_year):
    print('Dentro del Calcular_Demanda_ALGO_01')
    print(f'FORECAST control: {id_proveedor} - {etiqueta} - ventana: {period_length} - factores: {factor_last} - {factor_previous} - {factor_year}')
    
    start_time = time.time()
    
    # Convertir Parámetros a INT o FLOAT
    period_length = int(period_length)  # Asegurarse de que sea un entero
    factor_last = float(factor_last)
    factor_previous = float(factor_previous)
    factor_year = float(factor_year)
    # Convertir la columna 'Fecha' a tipo datetime si no lo está
    if not pd.api.types.is_datetime64_any_dtype(df['Fecha']):
        df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
        df.dropna(subset=['Fecha'], inplace=True)  # Eliminar filas con fechas inválidas
        
    
    # Definir rangos de fechas para cada período
    last_period_start = current_date - pd.Timedelta(days=period_length - 1)
    last_period_end = current_date

    previous_period_start = current_date - pd.Timedelta(days=2 * period_length - 1)
    previous_period_end = current_date - pd.Timedelta(days=period_length)

    same_period_last_year_start = current_date - pd.DateOffset(years=1) - pd.Timedelta(days=period_length - 1)
    same_period_last_year_end = current_date - pd.DateOffset(years=1)

    # Filtrar los datos para cada uno de los períodos
    df_last = df[(df['Fecha'] >= last_period_start) & (df['Fecha'] <= last_period_end)]
    df_previous = df[(df['Fecha'] >= previous_period_start) & (df['Fecha'] <= previous_period_end)]
    df_same_year = df[(df['Fecha'] >= same_period_last_year_start) & (df['Fecha'] <= same_period_last_year_end)]

    # Agregar las ventas (unidades) por artículo y sucursal para cada período
    sales_last = df_last.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                        .sum().reset_index().rename(columns={'Unidades': 'ventas_last'})
    sales_previous = df_previous.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                                .sum().reset_index().rename(columns={'Unidades': 'ventas_previous'})
    sales_same_year = df_same_year.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                                .sum().reset_index().rename(columns={'Unidades': 'ventas_same_year'})

    # Unir la información de los tres períodos
    df_forecast = pd.merge(sales_last, sales_previous, on=['Codigo_Articulo', 'Sucursal'], how='outer')
    df_forecast = pd.merge(df_forecast, sales_same_year, on=['Codigo_Articulo', 'Sucursal'], how='outer')
    df_forecast.fillna(0, inplace=True)

    # Calcular la demanda estimada como el promedio de las ventas de los tres períodos
    df_forecast['Forecast'] = (df_forecast['ventas_last'] * factor_last +
                               df_forecast['ventas_previous'] * factor_previous +
                               df_forecast['ventas_same_year'] * factor_year) 
                                # / (factor_year + factor_last + factor_previous) Antes dividía por la Sumatoria. Ahora sigo el peso absoluto de los factores.
    elapsed = round(time.time() - start_time, 2)
    print(f"🖼️ Preparación de Datos - Tiempo: {elapsed} seg")
    # Redondear la predicción al entero más cercano  y eliminar los Negativos
    df_forecast['Forecast'] = np.ceil(df_forecast['Forecast']).clip(lower=0) # type: ignore
    df_forecast['Average'] = round(df_forecast['Forecast'] /period_length ,3)
    # Agregar las columnas id_proveedor y ventana
    df_forecast['id_proveedor'] = id_proveedor
    df_forecast['algoritmo'] = 'ALGO_01'
    df_forecast['ventana'] = period_length
    df_forecast['f1'] = factor_last
    df_forecast['f2'] = factor_previous
    df_forecast['f3'] = factor_year
    df_forecast['Fecha_Pronostico'] = current_date 

    # Reordenar las columnas según la especificación
    df_forecast = df_forecast[['id_proveedor', 'Codigo_Articulo', 'Sucursal',  'algoritmo', 'ventana', 'f1', 'f2', 'f3', 'Fecha_Pronostico',
                            'Forecast', 'Average','ventas_last', 'ventas_previous', 'ventas_same_year']]
    
    elapsed = round(time.time() - start_time, 2)
    print(f"🖼️ Demanda Calculada - Tiempo: {elapsed} seg")
    return df_forecast


    # Borrar Columnas Innecesarias
    # forecast_df.drop(columns=['ventas_last', 'ventas_previous', 'ventas_same_year'], inplace=True)

###----------------------------------------------------------------
# ALGO_02 Doble Exponencial -  Modelo Holt (TENDENCIA)
###----------------------------------------------------------------
def Calcular_Demanda_ALGO_02(df, id_proveedor, etiqueta, ventana, current_date):
    print('Dentro del Calcular_Demanda_ALGO_02')
    print(f'FORECAST Holt control: {id_proveedor} - {etiqueta} - ventana: {ventana} ')

        # Ajustar el modelo Holt-Winters: 
        # - trend: 'add' para tendencia aditiva
        # - seasonal: 'add' para estacionalidad aditiva
        # - seasonal_periods: 7 (para estacionalidad semanal)
    # Configurar la ventana de pronóstico (por ejemplo, 30 días o 45 días)
    #forecast_window = 30  # Cambia a 45 si es necesario
    # Lista para almacenar los resultados del forecast
    resultados = []
    
    # Definir rangos de fechas para cada período
    last_period_start = current_date - pd.Timedelta(days=ventana - 1)
    last_period_end = current_date

    previous_period_start = current_date - pd.Timedelta(days=2 * ventana - 1)
    previous_period_end = current_date - pd.Timedelta(days=ventana)

    same_period_last_year_start = current_date - pd.DateOffset(years=1) - pd.Timedelta(days=ventana - 1)
    same_period_last_year_end = current_date - pd.DateOffset(years=1)

    # Filtrar los datos para cada uno de los períodos
    df_last = df[(df['Fecha'] >= last_period_start) & (df['Fecha'] <= last_period_end)]
    df_previous = df[(df['Fecha'] >= previous_period_start) & (df['Fecha'] <= previous_period_end)]
    df_same_year = df[(df['Fecha'] >= same_period_last_year_start) & (df['Fecha'] <= same_period_last_year_end)]

    # Agregar las ventas (unidades) por artículo y sucursal para cada período
    sales_last = df_last.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                        .sum().reset_index().rename(columns={'Unidades': 'ventas_last'})
    sales_previous = df_previous.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                                .sum().reset_index().rename(columns={'Unidades': 'ventas_previous'})
    sales_same_year = df_same_year.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                                .sum().reset_index().rename(columns={'Unidades': 'ventas_same_year'})

    # Agrupar los datos por 'Codigo_Articulo' y 'Sucursal'
    for (codigo, sucursal), grupo in df.groupby(['Codigo_Articulo', 'Sucursal']):
        # Ordenar cronológicamente y fijar 'Fecha' como índice
        grupo = grupo.set_index('Fecha').sort_index()
        
        # Resamplear a frecuencia diaria sumando las ventas y rellenando días sin datos
        ventas_diarias = grupo['Unidades'].resample('D').sum().fillna(0)
        
        # Verificar que la serie tenga suficientes datos para ajustar el modelo
        if len(ventas_diarias) < 2 * 7:  # por ejemplo, al menos dos ciclos de la estacionalidad semanal
            continue

        try:
            # Ajustar el modelo Holt
            # - trend: 'add' para tendencia aditiva
            # - seasonal: 'add' para estacionalidad aditiva
            # - seasonal_periods: 7 (para estacionalidad semanal)
            modelo = Holt(ventas_diarias)
            modelo_ajustado = modelo.fit(optimized=True)
            
            # Realizar el forecast para la ventana definida
            pronostico = modelo_ajustado.forecast(ventana)
            
            # La demanda esperada es la suma de las predicciones diarias en el periodo
            forecast_total = pronostico.sum()
        except Exception as e:
            # Si ocurre algún error en el ajuste, puedes asignar un valor nulo o manejarlo de otra forma
            forecast_total = None
        
        resultados.append({
            'Codigo_Articulo': codigo,
            'Sucursal': sucursal,
            'Forecast': round(forecast_total, 2) if forecast_total is not None else None
        })

        # Crear el DataFrame final con los resultados del forecast
    df_forecast = pd.DataFrame(resultados)
        # Redondear la predicción al entero más cercano
    df_forecast['Forecast'] = np.ceil(df_forecast['Forecast']).clip(lower=0) # type: ignore
    df_forecast['Average'] = round(df_forecast['Forecast'] /ventana ,3)
    
        # Agregar las columnas id_proveedor y ventana
    df_forecast['id_proveedor'] = id_proveedor
    df_forecast['ventana'] = ventana
    df_forecast['algoritmo'] = 'ALGO_02'
    df_forecast['f1'] = 'na'
    df_forecast['f2'] = 'na'
    df_forecast['f3'] = 'na'
    df_forecast['Fecha_Pronostico'] = current_date 
    
        # Unir las ventas de los períodos con el forecast
    df_forecast = pd.merge(df_forecast, sales_last, on=['Codigo_Articulo', 'Sucursal'], how='left')
    df_forecast = pd.merge(df_forecast, sales_previous, on=['Codigo_Articulo', 'Sucursal'], how='left')
    df_forecast = pd.merge(df_forecast, sales_same_year, on=['Codigo_Articulo', 'Sucursal'], how='left')
    df_forecast.fillna(0, inplace=True)

        # Reordenar las columnas según la especificación
    df_forecast = df_forecast[['id_proveedor', 'Codigo_Articulo', 'Sucursal',  'algoritmo', 'ventana', 'f1', 'f2', 'f3', 'Fecha_Pronostico',
                            'Forecast', 'Average','ventas_last', 'ventas_previous', 'ventas_same_year']]
    
    return df_forecast

###----------------------------------------------------------------
# ALGO_03 Suavizado Exponencial -  Modelo Holt-Winters (TENDENCIA + ESTACIONALIDAD)
###----------------------------------------------------------------
def Calcular_Demanda_ALGO_03(df, id_proveedor, etiqueta, ventana, current_date, periodos, f2, f3):
    print('Dentro del Calcular_Demanda_ALGO_03')
    print(f'FORECAST control: {id_proveedor} - {etiqueta} - ventana: {ventana} - factores: Períodos Estacionalidad  {periodos} - Tendencia: {f2} - Estacionalidad: {f3}')

        # Ajustar el modelo Holt-Winters: 
        # - trend: 'add' para tendencia aditiva
        # - seasonal: 'add' para estacionalidad aditiva
        # - seasonal_periods: 7 (para estacionalidad semanal)
    # Configurar la ventana de pronóstico (por ejemplo, 30 días o 45 días)
    #forecast_window = 30  # Cambia a 45 si es necesario
    # Lista para almacenar los resultados del forecast
    resultados = []
    
    # Definir rangos de fechas para cada período
    last_period_start = current_date - pd.Timedelta(days=ventana - 1)
    last_period_end = current_date

    previous_period_start = current_date - pd.Timedelta(days=2 * ventana - 1)
    previous_period_end = current_date - pd.Timedelta(days=ventana)

    same_period_last_year_start = current_date - pd.DateOffset(years=1) - pd.Timedelta(days=ventana - 1)
    same_period_last_year_end = current_date - pd.DateOffset(years=1)

    # Filtrar los datos para cada uno de los períodos
    df_last = df[(df['Fecha'] >= last_period_start) & (df['Fecha'] <= last_period_end)]
    df_previous = df[(df['Fecha'] >= previous_period_start) & (df['Fecha'] <= previous_period_end)]
    df_same_year = df[(df['Fecha'] >= same_period_last_year_start) & (df['Fecha'] <= same_period_last_year_end)]

    # Agregar las ventas (unidades) por artículo y sucursal para cada período
    sales_last = df_last.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                        .sum().reset_index().rename(columns={'Unidades': 'ventas_last'})
    sales_previous = df_previous.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                                .sum().reset_index().rename(columns={'Unidades': 'ventas_previous'})
    sales_same_year = df_same_year.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                                .sum().reset_index().rename(columns={'Unidades': 'ventas_same_year'})
    
    # Agrupar los datos por 'Codigo_Articulo' y 'Sucursal'
    for (codigo, sucursal), grupo in df.groupby(['Codigo_Articulo', 'Sucursal']):
        # Ordenar cronológicamente y fijar 'Fecha' como índice
        grupo = grupo.set_index('Fecha').sort_index()
        
        # Resamplear a frecuencia diaria sumando las ventas y rellenando días sin datos
        ventas_diarias = grupo['Unidades'].resample('D').sum().fillna(0)
        
        # Verificar que la serie tenga suficientes datos para ajustar el modelo
        if len(ventas_diarias) < 2 * 7:  # por ejemplo, al menos dos ciclos de la estacionalidad semanal
            continue

        try:
            # Ajustar el modelo Holt-Winters: 
            # - trend: 'add' para tendencia aditiva
            # - seasonal: 'add' para estacionalidad aditiva
            # - seasonal_periods: 7 (para estacionalidad semanal)
            modelo = ExponentialSmoothing(ventas_diarias, trend=f2, seasonal=f3, seasonal_periods=periodos)
            modelo_ajustado = modelo.fit(optimized=True)
            
            # Realizar el forecast para la ventana definida
            pronostico = modelo_ajustado.forecast(ventana)
            
            # La demanda esperada es la suma de las predicciones diarias en el periodo
            forecast_total = pronostico.sum()
        except Exception as e:
            # Si ocurre algún error en el ajuste, puedes asignar un valor nulo o manejarlo de otra forma
            forecast_total = None
        
        resultados.append({
            'Codigo_Articulo': codigo,
            'Sucursal': sucursal,
            'Forecast': round(forecast_total, 2) if forecast_total is not None else None
        })

    # Crear el DataFrame final con los resultados del forecast
    df_forecast = pd.DataFrame(resultados)
    # Redondear la predicción al entero más cercano
    df_forecast['Forecast'] = np.ceil(df_forecast['Forecast']).clip(lower=0) # type: ignore
    df_forecast['Average'] = round(df_forecast['Forecast'] /ventana ,3)
    
    # Agregar las columnas id_proveedor y ventana
    df_forecast['id_proveedor'] = id_proveedor
    df_forecast['ventana'] = ventana
    df_forecast['algoritmo'] = 'ALGO_03'
    df_forecast['f1'] = periodos
    df_forecast['f2'] = f2
    df_forecast['f3'] = f3
    df_forecast['Fecha_Pronostico'] = current_date 
    
        # Unir las ventas de los períodos con el forecast
    df_forecast = pd.merge(df_forecast, sales_last, on=['Codigo_Articulo', 'Sucursal'], how='left')
    df_forecast = pd.merge(df_forecast, sales_previous, on=['Codigo_Articulo', 'Sucursal'], how='left')
    df_forecast = pd.merge(df_forecast, sales_same_year, on=['Codigo_Articulo', 'Sucursal'], how='left')
    df_forecast.fillna(0, inplace=True)

    # Reordenar las columnas según la especificación
    df_forecast = df_forecast[['id_proveedor', 'Codigo_Articulo', 'Sucursal',  'algoritmo', 'ventana', 'f1', 'f2', 'f3', 'Fecha_Pronostico',
                            'Forecast', 'Average','ventas_last', 'ventas_previous', 'ventas_same_year']]
    return df_forecast

###----------------------------------------------------------------
# ALGO_04 Suavizado Exponencial Simple -  Modelo de Media Movil Exponencial Ponderada (EWMA) x Factor alpha
###----------------------------------------------------------------
def Calcular_Demanda_ALGO_04(df, id_proveedor, etiqueta, ventana, current_date, alpha):
    print('Dentro del Calcular_Demanda_ALGO_04')
    print(f'FORECAST control: {id_proveedor} - {etiqueta} - ventana: {ventana} - Fator Alpha: {alpha} ')

    # Configurar la ventana de pronóstico (por ejemplo, 30 o 45 días)
    #forecast_window = 45  # Puedes cambiarlo a 45 según tus necesidades
    # Parámetro de suavizado (alpha); valores cercanos a 1 dan más peso a los datos recientes
    #alpha = 0.3

    # Lista para almacenar los resultados del forecast
    resultados = []
    
    # Definir rangos de fechas para cada período
    last_period_start = current_date - pd.Timedelta(days=ventana - 1)
    last_period_end = current_date

    previous_period_start = current_date - pd.Timedelta(days=2 * ventana - 1)
    previous_period_end = current_date - pd.Timedelta(days=ventana)

    same_period_last_year_start = current_date - pd.DateOffset(years=1) - pd.Timedelta(days=ventana - 1)
    same_period_last_year_end = current_date - pd.DateOffset(years=1)

    # Filtrar los datos para cada uno de los períodos
    df_last = df[(df['Fecha'] >= last_period_start) & (df['Fecha'] <= last_period_end)]
    df_previous = df[(df['Fecha'] >= previous_period_start) & (df['Fecha'] <= previous_period_end)]
    df_same_year = df[(df['Fecha'] >= same_period_last_year_start) & (df['Fecha'] <= same_period_last_year_end)]

    # Agregar las ventas (unidades) por artículo y sucursal para cada período
    sales_last = df_last.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                        .sum().reset_index().rename(columns={'Unidades': 'ventas_last'})
    sales_previous = df_previous.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                                .sum().reset_index().rename(columns={'Unidades': 'ventas_previous'})
    sales_same_year = df_same_year.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                                .sum().reset_index().rename(columns={'Unidades': 'ventas_same_year'})
    
    # Agrupar los datos por 'Codigo_Articulo' y 'Sucursal'
    for (codigo, sucursal), grupo in df.groupby(['Codigo_Articulo', 'Sucursal']):
        # Ordenar cronológicamente y fijar 'Fecha' como índice
        grupo = grupo.set_index('Fecha').sort_index()
        
        # Resamplear a frecuencia diaria sumando las ventas y rellenando días sin datos
        ventas_diarias = grupo['Unidades'].resample('D').sum().fillna(0)
        
        # Calcular el suavizado exponencial (EWMA) sobre la serie de ventas diarias
        ewma_series = ventas_diarias.ewm(alpha=alpha, adjust=False).mean()
        
        # Tomamos el último valor suavizado como forecast diario
        ultimo_ewma = ewma_series.iloc[-1]
        
        # El pronóstico total para la ventana definida es el pronóstico diario multiplicado por la cantidad de días
        forecast_total = ultimo_ewma * ventana
        
        resultados.append({
            'Codigo_Articulo': codigo,
            'Sucursal': sucursal,
            'Forecast': round(forecast_total, 2),
            'Average': round(ultimo_ewma, 3)
        })

    # Crear el DataFrame final con los resultados
    df_forecast = pd.DataFrame(resultados)
    # Redondear la predicción al entero más cercano y evitar negativos
    df_forecast['Forecast'] = np.ceil(df_forecast['Forecast']).clip(lower=0) # type: ignore
    # Agregar las columnas id_proveedor y ventana
    df_forecast['id_proveedor'] = id_proveedor
    df_forecast['ventana'] = ventana
    df_forecast['algoritmo'] = 'ALGO_04'
    df_forecast['f1'] = alpha
    df_forecast['f2'] = 'na'
    df_forecast['f3'] = 'na'
    df_forecast['Fecha_Pronostico'] = current_date 
    
        # Unir las ventas de los períodos con el forecast
    df_forecast = pd.merge(df_forecast, sales_last, on=['Codigo_Articulo', 'Sucursal'], how='left')
    df_forecast = pd.merge(df_forecast, sales_previous, on=['Codigo_Articulo', 'Sucursal'], how='left')
    df_forecast = pd.merge(df_forecast, sales_same_year, on=['Codigo_Articulo', 'Sucursal'], how='left')
    df_forecast.fillna(0, inplace=True)
    
        # Reordenar las columnas según la especificación
    df_forecast = df_forecast[['id_proveedor', 'Codigo_Articulo', 'Sucursal',  'algoritmo', 'ventana', 'f1', 'f2', 'f3', 'Fecha_Pronostico',
                            'Forecast', 'Average','ventas_last', 'ventas_previous', 'ventas_same_year']]
    
    return df_forecast

###----------------------------------------------------------------
# ALGO_05 Promedio de Venta SIMPLE (PVS) (Metodo Actual que usan los Compradores)
###----------------------------------------------------------------
def Calcular_Demanda_ALGO_05(df, id_proveedor, etiqueta, ventana, current_date):
    # Lista para almacenar los resultados del pronóstico
    resultados = []

    # Definir rangos de fechas para cada período
    last_period_start = current_date - pd.Timedelta(days=ventana - 1)
    last_period_end = current_date

    previous_period_start = current_date - pd.Timedelta(days=2 * ventana - 1)
    previous_period_end = current_date - pd.Timedelta(days=ventana)

    same_period_last_year_start = current_date - pd.DateOffset(years=1) - pd.Timedelta(days=ventana - 1)
    same_period_last_year_end = current_date - pd.DateOffset(years=1)

    # Filtrar los datos para cada uno de los períodos
    df_last = df[(df['Fecha'] >= last_period_start) & (df['Fecha'] <= last_period_end)]
    df_previous = df[(df['Fecha'] >= previous_period_start) & (df['Fecha'] <= previous_period_end)]
    df_same_year = df[(df['Fecha'] >= same_period_last_year_start) & (df['Fecha'] <= same_period_last_year_end)]

    # Agregar las ventas (unidades) por artículo y sucursal para cada período
    sales_last = df_last.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                        .sum().reset_index().rename(columns={'Unidades': 'ventas_last'})
    sales_previous = df_previous.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                                .sum().reset_index().rename(columns={'Unidades': 'ventas_previous'})
    sales_same_year = df_same_year.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                                .sum().reset_index().rename(columns={'Unidades': 'ventas_same_year'})
    
    # Agrupar los datos por 'Codigo_Articulo' y 'Sucursal'
    for (codigo, sucursal), grupo in df.groupby(['Codigo_Articulo', 'Sucursal']):
        # Establecer 'Fecha' como índice y ordenar los datos
        grupo = grupo.set_index('Fecha').sort_index()
        
        # Resamplear a diario sumando las ventas
        ventas_diarias = grupo['Unidades'].resample('D').sum().fillna(0)
        
        # Seleccionar un periodo reciente para calcular la media; por ejemplo, los últimos 30 días
        # Si hay menos de 30 días de datos, se utiliza el periodo disponible
        ventas_recientes = ventas_diarias[-30:]
        media_diaria = ventas_recientes.mean()
        
        # Pronosticar la demanda para el periodo de reposición
        pronostico = media_diaria * ventana
        
        resultados.append({
            'Codigo_Articulo': codigo,
            'Sucursal': sucursal,
            'Forecast': round(pronostico, 2),
            'Average': round(media_diaria, 3)
        })

    # Crear el DataFrame de pronósticos
    df_forecast = pd.DataFrame(resultados)
        # Redondear la predicción al entero más cercano
    df_forecast['Forecast'] = np.ceil(df_forecast['Forecast']).clip(lower=0) # type: ignore
    df_forecast['Average'] = round(df_forecast['Forecast'] /ventana ,3)
    
    # Agregar las columnas id_proveedor y ventana
    df_forecast['id_proveedor'] = id_proveedor
    df_forecast['ventana'] = ventana
    df_forecast['algoritmo'] = 'ALGO_05'
    df_forecast['f1'] = 'na'
    df_forecast['f2'] = 'na'
    df_forecast['f3'] = 'na'
    df_forecast['Fecha_Pronostico'] = current_date 
    
    # Unir las ventas de los períodos con el forecast
    df_forecast = pd.merge(df_forecast, sales_last, on=['Codigo_Articulo', 'Sucursal'], how='left')
    df_forecast = pd.merge(df_forecast, sales_previous, on=['Codigo_Articulo', 'Sucursal'], how='left')
    df_forecast = pd.merge(df_forecast, sales_same_year, on=['Codigo_Articulo', 'Sucursal'], how='left')
    df_forecast.fillna(0, inplace=True)
    
        # Reordenar las columnas según la especificación
    df_forecast = df_forecast[['id_proveedor', 'Codigo_Articulo', 'Sucursal',  'algoritmo', 'ventana', 'f1', 'f2', 'f3', 'Fecha_Pronostico',
                            'Forecast', 'Average','ventas_last', 'ventas_previous', 'ventas_same_year']]

    return df_forecast

###----------------------------------------------------------------
# ALGO_06 Demanda Agrupada Semanal -  Modelo Holt (TENDENCIA)
###----------------------------------------------------------------
def Calcular_Demanda_ALGO_06(df, id_proveedor, etiqueta, ventana, current_date):
    print('Dentro del Calcular_Demanda_ALGO_06')
    print(f'FORECAST Holt control: {id_proveedor} - {etiqueta} - ventana: {ventana}')

    # Convertir ventana a entero y calcular forecast_window en semanas
    try:
        forecast_window = int(ventana) // 7  # Semanas de forecast
        if forecast_window < 4:
            raise ValueError("La ventana debe ser al menos 28 días para calcular el forecast.")
    except ValueError:
        print("Error: La ventana proporcionada no es válida.")
        return pd.DataFrame()  # Retornar DataFrame vacío en caso de error

    resultados = []

    # Definir rangos de fechas para cada período
    last_period_start = current_date - pd.Timedelta(days=ventana - 1)
    last_period_end = current_date

    previous_period_start = current_date - pd.Timedelta(days=2 * ventana - 1)
    previous_period_end = current_date - pd.Timedelta(days=ventana)

    same_period_last_year_start = current_date - pd.DateOffset(years=1) - pd.Timedelta(days=ventana - 1)
    same_period_last_year_end = current_date - pd.DateOffset(years=1)

    # Filtrar los datos para cada uno de los períodos
    df_last = df[(df['Fecha'] >= last_period_start) & (df['Fecha'] <= last_period_end)]
    df_previous = df[(df['Fecha'] >= previous_period_start) & (df['Fecha'] <= previous_period_end)]
    df_same_year = df[(df['Fecha'] >= same_period_last_year_start) & (df['Fecha'] <= same_period_last_year_end)]

    # Agregar las ventas (unidades) por artículo y sucursal para cada período
    sales_last = df_last.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                        .sum().reset_index().rename(columns={'Unidades': 'ventas_last'})
    sales_previous = df_previous.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                                .sum().reset_index().rename(columns={'Unidades': 'ventas_previous'})
    sales_same_year = df_same_year.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                                .sum().reset_index().rename(columns={'Unidades': 'ventas_same_year'})

    # Agrupar los datos por 'Codigo_Articulo' y 'Sucursal'
    for (codigo, sucursal), grupo in df.groupby(['Codigo_Articulo', 'Sucursal']):
        # Ordenar cronológicamente y fijar 'Fecha' como índice
        grupo = grupo.set_index('Fecha').sort_index()

        # Resamplear a frecuencia semanal sumando las ventas y rellenando semanas sin datos
        ventas_semanales = grupo['Unidades'].resample('W').sum().fillna(0)

        # Verificar que la serie tenga suficientes datos para ajustar el modelo
        if len(ventas_semanales) < 4:  # Se requieren al menos 4 semanas de datos
            continue

        try:
            # Ajustar el modelo Holt con tendencia aditiva
            modelo = Holt(ventas_semanales)
            modelo_ajustado = modelo.fit(smoothing_level=0.8, smoothing_slope=0.2)   # type: ignore # type: ignore # type: ignore # type: ignore
            
            # Realizar el forecast para la ventana definida (semanal)
            pronostico = modelo_ajustado.forecast(forecast_window)
            
            # La demanda esperada es la suma de las predicciones semanales en el periodo
            forecast_total = pronostico.sum()
        except Exception as e:
            print(f"Error al ajustar el modelo para Código_Articulo {codigo} y Sucursal {sucursal}: {e}")
            forecast_total = 0  # En caso de error, asignar 0

        # Agregar resultado al listado
        resultados.append({
            'Codigo_Articulo': codigo,
            'Sucursal': sucursal,
            'Forecast': round(forecast_total, 2)
        })

    # Crear el DataFrame final con los resultados del forecast
    df_forecast = pd.DataFrame(resultados)

    # Verificar si el DataFrame tiene datos antes de continuar
    if df_forecast.empty:
        print("Advertencia: No se generaron pronósticos debido a falta de datos.")
        return df_forecast  # Retornar DataFrame vacío

    # Redondear la predicción al entero más cercano y evitar valores negativos
    df_forecast['Forecast'] = np.ceil(df_forecast['Forecast']).clip(lower=0) # type: ignore
    
    # Calcular el promedio semanal si forecast_window > 0
    df_forecast['Average'] = round(df_forecast['Forecast'] / ventana, 3) if ventana > 0 else 0

    # Unir las ventas de los períodos con el forecast
    df_forecast = pd.merge(df_forecast, sales_last, on=['Codigo_Articulo', 'Sucursal'], how='left')
    df_forecast = pd.merge(df_forecast, sales_previous, on=['Codigo_Articulo', 'Sucursal'], how='left')
    df_forecast = pd.merge(df_forecast, sales_same_year, on=['Codigo_Articulo', 'Sucursal'], how='left')

    # Rellenar valores NaN con 0
    df_forecast.fillna(0, inplace=True)

    # Agregar las columnas id_proveedor, ventana y algoritmo
    df_forecast['id_proveedor'] = id_proveedor
    df_forecast['ventana'] = ventana
    df_forecast['algoritmo'] = 'ALGO_06'
    df_forecast['f1'] = 'na'
    df_forecast['f2'] = 'na'
    df_forecast['f3'] = 'na'
    df_forecast['Fecha_Pronostico'] = current_date 
    
        # Reordenar las columnas según la especificación
    df_forecast = df_forecast[['id_proveedor', 'Codigo_Articulo', 'Sucursal',  'algoritmo', 'ventana', 'f1', 'f2', 'f3', 'Fecha_Pronostico',
                            'Forecast', 'Average','ventas_last', 'ventas_previous', 'ventas_same_year']]

    return df_forecast

###----------------------------------------------------------------
# ALGO_07 Demanda Simple x Factor -  Fecha de Base Movil
###----------------------------------------------------------------
def Calcular_Demanda_ALGO_07(df, id_proveedor, etiqueta, periodo, current_date,  factor, fecha_base):
    print('Dentro del Calcular_Demanda_ALGO_07')
    print(f'FORECAST control: {id_proveedor} - {etiqueta} - ventana: {periodo} - Fecha Actual: {current_date} - factor: {factor}  - Fecha de Base: {fecha_base}')
    
    start_time = time.time()
    
    # Convertir Parámetros a INT o FLOAT
    period_length = int(periodo)  # Asegurarse de que sea un entero
    base_date = pd.to_datetime(fecha_base, errors='coerce')  # Convertir a datetime, manejar errores
    factor = float(factor)

    # Convertir la columna 'Fecha' a tipo datetime si no lo está
    if not pd.api.types.is_datetime64_any_dtype(df['Fecha']):
        df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
        df.dropna(subset=['Fecha'], inplace=True)  # Eliminar filas con fechas inválidas
    
    # Definir rango de fechas
    last_period_start = base_date     
    last_period_end = base_date + pd.Timedelta(days=period_length)

    # Filtrar los datos para cada uno de los períodos
    df_last = df[(df['Fecha'] >= last_period_start) & (df['Fecha'] <= last_period_end)]

    # Agregar las ventas (unidades) por artículo y sucursal para cada período
    sales_last = df_last.groupby(['Codigo_Articulo', 'Sucursal'])['Unidades'] \
                        .sum().reset_index().rename(columns={'Unidades': 'ventas_last'})

    # Unir la información de los tres períodos
    df_forecast = sales_last.copy() 
    df_forecast.fillna(0, inplace=True)

    # Calcular la demanda estimada como el promedio de las ventas del período multiplicado pro el factor
    df_forecast['Forecast'] = (df_forecast['ventas_last'] * factor)       #Aplico el peso absoluto de los factores.
    
    elapsed = round(time.time() - start_time, 2)
    print(f"🖼️ Preparación de Datos - Tiempo: {elapsed} seg")
    # Redondear la predicción al entero más cercano  y eliminar los Negativos
    df_forecast['Forecast'] = np.ceil(df_forecast['Forecast']).clip(lower=0) # type: ignore
    df_forecast['Average'] = round(df_forecast['Forecast'] /period_length ,3)
    
    # Agregar las columnas id_proveedor y ventana
    df_forecast['id_proveedor'] = id_proveedor
    df_forecast['algoritmo'] = 'ALGO_07'
    df_forecast['ventana'] = period_length
    df_forecast['f1'] = factor
    df_forecast['f2'] = 'na'
    df_forecast['f3'] = 'na'
    df_forecast['ventas_previous'] = 0  # Por compatibilidad con la estructura
    df_forecast['ventas_same_year'] = 0
    df_forecast['Fecha_Pronostico'] = base_date 

    # Reordenar las columnas según la especificación
    df_forecast = df_forecast[['id_proveedor', 'Codigo_Articulo', 'Sucursal',  'algoritmo', 'ventana', 'f1', 'f2', 'f3', 'Fecha_Pronostico',
                            'Forecast', 'Average','ventas_last', 'ventas_previous', 'ventas_same_year']]
    
    elapsed = round(time.time() - start_time, 2)
    print(f"🖼️ Demanda Calculada - Tiempo: {elapsed} seg")
    return df_forecast


    # Borrar Columnas Innecesarias
    # forecast_df.drop(columns=['ventas_last', 'ventas_previous', 'ventas_same_year'], inplace=True)

###----------------------------------------------------------------
# RUTINAS DE PROCESAMIENTO DE ALGORITMOS
###----------------------------------------------------------------

def Procesar_ALGO_07(data, proveedor, etiqueta, periodo, current_date, factor, fecha_base):    
    # Asignar valores por defecto si los factores no están definidos
    factor = 1 if factor is None else factor

    print(f'--> Procesar_ALGO_07 Período {periodo} - Factores Utilizados: Factor: {factor} Fecha Base: {fecha_base}')
        
    df_forecast = Calcular_Demanda_ALGO_07(data, proveedor, etiqueta,  periodo, current_date, factor, fecha_base)
    df_forecast['Codigo_Articulo']= df_forecast['Codigo_Articulo'].astype(int)
    df_forecast['Sucursal']= df_forecast['Sucursal'].astype(int)
    df_forecast.to_csv(f'{folder}/{etiqueta}_ALGO_07_Solicitudes_Compra.csv', index=False)   # Exportar el resultado a un CSV para su posterior procesamiento
    
    Exportar_Pronostico(df_forecast, proveedor, etiqueta, 'ALGO_07')  # Impactar Datos en la Interface        
    return  

def Procesar_ALGO_06(data, proveedor, etiqueta, ventana, fecha):
    print(f'--> Procesar_ALGO_06 ventana {ventana} - fecha {fecha} - No usa Factores')
    df_forecast = Calcular_Demanda_ALGO_06(data, proveedor, etiqueta, ventana, fecha)    # Exportar el resultado a un CSV para su posterior procesamiento
    df_forecast['Codigo_Articulo']= df_forecast['Codigo_Articulo'].astype(int)
    df_forecast['Sucursal']= df_forecast['Sucursal'].astype(int)
    df_forecast.to_csv(f'{folder}/{etiqueta}_ALGO_06_Solicitudes_Compra.csv', index=False)
    print(f'-> ** Solicitudes Exportadas: {etiqueta}_ALGO_06_Solicitudes_Compra.csv *** : ventana: {ventana}  - {fecha}')
    
    # df_validacion = Calcular_Demanda_Extendida_ALGO_06(data, ventana, proveedor, etiqueta, fecha)
    # df_validacion['Codigo_Articulo']= df_validacion['Codigo_Articulo'].astype(int)
    # df_validacion['Sucursal']= df_validacion['Sucursal'].astype(int)
    # df_validacion.to_csv(f'{folder}/{etiqueta}_ALGO_06_Datos_Validacion.csv', index=False)
    # print(f'-> ** Validación Exportada: {etiqueta}_ALGO_06_Datos_Validacion.csv *** : ventana: {ventana}  - {fecha}')
    
    Exportar_Pronostico(df_forecast, proveedor, etiqueta, 'ALGO_06')  # Impactar Datos en la Interface   
    return
    
def Procesar_ALGO_05(data, proveedor, etiqueta, ventana, fecha):
    
        # Determinar la fecha base
    if fecha is None:
        fecha = data['Fecha'].max()  # Se toma la última fecha en los datos
    else:
        fecha = pd.to_datetime(fecha)  # Se asegura que sea un objeto datetime
        
    print(f'--> Procesar_ALGO_05 ventana {ventana} - fecha {fecha} - No usa Factores')
        
    df_forecast = Calcular_Demanda_ALGO_05(data, proveedor, etiqueta, ventana, fecha)    # Exportar el resultado a un CSV para su posterior procesamiento
    df_forecast['Codigo_Articulo']= df_forecast['Codigo_Articulo'].astype(int)
    df_forecast['Sucursal']= df_forecast['Sucursal'].astype(int)
    df_forecast.to_csv(f'{folder}/{etiqueta}_ALGO_05_Solicitudes_Compra.csv', index=False)
    
    Exportar_Pronostico(df_forecast, proveedor, etiqueta, 'ALGO_05')  # Impactar Datos en la Interface   
    return

def Procesar_ALGO_04(data, proveedor, etiqueta, ventana, current_date=None,  alfa=None):    
    # Asignar valores por defecto si los factores no están definidos
    alfa = 0.5 if alfa is None else float(alfa)
    
    # Determinar la fecha base
    if current_date is None:
        current_date = data['Fecha'].max()  # Se toma la última fecha en los datos
    else:
        current_date = pd.to_datetime(current_date)  # Se asegura que sea un objeto datetime
    
    # Parámetro de suavizado (alpha); valores cercanos a 1 dan más peso a los datos recientes
    
    print(f'--> Procesar_ALGO_04 ventana {ventana} - fecha {current_date} Peso de los Factores Utilizados: Factor Alpha: {alfa} ')
        
    df_forecast = Calcular_Demanda_ALGO_04(data, proveedor, etiqueta, ventana, current_date, alfa)
    df_forecast['Codigo_Articulo']= df_forecast['Codigo_Articulo'].astype(int)
    df_forecast['Sucursal']= df_forecast['Sucursal'].astype(int)
    df_forecast.to_csv(f'{folder}/{etiqueta}_ALGO_04_Solicitudes_Compra.csv', index=False)   # Exportar el resultado a un CSV para su posterior procesamiento
    
    Exportar_Pronostico(df_forecast, proveedor, etiqueta, 'ALGO_04')  # Impactar Datos en la Interface        
    return

def Procesar_ALGO_03(data, proveedor, etiqueta, ventana, fecha, periodos=None, f2=None, f3=None):    
    # Asignar valores por defecto si los factores no están definidos
    periodos = 7 if periodos is None else int(periodos)
    f2 = 'add' if f2 is None else str(f2)  # Incorporar Efecto Estacionalidad
    f3 = 'add' if f3 is None else str(f3) # Informprar Efecto Tendencia Anual
    
    print(f'--> Procesar_ALGO_03 ventana {ventana} - Factores Utilizados: Períodos: {periodos} estacionalidad: {f2} tendencia: {f3}')
        
    df_forecast = Calcular_Demanda_ALGO_03(data, proveedor, etiqueta, ventana, fecha, periodos, f2, f3)
    df_forecast['Codigo_Articulo']= df_forecast['Codigo_Articulo'].astype(int)
    df_forecast['Sucursal']= df_forecast['Sucursal'].astype(int)
    df_forecast.to_csv(f'{folder}/{etiqueta}_ALGO_03_Solicitudes_Compra.csv', index=False)   # Exportar el resultado a un CSV para su posterior procesamiento
    print(f'-> ** Datos Exportados: {etiqueta}_ALGO_03_Solicitudes_Compra.csv *** : ventana: {ventana}  - {fecha}')
    Exportar_Pronostico(df_forecast, proveedor, etiqueta, 'ALGO_03')  # Impactar Datos en la Interface        
    return

def Procesar_ALGO_02(data, proveedor, etiqueta, ventana, fecha):    
    print(f'--> Procesar_ALGO_02 ventana {ventana} - Holt - No usa Factores')
        
    df_forecast = Calcular_Demanda_ALGO_02(data, proveedor, etiqueta, ventana, fecha)
    df_forecast['Codigo_Articulo']= df_forecast['Codigo_Articulo'].astype(int)
    df_forecast['Sucursal']= df_forecast['Sucursal'].astype(int)
    df_forecast.to_csv(f'{folder}/{etiqueta}_ALGO_02_Solicitudes_Compra.csv', index=False)   # Exportar el resultado a un CSV para su posterior procesamiento
    print(f'-> ** Datos Exportados: {etiqueta}_ALGO_02_Solicitudes_Compra.csv *** : ventana: {ventana}  - {fecha}')
    Exportar_Pronostico(df_forecast, proveedor, etiqueta, 'ALGO_02')  # Impactar Datos en la Interface        
    return

def Procesar_ALGO_01(data, proveedor, etiqueta, ventana, fecha, factor_last=None, factor_previous=None, factor_year=None):    
    # Asignar valores por defecto si los factores no están definidos
    factor_last = 0.77 if factor_last is None else factor_last
    factor_previous = 0.22 if factor_previous is None else factor_previous
    factor_year = 0.11 if factor_year is None else factor_year

    print(f'--> Procesar_ALGO_01 ventana {ventana} - Peso de los Factores Utilizados: último: {factor_last} previo: {factor_previous} año anterior: {factor_year}')
        
    df_forecast = Calcular_Demanda_ALGO_01(data, proveedor, etiqueta, ventana, fecha, factor_last, factor_previous, factor_year)
    df_forecast['Codigo_Articulo']= df_forecast['Codigo_Articulo'].astype(int)
    df_forecast['Sucursal']= df_forecast['Sucursal'].astype(int)
    df_forecast.to_csv(f'{folder}/{etiqueta}_ALGO_01_Solicitudes_Compra.csv', index=False)   # Exportar el resultado a un CSV para su posterior procesamiento
    
    Exportar_Pronostico(df_forecast, proveedor, etiqueta, 'ALGO_01')  # Impactar Datos en la Interface        
    return

###---------------------------------------------------------------- 
# RUTINA PRINCIPAL para SELECCIONAR  el ALGORITMO de FORECAST
###---------------------------------------------------------------- 
def get_forecast( id_proveedor, lbl_proveedor, period_lengh=30, algorithm='basic', f1=None, f2=None, f3=None, current_date=None ):
    """
    Genera la predicción de demanda según el algoritmo seleccionado.

    Parámetros:
    - id_proveedor: ID del proveedor.
    - lbl_proveedor: Etiqueta del proveedor.
    - period_lengh: Número de días del período a analizar (por defecto 30).
    - algorithm: Algoritmo a utilizar.
    - current_date: Fecha de referencia; si es None, se toma la fecha máxima de los datos.
    - factores de ponderación: F1, F2, F3  (No importa en que unidades estén, luego los hace relativos al total del peso)

    Retorna:
    - Un DataFrame con las predicciones.
    """
    
    print('Dentro del get_forecast')
    print(f'FORECAST control: {id_proveedor} - {lbl_proveedor} - ventana: {period_lengh} - {algorithm} factores: {f1} - {f2} - {f3}')
    # Generar los datos de entrada
    data, articulos = generar_datos(id_proveedor, lbl_proveedor, period_lengh) # type: ignore

        # Determinar la fecha base
    if current_date is None:
        current_date = data['Fecha'].max()  # type: ignore Se toma la última fecha en los datos
    else:
        current_date = pd.to_datetime(current_date)  # Se asegura que sea un objeto datetime
    print(f'Fecha actual {current_date}')
    

    # Selección del algoritmo de predicción
    match algorithm:
        case 'ALGO_01':
            return Procesar_ALGO_01(data, id_proveedor, lbl_proveedor, period_lengh, current_date, f1, f2, f3)  # Promedio Ponderado x 3 Factores
        case 'ALGO_02':
            return Procesar_ALGO_02(data, id_proveedor, lbl_proveedor, period_lengh, current_date) # Doble Exponencial - Modelo Holt (Tendencia)
        case 'ALGO_03':
            return Procesar_ALGO_03(data, id_proveedor, lbl_proveedor, period_lengh, current_date, f1, f2, f3) # Triple Exponencial Holt-WInter (Tendencia + Estacionalidad) (periodos, add, add)
        case 'ALGO_04':
            return Procesar_ALGO_04(data, id_proveedor, lbl_proveedor, period_lengh, current_date, f1) # EWMA con Factor alpha
        case 'ALGO_05':
            return Procesar_ALGO_05(data, id_proveedor, lbl_proveedor, period_lengh, current_date) # Promedio Venta Simple en Ventana
        case 'ALGO_06':
            return Procesar_ALGO_06(data, id_proveedor, lbl_proveedor, period_lengh, current_date) # Tendencias Ventas Semanales
        case _:
            raise ValueError(f"Error: El algoritmo '{algorithm}' no está implementado.")


# -----------------------------------------------------------
# 0. Rutinas Locales para la generación de gráficos
# -----------------------------------------------------------
def generar_mini_grafico( folder, name):
    # Recuperar Historial de Ventas
    df_ventas = pd.read_csv(f'{folder}/{name}_Ventas.csv')
    df_ventas['Codigo_Articulo']= df_ventas['Codigo_Articulo'].astype(int)
    df_ventas['Sucursal']= df_ventas['Sucursal'].astype(int)
    df_ventas['Fecha']= pd.to_datetime(df_ventas['Fecha'])
    
    # 🔄 Agrupar por Fecha, Código de Artículo y Sucursal, para consolidar múltiples precios
    df_ventas = (
        df_ventas
        .groupby(['Fecha', 'Codigo_Articulo', 'Sucursal'], as_index=False)
        .agg({'Unidades': 'sum'})
    )
    # Buscar filas duplicadas por clave compuesta
    duplicados = df_ventas[df_ventas.duplicated(subset=["Fecha", "Codigo_Articulo", "Sucursal"], keep=False)]
    # Ordenar para facilitar lectura
    duplicados = duplicados.sort_values(["Codigo_Articulo", "Sucursal", "Fecha"])
    # Mostrar o exportar
    print("⚠️ Filas duplicadas encontradas:")
    print(duplicados)

    # RUTINA DE MINIGRAFICO
    fecha_maxima = df_ventas["Fecha"].max()   # Obtener la fecha máxima
    primer_dia_mes_siguiente = (fecha_maxima + pd.offsets.MonthBegin(1)).normalize()   # Truncar al primer día del mes siguiente (evita tener un mes parcial)
    primer_dia_6_meses_atras = primer_dia_mes_siguiente - pd.DateOffset(months=6)   # Calcular el primer día 6 meses atrás

    # Filtrar el dataframe entre ese rango
    df_filtrado = df_ventas[(df_ventas["Fecha"] >= primer_dia_6_meses_atras) &
                            (df_ventas["Fecha"] < primer_dia_mes_siguiente)].copy()
    # Agrupar por mes
    df_filtrado["Mes"] = df_filtrado["Fecha"].dt.to_period("M").astype(str)
    df_mes = df_filtrado.groupby("Mes")["Unidades"].sum().reset_index()

    # Crear el gráfico compacto
    fig, ax = plt.subplots(figsize=(3, 1))  # Tamaño pequeño para una visualización compacta

    # Graficar las barras
    ax.bar(range(1, len(df_mes) + 1), df_mes["Unidades"], color=["blue"], alpha=0.7)

    # Eliminar ejes y etiquetas para que sea más compacto
    ax.set_xticks([])
    ax.set_yticks([])
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_visible(False)

    # Guardar gráfico en base64
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    plt.close()
    
    return base64.b64encode(buffer.getvalue()).decode("utf-8")    

def generar_grafico_base64(dfv, articulo, sucursal, Forecast, Average, ventas_last, ventas_previous, ventas_same_year):
    fecha_maxima = dfv["Fecha"].max()
    df_filtrado = dfv[(dfv["Codigo_Articulo"] == articulo) & (dfv["Sucursal"] == sucursal)]
    df_filtrado = df_filtrado[df_filtrado["Fecha"] >= (fecha_maxima - pd.Timedelta(days=50))]

    fig, ax = plt.subplots(
        figsize=(8, 6), nrows= 2, ncols= 2
    )
    fig.suptitle(f"Demanda Articulo {articulo} - Sucursal {sucursal}")
    current_ax = 0
    #Bucle para Llenar los gráficos
    colors =["red", "blue", "green", "orange", "purple", "brown", "pink", "gray", "olive", "cyan"]

    # Ventas Diarias
    df_filtrado["Media_Movil"] = df_filtrado["Unidades"].rolling(window=7, min_periods=1).mean()
    df_filtrado["Media_Movil"] = df_filtrado["Media_Movil"].fillna(0)

    # Ventas Diarias
    ax[0, 0].plot(df_filtrado["Fecha"], df_filtrado["Unidades"], marker="o", linestyle="-", label="Ventas", color=colors[0])
    ax[0, 0].plot(df_filtrado["Fecha"], df_filtrado["Media_Movil"], linestyle="--", label="Media Móvil (7 días)", color="black")
    ax[0, 0].set_title("Ventas Diarias")
    ax[0, 0].legend()
    ax[0, 0].set_xlabel("Fecha")
    ax[0, 0].set_ylabel("Unidades")
    ax[0, 0].tick_params(axis='x', rotation=45)

    # Ventas Semanales
    df_filtrado["Semana"] = df_filtrado["Fecha"].dt.to_period("W").astype(str)
    df_semanal = df_filtrado.groupby("Semana")["Unidades"].sum().reset_index()
    df_semanal["Semana_Num"] = df_filtrado.groupby("Semana")["Fecha"].min().reset_index()["Fecha"].dt.isocalendar().week.astype(int)
    df_semanal["Media_Movil"] = df_semanal["Unidades"].rolling(window=7).mean()
    df_semanal["Media_Movil"] =  df_semanal["Media_Movil"].fillna(0)

    # Histograma de ventas semanales
    ax[0, 1].bar(df_semanal["Semana_Num"], df_semanal["Unidades"], color=[colors[1]], alpha=0.7)
    ax[0, 1].set_xlabel("Semana del Año")
    ax[0, 1].set_ylabel("Unidades Vendidas")
    ax[0, 1].set_title("Histograma de Ventas Semanales")
    ax[0, 1].tick_params(axis='x', rotation=60)
    ax[0, 1].grid(axis="y", linestyle="--", alpha=0.7)

    # Graficar el Forecast vs Ventas Reales en la tercera celda
    labels = ["Forecast","Actual", "Anterior", "Año Ant"]
    values = [Forecast, ventas_last, ventas_previous, ventas_same_year]

    ax[1, 0].bar(labels, values, color=[colors[2], colors[3], colors[4], colors[5]], alpha=0.7)
    ax[1, 0].set_title("Forecast vs Ventas Anteriores")
    ax[1, 0].set_ylabel("Unidades")
    ax[1, 0].grid(axis="y", linestyle="--", alpha=0.7)

    # Definir fechas de referencia
    fecha_maxima = df_filtrado["Fecha"].max()
    fecha_inicio_ultimos30 = fecha_maxima - pd.Timedelta(days=30)
    fecha_inicio_previos30 = fecha_inicio_ultimos30 - pd.Timedelta(days=30)
    fecha_inicio_anio_anterior = fecha_inicio_ultimos30 - pd.DateOffset(years=1)
    fecha_fin_anio_anterior = fecha_inicio_previos30 - pd.DateOffset(years=1)

    # Calcular ventas de los últimos 30 días
    ventas_ultimos_30 = df_filtrado[(df_filtrado["Fecha"] > fecha_inicio_ultimos30)]["Unidades"].sum()

    # Calcular ventas de los 30 días previos a los últimos 30 días
    ventas_previos_30 = df_filtrado[
        (df_filtrado["Fecha"] > fecha_inicio_previos30) & (df_filtrado["Fecha"] <= fecha_inicio_ultimos30)
    ]["Unidades"].sum()

    # Simulación de datos para las ventas del año anterior
    df_filtrado_anio_anterior = df_filtrado.copy()
    df_filtrado_anio_anterior["Fecha"] = df_filtrado_anio_anterior["Fecha"] - pd.DateOffset(years=1)
    ventas_mismo_periodo_anio_anterior = df_filtrado_anio_anterior[
        (df_filtrado_anio_anterior["Fecha"] > fecha_inicio_anio_anterior) &
        (df_filtrado_anio_anterior["Fecha"] <= fecha_fin_anio_anterior)
    ]["Unidades"].sum()

    # Datos para el histograma
    labels = ["Últimos 30", "Anteriores 30", "Año anterior", "Average"]
    values = [ventas_ultimos_30, ventas_previos_30, ventas_mismo_periodo_anio_anterior, Average]

    # Graficar el histograma en la celda [1,1]
    ax[1, 1].bar(labels, values, color=[colors[0], colors[1], colors[2]], alpha=0.7)
    ax[1, 1].set_title("Comparación de Ventas en 3 Períodos")
    ax[1, 1].set_ylabel("Unidades Vendidas")
    ax[1, 1].grid(axis="y", linestyle="--", alpha=0.7)

    # Mostrar el gráfico
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])  # type: ignore # Ajustar para no solapar con el título

    # Guardar gráfico en base64
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    plt.close()
    
    return base64.b64encode(buffer.getvalue()).decode("utf-8")

def insertar_graficos_forecast(algoritmo, name, id_proveedor):
        
    # Recuperar Historial de Ventas
    df_ventas = pd.read_csv(f'{folder}/{name}_Ventas.csv')
    df_ventas['Codigo_Articulo']= df_ventas['Codigo_Articulo'].astype(int)
    df_ventas['Sucursal']= df_ventas['Sucursal'].astype(int)
    df_ventas['Fecha']= pd.to_datetime(df_ventas['Fecha'])
    
    # 🔄 Agrupar por Fecha, Código de Artículo y Sucursal, para consolidar múltiples precios
    df_ventas = (
        df_ventas
        .groupby(['Fecha', 'Codigo_Articulo', 'Sucursal'], as_index=False)
        .agg({'Unidades': 'sum'})
    )
    # Buscar filas duplicadas por clave compuesta
    duplicados = df_ventas[df_ventas.duplicated(subset=["Fecha", "Codigo_Articulo", "Sucursal"], keep=False)]
    # Ordenar para facilitar lectura
    duplicados = duplicados.sort_values(["Codigo_Articulo", "Sucursal", "Fecha"])
    # Mostrar o exportar
    print("⚠️ Filas duplicadas encontradas:")
    print(duplicados)

    # Recuperando Forecast Calculado
    df_forecast = pd.read_csv(f'{folder}/{algoritmo}_Solicitudes_Compra.csv')
    df_forecast.fillna(0)   # Por si se filtró algún missing value
    print(f"-> Datos Recuperados del CACHE: {id_proveedor}, Label: {name}")
    
    # Agregar la nueva columna de gráficos en df_forecast Iterando sobre todo el DATAFRAME
    df_forecast["GRAFICO"] = df_forecast.apply(
        lambda row: generar_grafico_base64(df_ventas, row["Codigo_Articulo"], row["Sucursal"], row["Forecast"], row["Average"], row["ventas_last"], row["ventas_previous"], row["ventas_same_year"]) if not pd.isna(row["Codigo_Articulo"]) and not pd.isna(row["Sucursal"]) else None, # type: ignore # type: ignore
        axis=1
    ) # type: ignore
    
    return df_forecast


def generar_grafico_base64_plotly(df_filtrado, articulo, sucursal, Forecast, Average, ventas_last, ventas_previous, ventas_same_year):
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            "Ventas Diarias",
            "Histograma de Ventas Semanales",
            "Forecast vs Ventas Anteriores",
            "Comparación de Ventas en 3 Períodos"
        )
    )

    fig.update_layout(title_text=f"Demanda Articulo {articulo} - Sucursal {sucursal}", height=800, width=1000)

    # Media móvil
    df_filtrado["Media_Movil"] = df_filtrado["Unidades"].rolling(window=7, min_periods=1).mean()
    df_filtrado["Media_Movil"] = df_filtrado["Media_Movil"].fillna(0)

    # Gráfico 1
    fig.add_trace(go.Scatter(x=df_filtrado["Fecha"], y=df_filtrado["Unidades"], mode='lines+markers', name='Ventas'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df_filtrado["Fecha"], y=df_filtrado["Media_Movil"], mode='lines', name='Media Móvil', line=dict(dash='dash')), row=1, col=1)

    # Gráfico 2
    df_filtrado["Semana"] = df_filtrado["Fecha"].dt.to_period("W").astype(str)
    df_semanal = df_filtrado.groupby("Semana").agg({"Unidades": "sum", "Fecha": "min"}).reset_index()
    df_semanal["Semana_Num"] = df_semanal["Fecha"].dt.isocalendar().week
    fig.add_trace(go.Bar(x=df_semanal["Semana_Num"], y=df_semanal["Unidades"], name="Semanas"), row=1, col=2)

    # Gráfico 3
    fig.add_trace(go.Bar(x=["Forecast", "Actual", "Anterior", "Año Ant"],
                        y=[Forecast, ventas_last, ventas_previous, ventas_same_year],
                        name="Comparación"), row=2, col=1)

    # Gráfico 4: cálculo períodos
    fecha_maxima = df_filtrado["Fecha"].max()
    fecha_inicio_ultimos30 = fecha_maxima - pd.Timedelta(days=30)
    fecha_inicio_previos30 = fecha_inicio_ultimos30 - pd.Timedelta(days=30)
    fecha_inicio_anio_anterior = fecha_inicio_ultimos30 - pd.DateOffset(years=1)
    fecha_fin_anio_anterior = fecha_inicio_previos30 - pd.DateOffset(years=1)

    ventas_ultimos_30 = df_filtrado[df_filtrado["Fecha"] > fecha_inicio_ultimos30]["Unidades"].sum()
    ventas_previos_30 = df_filtrado[
        (df_filtrado["Fecha"] > fecha_inicio_previos30) & (df_filtrado["Fecha"] <= fecha_inicio_ultimos30)
    ]["Unidades"].sum()
    df_anterior = df_filtrado.copy()
    df_anterior["Fecha"] = df_anterior["Fecha"] - pd.DateOffset(years=1)
    ventas_anio_anterior = df_anterior[
        (df_anterior["Fecha"] > fecha_inicio_anio_anterior) & (df_anterior["Fecha"] <= fecha_fin_anio_anterior)
    ]["Unidades"].sum()

    fig.add_trace(go.Bar(x=["Últimos 30", "Anteriores 30", "Año anterior", "Average"],
                        y=[ventas_ultimos_30, ventas_previos_30, ventas_anio_anterior, Average],
                        name="Comparación temporal"), row=2, col=2)

    # Exportar a base64
    buffer = BytesIO()
    fig.write_image(buffer, format="png")
    return base64.b64encode(buffer.getvalue()).decode("utf-8")

def guardar_grafico_base64(base64_str, path_archivo):
    with open(path_archivo, "wb") as f:
        f.write(base64.b64decode(base64_str))

# BLOQUE AGREGADO PARA INCORPORAR STOCK en formato DICCIONARIO
# from datetime import date, datetime, timedelta

# def convertir_stock_diario_a_dict(df_stock):
#     """Convierte df_stock en un diccionario {fecha: cantidad}, solo hasta ayer y con fechas válidas."""
#     def es_fecha_valida(anio, mes, dia):
#         try:
#             return date(anio, mes, dia)
#         except ValueError:
#             return None

#     resultado = {}
#     for _, row in df_stock.iterrows():
#         anio = int(row['c_anio'])
#         mes = int(row['c_mes'])

#         for col in df_stock.columns:
#             if col.startswith("q_dia"):
#                 dia = int(col.replace("q_dia", ""))
#                 fecha_valida = es_fecha_valida(anio, mes, dia)
#                 if fecha_valida and fecha_valida <= (datetime.now().date() - timedelta(days=1)):
#                     resultado[fecha_valida.isoformat()] = row[col]
#     return resultado

# # BLOQUE AGREGADO PARA INCORPORAR OFERTAS en formato DICCIONARIO
# def convertir_ofertas_a_dict(df_ofertas):
#     """Convierte df_ofertas en un diccionario {fecha: flag}, solo hasta ayer y con fechas válidas."""
#     def es_fecha_valida(anio, mes, dia):
#         try:
#             return date(anio, mes, dia)
#         except ValueError:
#             return None

#     resultado = {}
#     for _, row in df_ofertas.iterrows():
#         anio = int(row['c_anio'])
#         mes = int(row['c_mes'])

#         for col in df_ofertas.columns:
#             if col.startswith("m_oferta_dia"):
#                 dia = int(col.replace("m_oferta_dia", ""))
#                 fecha_valida = es_fecha_valida(anio, mes, dia)
#                 if fecha_valida and fecha_valida <= (datetime.now().date() - timedelta(days=1)):
#                     resultado[fecha_valida.isoformat()] = row[col]
#     return resultado

# def generar_grafico_json(dfv, dfs, dfo, articulo, sucursal, Forecast, Average, ventas_last, ventas_previous, ventas_same_year):
#     fecha_maxima = dfv["Fecha"].max()

#     # Filtrar ventas por artículo y sucursal
#     df_filtrado = dfv[(dfv["Codigo_Articulo"] == articulo) & (dfv["Sucursal"] == sucursal)]
#     df_filtrado = df_filtrado[df_filtrado["Fecha"] >= (fecha_maxima - pd.Timedelta(days=50))]

#     # Agrupar SIEMPRE por fecha para evitar duplicados silenciosos
#     df_filtrado = (
#         df_filtrado
#         .groupby("Fecha", as_index=False)
#         .agg({"Unidades": "sum"})
#     )

#     df_filtrado = df_filtrado.sort_values("Fecha").reset_index(drop=True)

#     # Media móvil
#     df_filtrado["Media_Movil"] = df_filtrado["Unidades"].rolling(window=7, min_periods=1).mean().fillna(0)
#     df_filtrado["Semana"] = df_filtrado["Fecha"].dt.to_period("W").astype(str)

#     # Agregación semanal
#     df_semanal = df_filtrado.groupby("Semana")["Unidades"].sum().reset_index()
#     semanas = df_filtrado.groupby("Semana")["Fecha"].min().reset_index()
#     df_semanal["Semana_Num"] = semanas["Fecha"].dt.isocalendar().week.astype(int)
#     df_semanal["Media_Movil"] = df_semanal["Unidades"].rolling(window=7, min_periods=1).mean()

#     # Cálculos históricos
#     fecha_inicio_ultimos30 = fecha_maxima - pd.Timedelta(days=30)
#     fecha_inicio_previos30 = fecha_inicio_ultimos30 - pd.Timedelta(days=30)
#     fecha_inicio_anio_anterior = fecha_inicio_ultimos30 - pd.DateOffset(years=1)
#     fecha_fin_anio_anterior = fecha_inicio_previos30 - pd.DateOffset(years=1)

#     ventas_ultimos_30 = float(df_filtrado[df_filtrado["Fecha"] > fecha_inicio_ultimos30]["Unidades"].sum())
#     ventas_previos_30 = float(
#         df_filtrado[
#             (df_filtrado["Fecha"] > fecha_inicio_previos30) &
#             (df_filtrado["Fecha"] <= fecha_inicio_ultimos30)
#         ]["Unidades"].sum()
#     )

#     df_filtrado_anio_anterior = df_filtrado.copy()
#     df_filtrado_anio_anterior["Fecha"] = df_filtrado_anio_anterior["Fecha"] - pd.DateOffset(years=1)
#     ventas_mismo_periodo_anio_anterior = float(
#         df_filtrado_anio_anterior[
#             (df_filtrado_anio_anterior["Fecha"] > fecha_inicio_anio_anterior) &
#             (df_filtrado_anio_anterior["Fecha"] <= fecha_fin_anio_anterior)
#         ]["Unidades"].sum()
#     )

#     # STOCK
#     dfs["c_articulo"] = dfs["c_articulo"].astype(int)
#     dfs["c_sucu_empr"] = dfs["c_sucu_empr"].astype(int)   
#     dfs_filtrado = dfs[(dfs["c_articulo"] == articulo) & (dfs["c_sucu_empr"] == sucursal)]
#     datos_stock = convertir_stock_diario_a_dict(dfs_filtrado)
    
#     # OFERTAS
#     dfo["c_articulo"] = dfo["c_articulo"].astype(int)
#     dfo["c_sucu_empr"] = dfo["c_sucu_empr"].astype(int)   
#     dfo_filtrado = dfo[(dfo["c_articulo"] == articulo) & (dfo["c_sucu_empr"] == sucursal)]
#     datos_ofertas = convertir_ofertas_a_dict(dfo_filtrado)

#     return {
#         "articulo": int(articulo),
#         "sucursal": int(sucursal),
#         "fechas": df_filtrado["Fecha"].dt.strftime("%Y-%m-%d").tolist(),
#         "unidades": [float(x) for x in df_filtrado["Unidades"]],
#         "media_movil": [float(x) for x in df_filtrado["Media_Movil"]],
#         "semana_num": df_semanal["Semana_Num"].astype(int).tolist(),
#         "ventas_semanales": [float(x) for x in df_semanal["Unidades"]],
#         "forecast": float(Forecast),
#         "ventas_last": float(ventas_last),
#         "ventas_previous": float(ventas_previous),
#         "ventas_same_year": float(ventas_same_year),
#         "average": float(Average),
#         "ventas_ultimos_30": float(ventas_ultimos_30),
#         "ventas_previos_30": float(ventas_previos_30),
#         "ventas_anio_anterior": float(ventas_mismo_periodo_anio_anterior),
#         "stock_diario": datos_stock,
#         "ofertas_diarias": datos_ofertas
#     }
# REFACTORIZAR FUNCION y AGREGAR DICCIONARIO MENSUAL
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta

def filtrar_ventas_recientes(dfv, articulo, sucursal, dias=50):
    fecha_maxima = dfv["Fecha"].max()
    df_filtrado = dfv[(dfv["Codigo_Articulo"] == articulo) & (dfv["Sucursal"] == sucursal)]
    df_filtrado = df_filtrado[df_filtrado["Fecha"] >= (fecha_maxima - pd.Timedelta(days=dias))]
    return df_filtrado.sort_values("Fecha").reset_index(drop=True), fecha_maxima

def calcular_media_movil(df, ventana=7):
    df["Media_Movil"] = df["Unidades"].rolling(window=ventana, min_periods=1).mean().fillna(0)
    df["Semana"] = df["Fecha"].dt.to_period("W").astype(str)
    return df

def agregar_ventas_semanales(df):
    df_semanal = df.groupby("Semana")[["Unidades"]].sum().reset_index()
    semanas = df.groupby("Semana")["Fecha"].min().reset_index()
    df_semanal["Semana_Num"] = semanas["Fecha"].dt.isocalendar().week.astype(int)
    df_semanal["Media_Movil"] = df_semanal["Unidades"].rolling(window=3, min_periods=1).mean()
    return df_semanal

def calcular_metricas_temporales(df, fecha_maxima):
    f30 = fecha_maxima - pd.Timedelta(days=30)
    f60 = f30 - pd.Timedelta(days=30)
    f30_aa = f30 - pd.DateOffset(years=1)
    f60_aa = f60 - pd.DateOffset(years=1)

    ultimos_30 = float(df[df["Fecha"] > f30]["Unidades"].sum())
    previos_30 = float(df[(df["Fecha"] > f60) & (df["Fecha"] <= f30)]["Unidades"].sum())

    df_anio_ant = df.copy()
    df_anio_ant["Fecha"] = df_anio_ant["Fecha"].apply(lambda x: x - pd.DateOffset(years=1))
    mismo_aa = float(df_anio_ant[(df_anio_ant["Fecha"] > f30_aa) & (df_anio_ant["Fecha"] <= f60_aa)]["Unidades"].sum())

    return ultimos_30, previos_30, mismo_aa

def calcular_ventas_mensuales_anio(dfv, articulo, sucursal, fecha_maxima):
    fecha_inicio_anio = fecha_maxima - pd.DateOffset(months=12)
    df_anual = dfv[(dfv["Codigo_Articulo"] == articulo) &
                    (dfv["Sucursal"] == sucursal) &
                    (dfv["Fecha"] >= fecha_inicio_anio)].copy()
    df_anual["Mes"] = df_anual["Fecha"].dt.to_period("M").astype(str)
    ventas_mensuales = df_anual.groupby("Mes")["Unidades"].sum().reset_index()
    return {str(row["Mes"]): float(row["Unidades"]) for _, row in ventas_mensuales.iterrows()}

def convertir_stock_diario_a_dict(df_stock):
    def es_fecha_valida(anio, mes, dia):
        try:
            return date(anio, mes, dia)
        except ValueError:
            return None

    resultado = {}
    for _, row in df_stock.iterrows():
        anio = int(row['c_anio'])
        mes = int(row['c_mes'])
        for col in df_stock.columns:
            if col.startswith("q_dia"):
                dia = int(col.replace("q_dia", ""))
                fecha_valida = es_fecha_valida(anio, mes, dia)
                if fecha_valida and fecha_valida <= (datetime.now().date() - timedelta(days=1)):
                    resultado[fecha_valida.isoformat()] = row[col]
    return resultado

def convertir_ofertas_a_dict(df_ofertas):
    def es_fecha_valida(anio, mes, dia):
        try:
            return date(anio, mes, dia)
        except ValueError:
            return None

    resultado = {}
    for _, row in df_ofertas.iterrows():
        anio = int(row['c_anio'])
        mes = int(row['c_mes'])
        for col in df_ofertas.columns:
            if col.startswith("m_oferta_dia"):
                dia = int(col.replace("m_oferta_dia", ""))
                fecha_valida = es_fecha_valida(anio, mes, dia)
                if fecha_valida and fecha_valida <= (datetime.now().date() - timedelta(days=1)):
                    resultado[fecha_valida.isoformat()] = row[col]
    return resultado

def preparar_stock_y_ofertas(dfs, dfo, articulo, sucursal, convertir_stock, convertir_ofertas):
    dfs["c_articulo"] = dfs["c_articulo"].astype(int)
    dfs["c_sucu_empr"] = dfs["c_sucu_empr"].astype(int)
    stock = convertir_stock(dfs[(dfs["c_articulo"] == articulo) & (dfs["c_sucu_empr"] == sucursal)])

    dfo["c_articulo"] = dfo["c_articulo"].astype(int)
    dfo["c_sucu_empr"] = dfo["c_sucu_empr"].astype(int)
    ofertas = convertir_ofertas(dfo[(dfo["c_articulo"] == articulo) & (dfo["c_sucu_empr"] == sucursal)])

    return stock, ofertas

def generar_grafico_json(dfv, dfs, dfo, articulo, sucursal, Forecast, Average,
                        ventas_last, ventas_previous, ventas_same_year,
                        convertir_stock_diario_a_dict, convertir_ofertas_a_dict):

    df_filtrado, fecha_maxima = filtrar_ventas_recientes(dfv, articulo, sucursal)
    df_filtrado = df_filtrado.groupby("Fecha", as_index=False).agg({"Unidades": "sum"})
    df_filtrado = calcular_media_movil(df_filtrado)
    df_semanal = agregar_ventas_semanales(df_filtrado)
    v30, vprev, vaanterior = calcular_metricas_temporales(df_filtrado, fecha_maxima)
    ventas_mensuales = calcular_ventas_mensuales_anio(dfv, articulo, sucursal, fecha_maxima)
    stock, ofertas = preparar_stock_y_ofertas(dfs, dfo, articulo, sucursal,
                                            convertir_stock_diario_a_dict,
                                            convertir_ofertas_a_dict)

    return {
        "articulo": int(articulo),
        "sucursal": int(sucursal),
        "fechas": df_filtrado["Fecha"].dt.strftime("%Y-%m-%d").tolist(),
        "unidades": [float(x) for x in df_filtrado["Unidades"]],
        "media_movil": [float(x) for x in df_filtrado["Media_Movil"]],
        "semana_num": df_semanal["Semana_Num"].astype(int).tolist(),
        "ventas_semanales": [float(x) for x in df_semanal["Unidades"]],
        "forecast": float(Forecast),
        "ventas_last": float(ventas_last),
        "ventas_previous": float(ventas_previous),
        "ventas_same_year": float(ventas_same_year),
        "average": float(Average),
        "ventas_ultimos_30": v30,
        "ventas_previos_30": vprev,
        "ventas_anio_anterior": vaanterior,
        "stock_diario": stock,
        "ofertas_diarias": ofertas,
        "ventas_mensuales": ventas_mensuales
    }

def convertir_json(obj):
    if isinstance(obj, (np.integer, int)):
        return int(obj)
    elif isinstance(obj, (np.floating, float)):
        return float(obj)
    elif isinstance(obj, (np.ndarray, list)):
        return [convertir_json(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convertir_json(v) for k, v in obj.items()}
    else:
        return obj
# Se aplicaría
# json.dumps(convertir_json(mi_diccionario))


def insertar_graficos_json(algoritmo, name, id_proveedor):
        
    # Recuperar Historial de Ventas
    df_ventas = pd.read_csv(f'{folder}/{name}_Ventas.csv')
    df_ventas['Codigo_Articulo']= df_ventas['Codigo_Articulo'].astype(int)
    df_ventas['Sucursal']= df_ventas['Sucursal'].astype(int)
    df_ventas['Fecha']= pd.to_datetime(df_ventas['Fecha'])
    
    # 🔄 Agrupar por Fecha, Código de Artículo y Sucursal, para consolidar múltiples precios
    df_ventas = (
        df_ventas
        .groupby(['Fecha', 'Codigo_Articulo', 'Sucursal'], as_index=False)
        .agg({'Unidades': 'sum'})
    )
    # Buscar filas duplicadas por clave compuesta
    duplicados = df_ventas[df_ventas.duplicated(subset=["Fecha", "Codigo_Articulo", "Sucursal"], keep=False)]
    # Ordenar para facilitar lectura
    duplicados = duplicados.sort_values(["Codigo_Articulo", "Sucursal", "Fecha"])
    # Mostrar o exportar
    print("⚠️ Filas duplicadas encontradas:")
    print(duplicados)

    # Recuperando Forecast Calculado
    df_forecast = pd.read_csv(f'{folder}/{algoritmo}_Solicitudes_Compra.csv')
    df_forecast.fillna(0)   # Por si se filtró algún missing value
    print(f"-> Datos Recuperados del CACHE: {id_proveedor}, Label: {name}")
    
    # Agregar la nueva columna de gráficos en df_forecast Iterando sobre todo el DATAFRAME
    df_forecast["GRAFICO"] = df_forecast.apply(
        lambda row: generar_grafico_json(df_ventas, row["Codigo_Articulo"], row["Sucursal"], row["Forecast"], row["Average"], row["ventas_last"], row["ventas_previous"], row["ventas_same_year"]) if not pd.isna(row["Codigo_Articulo"]) and not pd.isna(row["Sucursal"]) else None, # type: ignore
        axis=1
    ) # type: ignore
    
    return df_forecast


def graficar_desde_datos_json(datos_dict):
    fechas = pd.to_datetime(datos_dict["fechas"])
    unidades = datos_dict["unidades"]
    media_movil = datos_dict["media_movil"]
    semana_num = datos_dict["semana_num"]
    forecast = datos_dict["forecast"]
    ventas_last = datos_dict["ventas_last"]
    ventas_previous = datos_dict["ventas_previous"]
    ventas_same_year = datos_dict["ventas_same_year"]
    average = datos_dict["average"]
    ventas_semanales = datos_dict["ventas_semanales"]

    fig, ax = plt.subplots(figsize=(8, 6), nrows=2, ncols=2)
    fig.suptitle(f"Demanda Articulo {datos_dict['articulo']} - Sucursal {datos_dict['sucursal']}")

    # Ventas diarias
    ax[0, 0].plot(fechas, unidades, marker="o", label="Ventas", color="red")
    ax[0, 0].plot(fechas, media_movil, linestyle="--", label="Media Móvil (7 días)", color="black")
    ax[0, 0].set_title("Ventas Diarias")
    ax[0, 0].legend()
    ax[0, 0].tick_params(axis='x', rotation=45)

    # Histograma de ventas semanales
    ax[0, 1].bar(semana_num, ventas_semanales, color="blue", alpha=0.7)
    ax[0, 1].set_title("Histograma de Ventas Semanales")
    ax[0, 1].grid(axis="y", linestyle="--", alpha=0.7)

    # Forecast vs ventas anteriores
    ax[1, 0].bar(["Forecast", "Actual", "Anterior", "Año Ant."],    
                [forecast, ventas_last, ventas_previous, ventas_same_year],
                color=["orange", "green", "blue", "purple"])
    ax[1, 0].set_title("Forecast vs Ventas Anteriores")
    ax[1, 0].grid(axis="y", linestyle="--", alpha=0.7)

    # Comparación últimos 30 días
    ax[1, 1].bar(["Últimos 30", "Anteriores 30", "Año Anterior", "Average"],
                [ventas_last, ventas_previous, ventas_same_year, average],
                color=["red", "blue", "purple", "gray"])
    ax[1, 1].set_title("Comparación de Ventas en 3 Períodos")
    ax[1, 1].grid(axis="y", linestyle="--", alpha=0.7)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # type: ignore
    plt.show()
    
def graficar_desde_json(path_json, forecast, ventas_last, ventas_previous, ventas_same_year):
    with open(path_json, "r", encoding="utf-8") as f:
        datos = json.load(f)

    fechas = pd.to_datetime(datos["fechas"])
    unidades = datos["unidades"]
    media_movil = datos["media_movil"]
    semana_num = datos["semana_num"]
    ventas_semanales = datos["ventas_semanales"]

    fig, ax = plt.subplots(figsize=(8, 6), nrows=2, ncols=2)
    fig.suptitle(f"Demanda Artículo {datos['articulo']} - Sucursal {datos['sucursal']}")

    # Ventas diarias
    ax[0, 0].plot(fechas, unidades, marker="o", label="Ventas", color="red")
    ax[0, 0].plot(fechas, media_movil, linestyle="--", label="Media Móvil (7 días)", color="black")
    ax[0, 0].set_title("Ventas Diarias")
    ax[0, 0].legend()
    ax[0, 0].tick_params(axis='x', rotation=45)

    # Histograma de ventas semanales
    ax[0, 1].bar(semana_num, ventas_semanales, color="blue", alpha=0.7)
    ax[0, 1].set_title("Histograma de Ventas Semanales")
    ax[0, 1].grid(axis="y", linestyle="--", alpha=0.7)

    # Forecast vs ventas anteriores
    ax[1, 0].bar(["Forecast", "Actual", "Anterior", "Año Ant."], 
                [forecast, ventas_last, ventas_previous, ventas_same_year],
                color=["orange", "green", "blue", "purple"])
    ax[1, 0].set_title("Forecast vs Ventas Anteriores")
    ax[1, 0].grid(axis="y", linestyle="--", alpha=0.7)

    # Comparación últimos 30 días
    ax[1, 1].bar(["Últimos 30", "Anteriores 30", "Año Anterior", "Average"],
                [datos["ventas_last"], datos["ventas_previous"], datos["ventas_same_year"], datos["average"]],
                color=["red", "blue", "purple", "gray"])
    ax[1, 1].set_title("Comparación de Ventas en 3 Períodos")
    ax[1, 1].grid(axis="y", linestyle="--", alpha=0.7)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # type: ignore
    plt.show()
    

def generar_reporte_json(datos):
    # Generar reporte en formato JSON
    reporte = {
        "datos": datos,
        "fecha_creacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": "1.0"
    }


# -----------------------------------------------------------
# Desde un enfoque profesional, esta estrategia es ideal para sistemas donde el procesamiento anticipado es costoso o donde la visualización es esporádica y personalizada. 
# Además, permite escalar mejor en entornos donde se manejan grandes volúmenes de datos, como en retail o logística.

# Desde mi punto de vista, esta arquitectura representa una mejora significativa tanto en rendimiento como en mantenibilidad. 
# # Incluso se podría extender más adelante integrando visualizaciones interactivas con herramientas como Plotly, Dash o Bokeh.
# 🏁 Resultados esperados
# Tiempo de procesamiento reducido al evitar regenerar todos los gráficos.

# Gráficos generados bajo demanda, solo cuando se requiere visualización.
# Datos guardados como JSON, fácilmente integrables con bases de datos o APIs.
#-----------------------------------------------------------

# -----------------------------------------------------------
# 3. Operaciones CRUD para spl_supply_forecast_execution
# -----------------------------------------------------------
def get_execution(execution_id):
    conn = Open_Conn_Postgres()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        query = """
            SELECT id, description, name, "timestamp", supply_forecast_model_id, 
                ext_supplier_code, supplier_id, supply_forecast_execution_status_id
            FROM public.spl_supply_forecast_execution
            WHERE id = %s
        """
        cur.execute(query, (execution_id,))
        row = cur.fetchone()
        cur.close()
        if row:
            return {
                "id": row[0],
                "description": row[1],
                "name": row[2],
                "timestamp": row[3],
                "supply_forecast_model_id": row[4],
                "ext_supplier_code": row[5],
                "supplier_id": row[6],
                "supply_forecast_execution_status_id": row[7]
            }
        return None
    except Exception as e:
        print(f"Error en get_execution: {e}")
        return None
    finally:
        Close_Connection(conn)

def update_execution(execution_id, **kwargs):
    if not kwargs:
        print("No hay valores para actualizar")
        return None

    conn = Open_Conn_Postgres()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        set_clause = ", ".join([f"{key} = %s" for key in kwargs.keys()])
        values = list(kwargs.values())
        values.append(execution_id)

        query = f"""
            UPDATE public.spl_supply_forecast_execution
            SET {set_clause}
            WHERE id = %s
        """
        cur.execute(query, tuple(values))
        conn.commit()
        cur.close()
        return get_execution(execution_id)  # Retorna la ejecución actualizada
    
    except Exception as e:
        print(f"Error en update_execution: {e}")
        conn.rollback()
        return None
    finally:
        Close_Connection(conn)
        


# -----------------------------------------------------------
# 3.1 Operaciones CUSTOM para spl_supply_forecast_execution
# -----------------------------------------------------------

def get_execution_by_status(status):
    if not status:
        print("No hay estados para filtrar")
        return None
    
    conn = Open_Conn_Postgres()
    if conn is None:
        return None
    try:
        query = f"""
        SELECT id, description, name, "timestamp", supply_forecast_model_id, ext_supplier_code, graphic, 
                monthly_net_margin_in_millions, monthly_purchases_in_millions, monthly_sales_in_millions, sotck_days, sotck_days_colors, 
                supplier_id, supply_forecast_execution_status_id
                FROM public.spl_supply_forecast_execution
                WHERE supply_forecast_execution_status_id = {status};
        """
        # Ejecutar la consulta SQL
        fexsts = pd.read_sql(query, conn) # type: ignore
        return fexsts
    except Exception as e:
        print(f"Error en get_execution_status: {e}")
        return None
    finally:
        Close_Connection(conn)
        
# Comentario 1 antes del simbolo raro.

# Comentario 1
def get_execution_execute_by_status(status):
    if not status:
        print("No hay estados para filtrar")
        return None
    
    conn = Open_Conn_Postgres()
    if conn is None:
        return None
    try:
        query = f"""
        SELECT   e.name, 
            m.method,
            fee.ext_supplier_code, 
            fee.last_execution,
            fee.supply_forecast_execution_status_id as fee_status_id,
            fee.timestamp,
            e.supply_forecast_model_id as forecast_model_id,
            fee.supply_forecast_execution_id as forecast_execution_id, 
            fee.id as forecast_execution_execute_id,
            fee.supply_forecast_execution_schedule_id as forecast_execution_schedule_id,
            e.supplier_id
            
        FROM public.spl_supply_forecast_execution_execute as fee
            LEFT JOIN  public.spl_supply_forecast_execution as e
                ON fee.supply_forecast_execution_id = e.id
            LEFT JOIN public.spl_supply_forecast_model as m
                ON e.supply_forecast_model_id= m.id

        WHERE fee.supply_forecast_execution_status_id = {status}
            AND fee.last_execution = true;
        """
        # Ejecutar la consulta SQL
        fexsts = pd.read_sql(query, conn) # type: ignore
        return fexsts
    except Exception as e:
        print(f"Error en get_execution_status: {e}")
        return None
    finally:
        Close_Connection(conn)       

def get_full_parameters(supply_forecast_model_id, execution_id): # Parametros del Modelo(default_value) y de la Ejecución 
    conn = Open_Postgres_retry()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        ## Establecer orden específico de los parámetros
        query = """
            SELECT 
                mp.name, 
                mp.data_type, 
                mp.default_value, 
                ep.value, 
                mp.id,  
                mp.supply_forecast_model_id, 
                ep.supply_forecast_execution_id
            FROM public.spl_supply_forecast_model_parameter mp	
            FULL JOIN public.spl_supply_forecast_execution_parameter ep
                ON ep.supply_forecast_model_parameter_id = mp.id
            WHERE mp.supply_forecast_model_id = %s
                AND ep.supply_forecast_execution_id = %s
            ORDER BY 1;
        """

        # Ejecutar la consulta de manera segura
        cur.execute(query, (supply_forecast_model_id, execution_id))

        # Obtener los resultados
        result = cur.fetchall()

        # Convertir los resultados a un DataFrame de pandas
        columns = ["name", "data_type", "default_value", "value", "id", "supply_forecast_model_id", "supply_forecast_execution_id"]
        df_parameters = pd.DataFrame(result, columns=columns)

        cur.close()
        return df_parameters

    except Exception as e:
        print(f"Error en get_execution_parameter: {e}")
        return None
    finally:
        Close_Connection(conn)
                

# -----------------------------------------------------------
# 4. Operaciones CRUD para spl_supply_forecast_execution_parameter
# -----------------------------------------------------------
def create_execution_parameter(supply_forecast_execution_id, supply_forecast_model_parameter_id, value):
    conn = Open_Conn_Postgres()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        id_exec_param = id_aleatorio()
        timestamp = datetime.utcnow()
        query = """
            INSERT INTO public.spl_supply_forecast_execution_parameter(
                id, "timestamp", supply_forecast_execution_id, supply_forecast_model_parameter_id, value
            )
            VALUES (%s, %s, %s, %s, %s)
        """
        cur.execute(query, (id_exec_param, timestamp, supply_forecast_execution_id, supply_forecast_model_parameter_id, value))
        conn.commit()
        cur.close()
        return id_exec_param
    except Exception as e:
        print(f"Error en create_execution_parameter: {e}")
        conn.rollback()
        return None
    finally:
        Close_Connection(conn)

def get_execution_parameter(exec_param_id):
    conn = Open_Conn_Postgres()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        query = """
            SELECT id, "timestamp", supply_forecast_execution_id, supply_forecast_model_parameter_id, value
            FROM public.spl_supply_forecast_execution_parameter
            WHERE id = %s
        """
        cur.execute(query, (exec_param_id,))
        row = cur.fetchone()
        cur.close()
        if row:
            return {
                "id": row[0],
                "timestamp": row[1],
                "supply_forecast_execution_id": row[2],
                "supply_forecast_model_parameter_id": row[3],
                "value": row[4]
            }
        return None
    except Exception as e:
        print(f"Error en get_execution_parameter: {e}")
        return None
    finally:
        Close_Connection(conn)

def update_execution_parameter(exec_param_id, **kwargs):
    conn = Open_Conn_Postgres()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        set_clause =  ", ".join([f"{key} = %s" for key in kwargs.keys()])
        values = list(kwargs.values())
        values.append(exec_param_id)
        query = f"""
            UPDATE public.spl_supply_forecast_execution_parameter
            SET {set_clause}
            WHERE id = %s
        """
        cur.execute(query, tuple(values))
        conn.commit()
        cur.close()
        return get_execution_parameter(exec_param_id)
    except Exception as e:
        print(f"Error en update_execution_parameter: {e}")
        conn.rollback()
        return None
    finally:
        Close_Connection(conn)

def delete_execution_parameter(exec_param_id):
    conn = Open_Conn_Postgres()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        query = """
            DELETE FROM public.spl_supply_forecast_execution_parameter
            WHERE id = %s
        """
        cur.execute(query, (exec_param_id,))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"Error en delete_execution_parameter: {e}")
        conn.rollback()
        return False
    finally:
        Close_Connection(conn)

# -----------------------------------------------------------
# 5. Operaciones CRUD para spl_supply_forecast_execution_execute
# -----------------------------------------------------------

def create_execution_execute(data_dict):
    conn = Open_Conn_Postgres()
    if conn is None:
        return None

    try:
        cur = conn.cursor()
        id_exec = id_aleatorio()
        timestamp = datetime.utcnow()

        # Lista de columnas según la nueva estructura
        columns = [
            "id", "end_execution", "last_execution", "start_execution", "timestamp",
            "supply_forecast_execution_id", "supply_forecast_execution_schedule_id",
            "ext_supplier_code", "graphic", "monthly_net_margin_in_millions",
            "monthly_purchases_in_millions", "monthly_sales_in_millions", "sotck_days",
            "sotck_days_colors", "supplier_id", "supply_forecast_execution_status_id",
            "contains_breaks", "maximum_backorder_days", "otif", "total_products", "total_units"
        ]

        values = [
            id_exec,
            data_dict.get("end_execution"),
            data_dict.get("last_execution"),
            data_dict.get("start_execution"),
            timestamp,
            data_dict.get("supply_forecast_execution_id"),
            data_dict.get("supply_forecast_execution_schedule_id"),
            data_dict.get("ext_supplier_code"),
            data_dict.get("graphic"),
            data_dict.get("monthly_net_margin_in_millions"),
            data_dict.get("monthly_purchases_in_millions"),
            data_dict.get("monthly_sales_in_millions"),
            data_dict.get("sotck_days"),
            data_dict.get("sotck_days_colors"),
            data_dict.get("supplier_id"),
            data_dict.get("supply_forecast_execution_status_id"),
            data_dict.get("contains_breaks"),
            data_dict.get("maximum_backorder_days"),
            data_dict.get("otif"),
            data_dict.get("total_products"),
            data_dict.get("total_units")
        ]

        insert_query = f"""
            INSERT INTO public.spl_supply_forecast_execution_execute(
                {', '.join(columns)}
            ) VALUES (
                {', '.join(['%s'] * len(columns))}
            )
        """

        cur.execute(insert_query, values)
        conn.commit()
        cur.close()
        return id_exec

    except Exception as e:
        print(f"Error en create_execution_execute: {e}")
        conn.rollback()
        return None

    finally:
        Close_Connection(conn)

def get_execution_execute(exec_id):
    conn = Open_Conn_Postgres()
    if conn is None:
        return None

    try:
        cur = conn.cursor()

        columns = [
            "id", "end_execution", "last_execution", "start_execution", "timestamp",
            "supply_forecast_execution_id", "supply_forecast_execution_schedule_id",
            "ext_supplier_code", "graphic", "monthly_net_margin_in_millions",
            "monthly_purchases_in_millions", "monthly_sales_in_millions", "sotck_days",
            "sotck_days_colors", "supplier_id", "supply_forecast_execution_status_id",
            "contains_breaks", "maximum_backorder_days", "otif", "total_products", "total_units"
        ]

        select_query = f"""
            SELECT {', '.join(columns)}
            FROM public.spl_supply_forecast_execution_execute
            WHERE id = %s
        """

        cur.execute(select_query, (exec_id,))
        row = cur.fetchone()
        cur.close()

        if row:
            return dict(zip(columns, row))
        return None

    except Exception as e:
        print(f"Error en get_execution_execute: {e}")
        return None

    finally:
        Close_Connection(conn)

def update_execution_execute(exec_id, **kwargs):
    conn = Open_Conn_Postgres()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        set_clause = ", ".join([f"{key} = %s" for key in kwargs.keys()])
        values = list(kwargs.values())
        values.append(exec_id)
        query = f"""
            UPDATE public.spl_supply_forecast_execution_execute
            SET {set_clause}
            WHERE id = %s
        """
        cur.execute(query, tuple(values))
        conn.commit()
        cur.close()
        return get_execution_execute(exec_id)
    except Exception as e:
        print(f"Error en update_execution_execute: {e}")
        conn.rollback()
        return None
    finally:
        Close_Connection(conn)

def delete_execution_execute(exec_id):
    conn = Open_Conn_Postgres()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        query = """
            DELETE FROM public.spl_supply_forecast_execution_execute
            WHERE id = %s
        """
        cur.execute(query, (exec_id,))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"Error en delete_execution_execute: {e}")
        conn.rollback()
        return False
    finally:
        Close_Connection(conn)


# -----------------------------------------------------------
# 6. Operaciones CRUD para spl_supply_forecast_execution_execute_result
# -----------------------------------------------------------
def create_execution_execute_result(expected_demand, product_id, site_id, supply_forecast_execution_execute_id,
                                    algorithm, average, ext_product_code, ext_site_code, ext_supplier_code, 
                                    forcast, graphic, quantity_stock, sales_last, sales_previous, sales_same_year, 
                                    supplier_id, windows, deliveries_pending, quantity_confirmed, approved, 
                                    base_purchase_price, distribution_unit, layer_pallet, number_layer_pallet, 
                                    purchase_unit, sales_price, statistic_base_price, window_sales_days):
    
    conn = Open_Postgres_retry()
    if conn is None:
        print("❌ No se pudo conectar después de varios intentos")
        return None

    try:
        cur = conn.cursor()
        id_result = id_aleatorio()
        timestamp = datetime.utcnow()

        query = """
            INSERT INTO public.spl_supply_forecast_execution_execute_result (
                id, expected_demand, "timestamp", product_id, site_id, supply_forecast_execution_execute_id, 
                algorithm, average, ext_product_code, ext_site_code, ext_supplier_code, forcast, graphic, 
                quantity_stock, sales_last, sales_previous, sales_same_year, supplier_id, windows, 
                deliveries_pending, quantity_confirmed, approved, base_purchase_price, distribution_unit, 
                layer_pallet, number_layer_pallet, purchase_unit, sales_price, statistic_base_price, 
                window_sales_days
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        values = (
            id_result, expected_demand, timestamp, product_id, site_id, supply_forecast_execution_execute_id,
            algorithm, average, ext_product_code, ext_site_code, ext_supplier_code, forcast, graphic, 
            quantity_stock, sales_last, sales_previous, sales_same_year, supplier_id, windows, 
            deliveries_pending, quantity_confirmed, approved, base_purchase_price, distribution_unit,
            layer_pallet, number_layer_pallet, purchase_unit, sales_price, statistic_base_price,
            window_sales_days
        )

        cur.execute(query, values)
        conn.commit()
        cur.close()
        return id_result

    except Exception as e:
        print(f"❌ Error en create_execution_execute_result: {e}")
        conn.rollback()
        return None

    finally:
        Close_Connection(conn)
        
        
def get_execution_execute_result(result_id):
    conn = Open_Conn_Postgres()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        query = """
            SELECT * FROM public.spl_supply_forecast_execution_execute_result
            WHERE id = %s
        """
        cur.execute(query, (result_id,))
        row = cur.fetchone()
        cur.close()
        if row:
            columns = [desc[0] for desc in cur.description] # type: ignore
            return dict(zip(columns, row))
        return None
    except Exception as e:
        print(f"Error en get_execution_execute_result: {e}")
        return None
    finally:
        Close_Connection(conn)

def update_execution_execute_result(result_id, **kwargs):
    conn = Open_Conn_Postgres()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        set_clause = ", ".join([f"{key} = %s" for key in kwargs.keys()])
        values = list(kwargs.values())
        values.append(result_id)
        query = f"""
            UPDATE public.spl_supply_forecast_execution_execute_result
            SET {set_clause}
            WHERE id = %s
        """
        cur.execute(query, tuple(values))
        conn.commit()
        cur.close()
        return get_execution_execute_result(result_id)
    except Exception as e:
        print(f"Error en update_execution_execute_result: {e}")
        conn.rollback()
        return None
    finally:
        Close_Connection(conn)

def delete_execution_execute_result(result_id):
    conn = Open_Conn_Postgres()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        query = """
            DELETE FROM public.spl_supply_forecast_execution_execute_result
            WHERE id = %s
        """
        cur.execute(query, (result_id,))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"Error en delete_execution_execute_result: {e}")
        conn.rollback()
        return False
    finally:
        Close_Connection(conn)



# Final del MODULO FUNCIONES