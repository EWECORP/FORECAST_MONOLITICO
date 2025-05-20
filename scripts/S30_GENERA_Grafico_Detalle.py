"""
Nombre del módulo: S30_GENERA_Grafico_Detalle.py

Descripción:
Partiendo de los datos extendidos con estado 30, se generan los gráficos de detalle para cada artículo y sucursal.
Se guarda el archivo CSV con los datos extendidos y los gráficos en formato base64.
Utiliza estad intermedio 35 miestras está graficando. Al finalizar se actualiza el estado a 40 en la base de datos.

Autor: EWE - Zeetrex
Fecha de creación: [2025-03-22]
"""
import traceback
import time
from datetime import datetime

# Cargar configuración DINAMIDA de acuerdo al entorno
from dotenv import dotenv_values
import os
import sys

# Determinar la ruta base del proyecto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CORE_DIR = os.path.join(BASE_DIR, 'forecast_core')
sys.path.insert(0, CORE_DIR)
ENV_PATH = os.environ.get("FORECAST_ENV_PATH", "E:/ETL/FORECAST/.env")  # Toma Producción si está definido, o la ruta por defecto
if not os.path.exists(ENV_PATH):
    print(f"El archivo .env no existe en la ruta: {ENV_PATH}")
    print(f"Directorio actual: {os.getcwd()}")
    sys.exit(1)
    
secrets = dotenv_values(ENV_PATH)
folder = f"{secrets['BASE_DIR']}/{secrets['FOLDER_DATOS']}"

# Solo importa lo necesario desde el módulo de funciones
from funciones_forecast import (
    get_execution_execute_by_status,
    update_execution_execute,
    generar_grafico_base64,
    Open_Diarco_Data,
    Open_Postgres_retry,
    Close_Connection,
    generar_grafico_json
)

import pandas as pd # uso localmente la lectura de archivos.
# import ace_tools_open as tools

# RUTINA MEJORADA, Con RESGUARDO PARCIAL de Trabajo Realizado.
def insertar_graficos_forecast(algoritmo, name, id_proveedor):
    print("📊 Insertando Gráficos Forecast:   " + name)
    start_time = time.time()

    # Paths
    path_ventas = f'{folder}/{name}_Ventas.csv'
    path_forecast = f'{folder}/{algoritmo}_Pronostico_Extendido.csv'
    path_backup = f'{folder}/{algoritmo}_Pronostico_Extendido_Con_Graficos.csv'
    path_log = f'{folder}/log_graficos_{name}.txt'

    # Cargar historial de ventas
    df_ventas = pd.read_csv(path_ventas)
    df_ventas['Codigo_Articulo'] = df_ventas['Codigo_Articulo'].astype(int)
    df_ventas['Sucursal'] = df_ventas['Sucursal'].astype(int)
    df_ventas['Fecha'] = pd.to_datetime(df_ventas['Fecha'])

    # 🔄 Agrupar por Fecha, Código de Artículo y Sucursal, para consolidar múltiples precios
    df_ventas = (
        df_ventas
        .groupby(['Fecha', 'Codigo_Articulo', 'Sucursal'], as_index=False)
        .agg({'Unidades': 'sum'})
    )
    # Buscar filas duplicadas por clave compuesta
    duplicados = df_ventas[df_ventas.duplicated(subset=["Fecha", "Codigo_Articulo", "Sucursal"], keep=False)]
    if duplicados.empty:
        print("✅ No se encontraron filas duplicadas en el historial de ventas.")
    else:
        print(f"⚠️ Se encontraron {len(duplicados)} filas duplicadas en el historial de ventas - insertar_graficos_forecast dataframe df_ventas.")
        # Mostrar las filas duplicadas
        # Ordenar para facilitar lectura
        duplicados = duplicados.sort_values(["Codigo_Articulo", "Sucursal", "Fecha"])
        print(duplicados)
    
    # Cargar forecast extendido
    df_forecast = pd.read_csv(path_forecast)
    df_forecast.fillna(0, inplace=True)
    print(f"-> Datos Recuperados del CACHE: {id_proveedor}, Label: {name}")
    
    # Cargar STOCK por Proveedor
    """Consulta el stock y devuelve un dict {fecha: cantidad}, limitado a fechas válidas hasta ayer."""
    conn = Open_Diarco_Data()
    query_stock = f"""
    SELECT DISTINCT s.*
    FROM src.t710_estadis_stock s
    LEFT JOIN src.t050_articulos a
    ON s.c_articulo = a.c_articulo
    WHERE s.c_anio * 100 + s.c_mes >= TO_CHAR(CURRENT_DATE - INTERVAL '1 month', 'YYYYMM')::INTEGER
    AND a.c_proveedor_primario = {id_proveedor};
    """
    df_stock = pd.read_sql(query_stock, conn) # type: ignore
    Close_Connection(conn)
    if df_stock.empty:
        print(f"⚠️ No se encontraron datos de stock para el proveedor {id_proveedor} en el mes actual.")
        return {}
    df_stock['c_anio'] = df_stock['c_anio'].astype(int)
    df_stock['c_mes'] = df_stock['c_mes'].astype(int)
    df_stock['c_articulo'] = df_stock['c_articulo'].astype(int)
    df_stock['c_sucu_empr'] = df_stock['c_sucu_empr'].astype(int)

    # Verificar si ya existe archivo con avances
    if os.path.exists(path_backup):
        df_backup = pd.read_csv(path_backup)
        procesados = set(zip(df_backup['Codigo_Articulo'], df_backup['Sucursal']))
        print(f"🔁 Recuperando avance previo: {len(procesados)} registros ya procesados")
    else:
        df_backup = pd.DataFrame(columns=list(df_forecast.columns) + ['GRAFICO'])
        procesados = set()

    nuevos = 0
    total = len(df_forecast)

    for i, row in df_forecast.iterrows():
        clave = (row['Codigo_Articulo'], row['Sucursal'])
        if clave in procesados:
            continue

        try:
            grafico = generar_grafico_json(
                df_ventas,
                df_stock,
                row['Codigo_Articulo'],
                row['Sucursal'],
                row['Forecast'],
                row['Average'],
                row['ventas_last'],
                row['ventas_previous'],
                row['ventas_same_year']
            )
            row_data = row.to_dict()
            row_data['GRAFICO'] = grafico
            df_backup = pd.concat([df_backup, pd.DataFrame([row_data])], ignore_index=True)
            nuevos += 1

            if nuevos % 50 == 0 or i == total - 1:
                df_backup.to_csv(path_backup, index=False)
                elapsed = round(time.time() - start_time, 2)
                print(f"🖼️ Procesados {nuevos} nuevos registros ({i+1}/{total}) - Tiempo: {elapsed} seg") # type: ignore
                with open(path_log, "a", encoding="utf-8") as log:
                    log.write(f"[{datetime.now()}] {nuevos} registros procesados ({i+1}/{total}) - Tiempo: {elapsed} seg\n") # type: ignore

        except Exception as e:
            print(f"❌ Error procesando gráfico para Art {row['Codigo_Articulo']} - Suc {row['Sucursal']}: {e}")
            with open(path_log, "a", encoding="utf-8") as log:
                log.write(f"[{datetime.now()}] ERROR Art {row['Codigo_Articulo']} - Suc {row['Sucursal']}: {e}\n")
            continue

    # Guardar completo al final
    df_backup.to_csv(path_backup, index=False)
    elapsed = round(time.time() - start_time, 2)
    print(f"✅ Finalizado: {name} - Total nuevos: {nuevos} - Tiempo total: {elapsed} segundos")
    with open(path_log, "a", encoding="utf-8") as log:
        log.write(f"[{datetime.now()}] FINALIZADO: {nuevos} registros nuevos - Tiempo total: {elapsed} seg\n")

    return df_backup

# Punto de entrada
if __name__ == "__main__":
    fes = get_execution_execute_by_status(30)

    # Filtrar registros con supply_forecast_execution_status_id = 30  #FORECAST con DFATOSK
    for index, row in fes[fes["fee_status_id"].isin([30])].iterrows(): # type: ignore
        algoritmo = row["name"] 
        name = algoritmo.split('_ALGO')[0]
        execution_id = row["forecast_execution_id"]
        id_proveedor = row["ext_supplier_code"]
        forecast_execution_execute_id = row["forecast_execution_execute_id"]

        print(f"Algoritmo: {algoritmo}  - Name: {name}  exce_id: {execution_id}  Proveedor: {id_proveedor}")

        try:
            # Estado intermedio: 35 (procesando gráficos)
            print(f"🛠 Marcando como 'Procesando Gráficos' para {execution_id}")
            update_execution_execute(forecast_execution_execute_id, supply_forecast_execution_status_id=35)
            print(f"🛠 Iniciando graficación para {execution_id}...")

            # Generación del dataframe extendido con gráficos
            df_merged = insertar_graficos_forecast(algoritmo, name, id_proveedor)

            # Guardar el CSV con datos extendidos y gráficos
            file_path = f"{folder}/{algoritmo}_Pronostico_Extendido_FINAL.csv"
            df_merged.to_csv(file_path, index=False) # type: ignore
            print(f"📁 Archivo guardado correctamente: {file_path}")

            # ✅ Solo si todo fue exitoso, actualizamos el estado a 40
            update_execution_execute(forecast_execution_execute_id, supply_forecast_execution_status_id=40)
            print(f"✅ Estado actualizado a 40 para {execution_id}")

        except Exception as e:
            traceback.print_exc()
            print(f"❌ Error procesando {name}: {e}")
            
            log_path = os.path.join(folder, "errores_s30.log")
            with open(log_path, "a", encoding="utf-8") as log_file:
                log_file.write(f"[{name}] ID: {execution_id} - ERROR: {str(e)}\n")
            
            continue


