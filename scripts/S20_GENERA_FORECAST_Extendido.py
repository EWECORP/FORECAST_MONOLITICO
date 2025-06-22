"""
Nombre del módulo: S20_GENERA_Forecast_Extendido.py

Descripción:
Partiendo de los datos generados con estado 20, se adicionan al archivo local todos los datos relevantes que necesitanrán los próximos procesos.
Al finalizar se actualiza el estado a 30 en la base de datos.

Autor: EWE - Zeetrex
Fecha de creación: [2025-03-22]
"""
# Cargar configuración DINAMIDA de acuerdo al entorno
from dotenv import dotenv_values
import os
import sys

# Determinar la ruta base del proyecto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CORE_DIR = os.path.join(BASE_DIR, 'forecast_core')
sys.path.insert(0, CORE_DIR)
ENV_PATH = os.environ.get("FORECAST_ENV_PATH", "/srv/FORECAST/forecast_core/.env")  # Toma Producción si está definido, o la ruta por defecto
if not os.path.exists(ENV_PATH):
    print(f"El archivo .env no existe en la ruta: {ENV_PATH}")
    print(f"Directorio actual: {os.getcwd()}")
    sys.exit(1)
    
secrets = dotenv_values(ENV_PATH)
folder = f"{secrets['BASE_DIR']}/{secrets['FOLDER_DATOS']}"

# Solo importa lo necesario desde el módulo de funciones
from funciones_forecast  import (
    Open_Conn_Postgres,
    Close_Connection,
    get_execution_execute_by_status,
    update_execution,
    update_execution_execute
)

import pandas as pd # uso localmente la lectura de archivos.
#import ace_tools_open as tools

print(f"-> Datos Recuperados del CACHE: {secrets['FOLDER_DATOS']}")

# También podés importar funciones adicionales si tu módulo las necesita
def extender_datos_forecast(algoritmo, name, id_proveedor):
    # Recuperar Historial de Ventas
    df_ventas = pd.read_csv(f'{folder}/{name}_Ventas.csv')
    df_ventas['Codigo_Articulo']= df_ventas['Codigo_Articulo'].astype(int)
    df_ventas['Sucursal']= df_ventas['Sucursal'].astype(int)
    df_ventas['Fecha']= pd.to_datetime(df_ventas['Fecha'])

    # Recuperar Maestro de Artículos
    articulos = pd.read_csv(f'{folder}/{name}_articulos.csv')
    
    # Recuperar Maestro de Artículos
    stock_sucursal = pd.read_csv(f'{folder}/{name}_stock_sucursal.csv')

    # Recuperando Forecast Calculado
    df_forecast = pd.read_csv(f'{folder}/{algoritmo}_Solicitudes_Compra.csv')
    df_forecast.fillna(0)   # Por si se filtró algún missing value
    print(f"-> Datos Recuperados del CACHE: {id_proveedor}, Label: {name}")
    
    conn = Open_Conn_Postgres()
    
    # Obtener Sites
    stores = pd.read_sql("SELECT code, name, id FROM public.fnd_site", conn) # type: ignore
    stores = stores[pd.to_numeric(stores['code'], errors='coerce').notna()].copy()
    stores['code'] = stores['code'].astype(int)

    # Obtener Productos
    products = pd.read_sql("SELECT ext_code, description, id FROM public.fnd_product", conn) # type: ignore
    products = products[pd.to_numeric(products['ext_code'], errors='coerce').notna()].copy()
    products['ext_code'] = products['ext_code'].astype(int)

    Close_Connection(conn)

    # Unir con productos y validar
    df_merged = df_forecast.merge(products, left_on='Codigo_Articulo', right_on='ext_code', how='left')
    df_merged.rename(columns={'id': 'product_id'}, inplace=True)
    df_merged.drop(columns=['ext_code', 'description'], inplace=True)

    # Unir con sites y validar
    df_merged = df_merged.merge(stores, left_on='Sucursal', right_on='code', how='left')
    df_merged.rename(columns={'id': 'site_id'}, inplace=True)
    df_merged.drop(columns=['code', 'name'], inplace=True)

    # Validación de integridad referencial
    errores = df_merged[df_merged['site_id'].isna() | df_merged['product_id'].isna()]
    if not errores.empty:
        print(f"❌ Error: Se encontraron {len(errores)} registros con site_id o product_id nulos.")
        errores[['Codigo_Articulo', 'Sucursal']].drop_duplicates().to_csv(
            f"{folder}/{algoritmo}_Errores_Missing_UUID.csv", index=False)
        raise ValueError("Existen artículos o sucursales no presentes en Connexa. Revisión necesaria.")

    # Excluimos estos campos
        #'Q_BULTOS_PENDIENTE_OC', 'Q_PESO_PENDIENTE_OC', 'Q_UNID_PESO_PEND_RECEP_TRANSF',  
        #'M_FOLDER','C_CLASIFICACION_COMPRA',  'M_BAJA', 'Q_VENTA_ACUM_30',
    
    # Agregar datos de reposición DESDE NUEVA FUENTE SP
    # columnas_seleccionadas = [
    #     'C_SUCU_EMPR','C_ARTICULO','C_PROVEEDOR_PRIMARIO','ABASTECIMIENTO','COD_CD','HABILITADO','FECHA_REGISTRO',
    #     'FECHA_BAJA','UNID_TRANSFERENCIA','Q_UNID_TRANSFERENCIA','PEDIDO_MIN','FRENTE_LINEAL','CAPACID_GONDOLA','STOCK_MINIMO',
    #     'C_COMPRADOR','Q_FACTOR_COMPRA','PROMOCION','ACTIVE_FOR_PURCHASE','ACTIVE_FOR_SALE','ACTIVE_ON_MIX','DELIVERED_ID','PRODUCT_BASE_ID',
    #     'OWN_PRODUCTION','FULL_CAPACITY_PALLET','NUMBER_OF_LAYERS','NUMBER_OF_BOXES_PER_LAYER'

    #     # ###################     VERSIÖN VIEJA     #################################    
    #     # 'C_PROVEEDOR_PRIMARIO', 'C_COMPRADOR', 'C_ARTICULO', 'C_SUCU_EMPR', 'I_PRECIO_VTA', 'I_COSTO_ESTADISTICO',
    #     # 'Q_FACTOR_VTA_SUCU', 'Q_STOCK_UNIDADES', 'Q_STOCK_PESO', 'M_VENDE_POR_PESO','Q_STOCK', 'F_ULTIMA_VTA',
    #     # 'Q_VTA_ULTIMOS_15DIAS', 'Q_VTA_ULTIMOS_30DIAS', 'Q_TRANSF_PEND', 'Q_TRANSF_EN_PREP',
    #     # 'C_FAMILIA', 'C_RUBRO', 'Q_DIAS_CON_STOCK', 'M_OFERTA_SUCU', 'M_HABILITADO_SUCU', 
    #     # 'Q_REPONER', 'Q_REPONER_INCLUIDO_SOBRE_STOCK', 'Q_VENTA_DIARIA_NORMAL', 
    #     # 'Q_DIAS_STOCK', 'Q_DIAS_SOBRE_STOCK', 'Q_DIAS_ENTREGA_PROVEEDOR', 
    #     # 'Q_FACTOR_PROVEEDOR', 'U_PISO_PALETIZADO', 'U_ALTURA_PALETIZADO', 'I_LISTA_CALCULADO'
    # ]
    # # df_nuevo = articulos[columnas_seleccionadas].copy()
    
    df_nuevo = articulos.copy()   # Articulos ya tiene SP_BASE_ARTICULOS_SUCURSAL
    # -- COMBINAR ARTÍCULOS y STOCK --
    df_nuevo = pd.merge(df_nuevo, stock_sucursal, left_on=['C_ARTICULO', 'C_SUCU_EMPR'], right_on=['CODIGO_ARTICULO', 'CODIGO_SUCURSAL'], how='inner')
    df_nuevo.drop(columns=['CODIGO_ARTICULO', 'CODIGO_SUCURSAL'], inplace=True) 
    df_nuevo['C_SUCU_EMPR'] = df_nuevo['C_SUCU_EMPR'].astype(int)
    df_nuevo['C_ARTICULO'] = df_nuevo['C_ARTICULO'].astype(int)

    df_merged = df_merged.merge(
        df_nuevo,
        left_on=['Sucursal', 'Codigo_Articulo'],
        right_on=['C_SUCU_EMPR', 'C_ARTICULO'],
        how='left'
    )
    df_merged.drop(columns=['C_SUCU_EMPR', 'C_ARTICULO'], inplace=True)

    return df_merged

# Punto de entrada
if __name__ == "__main__":
    fes = get_execution_execute_by_status(20)
        
    if fes is None or fes.empty:
        print("No hay ejecuciones con estado 20 (FORECAST OK) para procesar.")
        sys.exit(0)

    # Filtrar registros con supply_forecast_execution_status_id = 20  # FORECAST OK
    for index, row in fes[fes["fee_status_id"] == 20].iterrows(): # type: ignore
        algoritmo = row["name"]
        name = algoritmo.split('_ALGO')[0]
        execution_id = row["forecast_execution_id"]
        id_proveedor = row["ext_supplier_code"]
        forecast_execution_execute_id = row["forecast_execution_execute_id"]

        print(f"Algoritmo: {algoritmo}  - Name: {name} exce_id: {execution_id} id: Proveedor {id_proveedor}")

        try:
            df_extendido = extender_datos_forecast(algoritmo, name, id_proveedor)

            # Guardar archivo extendido
            file_path = f"{folder}/{algoritmo}_Pronostico_Extendido.csv"
            df_extendido.to_csv(file_path, index=False)
            print(f"✅ Archivo guardado: {file_path}")

            # Actualizar el estado a 30 sólo si no hubo errores
            update_execution_execute(forecast_execution_execute_id, supply_forecast_execution_status_id=30)
            print(f"✅ Estado actualizado a 30 para {execution_id}")

        except ValueError as ve:
            print(f"❌ Error VALIDACIÓN {name}: {ve}")

        except Exception as e:
            print(f"❌ Error procesando {name}: {e}")


