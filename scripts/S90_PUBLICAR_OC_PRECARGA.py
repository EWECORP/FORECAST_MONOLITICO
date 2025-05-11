"""
Nombre del módulo: S90_PUBLICAR_OC_PRECARGA.py

Descripción:
Migración optimizada de datos de órdenes de compra precargadas desde PostgreSQL (Open_Diarco_Data)
a SQL Server de Test (Open_Diarco_Test), con log de auditoría y soporte para inserciones masivas.
Migración optimizada de datos desde PostgreSQL (Open_Diarco_Data)
a SQL Server de test (Open_Diarco_Test), con log y marca de publicación.

Autor: EWE - Zeetrex
Fecha: 2025-04-25
"""

import pandas as pd
import traceback
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from dotenv import dotenv_values
import pyodbc
import logging
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
folder_logs = f"{secrets['BASE_DIR']}/{secrets["FOLDER_LOGS"]}"

# Solo importa lo necesario desde el módulo de funciones
from forecast_core.funciones_forecast  import (
    Open_Diarco_Data,
    Open_Connection
) 

os.makedirs(folder_logs, exist_ok=True)
log_file = os.path.join(folder_logs, "publicacion_oc_precarga.log")

#
# Configurar logging
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def Open_Diarco_Test():
    secrets = dotenv_values("/srv/FORECAST/forecast_core/.env")
    conn_str = f'DRIVER={secrets["SQLT_DRIVER"]};SERVER={secrets["SQLT_SERVER"]};PORT={secrets["SQLT_PORT"]};DATABASE={secrets["SQLT_DATABASE"]};UID={secrets["SQLT_USER"]};PWD={secrets["SQLT_PASSWORD"]}'
    # print (conn_str) 
    try:    
        conn = pyodbc.connect(conn_str)
        return conn
    except:
        print('Error en la Conexión')
        return None

# Función para limpiar y normalizar los campos
def limpiar_campos_oc(df):
    # Normalizar textos respetando la longitud máxima del destino
    df["c_usuario_genero_oc"]   = df["c_usuario_genero_oc"].fillna("").astype(str).str[:10]
    df["c_terminal_genero_oc"]  = df["c_terminal_genero_oc"].fillna("").astype(str).str[:15]
    df["c_usuario_bloqueo"]     = df["c_usuario_bloqueo"].fillna("").astype(str).str[:10]
    df["m_procesado"]           = df["m_procesado"].fillna("N").astype(str).str[:1]
    df["c_compra_kikker"]       = df["c_compra_kikker"].fillna("").astype(str).str[:20]
    df["c_usuario_modif"]       = df["c_usuario_modif"].fillna("").astype(str).str[:20]

    # Números exactos
    df["u_prefijo_oc"] = pd.to_numeric(df["u_prefijo_oc"], errors="coerce").fillna(0).astype(int)
    df["u_sufijo_oc"]  = pd.to_numeric(df["u_sufijo_oc"], errors="coerce").fillna(0).astype(int)

    # Timestamps (permitimos NaT)
    df["f_genero_oc"] = df["f_genero_oc"].fillna(pd.Timestamp('1900-01-01 00:00:00.000'))
    df["f_procesado"] = df["f_procesado"].fillna(pd.Timestamp('1900-01-01 00:00:00.000'))
    #df["f_genero_oc"] = pd.to_datetime(df["f_genero_oc"], errors='coerce')
    #df["f_procesado"] = pd.to_datetime(df["f_procesado"], errors='coerce')

    return df

def validar_longitudes(df):
    campos_texto = [
        "c_usuario_genero_oc", "c_terminal_genero_oc", "c_usuario_bloqueo",
        "m_procesado", "c_compra_kikker", "c_usuario_modif"
    ]
    print("\n🧪 Validando longitudes máximas por columna de texto:")
    for col in campos_texto:
        max_len = df[col].astype(str).map(len).max()
        print(f"{col}: longitud máxima = {max_len}")


# Función principal de publicación
def publicar_oc_precarga():
    logging.info("🔄 Iniciando publicación de OC Precarga")

    try:
        # Conexión PostgreSQL
        engine_pg = Open_Diarco_Data()
        with engine_pg.begin() as conn_pg:
            query = """
            SELECT *
            FROM public.t080_oc_precarga_kikker
            WHERE m_publicado = false
            """
            df_oc = pd.read_sql(query, conn_pg)

            if df_oc.empty:
                logging.warning("⚠️ No hay registros pendientes de publicación")
                return

            total_rows = len(df_oc)
            logging.info(f"✅ Registros a publicar: {total_rows}")

            # Limpiar los campos antes de insertar
            df_oc = limpiar_campos_oc(df_oc)
            validar_longitudes(df_oc)
            
            print(df_oc.head(5))  # Mostrar las primeras filas del DataFrame

            # Conexión a SQL Server
            conn_sql = Open_Diarco_Test()
            cursor = conn_sql.cursor()
            cursor.fast_executemany = True

            insert_stmt = """
            INSERT INTO [dbo].[T080_OC_PRECARGA_KIKKER] (
                [C_PROVEEDOR], [C_ARTICULO], [C_SUCU_EMPR], [Q_BULTOS_KILOS_DIARCO],
                [F_ALTA_SIST], [C_USUARIO_GENERO_OC], [C_TERMINAL_GENERO_OC], [F_GENERO_OC],
                [C_USUARIO_BLOQUEO], [M_PROCESADO], [F_PROCESADO], [U_PREFIJO_OC],
                [U_SUFIJO_OC], [C_COMPRA_KIKKER], [C_USUARIO_MODIF], [C_COMPRADOR]
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            data_tuples = df_oc[[
                'c_proveedor', 'c_articulo', 'c_sucu_empr', 'q_bultos_kilos_diarco',
                'f_alta_sist', 'c_usuario_genero_oc', 'c_terminal_genero_oc', 'f_genero_oc',
                'c_usuario_bloqueo', 'm_procesado', 'f_procesado', 'u_prefijo_oc',
                'u_sufijo_oc', 'c_compra_kikker', 'c_usuario_modif', 'c_comprador'
            ]].itertuples(index=False, name=None)

            cursor.executemany(insert_stmt, list(data_tuples))
            conn_sql.commit()
            logging.info("🟢 Inserción completada en SQL Server")
            print(f"✔ Se insertaron {total_rows} registros en SQL Server.")

            # Actualización en PostgreSQL: marcar como publicado
            update_stmt = text("""
                UPDATE public.t080_oc_precarga_kikker
                SET m_publicado = true
                WHERE m_publicado = false
            """)
            result = conn_pg.execute(update_stmt)
            logging.info(f"🔁 {result.rowcount} registros marcados como publicados")
            print(f"✔ {result.rowcount} registros actualizados con m_publicado = true")

    except Exception as e:
        logging.error("❌ Error durante la publicación de OC Precarga")
        logging.error(traceback.format_exc())
        print("❌ Error durante la ejecución:", e)

    finally:
        try:
            cursor.close()
            conn_sql.close()
            logging.info("🔒 Conexiones cerradas correctamente")
        except Exception as e:
            logging.warning("⚠️ Error al cerrar conexiones")
            logging.warning(str(e))

if __name__ == "__main__":
    publicar_oc_precarga()
    print(f"📝 Proceso finalizado. Ver log en: {log_file}")