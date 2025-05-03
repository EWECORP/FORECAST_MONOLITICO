from prefect import flow, task, get_run_logger
from datetime import datetime
import subprocess
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import dotenv_values
import os

# Cargar configuración desde .env
secrets = dotenv_values(".env")

# Directorio base de los scripts
BASE_DIR = os.path.join(os.getcwd(), "scripts")

import sys
sys.path.append(BASE_DIR)

def get_pg_connection():
    print("Conectando a la base de datos PostgreSQL...")
    print(f"Conectando a {secrets['PGP_HOST']}:{secrets['PGP_PORT']}:{secrets['PGP_DB']}")
    return psycopg2.connect(
        dbname=secrets["PGP_DB"],
        user=secrets["PGP_USER"],
        password=secrets["PGP_PASSWORD"],
        host=secrets["PGP_HOST"],
        port=secrets["PGP_PORT"]
    )

def check_estado_ejecucion(programa_id: int, logger) -> bool:
    try:
        conn = get_pg_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 1
                FROM public.spl_supply_forecast_execution_execute AS fee
                LEFT JOIN public.spl_supply_forecast_execution AS e ON fee.supply_forecast_execution_id = e.id
                LEFT JOIN public.spl_supply_forecast_model AS m ON e.supply_forecast_model_id = m.id
                WHERE fee.last_execution = true
                AND fee.supply_forecast_execution_status_id >= 10
                AND fee.supply_forecast_execution_status_id < 90
                LIMIT 1
            """)
            row = cur.fetchone()
            estado = bool(row)
            logger.info(f"Ejecución pendiente detectada para programa {programa_id}: {estado}")
            return estado
    except Exception as e:
        logger.error(f"❌ Error al consultar ejecuciones pendientes para programa {programa_id}: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

@task
def ejecutar_script(nombre: str, programa_id: int, archivo: str):
    logger = get_run_logger()
    ruta_script = os.path.join(BASE_DIR, archivo)

    if not os.path.isfile(ruta_script):
        logger.error(f"❌ No se encontró el archivo: {ruta_script}")
        return

    if check_estado_ejecucion(programa_id, logger):
        logger.info(f"✅ Ejecutando {nombre} → {ruta_script}")
        try:
            subprocess.run(["python", ruta_script], check=True)
            logger.info(f"✅ Finalizó correctamente: {archivo}")
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Error al ejecutar {archivo}: {e}")
    else:
        logger.warning(f"⏭️  Saltando {nombre}, sin ejecuciones pendientes.")

@flow(name="pipeline_forecast_programas_secuencial")
def forecast_pipeline_diario():
    ejecutar_script("S10 - Forecast Planificado", 10, "S10_GENERA_FORECAST_Planificado.py")
    ejecutar_script("S20 - Forecast Extendido", 20, "S20_GENERA_FORECAST_Extendido.py")
    ejecutar_script("S30 - Generación de Gráficos", 30, "S30_GENERA_Grafico_Detalle.py")
    ejecutar_script("S40 - Subida a Connexa", 40, "S40_SUBIR_Forecast_Connexa.py")
    ejecutar_script("S90 - Publicación OC", 90, "S90_PUBLICAR_OC_PRECARGA.py")

if __name__ == "__main__":
    forecast_pipeline_diario()
