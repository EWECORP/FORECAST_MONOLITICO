# FORECAST/flows/flujo_actualizar_maestros.py
# Este script se encarga de ejecutar scripts específicos para la actualización de maestros en FORECAST  
 
from prefect import flow, task, get_run_logger
import subprocess
import sys
import os

@task(log_prints=True, retries=1, retry_delay_seconds=30)
def ejecutar_script(nombre, parametros=None, base_dir="/usr/local/bin/product"):
    logger = get_run_logger()
    ruta = os.path.join(base_dir, nombre)
    comando = [sys.executable, ruta] + (parametros or [])

    print(f"▶ Ejecutando: {' '.join(comando)}")
    logger.info(f"▶ Ejecutando: {' '.join(comando)}")

    try:
        resultado = subprocess.run(
            comando,
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"✅ Script ejecutado correctamente.\nSTDOUT:\n{resultado.stdout}")
        if resultado.stderr:
            logger.warning(f"⚠️ STDERR:\n{resultado.stderr}")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Script falló: {nombre}")
        logger.error(f"🧾 STDOUT:\n{e.stdout}")
        logger.error(f"🧾 STDERR:\n{e.stderr}")
        raise

    return resultado.stdout

@flow(name="Flujo Sincronización Completa")
def flujo_sincronizacion():
    scripts = [
        # Script 1: KIKKER con argumentos
        # {"nombre": "main.py", "parametros": ["--int", "kikker"], "base_dir": "/usr/local/bin/kikker"},
        # Script 2: PRODUCT SYNC sin argumentos
        {"nombre": "main.py", "parametros": None, "base_dir": "/usr/local/bin/product"},
    ]

    for s in scripts:
        ejecutar_script(s["nombre"], s["parametros"], s["base_dir"])

if __name__ == "__main__":
    flujo_sincronizacion()
