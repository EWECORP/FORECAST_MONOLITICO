# flujo_publicar_precarga.py
# Este script se encarga de ejecutar scripts específicos para la precarga de datos en Kikker
from prefect import flow, task, get_run_logger
import subprocess
import sys
import os

@task(log_prints=True, retries=1, retry_delay_seconds=30)
def ejecutar_script(nombre, parametros=None):
    logger = get_run_logger()
    ruta = f"/usr/local/bin/kikker/{nombre}"
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

@flow(name="Flujo Publicar Precarga")
def forecast_flow():
    scripts = [
        ("main.py", ["--int", "kikker"]),
        # ("otroscript.py", ["--param1", "valor1"]),  # Ejemplo futuro
    ]
    
    for script, args in scripts:
        ejecutar_script(script, args)

if __name__ == "__main__":
    forecast_flow()
    