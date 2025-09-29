from prefect import flow, task, get_run_logger
#from subprocess import run, CalledProcessError
import subprocess
import socket
import sys
import os

@task(log_prints=True, retries=2, retry_delay_seconds=60)
def ejecutar_script(nombre):
    logger = get_run_logger()
    ruta = f"/srv/FORECAST/scripts/{nombre}"
    # Obtener la IP del servidor
    ip_servidor = socket.gethostbyname(socket.gethostname())

    print(f"▶ Ejecutando: {ruta} con {sys.executable} en IP {ip_servidor}")

    
    try:      
        logger.info(f"▶ Ejecutando: /srv/FORECAST/scripts/{nombre} con /srv/FORECAST/venv/bin/python3")
        resultado = subprocess.run(
            [sys.executable, ruta],
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"✅ Script ejecutado correctamente.\nSTDOUT:\n{resultado.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Script falló: {nombre}")
        logger.error(f"🧾 STDOUT:\n{e.stdout}")
        logger.error(f"🧾 STDERR:\n{e.stderr}")
        raise
    
    if resultado.returncode != 0:
        print(f"❌ Error en {nombre}:\n{resultado.stderr}")
        raise Exception(f"Error ejecutando {nombre}")
    
    print(f"✅ {nombre} completado:\n{resultado.stdout}")
    return resultado.stdout

@flow(name="Flujo Forecast Principal")
def forecast_flow():
    scripts = [
        "S10_GENERA_FORECAST_Planificado.py",
        "S20_GENERA_FORECAST_Extendido.py",
        "S30_GENERA_Grafico_Detalle.py",
        "S40_SUBIR_Forecast_Connexa.py"
    ]
    for script in scripts:
        ejecutar_script(script)

if __name__ == "__main__":
    forecast_flow()
    
    
# 🔁 Patrón recomendado
# Todo script que se ejecute como subproceso debe seguir este patrón:

# resultado = subprocess.run(
#     [sys.executable, "/ruta/al/script.py"],
#     capture_output=True,
#     text=True
# )