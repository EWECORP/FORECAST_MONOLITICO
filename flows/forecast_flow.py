from prefect import flow, task
import subprocess
import sys
import os

@task(log_prints=True, retries=2, retry_delay_seconds=60)
def ejecutar_script(nombre):
    ruta = f"/srv/FORECAST/scripts/{nombre}"
    print(f"▶ Ejecutando: {ruta} con {sys.executable}")
    
    resultado = subprocess.run(
        [sys.executable, ruta],
        capture_output=True,
        text=True
    )
    
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