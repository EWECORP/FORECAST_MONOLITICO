#!/bin/bash

# Ruta base del proyecto
BASE_DIR="/srv/FORECAST"

# Ruta al entorno virtual
VENV_DIR="$BASE_DIR/venv"

# Ruta al .env
export FORECAST_ENV_PATH="$BASE_DIR/forecast_core/.env"

# Validar argumento
if [ -z "$1" ]; then
    echo "❌ Debes indicar el nombre del script Python a ejecutar (por ejemplo: S10_GENERA_FORECAST_Planificado.py)"
    exit 1
fi

SCRIPT_NAME="$1"
SCRIPT_PATH="$BASE_DIR/scripts/$SCRIPT_NAME"

# Validar existencia del script
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "❌ El archivo no existe: $SCRIPT_PATH"
    exit 1
fi

# Activar entorno virtual
source "$VENV_DIR/bin/activate"

# Confirmación visual
echo "🌐 Ejecutando script: $SCRIPT_NAME"
echo "📁 Working dir: $BASE_DIR"
echo "🐍 Python: $(which python)"
echo "🧪 .env path: $FORECAST_ENV_PATH"
echo "--------------------------------------------"

# Ejecutar script
python "$SCRIPT_PATH"

# # INSTRUCCIONES
# 🛠️ Hacerlo ejecutable

#     chmod +x /srv/FORECAST/run_forecast_script.sh
# ▶️ Cómo usarlo
# Desde terminal:

#     ./run_forecast_script.sh S10_GENERA_FORECAST_Planificado.py

# También podés ejecutar otros scripts como:
#     ./run_forecast_script.sh S40_GENERA_Forecast_OC.py

# 🧰 Bonus (opcional): Alias para tu usuario
# Si querés hacerlo más cómodo, agregá al final de tu ~/.bashrc:
#     alias forecast-run='/srv/FORECAST/run_forecast_script.sh'

# Y luego simplemente usás:

# forecast-run S10_GENERA_FORECAST_Planificado.py

# Perfecto. A continuación te presento un script bash reutilizable para ejecutar cualquier script Python de tu proyecto FORECAST, asegurando que:
#     se activa el entorno virtual,
#     se configura correctamente FORECAST_ENV_PATH,
#     se imprime diagnóstico claro,
#     y se ejecuta el script que vos elijas.
