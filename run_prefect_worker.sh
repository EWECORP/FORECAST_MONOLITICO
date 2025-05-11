#!/bin/bash

# Ruta base del proyecto
BASE_DIR="/srv/FORECAST"

# Ruta al entorno virtual
VENV_DIR="$BASE_DIR/venv"

# Nombre del work-pool
POOL_NAME="forecast-pool"

# Variable de entorno .env
export FORECAST_ENV_PATH="$BASE_DIR/forecast_core/.env"

# Activar entorno virtual
source "$VENV_DIR/bin/activate"

# Confirmación visual
echo "🌐 Ejecutando Prefect Worker en entorno virtual"
echo "📁 Working dir: $BASE_DIR"
echo "🐍 Python: $(which python)"
echo "🧪 .env path: $FORECAST_ENV_PATH"
echo "🚀 Work-pool: $POOL_NAME"
echo "--------------------------------------------"

# Iniciar worker Prefect
prefect worker start -p "$POOL_NAME"


# INSTRUCCIONES
# Para hacerlo ejecutable

# chmod +x /srv/FORECAST/run_prefect_worker.sh
# ▶️ Para ejecutarlo manualmente

# cd /srv/FORECAST
# ./run_prefect_worker.sh

# ✅ ¿Qué hace este script?
# Carga la variable FORECAST_ENV_PATH
# Activa el entorno virtual
# Imprime diagnósticos útiles
# Lanza el worker Prefect contra el pool forecast-pool

