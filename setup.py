# setup.py en /srv/FORECAST
from setuptools import setup, find_packages

setup(
    name="forecast_core",
    version="1.0",
    packages=find_packages(),
    install_requires=[],  # podrías agregar dependencias como 'pandas', 'sqlalchemy', etc.
    author="Zeetrex",
    description="Core funcional del sistema de Forecast de demanda"
)