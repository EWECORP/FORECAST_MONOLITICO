from prefect import flow, task, get_run_logger

@task
def saludar(nombre: str = "Eduardo"):
    logger = get_run_logger()
    logger.info(f"¡Hola, {nombre}! El flujo está funcionando correctamente.")

@flow(name="flujo_de_prueba")
def flujo_prueba():
    saludar()

if __name__ == "__main__":
    flujo_prueba()