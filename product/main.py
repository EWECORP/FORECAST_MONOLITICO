from category_sync.category_sync import run_category_sync
from product_sync.product_sync import run_product_sync
from barcode_sync.barcode_sync import run_barcode_sync
from supplier_sync.supplier_sync import run_supplier_sync
import logging
import os

def main():

    # Directorio del script actual
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Ruta del archivo de log dentro del mismo directorio
    log_path = os.path.join(script_dir, 'main.log')

    # Configurar logger
    logging.basicConfig(
        filename=log_path,
        level=logging.DEBUG,
        filemode='w',
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.info("Ejecutando category_sync ...")
    run_category_sync()
    logging.info("Category_sync ejecutado.")

    logging.info("Ejecutando product_sync ...")
    run_product_sync()
    logging.info("Product_sync ejecutado.")

    logging.info("Ejecutando barcode_sync ...")
    run_barcode_sync()
    logging.info("barcode_sync ejecutado.")

    logging.info("Ejecutando supplier_sync ...")
    run_supplier_sync()
    logging.info("supplier_sync ejecutado.")


if __name__ == '__main__':
    main()

