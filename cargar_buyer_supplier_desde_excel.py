#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ALTA MASIVA de proveedores por comprador (Connexa)
- Permite seleccionar interactivamente la planilla Excel:
  * Modo GUI (tkinter) si está disponible
  * Modo consola: elegir directorio y luego archivo
- Lee columnas: ext_code, name
- Inserta vínculos en public.prc_buyer_supplier (id UUID, timestamp NOW)
- Idempotente con ON CONFLICT (buyer_id, supplier_id) DO NOTHING

Uso típico:
  python cargar_buyer_supplier_desde_excel.py --buyer-code "C_ROX_FRUTOS" --dry-run
  python cargar_buyer_supplier_desde_excel.py --buyer-id "c0e8c1d0-..." 

Opcionalmente, se puede pasar el archivo directamente:
  python cargar_buyer_supplier_desde_excel.py --excel "/ruta/proveedores.xlsx" --buyer-code "C_ROX_FRUTOS"

Variables de entorno para PostgreSQL:
  PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
 
    bash:
        export PG_HOST="186.158.182.54"
        export PG_PORT="5432"
        export PG_DB="connexa_platform"
        export PG_USER="postgres"
        export PG_PASSWORD="********"
"""
import os
import sys
import argparse
import logging
from typing import List, Tuple, Optional
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from dotenv import load_dotenv
# Cargar variables de entorno desde .env
ENV_PATH = os.environ.get("FORECAST_ENV_PATH", "E:/ETL/FORECAST/.env")
if not os.path.exists(ENV_PATH):
    print(f"El archivo .env no existe en la ruta: {ENV_PATH}")
    print(f"Directorio actual: {os.getcwd()}")
    sys.exit(1)

load_dotenv(ENV_PATH)


# ====================== LOGGING ======================
def get_logger() -> logging.Logger:
    logger = logging.getLogger("bulk_buyer_supplier")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(handler)
    return logger

logger = get_logger()

# ====================== DB ===========================
def open_pg_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "186.158.182.54"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "connexa_platform"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres"),
        connect_timeout=10
    )

# ====================== EXCEL ========================
def load_excel(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, dtype={"ext_code": str, "name": str})
    # Normalización básica
    df["ext_code"] = df["ext_code"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    # Filtrar vacíos y duplicados de ext_code
    df = df[df["ext_code"] != ""].copy()
    df = df.drop_duplicates(subset=["ext_code"]).reset_index(drop=True)
    return df

def try_tk_file_dialog() -> Optional[Path]:
    """
    Intenta abrir un diálogo gráfico para seleccionar el Excel.
    Retorna Path o None si no es posible (sin GUI / error).
    """
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        file_path = filedialog.askopenfilename(
            title="Seleccione la planilla Excel de proveedores",
            filetypes=[("Excel files", "*.xlsx *.xls")],
        )
        root.destroy()
        if file_path:
            return Path(file_path)
        return None
    except Exception as e:
        logger.info(f"Modo GUI no disponible ({e}). Se usará modo consola.")
        return None

def console_pick_excel() -> Path:
    """
    Fallback por consola:
      1) Pregunta un directorio (ENTER = directorio actual)
      2) Lista archivos .xlsx/.xls
      3) Permite elegir por número
    """
    while True:
        base_dir_input = input(
            "Ingrese el directorio donde está la planilla Excel (ENTER para usar el directorio actual): "
        ).strip()
        base_dir = Path(base_dir_input) if base_dir_input else Path.cwd()

        if not base_dir.exists() or not base_dir.is_dir():
            print("Directorio inválido. Intente nuevamente.")
            continue

        excel_files = sorted(list(base_dir.glob("*.xlsx"))) + sorted(list(base_dir.glob("*.xls")))
        if not excel_files:
            print("No se encontraron archivos .xlsx/.xls en ese directorio. Intente otro.")
            continue

        print("\nArchivos encontrados:")
        for idx, f in enumerate(excel_files, 1):
            print(f"  [{idx}] {f.name}")

        choice = input("Seleccione el número de archivo a cargar: ").strip()
        if not choice.isdigit():
            print("Entrada inválida. Debe ser un número.")
            continue

        idx = int(choice)
        if 1 <= idx <= len(excel_files):
            return excel_files[idx - 1]
        else:
            print("Número fuera de rango. Intente nuevamente.")

def select_excel_interactively(passed_excel: Optional[str]) -> Path:
    """
    Lógica de selección del Excel:
      - Si --excel viene por argumento y existe → usarlo.
      - Si no, intentar GUI (tkinter).
      - Si falla GUI o el usuario cancela, usar modo consola (directorio + lista).
    """
    if passed_excel:
        p = Path(passed_excel)
        if not p.exists() or not p.is_file():
            raise FileNotFoundError(f"El archivo especificado no existe: {p}")
        return p

    # 1) Intento GUI
    p_gui = try_tk_file_dialog()
    if p_gui is not None:
        return p_gui

    # 2) Fallback consola
    return console_pick_excel()

# ====================== LÓGICA DE NEGOCIO ======================
def fetch_buyer_id(cur, buyer_id: Optional[str], buyer_code: Optional[str]) -> str:
    if buyer_id:
        cur.execute("SELECT id FROM public.prc_buyer WHERE id = %s AND active = TRUE", (buyer_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"buyer_id no existe o no está activo: {buyer_id}")
        return row[0]
    if buyer_code:
        cur.execute("SELECT id FROM public.prc_buyer WHERE ext_code = %s AND active = TRUE LIMIT 1", (buyer_code,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"ext_code de comprador no encontrado o inactivo: {buyer_code}")
        return row[0]
    raise ValueError("Debe indicar --buyer-id o --buyer-code")

def map_suppliers(cur, ext_codes: List[str]) -> Tuple[dict, List[str]]:
    if not ext_codes:
        return {}, []
    cur.execute(
        """
        SELECT ext_code, id
        FROM public.fnd_supplier
        WHERE ext_code = ANY(%s)
        """,
        (ext_codes,)
    )
    found = dict(cur.fetchall())  # ext_code -> id
    missing = [c for c in ext_codes if c not in found]
    return found, missing

def bulk_insert_links(cur, buyer_id: str, supplier_ids: List[str]) -> Tuple[int, int]:
    """
    Inserta vínculos (buyer_id, supplier_id) en prc_buyer_supplier.
    Genera id con gen_random_uuid() y timestamp NOW().
    ON CONFLICT (buyer_id, supplier_id) DO NOTHING.
    Retorna (insertados, omitidos).
    """
    if not supplier_ids:
        return 0, 0

    values = [(buyer_id, sid) for sid in supplier_ids]
    sql = """
        INSERT INTO public.prc_buyer_supplier (id, "timestamp", buyer_id, supplier_id)
        VALUES (gen_random_uuid(), NOW(), %s, %s)
        ON CONFLICT (buyer_id, supplier_id) DO NOTHING
        RETURNING supplier_id;
    """
    inserted = 0
    chunk = 500
    for i in range(0, len(values), chunk):
        batch = values[i:i+chunk]
        with cur.connection.cursor() as c2:
            execute_values(c2, sql, batch, template="(%s, %s)")
            returned = c2.fetchall() if c2.rowcount and c2.rowcount > 0 else []
            inserted += len(returned)
    omitted = len(supplier_ids) - inserted
    return inserted, omitted

# ====================== MAIN ======================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", help="Ruta al Excel con columnas: ext_code, name (opcional si van a elegir por diálogo)")
    ap.add_argument("--buyer-id", help="UUID del comprador (public.prc_buyer.id)")
    ap.add_argument("--buyer-code", help="Código externo del comprador (public.prc_buyer.ext_code)")
    ap.add_argument("--dry-run", action="store_true", help="Muestra qué sucedería, sin insertar")
    args = ap.parse_args()

    try:
        # Selección del Excel (GUI o Consola, o directo por --excel)
        excel_path = select_excel_interactively(args.excel)
        logger.info(f"Archivo seleccionado: {excel_path}")

        # Cargar Excel
        df = load_excel(excel_path)
        if df.empty:
            logger.error("La planilla no contiene filas válidas.")
            sys.exit(2)

        ext_codes = df["ext_code"].tolist()
        logger.info(f"Proveedores en planilla (únicos por ext_code): {len(ext_codes)}")

        with open_pg_conn() as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                # Validar comprador
                buyer_id = fetch_buyer_id(cur, args.buyer_id, args.buyer_code)
                logger.info(f"Comprador seleccionado -> id={buyer_id}")

                # Mapear proveedores
                mapping, missing = map_suppliers(cur, ext_codes)
                logger.info(f"Encontrados en fnd_supplier: {len(mapping)} | No encontrados: {len(missing)}")

                if missing:
                    logger.warning("ext_code inexistentes en fnd_supplier:")
                    for m in missing:
                        nm = df.loc[df["ext_code"] == m, "name"].iloc[0]
                        logger.warning(f"  - {m} | {nm}")

                supplier_ids = list(mapping.values())

                if args.dry_run:
                    logger.info("[DRY-RUN] No se insertará nada.")
                    logger.info(f"Candidatos a crear (buyer_id, supplier_id): {len(supplier_ids)}")
                    conn.rollback()
                    return

                # Insertar vínculos
                inserted, omitted = bulk_insert_links(cur, buyer_id, supplier_ids)
                conn.commit()

                logger.info("=== RESUMEN CARGA ===")
                logger.info(f"Insertados nuevos: {inserted}")
                logger.info(f"Omitidos por existir previamente: {omitted}")
                logger.info(f"No encontrados en catálogo (sin insertar): {len(missing)}")

    except KeyboardInterrupt:
        logger.error("Operación cancelada por el usuario.")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"Fallo en la carga: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
