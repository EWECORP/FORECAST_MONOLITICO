
# -*- coding: utf-8 -*-
"""
Nombre del módulo: S10_GENERA_Forecast_Algoritmos.py

Descripción:   FUNCIONES OPTIMIZADAS PARA EL PRONÓSTICO DE DEMANDA
Esta función articula y ejecuta los algoritmos definidos en la pantalla de CONNEXA.
Se ejecuta en forma programada cada vez que se requiere generar un pronóstico de demanda.
Parte de los FORECAST_executIONS de estado 10.
Genera toda la base de datos que se utilizará en el resto del proceso.

Todos los algoritmos se encuentran definidos en la librería central funciones_forecast.py.

Autor: EWE - Zeetrex
Fecha de creación: [2025-05-23]
Fecha de actualización: [2025-09-01] - Compatibilidad con filtros opcionales (adaptador de generar_datos)
Fecha de actualización: [2025-09-04] - Parser v2 por convención NN_Nombre y valores separados por coma
"""

import os
import sys
import re
import json
import time
import math
import inspect
import unicodedata
from datetime import datetime
from typing import Optional, Union, Iterable, Dict, Any, List

import pandas as pd
from dotenv import dotenv_values

# -------------------------
# Rutas y entorno
# -------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CORE_DIR = os.path.join(BASE_DIR, 'forecast_core')
if CORE_DIR not in sys.path:
    sys.path.insert(0, CORE_DIR)

print("Contenido de sys.path:")
for path in sys.path:
    print(path)

ENV_PATH = os.environ.get("FORECAST_ENV_PATH", "/srv/FORECAST/forecast_core/.env")
if not os.path.exists(ENV_PATH):
    print(f"El archivo .env no existe en la ruta: {ENV_PATH}")
    print(f"Directorio actual: {os.getcwd()}")
    sys.exit(1)

secrets = dotenv_values(ENV_PATH)
folder = f"{secrets['BASE_DIR']}/{secrets['FOLDER_DATOS']}"

print(f"Python executable: {sys.executable}")
print(f"PATH: {os.environ.get('PATH')}")
print(f"ENV_PATH: {ENV_PATH}")
print(f"PGP_DB:{secrets['PGP_DB']} - PGP_HOST:{secrets['PGP_HOST']} - PGP_USER:{secrets['PGP_USER']}")

# -------------------------
# Import del core
# -------------------------
from funciones_forecast import (
    Procesar_ALGO_01,
    Procesar_ALGO_02,
    Procesar_ALGO_03,
    Procesar_ALGO_04,
    Procesar_ALGO_05,
    Procesar_ALGO_06,
    Procesar_ALGO_07,
    get_execution_execute_by_status,
    get_full_parameters,
    update_execution_execute,
)
# Importamos la función real con alias para construir el adaptador
from funciones_forecast import generar_datos as _generar_datos_core

# -------------------------
# Adaptador de compatibilidad para generar_datos
# -------------------------
def generar_datos(id_proveedor: int, etiqueta: str, ventana: int, **kwargs):
    """
    Adaptador que llama a funciones_forecast.generar_datos filtrando kwargs no soportados.
    - Si el core ya está actualizado, pasará sucursales/rubros/subrubro*.
    - Si el core es antiguo (3 parámetros), descartará kwargs y llamará sin ellos.
    """
    try:
        sig = inspect.signature(_generar_datos_core)
        aceptados = set(sig.parameters.keys())
        kwargs_filtrados = {k: v for k, v in kwargs.items() if k in aceptados and v is not None}
        if kwargs and not kwargs_filtrados:
            print("[Compat] generar_datos core no acepta filtros; invocando firma antigua (3 parámetros).")
        elif kwargs_filtrados:
            print(f"[Compat] Pasando kwargs soportados a generar_datos core: {list(kwargs_filtrados.keys())}")
        return _generar_datos_core(id_proveedor, etiqueta, ventana, **kwargs_filtrados)  # type: ignore
    except TypeError:
        print("[Compat] TypeError en generar_datos core; reintentando sin kwargs.")
        return _generar_datos_core(id_proveedor, etiqueta, ventana)  # type: ignore

# -------------------------
# Parser v3 por nombre + data_type
# -------------------------

# -------------------------
# Compat: helpers para campo 'filtros' (opcional) y coerciones listas
# -------------------------
def _to_int_list_any(val: Optional[Union[int, Iterable[int], str]]):
    """Convierte int/list/str (CSV/JSON) a lista de enteros; None si no hay números."""
    if val is None:
        return None
    # iterable numérico
    if isinstance(val, (list, tuple, set)):
        out = []
        for x in val:
            if x is None:
                continue
            try:
                out.append(int(round(float(x))))
            except Exception:
                pass
        return out or None
    # string o escalar -> reutilizamos _to_list
    return _to_list(val)

def _parse_filtros(filtros: Optional[Union[str, dict]]) -> Dict[str, Any]:
    """
    Compat con el campo histórico 'filtros' (JSON o 'k=v; k=v').
    Devuelve dict con: sucursales, rubros, subrubro1..3 (listas de int), ventana (int?), fecha_base (str?).
    Si no hay 'filtros', retorna {} y no pisa los NN_Nombre.
    """
    out: Dict[str, Any] = {}
    if not filtros:
        return out

    # 1) normalizar a dict raw
    if isinstance(filtros, dict):
        raw = filtros
    else:
        s = str(filtros).strip()
        raw = None
        # JSON
        try:
            raw = json.loads(s)
        except Exception:
            pass
        # dict literal (ast)
        if raw is None and s.startswith("{") and s.endswith("}"):
            import ast
            try:
                raw = ast.literal_eval(s)
            except Exception:
                pass
        # pares k=v; k=v
        if raw is None:
            raw = {}
            for kv in re.split(r"[;\n]", s):
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    raw[k.strip().lower()] = v.strip()

    def pick(*keys):
        for k in keys:
            if k in raw and raw[k] not in (None, ""):
                return raw[k]
        return None

    # 2) construir out normalizado
    out["sucursales"] = _to_list(pick("sucursales", "sucu", "suc"))
    out["rubros"]     = _to_list(pick("rubros", "rubro"))
    out["subrubro1"]  = _to_list(pick("subrubro1", "sr1"))
    out["subrubro2"]  = _to_list(pick("subrubro2", "sr2"))
    out["subrubro3"]  = _to_list(pick("subrubro3", "sr3"))

    ven = pick("ventana", "period_length", "period_lengh", "dias", "n_dias")
    if ven is not None:
        try:
            out["ventana"] = int(round(float(str(ven).replace(",", "."))))
        except Exception:
            pass

    fb = pick("fecha_base", "base", "base_date")
    if fb:
        # coerción suave a ISO (usa el mismo helper del parser v3)
        iso = _to_scalar(fb, "date")
        out["fecha_base"] = iso or str(fb)

    return out
# -------------------------

from typing import Dict, Any, Optional, List

def _normalize_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = name.strip()
    if re.match(r"^\s*\d{2}[_-]", s):
        s = s[3:]
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s

def _unquote_layers(s: str) -> str:
    """Elimina comillas redundantes alrededor de todo el string: '\"\"[]\"\"' -> '[]'."""
    if not isinstance(s, str):
        return s
    prev = None
    cur = s.strip()
    # repite mientras todo el string esté entre comillas
    while prev != cur:
        prev = cur
        if (cur.startswith('"') and cur.endswith('"')) or (cur.startswith("'") and cur.endswith("'")):
            cur = cur[1:-1].strip()
        else:
            break
    return cur

def _to_list(val: Any) -> Optional[List[int]]:
    """Convierte JSON/CSV/enumerables a lista de int. Admite comas, pipes, ; y desanidado de comillas."""
    if val is None:
        return None
    if isinstance(val, str):
        s = _unquote_layers(val.strip())
        if s == "" or s.upper() in ("ALL", "*"):
            return None
        # JSON array
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                out = []
                for x in arr:
                    try:
                        out.append(int(round(float(x))))
                    except Exception:
                        pass
                return out or None
            except Exception:
                pass
        # CSV-like
        toks = re.split(r"[,\|;]", s)
        out = []
        for t in toks:
            t = t.strip()
            if t == "":
                continue
            try:
                out.append(int(round(float(t.replace(",", ".")))))
            except Exception:
                pass
        return out or None
    # iterable numérico
    try:
        out = []
        for x in list(val):  # type: ignore
            if x is None: 
                continue
            out.append(int(round(float(x))))
        return out or None
    except Exception:
        return None

def _to_scalar(val: Any, data_type: Optional[str]):
    """Coerce según data_type: int, float, date (ISO), str."""
    if val is None or (isinstance(val, str) and val.strip() == ""):
        return None
    dt = (data_type or "").strip().lower()
    s = val if not isinstance(val, str) else _unquote_layers(val.strip())
    try:
        if dt == "int":
            return int(round(float(s)))
        if dt == "float":
            return float(str(s).replace(",", "."))
        if dt == "date":
            ts = pd.to_datetime(s, errors="coerce")
            return None if pd.isna(ts) else ts.strftime("%Y-%m-%d")  # mantener como ISO str
        # default: str
        return str(s)
    except Exception:
        return None

def _coalesce_value(row, cols=("value","default_value")):
    for c in cols:
        if c in row and row[c] is not None and str(row[c]).strip() != "":
            return row[c]
    return None

def _extract_params_from_df_v3(df_params: Optional[pd.DataFrame], algorithm: Optional[str] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ventana": 30,
        "f1": None, "f2": None, "f3": None,
        "sucursales": None, "rubros": None, "subrubro1": None, "subrubro2": None, "subrubro3": None,
        "fecha_base": None,
    }
    if df_params is None or df_params.empty:
        return out

    cols = {c.lower(): c for c in df_params.columns}
    name_col = cols.get("name"); type_col = cols.get("data_type")
    val_col = cols.get("value"); def_col = cols.get("default_value")

    # 1) recolectar valores crudos normalizados
    raw: Dict[str, Any] = {}
    for _, r in df_params.iterrows():
        name = _normalize_name(r[name_col]) if name_col else ""
        v = _coalesce_value(r, (val_col, def_col))
        dt = r[type_col] if type_col else None

        # Filtros siempre como listas de int
        if name in ("sucursales","rubros","subrubro1","subrubro2","subrubro3"):
            raw[name] = _to_list(v)
            continue

        # Scalars por tipo
        raw[name] = _to_scalar(v, dt)

    algo = (algorithm or "").strip().upper()

    # 2) mapeo común
    if "ventana" in raw and isinstance(raw["ventana"], int) and raw["ventana"] > 0:
        out["ventana"] = raw["ventana"]
    for k in ("sucursales","rubros","subrubro1","subrubro2","subrubro3"):
        if raw.get(k) is not None:
            out[k] = raw[k]

    # 3) mapeo por algoritmo
    if algo == "ALGO_01":
        out["f1"] = raw.get("factor_actual")
        out["f2"] = raw.get("factor_previo")
        out["f3"] = raw.get("factor_ano_anterior")

    elif algo == "ALGO_02":
        pass  # sólo ventana + filtros

    elif algo == "ALGO_03":
        # periodos → f1 ; efecto_estacionalidad → f2 ; efecto_tendencia → f3
        out["f1"] = raw.get("periodos_estacionalidad")
        out["f2"] = raw.get("efecto_estacionalidad")
        out["f3"] = raw.get("efecto_tendencia")

    elif algo == "ALGO_04":
        out["f1"] = raw.get("factor_alpha")

    elif algo == "ALGO_05" or algo == "ALGO_06":
        pass

    elif algo == "ALGO_07":
        out["f1"] = raw.get("factor")
        fb = raw.get("fecha_base")
        if fb:
            out["f2"] = fb
            out["fecha_base"] = fb

    # 4) validaciones suaves
    if not isinstance(out["ventana"], int) or out["ventana"] <= 0:
        out["ventana"] = 30

    return out

# -------------------------
# Wrapper principal
# -------------------------
def get_forecast(
    id_proveedor: int,
    lbl_proveedor: str,
    period_lengh: int = 30,
    algorithm: str = "basic",
    f1: Optional[float] = None,
    f2: Optional[float] = None,
    f3: Optional[float] = None,
    current_date: Optional[str] = None,
    filtros: Optional[Union[str, dict]] = None,
    sucursales: Optional[Union[int, Iterable[int], str]] = None,
    rubros: Optional[Union[int, Iterable[int], str]] = None,
    subrubro1: Optional[Union[int, Iterable[int], str]] = None,
    subrubro2: Optional[Union[int, Iterable[int], str]] = None,
    subrubro3: Optional[Union[int, Iterable[int], str]] = None,
):
    print("Dentro del get_forecast (actualizado con filtros opcionales)")
    print(f"FORECAST control: prov={id_proveedor} - lbl={lbl_proveedor} - ventana base={period_lengh} - algo={algorithm} - factores: {f1}, {f2}, {f3}")
    print(f"Filtros crudos: {filtros}")
    print(f"Filtros NN_Nombre: sucursales={sucursales} rubros={rubros} sr1={subrubro1} sr2={subrubro2} sr3={subrubro3}")


    # 1) Compatibilidad con el campo 'filtros' (JSON/kv); si no viene, queda vacío
    fdict = _parse_filtros(filtros)

    # 2) Overrides provenientes del parser v2 (NN_Nombre)
    if sucursales is not None: fdict["sucursales"] = _to_int_list_any(sucursales)
    if rubros     is not None: fdict["rubros"]     = _to_int_list_any(rubros)
    if subrubro1  is not None: fdict["subrubro1"]  = _to_int_list_any(subrubro1)
    if subrubro2  is not None: fdict["subrubro2"]  = _to_int_list_any(subrubro2)
    if subrubro3  is not None: fdict["subrubro3"]  = _to_int_list_any(subrubro3)

    print("Filtros PARSEADOS ANTES INVOCAR:",
        f"sucursales={fdict.get('sucursales')}",
        f"rubros={fdict.get('rubros')}",
        f"sr1={fdict.get('subrubro1')}",
        f"sr2={fdict.get('subrubro2')}",
        f"sr3={fdict.get('subrubro3')}")



    # 3) Ventana efectiva (si viene en filtros, pisa)
    ventana = fdict.get("ventana")
    if isinstance(ventana, int) and ventana > 0:
        period_lengh = ventana

    # 4) Generar datos (adaptador decide si pasa o no kwargs según firma del core)
    data, articulos = generar_datos(
        int(id_proveedor),
        lbl_proveedor,
        period_lengh,
        sucursales=fdict.get("sucursales"),
        rubros=fdict.get("rubros"),
        subrubro1=fdict.get("subrubro1"),
        subrubro2=fdict.get("subrubro2"),
        subrubro3=fdict.get("subrubro3"),
    )  # type: ignore

    # 5) Fecha base
    if current_date is None:
        current_date = data["Fecha"].max() if data is not None and not data.empty else pd.Timestamp.today()
    else:
        current_date = pd.to_datetime(current_date)
    print(f"Fecha actual {current_date}")

    # 6) Selección del algoritmo
    match algorithm:
        case "ALGO_01":
            return Procesar_ALGO_01(data, articulos, id_proveedor, lbl_proveedor, period_lengh, current_date,
                                    factor_last=f1, factor_previous=f2, factor_year=f3)
        case "ALGO_02":
            return Procesar_ALGO_02(data, articulos, id_proveedor, lbl_proveedor, period_lengh, current_date)
        case "ALGO_03":
            return Procesar_ALGO_03(data, articulos, id_proveedor, lbl_proveedor, period_lengh, current_date,
                                    periodos=f1, estacionalidad=f2, tendencia=f3)
        case "ALGO_04":
            return Procesar_ALGO_04(data, articulos, id_proveedor, lbl_proveedor, period_lengh, current_date, alpha=f1)
        case "ALGO_05":
            return Procesar_ALGO_05(data, articulos, id_proveedor, lbl_proveedor, period_lengh, current_date)
        case "ALGO_06":
            return Procesar_ALGO_06(data, articulos, id_proveedor, lbl_proveedor, period_lengh, current_date)
        case "ALGO_07":
            # Si vino fecha_base desde filtros/NN_Nombre y f2 no se envió, la usamos
            if fdict.get("fecha_base") and f2 is None:
                f2 = fdict["fecha_base"]
            return Procesar_ALGO_07(data, articulos, id_proveedor, lbl_proveedor, period_lengh, current_date,
                                    factor=f1, fecha_base=f2)
        case _:
            raise ValueError(f"Error: El algoritmo '{algorithm}' no está implementado.")

# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    print("🕒 Iniciando ejecución programada del FORECAST ...")
    try:
        fes = get_execution_execute_by_status(10)

        if fes is None or fes.empty:
            print("⚠️ No hay ejecuciones con estado 10 (FORECAST PENDIENTE) para procesar.")
            sys.exit(0)

        for _, row in fes[fes["fee_status_id"] == 10].iterrows():  # type: ignore
            algoritmo = row["name"]
            name = algoritmo.split("_ALGO")[0]
            method = row["method"]
            execution_id = row["forecast_execution_id"]
            id_proveedor = row["ext_supplier_code"]
            forecast_execution_execute_id = row["forecast_execution_execute_id"]
            supply_forecast_model_id = row["forecast_model_id"]

            try:
                id_proveedor = int(float(id_proveedor))
            except Exception:
                pass

            print(f"Procesando ejecución: {name} - Método: {method}")
            start_time = time.time()

            try:
                df_params = get_full_parameters(supply_forecast_model_id, execution_id)
                # Extraemos parámetros con parser v3
                p = _extract_params_from_df_v3(df_params, method)
                ventana = p["ventana"]
                f1, f2, f3 = p["f1"], p["f2"], p["f3"]
                sucursales, rubros = p["sucursales"], p["rubros"]
                subrubro1, subrubro2, subrubro3 = p["subrubro1"], p["subrubro2"], p["subrubro3"]
                fecha_base = p.get("fecha_base")  # útil para logs

                print(f"[PARAMS] ventana={ventana} | f1={f1} | f2={f2} | f3={f3}")
                print(f"[PARAMS] filtros NN_Nombre -> sucursales={sucursales} rubros={rubros} sr1={subrubro1} sr2={subrubro2} sr3={subrubro3}")

                if fecha_base: print(f"[PARAMS] fecha_base={fecha_base}")

                ventana = p.get("ventana", 30)
                f1 = p.get("f1")
                f2 = p.get("f2")
                f3 = p.get("f3")
                fecha_base = p.get("fecha_base")

                # >>> parche
                if (method or "").upper() == "ALGO_07" and fecha_base and (f2 is None or str(f2).strip() == ""):
                    f2 = fecha_base
                    print(f"[PARAMS] fecha_base={f2} (NN_Nombre)")
                # <<< parche

                # Filtros individuales (NN_Nombre); mantenemos 'filtros' por compat si quisieran usar JSON único
                filtros_crudos = p.get("filtros")
                sucursales = p.get("sucursales")
                rubros = p.get("rubros")
                subrubro1 = p.get("subrubro1")
                subrubro2 = p.get("subrubro2")
                subrubro3 = p.get("subrubro3")

                print(f"[PARAMS] ventana={ventana} | f1={f1} | f2={f2} | f3={f3}")
                if filtros_crudos:
                    print(f"[PARAMS] filtros(raw)={filtros_crudos}")
                if any([sucursales, rubros, subrubro1, subrubro2, subrubro3]):
                    print(f"[PARAMS] filtros NN_Nombre -> sucursales={sucursales} rubros={rubros} sr1={subrubro1} sr2={subrubro2} sr3={subrubro3}")

                update_execution_execute(forecast_execution_execute_id, supply_forecast_execution_status_id=15)

                get_forecast(
                    id_proveedor=id_proveedor,
                    lbl_proveedor=name,
                    period_lengh=ventana,
                    algorithm=method,
                    f1=f1,
                    f2=f2,
                    f3=f3,
                    current_date=None,
                    filtros=filtros_crudos,   # compat: pueden dejarlo None si ya usan NN_Nombre
                    sucursales=sucursales,
                    rubros=rubros,
                    subrubro1=subrubro1,
                    subrubro2=subrubro2,
                    subrubro3=subrubro3,
                )

                update_execution_execute(forecast_execution_execute_id, supply_forecast_execution_status_id=20)

                elapsed = round(time.time() - start_time, 2)
                print(f"✅ FORECAST : {algoritmo} procesado - Tiempo parcial: {elapsed} segundos")
                print("✅ Ejecución completada con éxito.")

            except Exception as e:
                print(f"❌ Error durante la ejecución del forecast: {e}")
                try:
                    update_execution_execute(forecast_execution_execute_id, supply_forecast_execution_status_id=99)
                except Exception:
                    pass

    except Exception as e:
        print(f"❌ Error general al iniciar ejecuciones programadas: {e}")
