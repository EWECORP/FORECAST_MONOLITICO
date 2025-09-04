Excelente decisión. Con la nomenclatura NN_Nombre y tipos explícitos, ahora conviene centralizar el parseo, mapear por nombre (no posicional) y validar antes de ejecutar cada algoritmo. Abajo proponen un plan en tres capas + código listo para pegar.

1) Estándar de parámetros (catálogo canónico)

Tomando su tabla, sugerimos fijar este catálogo por algoritmo (nombre lógico normalizado entre paréntesis). La normalización descarta el prefijo NN_, pasa a minúsculas, y elimina tildes/espacios (p. ej., 04_Factor_Año_Anterior → factor_ano_anterior).

ALGO_01 (promedio ponderado)

01_Ventana → ventana (int)

02_Factor_Actual → factor_actual (float) → f1

03_Factor_Previo → factor_previo (float) → f2

04_Factor_Año_Anterior → factor_ano_anterior (float) → f3

10_Sucursales → sucursales (list[int])

11_Rubros → rubros (list[int])

12_Subrubro1 → subrubro1 (list[int])

13_Subrubro2 → subrubro2 (list[int])

Si desean también subrubro3, agréguenlo como 14_Subrubro3 para uniformidad con el core.

ALGO_02

01_Ventana → ventana

10..13 → filtros (ver arriba)

ALGO_03 (Holt-Winters)

01_Ventana → ventana

02_Periodos_Estacionalidad → periodos_estacionalidad (int) → f1

03_Efecto_Tendencia → efecto_tendencia (str: “add|mul”) → f3

04_Efecto_Estacionalidad → efecto_estacionalidad (str: “add|mul”) → f2

10..13 → filtros

Nota: En su firma actual de Procesar_ALGO_03, la posición es periodos=f1, estacionalidad=f2, tendencia=f3. Por eso mapeamos estacionalidad → f2 y tendencia → f3.

ALGO_04 (EWMA)

01_Ventana → ventana

02_Factor_Alpha → alpha (float) → f1

10..13 → filtros

ALGO_05 / ALGO_06

01_Ventana → ventana

10..13 → filtros

ALGO_07 (ventana base móvil x factor)

01_Ventana → ventana

02_Factor → factor (float) → f1

03_Fecha_base → fecha_base (date/str ISO) → f2

10..13 → filtros

2) Limpieza de lectura desde BD

Cambiar el FULL JOIN por LEFT JOIN con la condición de ejecución en el ON, para conservar defaults del modelo aunque no haya override de ejecución:


Matriz de mapeo (para sus llamadas actuales)

ALGO_01
f1 → factor_last, f2 → factor_previous, f3 → factor_year

ALGO_03
f1 → periodos, f2 → estacionalidad ('add'|'mul'), f3 → tendencia ('add'|'mul')

ALGO_04
f1 → alpha

ALGO_07
f1 → factor, f2 → fecha_base

En get_forecast ya lo están pasando así, por nombres, por lo que no requieren cambios.

Buenas prácticas que dejan todo “prolijo”

Keywords everywhere: en todas las invocaciones internas usar siempre nombre=valor.

Firmas tolerantes: en cada Procesar_/Calcular_… agregar **_ para ignorar parámetros extra sin romper.

Alias de ventana: aceptar ventana y period_length dentro de cada cálculo (como arriba).

Defaults seguros: factores con None → normalizar dentro del algoritmo (ej.: _norm_pesos).

Validación suave: si ventana inválida, forzar 30; si fecha_base falta (ALGO_07), resolver con current_date/máxima Fecha/hoy.

Versión neutral

Con la estandarización lograda, pueden mantener las invocaciones actuales. Solo aconsejan aceptar alias de parámetros y utilizar keywords para desacoplarse del orden posicional. Añadir **kwargs y valores por defecto hace que las funciones sean más robustas y compatibles hacia adelante.

Opinión de ChatGPT

Consideramos que la mejor relación costo/beneficio es no tocar las llamadas y blindar las firmas con alias + keywords. Si más adelante deciden renombrar internamente period_length → ventana, el cambio será local y no afectará al runner ni a otros algoritmos.